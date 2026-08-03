"""Verification gate (spec section 14).

Flow: RUNNING -> CLAIMED_DONE -> VERIFICATION_STARTED ->
VERIFICATION_PASSED / VERIFICATION_FAILED -> VERIFIED / FAILED.

An agent claim can NEVER directly set VERIFIED (14.1). VERIFIED requires at
least one evidence record (invariant 2); when a passing verifier returns no
evidence the gate synthesizes a summary evidence ref so the invariant holds.

Manual verification is handled pre-claim: the node goes RUNNING -> WAITING
(which the state machine allows) until a human resolves it.
"""

from __future__ import annotations

import hashlib

from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.domain.models import EvidenceRef, GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext


class VerificationOutcome:
    def __init__(self, passed: bool, pending: bool, summary: str, evidence: list[EvidenceRef]):
        self.passed = passed
        self.pending = pending
        self.summary = summary
        self.evidence = evidence


class VerificationGate:
    def __init__(
        self,
        graph_store,
        event_store,
        registry,
        workspace_dir: str,
    ):
        self._store = graph_store
        self._events = event_store
        self._registry = registry
        self._workspace_dir = workspace_dir

    # ----------------------------------------------------------- pre-execute
    def spec_for(self, node: GraphNode, worker_request: dict | None) -> VerificationSpec | None:
        raw = worker_request or node.verification_spec
        if raw is None:
            return None
        return VerificationSpec.from_raw(raw)

    def pre_execute_baselines(self, node: GraphNode) -> dict[str, str | None]:
        """Snapshot hashes for file_changed specs before the worker runs."""
        from pathlib import Path

        baselines: dict[str, str | None] = {}
        specs: list[dict] = []
        if node.verification_spec:
            specs.append(node.verification_spec)
        while specs:
            raw = specs.pop()
            spec = VerificationSpec.from_raw(raw)
            if spec.verifier_type == "file_changed":
                rel = spec.parameters.get("path")
                if rel:
                    path = (Path(self._workspace_dir).resolve() / rel).resolve()
                    baselines[rel] = (
                        hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
                    )
            for child in spec.parameters.get("children", []):
                specs.append(child)
        return baselines

    def is_manual(self, node: GraphNode, worker_request: dict | None = None) -> bool:
        spec = self.spec_for(node, worker_request)
        return spec is not None and spec.verifier_type == "manual"

    # ----------------------------------------------------------------- verify
    def verify(
        self,
        node: GraphNode,
        worker_result,
        baselines: dict[str, str | None] | None = None,
        causation_id: str | None = None,
    ) -> VerificationOutcome:
        """Run the verification flow. ``node`` must be CLAIMED_DONE."""
        spec = self.spec_for(node, worker_result.verification_request)
        started = self._events.append(
            RuntimeEvent(
                run_id=node.run_id,
                event_type=EventType.VERIFICATION_STARTED,
                actor_type=ActorType.VERIFIER,
                actor_id=node.id,
                causation_id=causation_id,
                payload={
                    "node_id": node.id,
                    "spec": spec.model_dump() if spec else None,
                },
            )
        )
        if spec is None:
            result = VerificationResult(
                passed=False,
                summary="no verification spec: claim cannot be verified",
            )
        else:
            context = VerificationContext(
                run_id=node.run_id,
                workspace_dir=self._workspace_dir,
                worker_result=worker_result.model_dump(),
                baseline_hashes=baselines or {},
            )
            verifier = self._registry.get(spec.verifier_type)
            result = verifier.verify(node, spec, context)

        if result.pending:
            # Should not happen post-claim (manual is handled pre-claim), but
            # keep the invariant: never verify a pending result.
            return VerificationOutcome(False, True, result.summary, [])

        evidence_refs = self._persist_evidence(node, result, started.id)
        if result.passed:
            if not evidence_refs:
                evidence_refs = self._persist_evidence(
                    node,
                    VerificationResult(
                        passed=True,
                        summary=result.summary,
                        evidence=[
                            {
                                "evidence_type": "verification_summary",
                                "summary": result.summary,
                                "content_hash": hashlib.sha256(
                                    result.summary.encode("utf-8")
                                ).hexdigest(),
                                "metadata": {"verifier_type": spec.verifier_type if spec else None},
                            }
                        ],
                    ),
                    started.id,
                )
            with self._store._db.transaction():
                self._events.append(
                    RuntimeEvent(
                        run_id=node.run_id,
                        event_type=EventType.VERIFICATION_PASSED,
                        actor_type=ActorType.VERIFIER,
                        actor_id=node.id,
                        causation_id=causation_id,
                        evidence_ids=[e.id for e in evidence_refs],
                        payload={
                            "node_id": node.id,
                            "summary": result.summary,
                            "evidence": [e.model_dump(mode="json") for e in evidence_refs],
                        },
                    )
                )
                self._store.set_state(
                    node.id,
                    NodeState.VERIFIED,
                    actor=ActorType.VERIFIER,
                    evidence_ids=[e.id for e in evidence_refs],
                )
            return VerificationOutcome(True, False, result.summary, evidence_refs)

        with self._store._db.transaction():
            self._events.append(
                RuntimeEvent(
                    run_id=node.run_id,
                    event_type=EventType.VERIFICATION_FAILED,
                    actor_type=ActorType.VERIFIER,
                    actor_id=node.id,
                    causation_id=causation_id,
                    payload={"node_id": node.id, "summary": result.summary},
                )
            )
            self._store.set_state(
                node.id,
                NodeState.FAILED,
                actor=ActorType.VERIFIER,
                payload_extra={"summary": result.summary},
            )
        return VerificationOutcome(False, False, result.summary, evidence_refs)

    def _persist_evidence(
        self, node: GraphNode, result: VerificationResult, source_event_id: str
    ) -> list[EvidenceRef]:
        refs: list[EvidenceRef] = []
        for raw in result.evidence:
            metadata = dict(raw.get("metadata", {}))
            metadata.setdefault("node_id", node.id)
            ref = EvidenceRef(
                run_id=node.run_id,
                evidence_type=raw.get("evidence_type", "verification"),
                source_event_id=source_event_id,
                uri=raw.get("uri"),
                content_hash=raw.get("content_hash"),
                summary=raw.get("summary"),
                metadata=metadata,
            )
            self._store.add_evidence(
                ref, actor=ActorType.VERIFIER, event_type=EventType.ARTIFACT_CREATED
            )
            refs.append(ref)
        return refs
