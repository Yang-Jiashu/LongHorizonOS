"""Graph-scoped Context Compiler (spec section 10).

Deterministic graph traversal only — no embeddings (10.4). Never includes the
full transcript, unrelated siblings, invalidated artifacts, or stale nodes
(10.3). Cache key per 10.5: node_id + node_version + dependency_versions +
artifact_hashes + prompt_version; identical keys yield identical hashes and a
cached packet.
"""

from __future__ import annotations

import hashlib
import json
from collections import deque

from pydantic import BaseModel, Field

from lhos.domain.enums import NodeKind, NodeState
from lhos.domain.events import EventType
from lhos.domain.models import EvidenceRef, GraphNode
from lhos.graph.queries import ProgressGraph

PROMPT_VERSION = "context-compiler.v1"

# Traversal weights (spec 10.4).
W_DIRECT_DEPENDENCY = 1.0
W_CONSTRAINT = 1.0
W_ARTIFACT_PRODUCER = 0.9
W_TWO_HOP = 0.6
W_THREE_HOP = 0.3
W_STALE = 0.0


class ContextRequest(BaseModel):
    """Spec 10.1."""

    run_id: str
    node_id: str
    max_tokens: int
    include_last_failures: int = 2
    max_dependency_hops: int = 3


class ContextPacket(BaseModel):
    """Spec 10.2."""

    node_id: str
    context_hash: str
    global_goal: str
    current_task: str
    constraints: list[str] = Field(default_factory=list)
    dependency_summaries: list[str] = Field(default_factory=list)
    artifact_refs: list[EvidenceRef] = Field(default_factory=list)
    previous_failures: list[str] = Field(default_factory=list)
    verification_requirements: list[str] = Field(default_factory=list)
    estimated_tokens: int = 0


class ContextCompiler:
    def __init__(
        self,
        graph_store,
        event_store,
        prompt_version: str = PROMPT_VERSION,
    ):
        self._store = graph_store
        self._events = event_store
        self._prompt_version = prompt_version
        self._cache: dict[str, ContextPacket] = {}

    # ------------------------------------------------------------------ cache
    def _cache_key(
        self,
        node: GraphNode,
        included: dict[str, float],
        graph: ProgressGraph,
        artifact_hashes: list[str],
    ) -> str:
        key_material = {
            "node_id": node.id,
            "node_version": node.version,
            "dependency_versions": sorted(
                (dep_id, graph.nodes[dep_id].version) for dep_id in included
            ),
            "artifact_hashes": sorted(artifact_hashes),
            "prompt_version": self._prompt_version,
        }
        canonical = json.dumps(key_material, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    # --------------------------------------------------------------- compile
    def compile(self, request: ContextRequest) -> ContextPacket:
        graph = self._store.load_graph(request.run_id)
        node = graph.get_node(request.node_id)
        run = self._store.get_run(request.run_id)
        all_evidence = self._store.list_evidence(request.run_id)

        included = self._traverse(graph, node, request.max_dependency_hops)
        artifact_refs = self._select_artifacts(graph, node, included, all_evidence)
        artifact_hashes = [e.content_hash or "" for e in artifact_refs]

        cache_key = self._cache_key(node, included, graph, artifact_hashes)
        if cache_key in self._cache:
            return self._cache[cache_key]

        packet = self._build_packet(
            request, graph, node, run.goal, included, artifact_refs, cache_key
        )
        self._cache[cache_key] = packet
        return packet

    # -------------------------------------------------------------- traversal
    def _traverse(self, graph: ProgressGraph, node: GraphNode, max_hops: int) -> dict[str, float]:
        """node_id -> weight for every node included in the context.

        Stale/invalidated nodes get weight 0 and are excluded (spec 10.4).
        """
        included: dict[str, float] = {}

        # Direct dependencies via DEPENDS_ON (weight 1.0), then BFS outward.
        queue: deque[tuple[str, int]] = deque()
        for dep in graph.dependencies(node.id):
            if dep.state not in {NodeState.STALE, NodeState.INVALIDATED, NodeState.ABORTED}:
                if included.get(dep.id, 0.0) < W_DIRECT_DEPENDENCY:
                    included[dep.id] = W_DIRECT_DEPENDENCY
                queue.append((dep.id, 1))

        hop_weight = {2: W_TWO_HOP, 3: W_THREE_HOP}
        while queue:
            current_id, hop = queue.popleft()
            if hop >= max_hops:
                continue
            next_hop = hop + 1
            weight = hop_weight.get(next_hop)
            if weight is None:
                continue
            for dep in graph.dependencies(current_id):
                if dep.state in {NodeState.STALE, NodeState.INVALIDATED, NodeState.ABORTED}:
                    continue
                if included.get(dep.id, 0.0) < weight:
                    included[dep.id] = weight
                    queue.append((dep.id, next_hop))

        # Producers of artifacts this node consumes (weight 0.9).
        for artifact in graph.consumed_artifacts(node.id):
            for producer in graph.producers_of(artifact.id):
                if producer.state in {NodeState.STALE, NodeState.INVALIDATED, NodeState.ABORTED}:
                    continue
                if included.get(producer.id, 0.0) < W_ARTIFACT_PRODUCER:
                    included[producer.id] = W_ARTIFACT_PRODUCER

        # Active constraint nodes (weight 1.0).
        for other in graph.nodes.values():
            if other.kind == NodeKind.CONSTRAINT and other.state not in {
                NodeState.INVALIDATED,
                NodeState.ABORTED,
            }:
                included.setdefault(other.id, W_CONSTRAINT)

        included.pop(node.id, None)
        return included

    def _select_artifacts(
        self,
        graph: ProgressGraph,
        node: GraphNode,
        included: dict[str, float],
        all_evidence: list[EvidenceRef],
    ) -> list[EvidenceRef]:
        """Artifacts: evidence produced by included verified nodes. Invalidated
        artifacts and stale producers are excluded (spec 10.3)."""
        refs: list[EvidenceRef] = []
        for ev in all_evidence:
            owner = ev.metadata.get("node_id")
            if owner is None or owner not in included:
                continue
            owner_node = graph.nodes.get(owner)
            if owner_node is None or owner_node.state != NodeState.VERIFIED:
                continue
            refs.append(ev)
        refs.sort(key=lambda e: e.id)
        return refs

    # ----------------------------------------------------------------- build
    def _previous_failures(self, request: ContextRequest) -> list[str]:
        """Build structured failure feedback for the worker context.

        Step 3: includes verification failure details (command, exit code,
        stderr) so the worker can take corrective action on retry.
        """
        from lhos.runtime.verification_feedback import build_feedback_from_verification

        failures: list[tuple[int, str]] = []
        for event in self._events.list_events(request.run_id):
            if event.event_type not in {
                EventType.VERIFICATION_FAILED,
                EventType.EXECUTION_FAILED,
            }:
                continue
            if event.payload.get("node_id") != request.node_id:
                continue

            if event.event_type == EventType.VERIFICATION_FAILED:
                # Build structured feedback.
                summary = event.payload.get("summary", "")
                spec = event.payload.get("spec", {})
                spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
                verifier_type = (
                    spec.get("verifier_type", "unknown") if isinstance(spec, dict) else "unknown"
                )
                evidence = event.payload.get("evidence", [])
                feedback = build_feedback_from_verification(
                    verifier_type=verifier_type,
                    summary=summary,
                    spec_params=spec_params,
                    evidence=evidence if isinstance(evidence, list) else [],
                )
                failures.append((event.sequence, feedback.to_context_string()))
            else:
                summary = (
                    event.payload.get("summary") or event.payload.get("reason") or event.event_type
                )
                failures.append((event.sequence, f"[{event.event_type}] {summary}"))

        failures.sort(key=lambda t: t[0])
        return [text for _, text in failures[-request.include_last_failures :]]

    def _build_packet(
        self,
        request: ContextRequest,
        graph: ProgressGraph,
        node: GraphNode,
        goal: str,
        included: dict[str, float],
        artifact_refs: list[EvidenceRef],
        context_hash: str,
    ) -> ContextPacket:
        constraints = sorted(
            (
                f"{graph.nodes[nid].title}: {graph.nodes[nid].specification}"
                for nid, w in included.items()
                if graph.nodes[nid].kind == NodeKind.CONSTRAINT
            )
        )
        dep_summaries = sorted(
            (
                f"[w={w:.1f}] {graph.nodes[nid].title} "
                f"({graph.nodes[nid].state}): {graph.nodes[nid].specification}"
                for nid, w in included.items()
                if graph.nodes[nid].kind != NodeKind.CONSTRAINT
            ),
            reverse=True,  # higher weight first
        )
        verification_requirements: list[str] = []
        if node.verification_spec:
            verification_requirements.append(
                json.dumps(node.verification_spec, sort_keys=True, default=str)
            )

        packet = ContextPacket(
            node_id=node.id,
            context_hash=context_hash,
            global_goal=goal,
            current_task=node.specification,
            constraints=constraints,
            dependency_summaries=dep_summaries,
            artifact_refs=artifact_refs,
            previous_failures=self._previous_failures(request),
            verification_requirements=verification_requirements,
        )

        # Step 3: inject repair feedback from node metadata so the worker
        # sees local repair guidance on the next retry.
        repair_feedback = node.metadata.get("repair_feedback")
        if repair_feedback:
            packet.previous_failures.append(f"[REPAIR GUIDANCE] {repair_feedback}")
        # Also include failure code for quick reference.
        failure_code = node.metadata.get("failure_code")
        if failure_code and failure_code != "verification_failed":
            packet.previous_failures.append(
                f"[FAILURE CODE] {failure_code}"
                + (" (NOT retryable)" if not node.metadata.get("retryable", True) else "")
            )

        # Priority ordering (spec 10.3): current spec, verification spec,
        # global goal, constraints, direct evidence, artifacts, failures,
        # 2nd/3rd-hop summaries. Enforce max_tokens by trimming the
        # lowest-priority sections first.
        packet.estimated_tokens = self._estimate_tokens(packet)
        while packet.estimated_tokens > request.max_tokens:
            if packet.dependency_summaries:
                # drop the weakest summary first
                packet.dependency_summaries.pop()
            elif packet.previous_failures:
                packet.previous_failures.pop()
            elif packet.artifact_refs:
                packet.artifact_refs.pop()
            elif packet.constraints:
                packet.constraints.pop()
            else:
                break
            packet.estimated_tokens = self._estimate_tokens(packet)
        return packet

    @staticmethod
    def _estimate_tokens(packet: ContextPacket) -> int:
        chars = len(packet.global_goal) + len(packet.current_task)
        chars += sum(len(c) for c in packet.constraints)
        chars += sum(len(d) for d in packet.dependency_summaries)
        chars += sum(len(f) for f in packet.previous_failures)
        chars += sum(len(v) for v in packet.verification_requirements)
        chars += sum(len(e.summary or "") for e in packet.artifact_refs)
        return max(1, chars // 4)
