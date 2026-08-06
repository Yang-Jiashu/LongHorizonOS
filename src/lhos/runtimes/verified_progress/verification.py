"""Evidence validation + Task VERIFIED derivation.

D1 Evidence validity contract:

    evidence_is_valid(E)  iff
        1. E.result == "pass"
        2. source_action_id refers to a real Kernel Action
        3. source Action is in an allowed terminal state (COMMITTED)
        4. all artifact_bindings exist (committed, hash matches)
        5. evidence_content_ref (if present) hash matches
        6. E -> Verification produces edge exists
        7. Verification -> Task verifies edge exists
        8. Task currently pins E's exact artifact versions (no silent cross-version validation)
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import VPGCode
from .models import (
    ArtifactRefNode,
    EdgeType,
    EvidenceNode,
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from .protocols import ArtifactFactProvider, KernelEventProvider

ALLOWED_ACTION_TERMINAL_STATES = frozenset({"committed"})


@dataclass
class EvidenceCheckResult:
    valid: bool
    code: VPGCode | None
    message: str


def _fail(code: VPGCode, message: str) -> EvidenceCheckResult:
    return EvidenceCheckResult(False, code, message)


def validate_evidence(
    evidence: EvidenceNode,
    *,
    existing_nodes: dict,
    existing_edges: list[VPGEdge],
    facts_artifact: ArtifactFactProvider | None,
    facts_kernel: KernelEventProvider | None,
) -> EvidenceCheckResult:
    """Determine whether an EvidenceNode is currently valid."""
    if evidence.result.value != "pass":
        code = (
            VPGCode.EVIDENCE_FAIL_REJECTED
            if evidence.result.value == "fail"
            else VPGCode.EVIDENCE_INCONCLUSIVE_REJECTED
        )
        return _fail(code, f"evidence.result is '{evidence.result.value}'")

    if not evidence.source_action_id:
        return _fail(
            VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND,
            "evidence.source_action_id is empty",
        )

    if (
        facts_kernel is None
        or facts_kernel.get_action(evidence.source_action_id) is None
    ):
        return _fail(
            VPGCode.EVIDENCE_SOURCE_ACTION_NOT_FOUND,
            f"action {evidence.source_action_id} not in kernel journal",
        )

    action = facts_kernel.get_action(evidence.source_action_id)

    if action.state not in ALLOWED_ACTION_TERMINAL_STATES:  # type: ignore[union-attr]
        return _fail(
            VPGCode.EVIDENCE_SOURCE_ACTION_NOT_TERMINAL,
            f"action state {action.state} not terminal-pass",  # type: ignore[union-attr]
        )

    if facts_artifact is not None:
        from .models import ArtifactVersionBinding  # local for clarity

        producer_pid = evidence.produced_by_pid or action.pid  # type: ignore[union-attr]
        b: ArtifactVersionBinding
        for b in evidence.artifact_bindings:
            if not facts_artifact.verify_binding(producer_pid, b):
                return _fail(
                    VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH,
                    f"artifact binding {b.canonical_uri}@{b.version} failed",
                )
        if evidence.evidence_content_ref is not None and not facts_artifact.verify_binding(
            producer_pid, evidence.evidence_content_ref
        ):
            return _fail(
                VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH,
                "evidence_content_ref failed",
            )

    if evidence.source_verification_id is None:
        return _fail(
            VPGCode.EVIDENCE_VERIFICATION_EDGE_MISSING,
            "evidence.source_verification_id is None",
        )

    # 6. producers edge Verification -> Evidence must exist
    has_produces_to_verification = any(
        e.edge_type == EdgeType.PRODUCES
        and e.source_node_id == evidence.source_verification_id
        and e.target_node_id == evidence.node_id
        for e in existing_edges
    )
    if not has_produces_to_verification:
        return _fail(
            VPGCode.EVIDENCE_PRODUCES_EDGE_MISSING,
            "no produces edge Verification -> Evidence",
        )

    verification = existing_nodes.get(evidence.source_verification_id)
    if not isinstance(verification, VerificationNode):
        return _fail(
            VPGCode.EVIDENCE_VERIFICATION_EDGE_MISSING,
            f"verification {evidence.source_verification_id} missing/invalid",
        )

    target_task_id = _verifies_target(verification.node_id, existing_edges)
    if target_task_id is None:
        return _fail(
            VPGCode.EVIDENCE_VERIFICATION_EDGE_MISSING,
            "no verifies edge Verification -> Task",
        )

    task_node = existing_nodes.get(target_task_id)
    if isinstance(task_node, TaskNode):
        pinned = _task_current_artifact_versions(
            task_node.node_id, existing_nodes, existing_edges
        )
        evidence_versions = {
            (b.canonical_uri, b.version) for b in evidence.artifact_bindings
        }
        # D1: evidence artifact versions must EXACTLY match the task's
        # currently-pinned versions — no silent cross-version validation.
        if evidence_versions and evidence_versions != pinned:
            return _fail(
                VPGCode.EVIDENCE_ARTIFACT_HASH_MISMATCH,
                "evidence artifact versions no longer match task pins",
            )

    return EvidenceCheckResult(True, None, "ok")


def _verifies_target(verification_id: str, edges: list[VPGEdge]) -> str | None:
    for e in edges:
        if e.edge_type == EdgeType.VERIFIES and e.source_node_id == verification_id:
            return e.target_node_id
    return None


def _task_current_artifact_versions(
    task_id: str,
    nodes: dict,
    edges: list[VPGEdge],
) -> set[tuple[str, int]]:
    """Latest pinned artifact version per canonical_uri for the task."""
    latest: dict[str, tuple[str, int]] = {}
    for e in edges:
        if e.edge_type == EdgeType.PRODUCES and e.source_node_id == task_id:
            n = nodes.get(e.target_node_id)
            if isinstance(n, ArtifactRefNode):
                cur = latest.get(n.canonical_uri)
                if cur is None or n.version > cur[1]:
                    latest[n.canonical_uri] = (n.canonical_uri, n.version)
    return set(latest.values())


def _find_verifications(task_id: str, nodes: dict, edges: list[VPGEdge]) -> list[VerificationNode]:
    out: list[VerificationNode] = []
    for e in edges:
        if e.edge_type == EdgeType.VERIFIES and e.target_node_id == task_id:
            n = nodes.get(e.source_node_id)
            if isinstance(n, VerificationNode):
                out.append(n)
    return out


def _find_evidence_for_verification(
    verification_id: str, nodes: dict, edges: list[VPGEdge]
) -> list[EvidenceNode]:
    out: list[EvidenceNode] = []
    for e in edges:
        # D1 produces edge: Verification -> Evidence
        if e.edge_type == EdgeType.PRODUCES and e.source_node_id == verification_id:
            n = nodes.get(e.target_node_id)
            if isinstance(n, EvidenceNode):
                out.append(n)
    return out


def _task_deps_verified(task: TaskNode, nodes: dict, edges: list[VPGEdge]) -> bool:
    for e in edges:
        if (
            e.edge_type == EdgeType.DEPENDS_ON
            and e.source_node_id == task.node_id
            and e.target_node_id in nodes
        ):
            dep = nodes[e.target_node_id]
            if not isinstance(dep, TaskNode):
                return False
            if dep.validity != NodeValidity.VERIFIED:
                return False
            if dep.lifecycle not in {
                NodeLifecycle.ADMITTED,
                NodeLifecycle.ACTIVE,
                NodeLifecycle.CLOSED,
            }:
                return False
    return True


def task_is_verified(
    task: TaskNode,
    *,
    nodes: dict,
    edges: list[VPGEdge],
    facts_artifact: ArtifactFactProvider | None,
    facts_kernel: KernelEventProvider | None,
) -> bool:
    """D1 Task VERIFIED predicate.

    Note: STALE tasks are allowed — a previously-verified task whose pin
    changed may be re-verified by fresh matching evidence.  The validity
    exclusion only disqualifies INVALID tasks.
    """
    if task.lifecycle not in {
        NodeLifecycle.ADMITTED,
        NodeLifecycle.ACTIVE,
        NodeLifecycle.CLOSED,
    }:
        return False
    if task.validity == NodeValidity.INVALID:
        return False

    verifications = _find_verifications(task.node_id, nodes, edges)
    if len(verifications) < max(1, task.required_verification_count):
        return False

    for v in verifications:
        evidence_list = _find_evidence_for_verification(v.node_id, nodes, edges)
        has_valid = any(
            validate_evidence(
                ev,
                existing_nodes=nodes,
                existing_edges=edges,
                facts_artifact=facts_artifact,
                facts_kernel=facts_kernel,
            ).valid
            for ev in evidence_list
        )
        if not has_valid:
            return False

    return _task_deps_verified(task, nodes, edges)
