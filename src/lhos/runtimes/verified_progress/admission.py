"""Admission Engine.

On node creation (Patch Commit) each freshly created node starts as PROPOSED
UNVERIFIED.  The Admission Engine deterministically checks schema / required
fields / graph ownership / forbidden contents and derives:

    - PROPOSED  -> ADMITTED   (valid)
    - PROPOSED  -> INVALID    (permanently broken — Agent cannot fix via Patch)

The Agent NEVER controls Admission outcome.
"""

from __future__ import annotations

from collections.abc import Iterable

from .models import (
    AnyNode,
    ArtifactRefNode,
    EvidenceNode,
    GoalNode,
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VerificationNode,
)


class AdmissionResult:
    """Result of admitting a single node."""

    def __init__(
        self,
        node: AnyNode,
        admitted: bool,
        messages: tuple[str, ...],
    ) -> None:
        self.node = node
        self.admitted = admitted
        self.messages = messages


def _check_task_execution_spec(spec: dict) -> str | None:
    """Return an error string if spec contains forbidden content."""
    for key, val in spec.items():
        lk = key.lower()
        if lk in {"callback", "callable", "fn", "function", "lambda_code", "exec"}:
            return f"execution_spec key '{key}' forbidden"
        if callable(val):
            return "execution_spec contains callable"
        if isinstance(val, str) and val.strip().startswith("/"):
            return f"execution_spec contains host path: {val}"
    return None


def admit(node: AnyNode, graph_id: str) -> AdmissionResult:
    """Run admission checks on a freshly PROPOSED node.

    All checks are deterministic and pure (no I/O).
    """
    msgs: list[str] = []

    if node.graph_id != graph_id:
        msgs.append(f"node.graph_id={node.graph_id} != graph_id={graph_id}")

    if node.lifecycle != NodeLifecycle.PROPOSED:
        msgs.append(f"fresh nodes must be PROPOSED, got {node.lifecycle}")

    if node.validity != NodeValidity.UNVERIFIED:
        msgs.append(f"fresh nodes must be UNVERIFIED, got {node.validity}")

    if isinstance(node, GoalNode):
        if not node.title:
            msgs.append("GoalNode.title required")
    elif isinstance(node, TaskNode):
        err = _check_task_execution_spec(node.execution_spec)
        if err:
            msgs.append(err)
        if node.required_verification_count < 1:
            msgs.append("TaskNode.required_verification_count must be >= 1")
    elif isinstance(node, ArtifactRefNode):
        if (
            not node.canonical_uri
            or not node.artifact_id
            or not node.content_hash
        ):
            msgs.append(
                "ArtifactRefNode requires canonical_uri/artifact_id/content_hash"
            )
        if node.version is None or node.version < 0:
            msgs.append("ArtifactRefNode.version must be set")
    elif isinstance(node, VerificationNode):
        if not node.verification_kind:
            msgs.append("VerificationNode.verification_kind required")
    elif isinstance(node, EvidenceNode):
        # Agent *may* create EvidenceNode proposals (lifecycle stays
        # ADMIN by default).  Whether the Evidence becomes VALID is decided
        # later by the verification engine, which requires a real Kernel
        # Action + matching hash — the agent cannot shortcut VERIFIED via
        # direct AddNode.
        if not node.source_action_id:
            msgs.append("EvidenceNode.source_action_id required")
        if node.result.value not in {"pass", "fail", "inconclusive"}:
            msgs.append(f"invalid evidence result: {node.result}")
    else:
        msgs.append(f"unknown node type: {node.node_type}")

    admitted = len(msgs) == 0
    if admitted:
        node.lifecycle = NodeLifecycle.ADMITTED
        node.validity = NodeValidity.UNVERIFIED
    else:
        node.validity = NodeValidity.INVALID

    return AdmissionResult(
        node=node,
        admitted=admitted,
        messages=tuple(msgs),
    )


def admit_each(
    nodes: Iterable[AnyNode], graph_id: str
) -> list[AdmissionResult]:
    """Admit each node in deterministic order."""
    return [admit(n, graph_id) for n in nodes]
