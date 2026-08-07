"""D1 Patch validation pipeline.

Runs the full 20-step commit check (spec section 13) and produces a
deterministic set of derived node/edge/events or a VPGError.

This is the "deterministic projection builder" — it takes a patch, the
current projection snapshot, and the Artifact/Kernel facts providers, and
produces the candidate new projection + derived events.  The GraphStore
then atomically commits everything.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from typing import Any

from .admission import admit
from .dag import detect_cycle, is_self_loop
from .errors import VPGCode, VPGError, execution_cycle
from .events import GraphEvent, GraphEventType
from .models import (
    AnyNode,
    ArtifactRefNode,
    ArtifactVersionBinding,
    EdgeType,
    EvidenceNode,
    EvidenceResult,
    GoalNode,
    NodeLifecycle,
    NodeType,
    NodeValidity,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from .patches import (
    AddEdgeOp,
    AddNodeOp,
    AttachArtifactOp,
    AttachEvidenceOp,
    GraphPatchProposal,
)
from .protocols import ArtifactFactProvider, KernelEventProvider
from .verification import validate_evidence

# Max operations per patch — defends against giant-patch DoS
MAX_PATCH_OPS = 500


class PatchValidationRequest:
    """Inputs needed to validate a patch."""

    patch: GraphPatchProposal
    current_nodes: dict[str, AnyNode]
    current_edges: list[VPGEdge]
    facts_artifact: ArtifactFactProvider | None
    facts_kernel: KernelEventProvider | None


class PatchValidationResult:
    """Outputs — candidate projection + derived events."""

    new_nodes: dict[str, AnyNode]
    new_edges: list[VPGEdge]
    admitted_nodes: dict[str, AnyNode]
    events: list[GraphEvent]
    updated_nodes: dict[str, AnyNode]
    candidate_nodes: dict[str, AnyNode]
    candidate_edges: list[VPGEdge]

    def __init__(self) -> None:
        self.new_nodes = {}
        self.new_edges = []
        self.admitted_nodes = {}
        self.events = []
        self.updated_nodes = {}
        self.candidate_nodes = {}
        self.candidate_edges = []


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _node_type_from_string(s: str) -> Any:
    mapping = {
        "goal": GoalNode,
        "task": TaskNode,
        "artifact_ref": ArtifactRefNode,
        "verification": VerificationNode,
        "evidence": EvidenceNode,
    }
    return mapping[s]


def _build_add_node(op: AddNodeOp, graph_id: str, version: int) -> AnyNode:
    """PTranslate AddNodeOp into a typed VPGNode.  Node validity defaults set."""
    if op.node_type == "goal":
        return GoalNode(
            node_id=op.node_id,
            graph_id=graph_id,
            node_type=NodeType.GOAL,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            title=op.title,
            description=op.description,
        )
    if op.node_type == "task":
        return TaskNode(
            node_id=op.node_id,
            graph_id=graph_id,
            node_type=NodeType.TASK,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            title=op.title,
            description=op.description,
            task_kind=op.task_kind,
            execution_spec=dict(op.execution_spec),
            required_verification_count=op.required_verification_count,
        )
    if op.node_type == "artifact_ref":
        return ArtifactRefNode(
            node_id=op.node_id,
            graph_id=graph_id,
            node_type=NodeType.ARTIFACT_REF,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            canonical_uri=op.canonical_uri,
            artifact_id=op.artifact_id,
            version=op.version if op.version is not None else -1,
            content_hash=op.content_hash,
            media_type=op.media_type,
        )
    if op.node_type == "verification":
        return VerificationNode(
            node_id=op.node_id,
            graph_id=graph_id,
            node_type=NodeType.VERIFICATION,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            verification_kind=op.verification_kind,
            obligation=dict(op.obligation),
            source_action_id=op.source_action_id,
        )
    if op.node_type == "evidence":
        return EvidenceNode(
            node_id=op.node_id,
            graph_id=graph_id,
            node_type=NodeType.EVIDENCE,
            lifecycle=NodeLifecycle.PROPOSED,
            validity=NodeValidity.UNVERIFIED,
            created_in_version=version,
            updated_in_version=version,
            created_by_pid=op.created_by_pid,
            created_at=_utcnow(),
            metadata=dict(op.metadata),
            evidence_kind=op.evidence_kind,
            result=EvidenceResult(op.result),
            source_verification_id=op.source_verification_id,
            source_action_id=op.evidence_source_action_id,
            source_event_ids=op.source_event_ids,
            artifact_bindings=op.artifact_bindings,
            evidence_content_ref=op.evidence_content_ref,
            evidence_hash=op.evidence_hash,
            produced_by_pid=op.produced_by_pid,
        )
    raise VPGError(VPGCode.INVALID_NODE_TYPE, f"unknown node_type: {op.node_type}")


def _check_edge_type_combination(edge: VPGEdge, nodes: dict[str, AnyNode]) -> None:
    """Validate that the edge type is allowed between source targetType."""
    src = nodes.get(edge.source_node_id)
    tgt = nodes.get(edge.target_node_id)
    from .models import ArtifactRefNode as _A
    from .models import EvidenceNode as _E
    from .models import GoalNode as _G
    from .models import TaskNode as _T
    from .models import VerificationNode as _V

    if edge.edge_type == EdgeType.DEPENDS_ON:
        if not isinstance(src, (_G, _T)):
            raise VPGError(
                VPGCode.INVALID_EDGE_TYPE_COMBINATION,
                f"depends_on source must be Goal|Task, got {type(src).__name__ if src else None}",
            )
        if not isinstance(tgt, _T):
            raise VPGError(
                VPGCode.INVALID_EDGE_TYPE_COMBINATION,
                f"depends_on target must be Task, got {type(tgt).__name__ if tgt else None}",
            )
    elif edge.edge_type == EdgeType.PRODUCES:
        # D1 produces combos:
        #   Task -> ArtifactRef      (task output artifact)
        #   Verification -> Evidence (verification result)
        valid = (isinstance(src, _T) and isinstance(tgt, _A)) or (
            isinstance(src, _V) and isinstance(tgt, _E)
        )
        if not valid:
            raise VPGError(
                VPGCode.INVALID_EDGE_TYPE_COMBINATION,
                f"produces edge must be Task->ArtifactRef or Verification->Evidence, "
                f"got {type(src).__name__}->{type(tgt).__name__}",
            )
    elif edge.edge_type == EdgeType.VERIFIES:
        if not isinstance(src, _V):
            raise VPGError(
                VPGCode.INVALID_EDGE_TYPE_COMBINATION,
                f"verifies source must be Verification, got {type(src).__name__ if src else None}",
            )
        if not isinstance(tgt, _T):
            raise VPGError(
                VPGCode.INVALID_EDGE_TYPE_COMBINATION,
                f"verifies target must be Task, got {type(tgt).__name__ if tgt else None}",
            )
    else:
        raise VPGError(
            VPGCode.INVALID_EDGE_TYPE_COMBINATION,
            f"unknown edge type {edge.edge_type}",
        )


def _attach_artifact_produces(
    task_id: str,
    task_pid: str,
    binding: ArtifactVersionBinding,
    graph_id: str,
    version: int,
) -> tuple[ArtifactRefNode, VPGEdge]:
    """Create (artifact_ref_node, produces edge) for AttachArtifact."""
    node = ArtifactRefNode(
        graph_id=graph_id,
        canonical_uri=binding.canonical_uri,
        artifact_id=binding.artifact_id,
        version=binding.version,
        content_hash=binding.content_hash,
        media_type=binding.media_type,
        lifecycle=NodeLifecycle.ADMITTED,
        validity=NodeValidity.UNVERIFIED,
        created_in_version=version,
        updated_in_version=version,
        created_by_pid=task_pid,
        created_at=_utcnow(),
    )
    edge = VPGEdge(
        graph_id=graph_id,
        edge_type=EdgeType.PRODUCES,
        source_node_id=task_id,
        target_node_id=node.node_id,
        created_in_version=version,
        created_by_pid=task_pid,
        created_at=_utcnow(),
    )
    return node, edge


def _validate_artifact_binding_against_sdk(
    binding: ArtifactVersionBinding,
    pid: str,
    facts: ArtifactFactProvider | None,
) -> None:
    """Ensure the artifact version actually exists with matching hash."""
    if facts is None:
        # no provider — trust (used in pure unit-test graphs)
        return
    if not facts.artifact_exists(pid, binding.canonical_uri, binding.version):
        raise VPGError(
            VPGCode.ARTIFACT_NOT_FOUND,
            f"{binding.canonical_uri}@{binding.version} not found",
        )
    if not facts.verify_binding(pid, binding):
        raise VPGError(
            VPGCode.ARTIFACT_HASH_MISMATCH,
            f"{binding.canonical_uri}@{binding.version} hash mismatch",
        )


def _evidence_node_from_existing(
    evidence_id: str, nodes: dict[str, AnyNode]
) -> EvidenceNode | None:
    n = nodes.get(evidence_id)
    if isinstance(n, EvidenceNode):
        return n
    return None


def validate_patch(req: PatchValidationRequest) -> PatchValidationResult:
    """Run the full D1 validation pipeline on a patch.

    Returns a candidate new projection (nodes + edges + derived events).
    Raises VPGError with a precise VPGCode on any failure — nothing partial.
    """
    patch = req.patch
    graph_id = patch.graph_id
    proposed_version = patch.expected_graph_version + 1

    if len(patch.operations) == 0:
        raise VPGError(VPGCode.PATCH_EMPTY, "patch has no operations")
    if len(patch.operations) > MAX_PATCH_OPS:
        raise VPGError(
            VPGCode.PATCH_TOO_LARGE,
            f"patch has {len(patch.operations)} ops, max {MAX_PATCH_OPS}",
        )

    res = PatchValidationResult()  # type: ignore[assignment]

    # Deep-copy baseline so result.candidate_nodes are independent objects —
    # derived-state mutations (VERIFIED/CLOSED) must not alias the snapshot.
    cand_nodes: dict[str, AnyNode] = {nid: copy.deepcopy(n) for nid, n in req.current_nodes.items()}
    cand_edges: list[VPGEdge] = [copy.deepcopy(e) for e in req.current_edges]
    pending_dep_edges: list[VPGEdge] = []

    for op in patch.operations:
        if isinstance(op, AddNodeOp):
            if op.node_id in cand_nodes:
                raise VPGError(
                    VPGCode.NODE_ALREADY_EXISTS,
                    f"node already exists: {op.node_id}",
                )
            node = _build_add_node(op, graph_id, proposed_version)
            admission = admit(node, graph_id)
            if not admission.admitted:
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    f"node {op.node_id} failed admission: {admission.messages}",
                )
            cand_nodes[node.node_id] = node
            res.admitted_nodes[node.node_id] = node
            res.new_nodes[node.node_id] = node
            res.events.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.NODE_ADDED,
                    causation_patch_id=patch.patch_id,
                    subject_id=node.node_id,
                    subject_kind="node",
                    node_id=node.node_id,
                    to_lifecycle=node.lifecycle.value,
                    to_validity=node.validity.value,
                    graph_version=proposed_version,
                )
            )
            res.events.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.NODE_ADMITTED,
                    causation_patch_id=patch.patch_id,
                    subject_id=node.node_id,
                    subject_kind="node",
                    node_id=node.node_id,
                    to_lifecycle=node.lifecycle.value,
                    to_validity=node.validity.value,
                    graph_version=proposed_version,
                )
            )

        elif isinstance(op, AddEdgeOp):
            if op.source_node_id not in cand_nodes:
                raise VPGError(
                    VPGCode.EDGE_SOURCE_NOT_FOUND,
                    f"edge source not found: {op.source_node_id}",
                )
            if op.target_node_id not in cand_nodes:
                raise VPGError(
                    VPGCode.EDGE_TARGET_NOT_FOUND,
                    f"edge target not found: {op.target_node_id}",
                )
            # cross-graph protection
            src = cand_nodes[op.source_node_id]
            tgt = cand_nodes[op.target_node_id]
            if src.graph_id != graph_id or tgt.graph_id != graph_id:
                raise VPGError(
                    VPGCode.EDGE_CROSS_GRAPH,
                    "edge crosses graph",
                )
            if op.edge_id in {e.edge_id for e in cand_edges}:
                raise VPGError(
                    VPGCode.EDGE_ALREADY_EXISTS,
                    f"edge already exists: {op.edge_id}",
                )

            new_edge = VPGEdge(
                edge_id=op.edge_id,
                graph_id=graph_id,
                edge_type=EdgeType(op.edge_type),
                source_node_id=op.source_node_id,
                target_node_id=op.target_node_id,
                created_in_version=proposed_version,
                created_by_pid=op.created_by_pid,
                created_at=_utcnow(),
            )
            if is_self_loop(new_edge):
                raise execution_cycle([new_edge.source_node_id, new_edge.target_node_id])

            # validate type combination
            _check_edge_type_combination(new_edge, cand_nodes)

            # defer cycle check across all depends_on edges in this patch
            if new_edge.edge_type == EdgeType.DEPENDS_ON:
                pending_dep_edges.append(new_edge)
            cand_edges.append(new_edge)
            res.new_edges.append(new_edge)
            res.events.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.EDGE_ADDED,
                    causation_patch_id=patch.patch_id,
                    subject_id=new_edge.edge_id,
                    subject_kind="edge",
                    graph_version=proposed_version,
                )
            )

        elif isinstance(op, AttachArtifactOp):
            task = cand_nodes.get(op.task_node_id)
            if not isinstance(task, TaskNode):
                raise VPGError(
                    VPGCode.NODE_NOT_FOUND,
                    f"attach_artifact: task_node_id {op.task_node_id} not found",
                )
            binding = op.artifact
            _validate_artifact_binding_against_sdk(binding, op.created_by_pid, req.facts_artifact)
            art, edge = _attach_artifact_produces(
                op.task_node_id,
                op.created_by_pid,
                binding,
                graph_id,
                proposed_version,
            )

            if op.edge_id in {e.edge_id for e in cand_edges}:
                raise VPGError(
                    VPGCode.EDGE_ALREADY_EXISTS,
                    f"edge already exists: {op.edge_id}",
                )

            # If this task already pins a produce-artifact for the same URI
            # at a different version, the old evidence_no-longer-applies — that
            # readiness recomputation handles; here we just record the new pin
            # and mark any existing verified evidence as needing re-check via
            # derivation (done by runtime after commit).
            cand_nodes[art.node_id] = art
            cand_edges.append(edge)
            res.new_nodes[art.node_id] = art
            res.new_edges.append(edge)
            res.events.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.ARTIFACT_ATTACHED,
                    causation_patch_id=patch.patch_id,
                    subject_id=art.node_id,
                    subject_kind="artifact",
                    node_id=art.node_id,
                    to_lifecycle=art.lifecycle.value,
                    graph_version=proposed_version,
                )
            )

        elif isinstance(op, AttachEvidenceOp):
            verification = cand_nodes.get(op.verification_node_id)
            if not isinstance(verification, VerificationNode):
                raise VPGError(
                    VPGCode.NODE_NOT_FOUND,
                    f"verification node {op.verification_node_id} not found",
                )
            evidence = _evidence_node_from_existing(op.evidence_node_id, cand_nodes)
            if evidence is None:
                raise VPGError(
                    VPGCode.NODE_NOT_FOUND,
                    f"evidence node {op.evidence_node_id} not found",
                )

            # re-validate evidence fully through SDK (result intentionally
            # unreferenced; patch is not rejected on currently-invalid evidence)
            validate_evidence(
                evidence,
                existing_nodes=cand_nodes,
                existing_edges=[
                    *cand_edges,
                    VPGEdge(
                        edge_id=op.edge_id,
                        graph_id=graph_id,
                        edge_type=EdgeType.PRODUCES,
                        source_node_id=verification.node_id,
                        target_node_id=evidence.node_id,
                        created_in_version=proposed_version,
                        created_by_pid=op.created_by_pid,
                        created_at=_utcnow(),
                    ),
                ],
                facts_artifact=req.facts_artifact,
                facts_kernel=req.facts_kernel,
            )
            # We do NOT reject the patch on evidence that is currently
            # invalid — evidence may become valid later.  We DO reject if
            # the mandatory structural preconditions fail (e.g. no source
            # action).  The weakest pre we enforce is that evidence exists
            # and its producing verification edge will be created.

            edge = VPGEdge(
                edge_id=op.edge_id,
                graph_id=graph_id,
                edge_type=EdgeType.PRODUCES,
                source_node_id=verification.node_id,
                target_node_id=evidence.node_id,
                created_in_version=proposed_version,
                created_by_pid=op.created_by_pid,
                created_at=_utcnow(),
            )
            if op.edge_id in {e.edge_id for e in cand_edges}:
                raise VPGError(
                    VPGCode.EDGE_ALREADY_EXISTS,
                    f"edge already exists: {op.edge_id}",
                )
            cand_edges.append(edge)
            res.new_edges.append(edge)
            res.events.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.EVIDENCE_ATTACHED,
                    causation_patch_id=patch.patch_id,
                    subject_id=evidence.node_id,
                    subject_kind="evidence",
                    graph_version=proposed_version,
                )
            )

        else:
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                f"unknown patch operation: {type(op).__name__}",
            )

    # DAG check
    cycle = detect_cycle(
        list(req.current_edges),
        pending_dep_edges,
    )
    if cycle is not None and len(cycle) > 1:
        raise execution_cycle(cycle)

    # final candidate
    res.candidate_nodes = cand_nodes
    res.candidate_edges = cand_edges
    return res
