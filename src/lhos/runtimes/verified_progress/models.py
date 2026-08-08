"""Verified Progress Graph — core domain models.

Defines GraphRecord, GraphVersion, edge binding types, ArtifactVersionBinding,
and the discriminated VPGNode hierarchy (Goal/Task/ArtifactRef/Verification/Evidence).

Reference: Phase D1 spec, sections 7-10.

All nodes and edges are Pydantic BaseModels.  All derived state (VERIFIED,
STALE, CLOSED, INVALID) is produced deterministically by the Runtime —
never by direct Agent patch.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


# ── UUID helper (mirrors kernel convention) ─────────────────────────────────
def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(UTC)


# ── Enums ────────────────────────────────────────────────────────────────────
class NodeType(StrEnum):
    GOAL = "goal"
    TASK = "task"
    ARTIFACT_REF = "artifact_ref"
    VERIFICATION = "verification"
    EVIDENCE = "evidence"


class EdgeType(StrEnum):
    DEPENDS_ON = "depends_on"
    PRODUCES = "produces"
    VERIFIES = "verifies"


class NodeLifecycle(StrEnum):
    PROPOSED = "proposed"
    ADMITTED = "admitted"
    ACTIVE = "active"
    CLOSED = "closed"


class NodeValidity(StrEnum):
    UNVERIFIED = "unverified"
    VERIFIED = "verified"
    STALE = "stale"
    INVALID = "invalid"


class EvidenceResult(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    INCONCLUSIVE = "inconclusive"


# ── GraphRecord ──────────────────────────────────────────────────────────────
class GraphRecord(BaseModel):
    """Top-level container for a verified progress graph.

    - graph_id is stable for the lifetime of the graph.
    - current_version starts at 0 and is incremented by exactly 1 per
      successful Patch commit.
    - closed graphs reject further Agent patches (the terminal state).
    """

    graph_id: str = Field(default_factory=_uuid)
    owner_pid: str

    current_version: int = 0
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)

    closed: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


# ── GraphVersion ─────────────────────────────────────────────────────────────
class GraphVersion(BaseModel):
    """Immutable snapshot pointer — one per committed Patch.

    parent_version == None for the initial (v0) GraphVersion.
    """

    graph_id: str
    version: int

    parent_version: int | None
    patch_id: str

    projection_hash: str
    committed_by_pid: str
    committed_at: datetime


# ── ArtifactVersionBinding ────────────────────────────────────────────────────
class ArtifactVersionBinding(BaseModel):
    """Pin to an exact committed ArtifactVersion (no "latest", no aliases)."""

    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str

    media_type: str = "application/octet-stream"


# ── VPGNode (base) ───────────────────────────────────────────────────────────
class VPGNode(BaseModel):
    """Common fields shared across all node types."""

    node_id: str = Field(default_factory=_uuid)
    graph_id: str
    node_type: NodeType

    lifecycle: NodeLifecycle = NodeLifecycle.PROPOSED
    validity: NodeValidity = NodeValidity.UNVERIFIED

    created_in_version: int
    updated_in_version: int

    created_by_pid: str
    created_at: datetime = Field(default_factory=_utcnow)

    metadata: dict[str, Any] = Field(default_factory=dict)


# ── Specialized Nodes ────────────────────────────────────────────────────────
class GoalNode(VPGNode):
    """A declarative objective.  Closure derived from depends_on Tasks."""

    node_type: Literal[NodeType.GOAL] = NodeType.GOAL
    title: str = ""
    description: str = ""


class TaskNode(VPGNode):
    """Executable unit.  Requires evidence-backed verification to become VERIFIED."""

    node_type: Literal[NodeType.TASK] = NodeType.TASK
    title: str = ""
    description: str = ""

    task_kind: str = ""
    execution_spec: dict[str, Any] = Field(default_factory=dict)

    required_verification_count: int = 1


class ArtifactRefNode(VPGNode):
    """A node pinned to an exact committed ArtifactVersion."""

    node_type: Literal[NodeType.ARTIFACT_REF] = NodeType.ARTIFACT_REF

    canonical_uri: str
    artifact_id: str
    version: int
    content_hash: str

    media_type: str = "application/octet-stream"


class VerificationNode(VPGNode):
    """An obligation that a Task is verified by the given kind."""

    node_type: Literal[NodeType.VERIFICATION] = NodeType.VERIFICATION

    verification_kind: str = "command_result"
    obligation: dict[str, Any] = Field(default_factory=dict)

    source_action_id: str | None = None


class EvidenceNode(VPGNode):
    """A recorded verification result, bound to exact ArtifactVersions + Action."""

    node_type: Literal[NodeType.EVIDENCE] = NodeType.EVIDENCE

    evidence_kind: str = "command_result"

    result: EvidenceResult = EvidenceResult.INCONCLUSIVE

    source_verification_id: str | None = None
    source_action_id: str | None = None
    source_event_ids: tuple[str, ...] = ()

    artifact_bindings: tuple[ArtifactVersionBinding, ...] = ()
    evidence_content_ref: ArtifactVersionBinding | None = None
    evidence_hash: str = ""

    produced_by_pid: str = ""
    produced_at: datetime = Field(default_factory=_utcnow)


# ── Edge ──────────────────────────────────────────────────────────────────────
class VPGEdge(BaseModel):
    """Directed edge within the graph.  Only three edge types are allowed."""

    edge_id: str = Field(default_factory=_uuid)
    graph_id: str

    edge_type: EdgeType
    source_node_id: str
    target_node_id: str

    created_in_version: int
    created_by_pid: str
    created_at: datetime = Field(default_factory=_utcnow)


# ── Readiness ─────────────────────────────────────────────────────────────────
class ReadinessProof(BaseModel):
    """Witness that a task satisfied the D1 readiness predicate."""

    graph_id: str
    graph_version: int
    task_id: str

    lifecycle_ok: bool
    validity_ok: bool
    all_deps_verified: bool
    has_execution_attempt: bool


class TaskDispatchCandidate(BaseModel):
    """Ready task surfaced to the Kernel/Harness for operational admission.

    D1 strictly separates logical READY (this struct) from operationally
    RUNNABLE (checked later by the Kernel / Harness against capability,
    budget, etc.).
    """

    graph_id: str
    graph_version: int
    task_id: str
    readiness_proof: ReadinessProof
    execution_spec: dict[str, Any]


# Union alias for convenience inside runtime code
AnyNode = GoalNode | TaskNode | ArtifactRefNode | VerificationNode | EvidenceNode
