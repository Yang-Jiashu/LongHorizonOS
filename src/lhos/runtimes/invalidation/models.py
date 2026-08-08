"""D3 — Version-aware causal invalidation + local repair models.

This module defines the PURE DATA model for the D3 invalidation engine.  It
holds no runtime logic and no reference to Kernel, ArtifactFS, ContextVM,
VPG runtime internals, or the D2 Scheduler.  It only types the semantic
concept objects exchanged across the D3 protocol so that Authority Boundary
(§3, §39) can be enforced by import-layering.

Design rules frozen here (§47):
  - Artifact history is immutable.
  - Evidence history is immutable.
  - Current semantic validity is DERIVED — never stored as historical fact.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

# ── Invalidation cause kinds (§6) ─────────────────────────────────────────────
InvalidationCauseType = Literal[
    "ARTIFACT_VERSION_SUPERSEDED",
    "EVIDENCE_ARTIFACT_INVALID",
    "SOURCE_ACTION_INVALID",
    "SOURCE_EVENT_INVALID",
]


class InvalidationCause(BaseModel):
    """A single authoritative fact that makes some Evidence inapplicable.

    cause_type is one of the closed set in §6 — we DO NOT generalise into a
    belief-revision system.
    """

    cause_id: str
    graph_id: str
    graph_version: int
    cause_type: InvalidationCauseType
    source_node_id: str | None = None

    artifact_id: str | None = None
    old_version: int | None = None
    new_version: int | None = None

    evidence_id: str | None = None
    action_id: str | None = None

    reason: str


class EvidenceApplicability(BaseModel):
    """Derived current-applicability verdict for one piece of Evidence.

    The Evidence row itself is IMMUTABLE history (see EvidenceNode).  This
    object is the *derivation* of whether it still applies to the CURRENT
    semantic state — the two are deliberately disjoint concepts (§4).
    """

    graph_id: str
    graph_version: int
    evidence_id: str
    applies: bool
    reason: str
    cause_id: str | None = None
    derived_at_version: int


class InvalidationCone(BaseModel):
    """Deterministic closure of a set of invalidation seeds (§8).

    Propagation follows ONLY semantic dependency direction
    (DEPENDS_ON edges) and NEVER leaks into unaffected branches (§9, §12).
    """

    graph_id: str
    base_graph_version: int

    causes: tuple[InvalidationCause, ...]
    seed_node_ids: tuple[str, ...]
    affected_node_ids: tuple[str, ...]
    preserved_node_ids: tuple[str, ...]

    # "source->target" one entry per propagation step
    propagation_edges: tuple[str, ...]

    cone_hash: str


class InvalidationProof(BaseModel):
    """Reasoner for WHY a single Task became STALE (§22)."""

    graph_id: str
    graph_version: int

    task_id: str

    root_causes: tuple[str, ...]
    causal_path: tuple[str, ...]

    previous_validity: str
    resulting_validity: str

    proof_hash: str


class RepairCandidate(BaseModel):
    """A Task in the current minimal Repair Frontier §13."""

    task_id: str
    causes: tuple[str, ...]
    invalidated_by: tuple[str, ...]
    dependency_proof: tuple[str, ...]


class RepairFrontier(BaseModel):
    """Minimal set of immediately re-executable repair Tasks §13-14."""

    graph_id: str
    graph_version: int

    candidates: tuple[RepairCandidate, ...]
    frontier_hash: str


# ── Invalidation transaction events (§21) ────────────────────────────────────
class D3Event(BaseModel):
    """One derived event written into the D3 journal (not Agent-created)."""

    event_id: str
    graph_id: str
    graph_version: int

    event_type: str  # one of the D3_EVENT_TYPES below

    cause_ids: tuple[str, ...] = ()
    source_node_id: str | None = None
    affected_node_id: str | None = None
    causal_edge: str | None = None

    old_validity: str | None = None
    new_validity: str | None = None

    occurred_at_version: int
    reason: str = ""


D3_EVENT_TYPES: tuple[str, ...] = (
    "INVALIDATION_STARTED",
    "INVALIDATION_CAUSE_VALIDATED",
    "EVIDENCE_APPLICABILITY_LOST",
    "TASK_STALE_DERIVED",
    "TASK_REVERIFIED_DERIVED",
    "INVALIDATION_PROPAGATED",
    "GOAL_REOPENED_DERIVED",
    "REPAIR_FRONTIER_UPDATED",
    "INVALIDATION_COMPLETED",
    "INVALIDATION_ABORTED",
    "D3_RECOVERY_STARTED",
    "D3_RECOVERY_COMPLETED",
)


class InvalidationResult(BaseModel):
    """Outcome of one D3 invalidation transaction (§20).

    This is the ATOMIC semantic transaction.  Either all fields commit
    together (as a single derived state), or the transaction aborts with
    zero semantic effect (§20, §25).
    """

    graph_id: str
    committed_graph_version: int

    causes: tuple[InvalidationCause, ...]
    cone: InvalidationCone
    proofs: tuple[InvalidationProof, ...]
    frontier: RepairFrontier

    stale_nodes: tuple[str, ...]
    reopened_goals: tuple[str, ...]
    preserved_nodes: tuple[str, ...]

    events: tuple[D3Event, ...]

    result_hash: str

    metadata: dict[str, Any] = Field(default_factory=dict)
