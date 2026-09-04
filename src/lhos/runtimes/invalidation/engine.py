"""D3 — invalidation engine: atomic semantic transaction (§20, §25).

The engine ties together Evidence applicability derivation, cone
computation, proof construction, goal-reopen derivation, Repair Frontier,
and the D3 event journal into ONE atomic operation.

Atomicity (§20): the caller applies either the FULL InvalidationResult to the
D3 projection or NOTHING.  There is never a "half started" commit because
the runtime does NOT write to VPG task nodes directly; it produces a
derived state which the host commits atomically via a VPG Patch (goal/validity
derivation) or not at all.  See `InvalidationRuntime.commit` in runtime.py.

GraphVersion race (§19): every computation binds to base_graph_version.  The
host re-validates `current_version == base_graph_version` before commit.  If
a new Patch created a newer version DURING computation, the host must abort
and recompute against the newest version — never silently merge.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from .cone import (
    _cone_hash,
    build_proofs,
    compute_invalidation_cone,
)
from .evidence import (
    causes_from_applicability,
    evidence_applicability_for_graph,
)
from .frontier import compute_repair_frontier
from .models import (
    D3Event,
    EvidenceApplicability,
    InvalidationCause,
    InvalidationCone,
    InvalidationProof,
    InvalidationResult,
    RepairFrontier,
)

STALE = "stale"
VERIFIED = "verified"
CLOSED = "closed"


def _hash(*parts: Any) -> str:
    j = "\x1f".join(("" if p is None else str(p)) for p in parts)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()


@dataclass
class EngineInputs:
    graph_id: str
    current_version: int

    task_nodes: dict[str, Any]
    goal_nodes: dict[str, Any]
    evidence_nodes: dict[str, Any]
    edges: Iterable[Any]

    # optional overrides for seed derivation
    current_output_versions: dict[str, int] | None = None
    verify_binding: Callable[[str, int, str], bool] | None = None
    action_valid: Callable[[str], bool] | None = None
    event_valid: Callable[[str], bool] | None = None

    # explicit causes (may be precomputed + provided directly)
    explicit_causes: tuple[InvalidationCause, ...] | None = None

    # D2 ownership hook: callable(task_id) -> bool (has active claim)
    has_active_claim: Callable[[str], bool] | None = None

    # goal direct-deps: list of (goal_id, tuple of task_ids it depends on)
    goal_direct_tasks: dict[str, tuple[str, ...]] | None = None

    # evidence_of: task_id -> tuple evidence ids
    evidence_of: dict[str, tuple[str, ...]] | None = None


@dataclass
class EngineResult:
    applicability: tuple[EvidenceApplicability, ...]
    causes: tuple[InvalidationCause, ...]
    cone: InvalidationCone
    proofs: tuple[InvalidationProof, ...]
    frontier: RepairFrontier
    reopened_goals: tuple[str, ...]
    events: tuple[D3Event, ...]


def _derive_goal_reopen(
    goal_nodes: dict[str, Any],
    cone: InvalidationCone,
    goal_direct_tasks: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    """A CLOSED goal whose any direct dependency is now stale/unverified must
    reopen (derive from cone).  Goals are never manually reopened (§17)."""
    reopened: list[str] = []
    for gid in sorted(goal_nodes.keys()):
        gnode = goal_nodes[gid]
        if getattr(gnode, "closed", False) is not True and gnode.lifecycle.value != CLOSED:
            # only CLOSED goals can reopen
            continue
        deps = goal_direct_tasks.get(gid, ())
        for tid in deps:
            if tid in cone.affected_node_ids:
                reopened.append(gid)
                break
    return tuple(reopened)


def run_invalidation_engine(
    inp: EngineInputs,
) -> EngineResult:
    """Run the D3 invalidation derivation pipeline (PURE — no DB writes).

    Steps:
      1. Derive Evidence applicability from current ArtifactVersion truth.
      2. Convert lost-applicability into authoritative InvalidationCause seeds.
      3. Compute deterministic causal cone.
      4. Derive Goal reopenings.
      5. Compute minimal Repair Frontier.
      6. Build D3 event history.
    """
    verds = evidence_applicability_for_graph(
        inp.graph_id,
        inp.current_version,
        inp.evidence_nodes,
        verify_binding=inp.verify_binding,
        action_valid=inp.action_valid,
        event_valid=inp.event_valid,
        current_output_versions=inp.current_output_versions,
    )
    if inp.explicit_causes is not None:
        causes = inp.explicit_causes
    else:
        causes = causes_from_applicability(inp.graph_id, inp.current_version, verds)

    if not causes:
        # no seeds: nothing is invalidated.
        cone = InvalidationCone(
            graph_id=inp.graph_id,
            base_graph_version=inp.current_version,
            causes=(),
            seed_node_ids=(),
            affected_node_ids=(),
            preserved_node_ids=tuple(sorted(inp.task_nodes.keys())),
            propagation_edges=(),
            cone_hash="",
        )
        frontier = compute_repair_frontier(
            inp.graph_id,
            inp.current_version,
            inp.task_nodes,
            inp.edges,
            has_active_claim=inp.has_active_claim,
        )
        frontier.frontier_hash = frontier.frontier_hash
        events = _build_events(inp, causes, cone, (), frontier, (), started=True, completed=True)
        return EngineResult(verds, (), cone, (), frontier, (), events)

    cone = compute_invalidation_cone(
        inp.graph_id,
        inp.current_version,
        inp.task_nodes,
        inp.edges,
        causes,
        verds,
        evidence_of=inp.evidence_of,
    )
    cone_hash = _cone_hash(cone)
    cone.cone_hash = cone_hash

    proofs = build_proofs(cone, inp.task_nodes, inp.edges)

    reopened: tuple[str, ...] = ()
    if inp.goal_direct_tasks:
        reopened = _derive_goal_reopen(inp.goal_nodes, cone, inp.goal_direct_tasks)

    derived_validity = {
        tid: (STALE if tid in cone.affected_node_ids else cur_validity)
        for tid, n in inp.task_nodes.items()
        for cur_validity in [n.validity.value]
    }
    frontier = compute_repair_frontier(
        inp.graph_id,
        inp.current_version,
        inp.task_nodes,
        inp.edges,
        stale_or_unverified=set(cone.affected_node_ids),
        derived_validity=derived_validity,
        has_active_claim=inp.has_active_claim,
    )

    events = _build_events(
        inp, causes, cone, proofs, frontier, reopened, started=True, completed=True
    )
    return EngineResult(verds, causes, cone, proofs, frontier, reopened, events)


def _build_events(
    inp: EngineInputs,
    causes: tuple[InvalidationCause, ...],
    cone: InvalidationCone,
    proofs: tuple[InvalidationProof, ...],
    frontier: RepairFrontier,
    reopened: tuple[str, ...],
    *,
    started: bool,
    completed: bool,
) -> tuple[D3Event, ...]:
    events: list[D3Event] = []
    cid = f"{inp.graph_id}:v{inp.current_version}"
    if started:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "STARTED"),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="INVALIDATION_STARTED",
                cause_ids=tuple(c.cause_id for c in causes),
                occurred_at_version=inp.current_version,
                reason="invalidation transaction started",
            )
        )
    for c in causes:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "CAUSE", c.cause_id),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="INVALIDATION_CAUSE_VALIDATED",
                cause_ids=(c.cause_id,),
                source_node_id=c.source_node_id,
                occurred_at_version=inp.current_version,
                reason=f"{c.cause_type} :: {c.reason}",
            )
        )
    for tid in cone.affected_node_ids:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "STALE", tid),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="TASK_STALE_DERIVED",
                affected_node_id=tid,
                old_validity=VERIFIED,
                new_validity=STALE,
                occurred_at_version=inp.current_version,
                reason="dependency lost VERIFIED proof",
            )
        )
    for edge in cone.propagation_edges:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "PROP", edge),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="INVALIDATION_PROPAGATED",
                causal_edge=edge,
                occurred_at_version=inp.current_version,
                reason="stale propagated along dependency",
            )
        )
    for g in reopened:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "REOPEN", g),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="GOAL_REOPENED_DERIVED",
                affected_node_id=g,
                old_validity=CLOSED,
                new_validity=STALE,
                occurred_at_version=inp.current_version,
                reason="goal dependency became stale",
            )
        )
    events.append(
        D3Event(
            event_id=_hash("evt", cid, "FRONTIER"),
            graph_id=inp.graph_id,
            graph_version=inp.current_version,
            event_type="REPAIR_FRONTIER_UPDATED",
            occurred_at_version=inp.current_version,
            reason=f"{len(frontier.candidates)} repair candidates",
        )
    )
    if completed:
        events.append(
            D3Event(
                event_id=_hash("evt", cid, "COMPLETED"),
                graph_id=inp.graph_id,
                graph_version=inp.current_version,
                event_type="INVALIDATION_COMPLETED",
                occurred_at_version=inp.current_version,
                reason=f"cone={len(cone.affected_node_ids)} affected, "
                f"{len(cone.preserved_node_ids)} preserved, "
                f"frontier={len(frontier.candidates)}",
            )
        )
    events.sort(key=lambda e: e.event_id)
    return tuple(events)


def build_invalidation_result(
    inp: EngineInputs,
    engine_result: EngineResult,
) -> InvalidationResult:
    """Assemble the full atomic InvalidationResult (§20)."""
    stale = tuple(sorted(engine_result.cone.affected_node_ids))
    preserved = tuple(sorted(engine_result.cone.preserved_node_ids))
    result = InvalidationResult(
        graph_id=inp.graph_id,
        committed_graph_version=inp.current_version,
        causes=engine_result.causes,
        cone=engine_result.cone,
        proofs=engine_result.proofs,
        frontier=engine_result.frontier,
        stale_nodes=stale,
        reopened_goals=engine_result.reopened_goals,
        preserved_nodes=preserved,
        events=engine_result.events,
        result_hash="",
    )
    h = _hash(
        "result",
        inp.graph_id,
        inp.current_version,
        engine_result.cone.cone_hash,
        tuple(c.cause_id for c in engine_result.causes),
        tuple(p.task_id for p in engine_result.proofs),
        tuple(r.task_id for r in engine_result.frontier.candidates),
        tuple(engine_result.reopened_goals),
    )
    result.result_hash = h
    return result
