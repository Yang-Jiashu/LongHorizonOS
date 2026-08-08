"""D3 — deterministic causal invalidation cone computation.

This is the CORE of the version-aware local repair engine.

--- The only semantics we propagate along ---
We reuse the VPG's own edge vocabulary (DEPENDS_ON, PRODUCES, VERIFIES) to
derive semantic causality (§7).  No new graph model is created: D3 observes
the VPG graph and derives the causal closure purely from it.

The propagation rule (§10, §11):

    A source Task T_src is STALE (its current semantic validity lost)
    AND Task T_dep depends_on T_src
    AND T_dep is currently VERIFIED
  => T_dep's VERIFIED derivation is no longer sound
     (its deps are no longer all-VERIFIED)
  => T_dep becomes STALE

Non-goals (explicitly NOT done — §1, §9, §12):
  - Do NOT invalidate independent branches (over-invalidation forbidden).
  - Do NOT propagate "backwards" to a Task that DEPENDS ON the stale one
    only via a diamond where the other path remains VERIFIED — unless that
    Task actually depends on the stale one (which a TRUE DEPENDS_ON edge
    always means).  Propagation is strict, monotonic, and direction-only.

Determinism (§18):
  - Seeds are sorted by node_id (not insertion order).
  - Traversal is a work-list keyed by node_id in lexicographic order.
  - No set-iteration, no hash-order, no DB insertion order leaks.
"""

from __future__ import annotations

import hashlib
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from .models import (
    EvidenceApplicability,
    InvalidationCause,
    InvalidationCone,
    InvalidationProof,
)


def _hash(*parts: Any) -> str:
    j = "\x1f".join(("" if p is None else str(p)) for p in parts)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()


STALE = "stale"
VERIFIED = "verified"
UNVERIFIED = "unverified"
INVALID = "invalid"


@dataclass(frozen=True)
class _GraphView:
    """Snapshot of the semantic causality relation for one base version."""

    task_nodes: dict[str, Any]  # node_id -> TaskNode
    # forward: task -> tuple of DEPENDS_ON target task ids (execution order)
    forward_deps: dict[str, tuple[str, ...]]
    # reverse: task -> set/tuple of DEPENDS_ON sources that depend on this task
    reverse_deps: dict[str, tuple[str, ...]]


def _build_view(task_nodes: dict[str, Any], edges: Iterable[Any]) -> _GraphView:
    forward: dict[str, list[str]] = {tid: [] for tid in task_nodes}
    reverse: dict[str, list[str]] = {tid: [] for tid in task_nodes}
    for e in edges:
        # Only DEPENDS_ON edges carry semantic dependency (§7).
        et = getattr(e, "edge_type", None)
        if et is None or et.value != "depends_on":
            continue
        s = e.source_node_id
        t = e.target_node_id
        if s in task_nodes and t in task_nodes:
            forward.setdefault(s, []).append(t)
            reverse.setdefault(t, []).append(s)
    # Deterministic order.
    for k in forward:
        forward[k].sort()
    for k in reverse:
        reverse[k].sort()
    return _GraphView(
        task_nodes=dict(task_nodes),
        forward_deps={k: tuple(v) for k, v in forward.items()},
        reverse_deps={k: tuple(v) for k, v in reverse.items()},
    )


def _applies(applicability: Iterable[EvidenceApplicability], evidence_id: str) -> bool | None:
    """Return True/False if an applicability verdict exists, else None."""
    for a in applicability:
        if a.evidence_id == evidence_id:
            return a.applies
    return None


def compute_invalidation_cone(
    graph_id: str,
    base_graph_version: int,
    task_nodes: dict[str, Any],
    edges: Iterable[Any],
    causes: tuple[InvalidationCause, ...],
    evidence_applicability: Iterable[EvidenceApplicability] = (),
    *,
    evidence_of: dict[str, tuple[str, ...]] | None = None,
) -> InvalidationCone:
    """Compute the deterministic causal invalidation cone.

    Parameters
    ----------
    task_nodes : dict[str, TaskNode]
        map of task_id -> TaskNode (all Tasks in the graph at base version).
    edges : Iterable[VPGEdge]
        all VPG edges at base version.
    causes : tuple[InvalidationCause, ...]
        the authoritative seeds.
    evidence_applicability : iterable of EvidenceApplicability
        verdicts (may be empty; engine seeds precomputed elsewhere).
    evidence_of : optional dict task_id -> tuple of evidence_ids directly
        verifying that task (used to join EVD-loss -> Task STALE when the
        applicability verdicts are supplied).

    Propagation:
      1. seed_tasks  = those Task nodes whose own producing Evidence lost
         applicability (or whose cause_type targets them directly).
      2. stale = work-list closure of seed_tasks under reverse_deps using
         the rule above.
    """
    view = _build_view(task_nodes, edges)
    applic_map: dict[str, bool] = {a.evidence_id: a.applies for a in evidence_applicability}

    seed_ids: list[str] = []
    for cause in causes:
        if (
            cause.source_node_id
            and cause.source_node_id in task_nodes
            and (cause.source_node_id not in seed_ids)
        ):
            seed_ids.append(cause.source_node_id)
    # Also any task whose evidence lost applicability.
    if evidence_of:
        for tid, evids in evidence_of.items():
            if tid not in task_nodes:
                continue
            lost = any(eid in applic_map and applic_map[eid] is False for eid in evids)
            if lost and tid not in seed_ids:
                seed_ids.append(tid)

    seed_ids.sort()

    # Propagate deterministically.
    stale: set[str] = set()
    queue: deque[str] = deque(seed_ids)
    propagation_edges: list[str] = []
    while queue:
        node_id = queue.popleft()
        if node_id in stale:
            continue
        if node_id not in task_nodes:
            continue
        stale.add(node_id)
        # Any task that DEPENDS ON node_id, if currently VERIFIED, becomes stale.
        for rely in view.reverse_deps.get(node_id, ()):
            if rely in stale:
                continue
            # only invalidate VERIFIED dependents -> preserve UNVERIFIED/others.
            cur = task_nodes[rely].validity.value
            if cur == VERIFIED:
                propagation_edges.append(f"{node_id}->{rely}")
                queue.append(rely)
        # Important: we DO NOT propagate to forward deps (that would be
        # the reverse direction).  Only DEPENDS_ON semantics matter.

    # Deterministic ordering of affected nodes for output.
    affected = tuple(sorted(stale))
    preserved = tuple(sorted(tid for tid in task_nodes if tid not in stale))
    propagation_edges.sort()

    cone = InvalidationCone(
        graph_id=graph_id,
        base_graph_version=base_graph_version,
        causes=causes,
        seed_node_ids=tuple(seed_ids),
        affected_node_ids=affected,
        preserved_node_ids=preserved,
        propagation_edges=tuple(propagation_edges),
        cone_hash="",
    )
    # NOTE: needs model dump to compute final hash; done by engine.
    return cone


def _cone_hash(cone: InvalidationCone) -> str:
    core = (
        cone.graph_id,
        cone.base_graph_version,
        tuple(sorted(c.cause_id for c in cone.causes)),
        cone.seed_node_ids,
        cone.affected_node_ids,
        cone.propagation_edges,
    )
    return _hash("cone", *core)


def build_proofs(
    cone: InvalidationCone,
    task_nodes: dict[str, Any],
    edges: Iterable[Any],
) -> tuple[InvalidationProof, ...]:
    """Construct a human-reasoner proof for every stale node in the cone."""
    view = _build_view(task_nodes, edges)
    root_causes = {
        c.source_node_id: c.cause_id for c in cone.causes if c.source_node_id is not None
    }
    proofs: list[InvalidationProof] = []
    for tid in cone.affected_node_ids:
        # causal_path: walk reverse_deps from seed to tid.
        roots: list[str] = []
        # Simple BFS from tid upward to find a seed.
        fringe: deque[tuple[str, tuple[str, ...]]] = deque([(tid, (tid,))])
        found: tuple[str, ...] | None = None
        visited: set[str] = {tid}
        while fringe:
            cur, p = fringe.popleft()
            if cur in root_causes:
                roots.append(root_causes[cur])
                found = p
                break
            for parent in view.reverse_deps.get(cur, ()):
                if parent not in visited:
                    visited.add(parent)
                    fringe.append((parent, (parent, *p)))
        if found is None:
            # if tid itself is a seed
            if tid in root_causes:
                roots = [root_causes[tid]]
                found = (tid,)
            else:
                found = (tid,)
        prev = task_nodes[tid].validity.value
        proofs.append(
            InvalidationProof(
                graph_id=cone.graph_id,
                graph_version=cone.base_graph_version,
                task_id=tid,
                root_causes=tuple(roots),
                causal_path=tuple(ch for ch in found),
                previous_validity=prev,
                resulting_validity=STALE,
                proof_hash="",
            )
        )
    proofs.sort(key=lambda pr: pr.task_id)
    for pr in proofs:
        pr.proof_hash = _hash("proof", pr.task_id, pr.root_causes, pr.causal_path)
    return tuple(proofs)
