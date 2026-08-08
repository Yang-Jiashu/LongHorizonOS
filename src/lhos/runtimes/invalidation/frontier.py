"""D3 — deterministic minimal Repair Frontier computation (§13, §14).

RepairReady(T) iff
    T.validity in {STALE, UNVERIFIED}
    AND all T's DEPENDS_ON dependencies are VERIFIED
    AND T has no valid active D2 claim

Minimality (§14): given a chain T1->T2->T3->T4, if T1 is stale the
frontier is [T1] ONLY.  As T1 is re-verified and returns to VERIFIED,
T2 becomes front-ready, etc.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

from .models import RepairCandidate, RepairFrontier

VERIFIED = "verified"
STALE = "stale"
UNVERIFIED = "unverified"


def _hash(*parts: Any) -> str:
    j = "\x1f".join(("" if p is None else str(p)) for p in parts)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()


def compute_repair_frontier(
    graph_id: str,
    graph_version: int,
    task_nodes: dict[str, Any],
    edges: Iterable[Any],
    *,
    stale_or_unverified: set[str] | None = None,
    derived_validity: dict[str, str] | None = None,
    has_active_claim: Any = None,  # callable(task_id) -> bool (D2 ownership hook)
) -> RepairFrontier:
    """Compute the minimal set of immediately-re-executable repair Tasks.

    A candidate claims T iff (RepairReady):
        - T is stale/unverified (in the DERIVED validity state)
        - every Task that T depends_on (DEPENDS_ON edge target) is VERIFIED
          in the DERIVED validity state
        - T is not under an active D2 claim

    ``derived_validity`` maps task_id -> effective validity AFTER the
    invalidation cone was applied (layered on top of original task_nodes).
    If omitted, the original task_nodes validity is used.
    """
    forward: dict[str, list[str]] = {}
    for e in edges:
        et = getattr(e, "edge_type", None)
        if et is None or et.value != "depends_on":
            continue
        s = e.source_node_id
        t = e.target_node_id
        if s in task_nodes and t in task_nodes:
            forward.setdefault(s, []).append(t)
    for k in forward:
        forward[k].sort()

    def _validity(tid: str) -> str:
        if derived_validity is not None and tid in derived_validity:
            return derived_validity[tid]
        n = task_nodes.get(tid)
        return n.validity.value if n is not None else "missing"

    if stale_or_unverified is None:
        if derived_validity is not None:
            eligible = {tid for tid in task_nodes if _validity(tid) in {STALE, UNVERIFIED}}
        else:
            eligible = {
                tid for tid, n in task_nodes.items() if n.validity.value in {STALE, UNVERIFIED}
            }
    else:
        eligible = set(stale_or_unverified)

    candidates: list[RepairCandidate] = []
    for tid in sorted(eligible):
        deps = forward.get(tid, ())
        dep_proof: list[str] = []
        ok = True
        for d in deps:
            dval = _validity(d)
            if dval not in {VERIFIED}:
                dep_proof.append(f"{d}:{dval}")
                ok = False
            else:
                dep_proof.append(f"{d}:verified")
        if not ok:
            continue
        if has_active_claim is not None and has_active_claim(tid):
            dep_proof.append("active_claim")
            continue
        candidates.append(
            RepairCandidate(
                task_id=tid,
                causes=(),
                invalidated_by=(),
                dependency_proof=tuple(dep_proof),
            )
        )
    # Deterministic order.
    candidates.sort(key=lambda c: c.task_id)
    frontier = RepairFrontier(
        graph_id=graph_id,
        graph_version=graph_version,
        candidates=tuple(candidates),
        frontier_hash="",
    )
    h = _hash(
        "frontier",
        graph_id,
        graph_version,
        tuple(c.task_id for c in candidates),
    )
    frontier.frontier_hash = h
    return frontier
