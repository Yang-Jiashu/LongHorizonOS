"""Scheduler Recovery — restart-time reconstruction of Scheduler state from
authoritative Kernel + VPG + event history (Section 29, projection rebuild).

Projection rebuild MUST be deterministic: identical inputs yield byte-
identical normalized projection state across repeated rebuilds.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .models import ClaimState
from .projections import SchedulerProjection


def _now() -> datetime:
    return datetime.now(timezone.utc)


def rebuild_projection(
    agents: list[Any],
    claims: list[Any],
    attempts: list[Any],
    *,
    lease_is_live: Any,
    process_is_alive: Any,
) -> SchedulerProjection:
    """Drop and rebuild SchedulerProjection from authoritative inputs.

    The projection is a rebuilt materialized view; callers are responsible
    for reconciling live claims against kernel state first.
    """
    proj = SchedulerProjection()
    proj.rebuild(agents, claims, attempts)
    return proj


def projection_fingerprint(proj: SchedulerProjection) -> str:
    """Content-hash snapshot for deterministic-rebuild audit.

    Normalises agents/claims/attempts/loads into a sorted, stable
    canonical string and returns its sha256 hexdigest.  Used by the
    projection-replay audit to verify byte-identical rebuilds.
    """
    agents = sorted(proj.agents.keys())
    claims = sorted(
        (
            c.claim_id, c.graph_id, c.task_id, c.agent_id, c.process_id,
            c.state.value, c.lease_id or "",
        )
        for c in proj.claims.values()
    )
    attempts = sorted(
        (a.attempt_id, a.task_id, a.agent_id, a.state.value)
        for a in proj.attempts.values()
    )
    loads = sorted(
        (l.agent_id, l.active_claims, l.max_concurrency)
        for l in proj.loads.values()
    )
    payload = json.dumps(
        {"agents": agents, "claims": claims, "attempts": attempts, "loads": loads},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def finalize_after_restart(
    claims: list[Any],
    *,
    lease_is_live: Any,
    process_is_alive: Any,
    release_lease: Any,
) -> dict[str, int]:
    """Post-restart clean-up: mark ACTIVE claims whose authoritative backing
    is gone, and ensure orphan leases are released.  Returns a tally of
    repairs for logging/audit.
    """
    tally: dict[str, int] = {"claims_marked_lost": 0, "orphan_leases_released": 0}
    for c in list(claims):
        if c.state != ClaimState.ACTIVE:
            continue
        if c.lease_id and not lease_is_live(c.lease_id):
            try:
                release_lease(c.lease_id)
                tally["orphan_leases_released"] += 1
            except Exception:
                pass
            c.state = ClaimState.LOST
            c.released_at = _now()
            c.reason = "restart_no_live_lease"
            tally["claims_marked_lost"] += 1
            continue
        if not process_is_alive(c.process_id):
            if c.lease_id:
                try:
                    release_lease(c.lease_id)
                    tally["orphan_leases_released"] += 1
                except Exception:
                    pass
            c.state = ClaimState.LOST
            c.released_at = _now()
            c.reason = "restart_process_dead"
            tally["claims_marked_lost"] += 1
    return tally
