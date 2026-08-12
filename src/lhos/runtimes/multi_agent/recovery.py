"""Scheduler Recovery — restart-time reconstruction of Scheduler state from
authoritative Kernel + VPG + event history (Section 29, projection rebuild).

Projection rebuild MUST be deterministic: identical inputs yield byte-
identical normalized projection state across repeated rebuilds.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from .errors import LeaseReleaseFailed
from .models import ClaimState
from .projections import SchedulerProjection


def _now() -> datetime:
    return datetime.now(UTC)


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
            c.claim_id,
            c.graph_id,
            c.task_id,
            c.agent_id,
            c.process_id,
            c.state.value,
            c.lease_id or "",
        )
        for c in proj.claims.values()
    )
    attempts = sorted(
        (a.attempt_id, a.task_id, a.agent_id, a.state.value) for a in proj.attempts.values()
    )
    loads = sorted(
        (load.agent_id, load.active_claims, load.max_concurrency) for load in proj.loads.values()
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
    lease_lookup: Any | None = None,
) -> dict[str, int]:
    """Post-restart clean-up: mark ACTIVE claims whose authoritative backing
    is gone, and ensure orphan leases are released.

    ``lease_is_live`` answers whether the lease has not expired.  When the
    optional ``lease_lookup`` callback is supplied, it is used to distinguish
    a lease that is genuinely absent (``None``) from a lease that still exists
    but is expired.  An absent lease is already cleaned up and is therefore
    handled idempotently without calling ``release_lease``.  If a lease is
    present, a release that returns ``False`` or raises is treated as an
    authority failure and raises :class:`LeaseReleaseFailed`; the claim stays
    ``ACTIVE`` (fail-closed) rather than freeing Scheduler capacity.

    For backwards compatibility, callers that do not provide ``lease_lookup``
    are conservative: a lease id is treated as potentially owned, so a
    failed release is never silently ignored.
    """
    tally: dict[str, int] = {"claims_marked_lost": 0, "orphan_leases_released": 0}
    for c in list(claims):
        if c.state != ClaimState.ACTIVE:
            continue

        # An ACTIVE claim without a lease cannot safely retain ownership after
        # restart.  There is no provider call to make, so this transition is
        # deterministic and leak-free.
        if not c.lease_id:
            c.state = ClaimState.LOST
            c.released_at = _now()
            c.reason = "restart_active_without_lease"
            tally["claims_marked_lost"] += 1
            continue

        # A lookup, when available, is authoritative for existence.  Without
        # one we conservatively assume the lease may still exist and require a
        # confirmed release from the provider.
        lease_present = True
        if lease_lookup is not None:
            lease_present = lease_lookup(c) is not None

        if not lease_is_live(c.lease_id):
            _release_if_present(
                c,
                release_lease=release_lease,
                lease_present=lease_present,
                tally=tally,
            )
            c.state = ClaimState.LOST
            c.released_at = _now()
            c.reason = "restart_no_live_lease"
            tally["claims_marked_lost"] += 1
            continue

        if not process_is_alive(c.process_id):
            _release_if_present(
                c,
                release_lease=release_lease,
                lease_present=lease_present,
                tally=tally,
            )
            c.state = ClaimState.LOST
            c.released_at = _now()
            c.reason = "restart_process_dead"
            tally["claims_marked_lost"] += 1
    return tally


def _release_if_present(
    claim: Any,
    *,
    release_lease: Any,
    lease_present: bool,
    tally: dict[str, int],
) -> None:
    """Release a claim's lease, failing closed when ownership is uncertain."""
    if not lease_present:
        # The authoritative lookup already confirmed disappearance.  Calling
        # a provider with the stale id can return False and would turn an
        # idempotent cleanup race into a spurious recovery failure.
        return
    try:
        released = release_lease(claim.lease_id)
    except Exception as exc:
        raise LeaseReleaseFailed(claim.claim_id, claim.lease_id, str(exc)) from exc
    if not released:
        raise LeaseReleaseFailed(
            claim.claim_id,
            claim.lease_id,
            "lease provider did not confirm release",
        )
    tally["orphan_leases_released"] += 1
