"""Scheduler Reconciliation (Section 27).

Compares four authorities:
    1. VPG semantic state (Task validity + lifecycle) — unmodifiable
    2. Kernel Process state — unmodifiable
    3. Kernel Lease state — unmodifiable
    4. Scheduler projection/events — rebuildable

Reconciliation fixes the projection; it NEVER rewrites VPG semantics,
Kernel process history, or Kernel lease history.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .errors import LeaseReleaseFailed
from .models import AttemptState, ClaimState, ScheduledExecutionAttempt, TaskClaim


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class ReconciliationIssue:
    issue_id: str
    summary: str
    claim_id: str = ""
    task_id: str = ""
    agent_id: str = ""


@dataclass
class ReconciliationResult:
    started_at: datetime = field(default_factory=_now)
    finished_at: datetime | None = None
    issues: list[ReconciliationIssue] = field(default_factory=list)
    claims_marked_lost: int = 0
    claims_completed: int = 0
    orphan_leases_released: int = 0
    projection_repairs: int = 0

    def add(self, issue: ReconciliationIssue) -> None:
        self.issues.append(issue)

    def finish(self) -> None:
        self.finished_at = _now()


def reconcile(
    claims: list[TaskClaim],
    attempts: list[ScheduledExecutionAttempt],
    *,
    # Authoritative lookups (injected; NEVER from the scheduler projection):
    lease_is_live: Any,  # (lease_id) -> bool  (looks up Kernel leases)
    process_is_alive: Any,  # (pid) -> bool       (looks up Kernel process)
    vpg_task_verified: Any,  # (graph_id, task_id) -> bool
    vpg_task_stale: Any,  # (graph_id, task_id) -> bool
    lease_lookup: Any,  # (claim) -> lease_info | None
    release_lease: Any,  # (lease_id) -> bool
    clock_now: Any = _now,
) -> ReconciliationResult:
    """Walk every stored claim and confirm it agrees with authoritative
    Kernel + VPG state.  Returns the list of repairs made.

    This function is the ONLY routine allowed to move an ACTIVE claim
    to LOST / COMPLETED based on authoritative truth.
    """
    res = ReconciliationResult()

    for claim in list(claims):
        if claim.state in {
            ClaimState.RELEASED,
            ClaimState.LOST,
            ClaimState.COMPLETED,
            ClaimState.REJECTED,
        }:
            continue

        if claim.state == ClaimState.ACTIVE:
            _reconcile_active_claim(
                claim,
                res,
                lease_is_live=lease_is_live,
                process_is_alive=process_is_alive,
                vpg_task_verified=vpg_task_verified,
                vpg_task_stale=vpg_task_stale,
                lease_lookup=lease_lookup,
                release_lease=release_lease,
                clock_now=clock_now,
            )
        elif claim.state in {ClaimState.PROPOSED, ClaimState.ACQUIRING}:
            # A lingering PROPOSED/ACQUIRING claim is an interrupted
            # acquisition. If a matching Kernel lease exists, it was acquired
            # before Scheduler activation became durable and must be reclaimed
            # rather than promoted to executable ownership.
            lease = lease_lookup(claim)
            if lease is None:
                claim.state = ClaimState.LOST
                claim.released_at = clock_now()
                claim.reason = "reconciled_orphan_proposed_no_kernel_lease"
                res.add(
                    ReconciliationIssue(
                        issue_id="orphan_proposed_claim",
                        summary="PROPOSED claim without live Kernel lease",
                        claim_id=claim.claim_id,
                        task_id=claim.task_id,
                        agent_id=claim.agent_id,
                    )
                )
                res.claims_marked_lost += 1
            else:
                _bind_claim_lease(claim, lease)
                _lose_claim(
                    claim,
                    res,
                    reason="reconciled_uncommitted_claim_with_kernel_lease",
                    release_lease=release_lease,
                    lease_present=True,
                    clock_now=clock_now,
                )

    # Reconcile attempts independently of claims.
    for attempt in list(attempts):
        _reconcile_attempt(
            attempt,
            res,
            process_is_alive=process_is_alive,
            clock_now=clock_now,
        )

    res.finish()
    return res


def _reconcile_active_claim(
    claim: TaskClaim,
    res: ReconciliationResult,
    *,
    lease_is_live: Any,
    process_is_alive: Any,
    vpg_task_verified: Any,
    vpg_task_stale: Any,
    lease_lookup: Any,
    release_lease: Any,
    clock_now: Any = _now,
) -> None:
    # VPG is the semantic authority.  A verified task completes even when
    # process/lease cleanup races with this reconciliation pass.
    if _invoke_task_predicate(vpg_task_verified, claim):
        lease = lease_lookup(claim)
        if lease is not None:
            # A crash can leave an ACTIVE projection row durable before the
            # lease binding fields were persisted.  If the authoritative
            # lookup recovered a matching lease, bind it before attempting
            # cleanup; otherwise _release_claim_lease would silently no-op
            # because claim.lease_id is still None and leak Kernel ownership.
            _bind_discovered_lease(claim, lease)
            _release_claim_lease(claim, res, release_lease)
        claim.state = ClaimState.COMPLETED
        claim.released_at = clock_now()
        claim.reason = "vpg_task_verified_claim_completed"
        res.claims_completed += 1
        res.add(
            ReconciliationIssue(
                issue_id="task_verified_claim_completed",
                summary="Task verified by VPG; claim completed",
                claim_id=claim.claim_id,
                task_id=claim.task_id,
                agent_id=claim.agent_id,
            )
        )
        return

    # STALE means the work is no longer semantically executable.  Reclaim
    # ownership even if both the process and lease still appear live.
    if _invoke_task_predicate(vpg_task_stale, claim):
        lease = lease_lookup(claim)
        _bind_discovered_lease(claim, lease)
        _lose_claim(
            claim,
            res,
            reason="vpg_task_stale_claim_lost",
            release_lease=release_lease,
            lease_present=lease is not None,
            clock_now=clock_now,
        )
        return

    # Kernel process dead -> claim is LOST.
    if not process_is_alive(claim.process_id):
        lease = lease_lookup(claim)
        _bind_discovered_lease(claim, lease)
        _lose_claim(
            claim,
            res,
            reason="process_dead_claim_lost",
            release_lease=release_lease,
            lease_present=lease is not None,
            clock_now=clock_now,
        )
        return

    # Kernel lease gone / expired -> claim is LOST.
    live_lease = lease_lookup(claim)
    _bind_discovered_lease(claim, live_lease)
    if live_lease is None or not _invoke_lease_live(lease_is_live, live_lease):
        _lose_claim(
            claim,
            res,
            reason="kernel_lease_vanished_claim_lost",
            release_lease=release_lease,
            lease_present=live_lease is not None,
            clock_now=clock_now,
        )
        return


def _invoke_task_predicate(predicate: Any, claim: TaskClaim) -> bool:
    """Support legacy one-arg and modern graph/claim-aware callbacks.

    Signature inspection avoids catching a ``TypeError`` raised by callback
    logic and incorrectly treating it as an arity mismatch.
    """
    candidates = (
        (claim.graph_id, claim.task_id, claim),
        (claim.graph_id, claim.task_id),
        (claim.task_id,),
    )
    try:
        signature = inspect.signature(predicate)
    except (TypeError, ValueError):
        return bool(predicate(claim.graph_id, claim.task_id))
    for args in candidates:
        try:
            signature.bind(*args)
        except TypeError:
            continue
        return bool(predicate(*args))
    raise TypeError(
        "task authority callback must accept (task_id), "
        "(graph_id, task_id), or (graph_id, task_id, claim)"
    )


def _invoke_lease_live(predicate: Any, lease: Any) -> bool:
    """Invoke lease liveness callbacks with an object, with legacy ID support.

    The Scheduler's ``LeaseAdapter`` requires the full lease object so expiry
    can be checked.  Older direct callers supplied ``lease_id`` callbacks;
    retain that narrow compatibility path for callbacks whose parameter is
    explicitly named ``lid``/``lease_id``.
    """
    try:
        signature = inspect.signature(predicate)
        parameters = list(signature.parameters.values())
    except (TypeError, ValueError):
        parameters = []
    if parameters and parameters[0].name in {"lid", "lease_id"}:
        return bool(predicate(getattr(lease, "lease_id", lease)))
    return bool(predicate(lease))


def _reconcile_attempt(
    attempt: ScheduledExecutionAttempt,
    res: ReconciliationResult,
    *,
    process_is_alive: Any,
    clock_now: Any = _now,
) -> None:
    # A DISPATCHED/RUNNING attempt whose owning process is dead is CRASHED.
    if attempt.state.value in {"dispatched", "running"} and not process_is_alive(
        attempt.process_id
    ):
        attempt.state = AttemptState.CRASHED
        attempt.ended_at = clock_now()
        attempt.error = "process_dead_attempt_crashed"
        res.add(
            ReconciliationIssue(
                issue_id="attempt_crashed_dead_process",
                summary="Attempt's process died; marked crashed",
                claim_id=attempt.claim_id,
                task_id=attempt.task_id,
                agent_id=attempt.agent_id,
            )
        )


def _lose_claim(
    claim: TaskClaim,
    res: ReconciliationResult,
    *,
    reason: str,
    release_lease: Any | None = None,
    lease_present: bool = True,
    clock_now: Any = _now,
) -> None:
    # Kernel lease state is authoritative.  A lookup that already returned
    # no matching lease means ownership disappeared before reconciliation;
    # release is then idempotently complete.  Conversely, if a matching lease
    # is still present, a False/exceptional release must fail closed and leave
    # the Claim projection untouched.
    if lease_present and release_lease is not None:
        _release_claim_lease(claim, res, release_lease)
    claim.state = ClaimState.LOST
    claim.released_at = clock_now()
    claim.reason = reason
    res.claims_marked_lost += 1
    res.add(
        ReconciliationIssue(
            issue_id="claim_lost",
            summary=reason,
            claim_id=claim.claim_id,
            task_id=claim.task_id,
            agent_id=claim.agent_id,
        )
    )


def _release_claim_lease(
    claim: TaskClaim,
    res: ReconciliationResult,
    release_lease: Any,
) -> None:
    if not claim.lease_id:
        return
    try:
        released = release_lease(claim.lease_id)
    except Exception as exc:
        raise LeaseReleaseFailed(claim.claim_id, claim.lease_id, str(exc)) from exc
    if not released:
        # A lease was expected to exist and the authority did not confirm
        # release.  Do not advance the claim projection: doing so could free
        # Scheduler-side capacity while Kernel ownership remains live.
        raise LeaseReleaseFailed(
            claim.claim_id,
            claim.lease_id,
            "lease provider did not confirm release",
        )
    res.orphan_leases_released += 1


def _bind_claim_lease(claim: TaskClaim, lease: Any) -> None:
    """Attach a recovered pre-activation lease so cleanup is auditable."""
    claim.lease_id = str(lease.lease_id)
    claim.lease_owner_pid = getattr(lease, "owner_pid", claim.process_id)
    token = getattr(lease, "fencing_token", None)
    claim.lease_fencing_token = int(token) if token is not None else None
    claim.lease_expires_at = getattr(lease, "expires_at", None)


def _bind_discovered_lease(claim: TaskClaim, lease: Any | None) -> None:
    """Bind a lease found for a claim whose durable row lost its lease id.

    ``lease_lookup`` is authoritative and is expected to return a lease
    matching the claim's graph/task (or owner process for legacy rows).  Never
    overwrite a non-null claim lease id here: a provider implementation that
    violates that matching contract must not silently retarget ownership.
    """
    if lease is not None and claim.lease_id is None:
        _bind_claim_lease(claim, lease)


def detect_invariants_violations(
    claims: list[TaskClaim],
    *,
    lease_is_live: Any,
    process_is_alive: Any,
    lease_lookup: Any | None = None,
) -> list[str]:
    """Lightweight invariant check returning human-readable violations.

    ``LeaseAdapter.is_lease_active`` consumes a full lease object so it can
    evaluate expiry.  Older callers passed an ID-only callback instead.  If
    ``lease_lookup`` is supplied, this checker resolves the authoritative
    object before invoking the liveness callback; otherwise it preserves the
    legacy ID path.
    """
    violations: list[str] = []
    # D2-I4: at most one ACTIVE claim per task.
    by_task: dict[str, list[TaskClaim]] = {}
    for c in claims:
        if c.state == ClaimState.ACTIVE:
            by_task.setdefault(c.task_id, []).append(c)
    for tid, active in by_task.items():
        if len(active) > 1:
            violations.append(f"D2-I4 violation: task {tid!r} has {len(active)} ACTIVE claims")

    for c in claims:
        if c.state == ClaimState.ACTIVE:
            # D2-I5: active claim must have a lease_id.
            if c.lease_id is None:
                violations.append(f"D2-I5 violation: ACTIVE claim {c.claim_id} has no lease_id")
                continue
            # D2-I5b: lease must be live.  A lookup result of ``None`` is
            # authoritative lease disappearance, not a provider-release
            # failure.
            lease = lease_lookup(c) if lease_lookup is not None else c.lease_id
            if lease is None or not _invoke_lease_live(lease_is_live, lease):
                violations.append(
                    f"D2-I5 violation: ACTIVE claim {c.claim_id} lease {c.lease_id} not live"
                )
            # D2-I7: owning process must be alive.
            if not process_is_alive(c.process_id):
                violations.append(
                    f"D2-I7 violation: ACTIVE claim {c.claim_id} process {c.process_id} not alive"
                )
    return violations
