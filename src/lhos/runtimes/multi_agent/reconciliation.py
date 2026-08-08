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

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .errors import KernelLeaseRequired
from .models import ClaimState, ScheduledExecutionAttempt, TaskClaim


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
    lease_is_live: Any,        # (lease_id) -> bool  (looks up Kernel leases)
    process_is_alive: Any,     # (pid) -> bool       (looks up Kernel process)
    vpg_task_verified: Any,    # (task_id) -> bool   (looks up VPG validity)
    vpg_task_stale: Any,       # (task_id) -> bool   (STale / not dispatchable)
    lease_lookup: Any,         # (claim) -> lease_info | None
    release_lease: Any,        # (lease_id) -> bool
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
                claim, res,
                lease_is_live=lease_is_live,
                process_is_alive=process_is_alive,
                vpg_task_verified=vpg_task_verified,
                vpg_task_stale=vpg_task_stale,
                lease_lookup=lease_lookup,
                release_lease=release_lease,
            )
        elif claim.state in {ClaimState.PROPOSED, ClaimState.ACQUIRING}:
            # A lingering PROPOSED/ACQUIRING claim is a sign of a lost
            # scheduling race.  If the Kernel has no live lease for its
            # resource, it is safe to LOST it.
            leases = lease_lookup(claim)
            if leases is None:
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

    # Reconcile attempts independently of claims.
    for attempt in list(attempts):
        _reconcile_attempt(attempt, res, process_is_alive=process_is_alive)

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
) -> None:
    # 1. Kernel process dead -> claim is LOST.
    if not process_is_alive(claim.process_id):
        _lose_claim(
            claim, res,
            reason="process_dead_claim_lost",
            release_lease=release_lease,
        )
        return

    # 2. Kernel lease gone / expired -> claim is LOST.
    live_leases = lease_lookup(claim)
    if not live_leases or not lease_is_live(live_leases.lease_id):
        # Make sure any lingering lease resource is reclaimed.
        try:
            if claim.lease_id:
                release_lease(claim.lease_id)
                res.orphan_leases_released += 1
        except Exception:
            pass
        _lose_claim(claim, res, reason="kernel_lease_vanished_claim_lost")
        return

    # 3. Task verified by VPG -> claim COMPLETED, lease released.
    if vpg_task_verified(claim.task_id):
        try:
            if claim.lease_id:
                release_lease(claim.lease_id)
                res.orphan_leases_released += 1
        except Exception:
            pass
        claim.state = ClaimState.COMPLETED
        claim.released_at = _now()
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


def _reconcile_attempt(
    attempt: ScheduledExecutionAttempt,
    res: ReconciliationResult,
    *,
    process_is_alive: Any,
) -> None:
    # A DISPATCHED/RUNNING attempt whose owning process is dead is CRASHED.
    if attempt.state.value in {"dispatched", "running"} and not process_is_alive(
        attempt.process_id
    ):
        attempt.state = "crashed"
        attempt.ended_at = _now()
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
) -> None:
    try:
        if release_lease is not None and claim.lease_id:
            release_lease(claim.lease_id)
            res.orphan_leases_released += 1
    except Exception:
        pass
    claim.state = ClaimState.LOST
    claim.released_at = _now()
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


def detect_invariants_violations(
    claims: list[TaskClaim],
    *,
    lease_is_live: Any,
    process_is_alive: Any,
) -> list[str]:
    """Lightweight invariant check returning human-readable violations."""
    violations: list[str] = []
    # D2-I4: at most one ACTIVE claim per task.
    by_task: dict[str, list[TaskClaim]] = {}
    for c in claims:
        if c.state == ClaimState.ACTIVE:
            by_task.setdefault(c.task_id, []).append(c)
    for tid, active in by_task.items():
        if len(active) > 1:
            violations.append(
                f"D2-I4 violation: task {tid!r} has "
                f"{len(active)} ACTIVE claims"
            )

    for c in claims:
        if c.state == ClaimState.ACTIVE:
            # D2-I5: active claim must have a lease_id.
            if c.lease_id is None:
                violations.append(
                    f"D2-I5 violation: ACTIVE claim {c.claim_id} has no lease_id"
                )
                continue
            # D2-I5b: lease must be live.
            if not lease_is_live(c.lease_id):
                violations.append(
                    f"D2-I5 violation: ACTIVE claim {c.claim_id} lease "
                    f"{c.lease_id} not live"
                )
            # D2-I7: owning process must be alive.
            if not process_is_alive(c.process_id):
                violations.append(
                    f"D2-I7 violation: ACTIVE claim {c.claim_id} process "
                    f"{c.process_id} not alive"
                )
    return violations
