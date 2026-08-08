"""TaskClaim lifecycle and ownership-linearization guards (Section 15-18).

Key invariants:
- An ACTIVE claim MUST have a live Kernel lease_id (D2-I5).
- Ownership ONLY linearizes when the Kernel exclusive ResourceLease is
  successfully acquired (D2-17).
- A Scheduler-side claim row is NEVER the ownership authority.
"""

from __future__ import annotations

from datetime import UTC, datetime

from .errors import (
    KernelLeaseRequired,
)
from .lease_adapter import LeaseAdapter
from .models import ClaimState, TaskClaim


def _now() -> datetime:
    return datetime.now(UTC)


class ClaimManager:
    """Owns TaskClaim records and enforces the binding to Kernel leases.

    The ClaimManager owns the *projection* of claim truth.  The Kernel
    owns real resource exclusivity.  Every ACTIVE claim stored here is
    expected to mirror a live Kernel lease; reconcile() fixes drift.
    """

    def __init__(self, lease_adapter: LeaseAdapter) -> None:
        self._adapter = lease_adapter
        self._claims: dict[str, TaskClaim] = {}

    # ── internal booking ───────────────────────────────────────────────────
    def _book(self, claim: TaskClaim) -> TaskClaim:
        self._claims[claim.claim_id] = claim
        return claim

    def get(self, claim_id: str) -> TaskClaim | None:
        return self._claims.get(claim_id)

    def all_claims(self) -> list[TaskClaim]:
        return list(self._claims.values())

    def active_claims_for_task(self, graph_id: str, task_id: str) -> list[TaskClaim]:
        return [
            c
            for c in self._claims.values()
            if c.graph_id == graph_id and c.task_id == task_id and c.state == ClaimState.ACTIVE
        ]

    def active_claims_for_agent(self, agent_id: str) -> list[TaskClaim]:
        return [
            c
            for c in self._claims.values()
            if c.agent_id == agent_id and c.state == ClaimState.ACTIVE
        ]

    # ── lifecycle transitions ──────────────────────────────────────────────
    def propose(
        self,
        *,
        claim_id: str,
        graph_id: str,
        graph_version: int,
        task_id: str,
        agent_id: str,
        process_id: str,
        lease_resource: str,
        attempt_number: int = 0,
    ) -> TaskClaim:
        """Create a PROPOSED claim — no ownership has been linearized yet."""
        claim = TaskClaim(
            claim_id=claim_id,
            graph_id=graph_id,
            graph_version=graph_version,
            task_id=task_id,
            agent_id=agent_id,
            process_id=process_id,
            lease_resource=lease_resource,
            state=ClaimState.PROPOSED,
            attempt_number=attempt_number,
            reason="awaiting_kernel_lease",
        )
        return self._book(claim)

    def mark_acquiring(self, claim: TaskClaim) -> None:
        claim.state = ClaimState.ACQUIRING
        reason = claim.reason or ""
        claim.reason = reason + "; requesting_kernel_lease"

    def try_acquire_lease(self, claim: TaskClaim) -> bool:
        """Attempt to acquire the exclusive Kernel lease for this claim.

        Returns True on success (claim -> ACTIVE), False on refusal
        (claim stays ACQUIRING; caller may retry or release).
        """
        lease = self._adapter.acquire(claim.graph_id, claim.task_id, claim.process_id)
        if lease is None:
            claim.state = ClaimState.REJECTED
            claim.reason = "kernel_exclusive_lease_refused"
            claim.released_at = _now()
            return False
        claim.lease_id = lease.lease_id
        claim.state = ClaimState.ACTIVE
        claim.activated_at = _now()
        claim.reason = "kernel_lease_acquired_ownership_linearized"
        return True

    def complete(self, claim: TaskClaim) -> None:
        """Mark claim COMPLETED and release the Kernel lease."""
        self._safe_release(claim)
        claim.state = ClaimState.COMPLETED
        claim.released_at = _now()
        claim.reason = "task_verified_claim_completed"

    def release(self, claim: TaskClaim, reason: str = "released") -> None:
        """Voluntarily release a non-terminal claim."""
        self._safe_release(claim)
        claim.state = ClaimState.RELEASED
        claim.released_at = _now()
        claim.reason = reason

    def mark_lost(self, claim: TaskClaim, reason: str = "lease_lost") -> None:
        """Drop an ACTIVE claim after the Kernel lease vanished/process died."""
        claim.state = ClaimState.LOST
        claim.released_at = _now()
        claim.reason = reason

    def _safe_release(self, claim: TaskClaim) -> None:
        if claim.lease_id is not None:
            try:
                self._adapter.release(claim.lease_id)
            except Exception:
                claim.lease_id = None

    # ── invariant checks ───────────────────────────────────────────────────
    def assert_active_has_live_lease(self, claim: TaskClaim) -> None:
        """D2-I5: an ACTIVE claim must have a live Kernel lease."""
        if claim.state != ClaimState.ACTIVE:
            return
        if claim.lease_id is None:
            raise KernelLeaseRequired(claim.claim_id, claim.task_id)

    def current_active_claims(self, agent_id: str) -> int:
        return len(self.active_claims_for_agent(agent_id))
