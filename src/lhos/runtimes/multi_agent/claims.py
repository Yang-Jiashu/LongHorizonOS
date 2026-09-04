"""TaskClaim lifecycle and ownership-linearization guards (Section 15-18).

Key invariants:
- An ACTIVE claim MUST have a live Kernel lease_id (D2-I5).
- Ownership ONLY linearizes when the Kernel exclusive ResourceLease is
  successfully acquired (D2-17).
- A Scheduler-side claim row is NEVER the ownership authority.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from .errors import (
    KernelLeaseRequired,
    LeaseReleaseFailed,
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

        The lease provider is an authority boundary.  A provider response
        carrying a malformed fencing token must never be allowed to
        linearize ownership (or leave an acquired lease orphaned).  In that
        case we fail closed, release the just-acquired lease, and return
        ``False``.  If the release cannot be confirmed, a
        :class:`LeaseReleaseFailed` is raised so callers do not mistake an
        unresolved Kernel lease for an ordinary admission refusal.
        """
        lease = self._adapter.acquire(claim.graph_id, claim.task_id, claim.process_id)
        if lease is None:
            claim.state = ClaimState.REJECTED
            claim.reason = "kernel_exclusive_lease_refused"
            claim.released_at = _now()
            return False

        # Validate and normalize provider data before mutating the claim into
        # ACTIVE.  ``LeaseInfo.fencing_token`` is typed as ``int`` but an
        # injected/remote provider is untrusted at runtime and may return a
        # string, ``None``, or an arbitrary object.
        lease_id = getattr(lease, "lease_id", None)
        if not isinstance(lease_id, str) or not lease_id:
            claim.state = ClaimState.REJECTED
            claim.reason = "kernel_lease_missing_lease_id"
            claim.released_at = _now()
            return False

        owner_pid = getattr(lease, "owner_pid", claim.process_id)
        token = getattr(lease, "fencing_token", None)
        try:
            normalized_token = int(token) if token is not None else None
        except (TypeError, ValueError, OverflowError) as exc:
            # We already acquired a Kernel lease, so clean it up before
            # exposing a terminal rejection to the Scheduler.  Do not call
            # ``_safe_release`` here: the claim has not yet been bound to the
            # lease and the helper intentionally assumes a complete claim.
            try:
                released = self._adapter.release(lease_id)
            except Exception as release_exc:
                claim.lease_id = lease_id
                claim.lease_owner_pid = owner_pid
                claim.lease_expires_at = getattr(lease, "expires_at", None)
                # The Kernel lease is still potentially live.  Keep the
                # projection non-terminal so reconciliation can rediscover
                # and reclaim it; a terminal REJECTED row would be skipped
                # by reconciliation and could leak ownership forever.
                claim.state = ClaimState.ACQUIRING
                claim.released_at = None
                claim.reason = (
                    f"kernel_lease_malformed_fencing_token; release_failed: {release_exc}"
                )
                raise LeaseReleaseFailed(
                    claim.claim_id,
                    lease_id,
                    f"malformed fencing token ({exc}); release failed: {release_exc}",
                ) from release_exc
            if not released:
                claim.lease_id = lease_id
                claim.lease_owner_pid = owner_pid
                claim.lease_expires_at = getattr(lease, "expires_at", None)
                # Leave the claim ACQUIRING for the same reason as above:
                # release was not confirmed, so only reconciliation can
                # safely retry cleanup against the authoritative provider.
                claim.state = ClaimState.ACQUIRING
                claim.released_at = None
                claim.reason = "kernel_lease_malformed_fencing_token; release_unconfirmed"
                raise LeaseReleaseFailed(
                    claim.claim_id,
                    lease_id,
                    f"malformed fencing token ({exc}); lease release unconfirmed",
                ) from exc

            claim.state = ClaimState.REJECTED
            claim.reason = "kernel_lease_malformed_fencing_token"
            claim.released_at = _now()
            return False

        claim.lease_id = lease_id
        claim.lease_owner_pid = owner_pid
        claim.lease_fencing_token = normalized_token
        claim.lease_expires_at = getattr(lease, "expires_at", None)
        claim.state = ClaimState.ACTIVE
        claim.activated_at = _now()
        claim.reason = "kernel_lease_acquired_ownership_linearized"
        return True

    def renew_claim(
        self,
        claim: TaskClaim,
        ttl: timedelta | None = None,
    ) -> Any | None:
        """Renew the authoritative lease backing one ACTIVE claim.

        The Kernel lease remains the ownership authority.  A renewal is
        accepted only when the provider returns the *same* lease identity,
        resource, owner, and fencing generation.  A missing or malformed
        result fails closed and leaves the claim projection unchanged, so the
        caller can stop committing work and let reconciliation classify lease
        loss.
        """
        if claim.state != ClaimState.ACTIVE or not claim.lease_id:
            return None
        lease = self._adapter.renew(claim.lease_id, ttl)
        if lease is None:
            return None

        if getattr(lease, "lease_id", None) != claim.lease_id:
            return None
        resource_id = getattr(lease, "resource_id", None)
        if claim.lease_resource and resource_id != claim.lease_resource:
            return None
        owner_pid = getattr(lease, "owner_pid", None)
        expected_owner = claim.lease_owner_pid or claim.process_id
        if owner_pid is not None and owner_pid != expected_owner:
            return None
        expected_token = claim.lease_fencing_token
        returned_token = getattr(lease, "fencing_token", None)
        normalized_token: int | None = None
        if expected_token is not None:
            try:
                token_matches = returned_token is not None and int(returned_token) == int(
                    expected_token
                )
            except (TypeError, ValueError, OverflowError):
                # Provider output is an untrusted authority boundary.  A
                # malformed generation must never be allowed to renew a
                # claim, nor should it crash the scheduler heartbeat loop.
                token_matches = False
            if not token_matches:
                return None
        elif returned_token is not None:
            # Preserve the generation returned by an authoritative provider
            # for legacy claims that were restored without the token field.
            try:
                normalized_token = int(returned_token)
            except (TypeError, ValueError, OverflowError):
                return None

        expiry = getattr(lease, "expires_at", None)
        if expiry is None:
            return None
        if isinstance(expiry, str):
            try:
                expiry = datetime.fromisoformat(expiry)
            except ValueError:
                return None
        if not isinstance(expiry, datetime):
            return None
        if expiry.tzinfo is None or expiry.utcoffset() is None:
            # Lease timestamps are UTC-aware authority data.  Treat a naive
            # value as malformed rather than silently interpreting it in the
            # host's local timezone.
            return None
        # Commit all projection updates only after the complete authority
        # response has passed validation.  A malformed expiry must not leave a
        # newly discovered fencing token behind on a legacy claim.
        if normalized_token is not None:
            claim.lease_fencing_token = normalized_token
        claim.lease_expires_at = expiry
        claim.reason = "kernel_lease_renewed"
        return lease

    def complete(self, claim: TaskClaim) -> None:
        """Mark claim COMPLETED and release the Kernel lease."""
        self._safe_release(claim)
        claim.state = ClaimState.COMPLETED
        claim.released_at = _now()
        claim.reason = "task_verified_claim_completed"

    def release(self, claim: TaskClaim, reason: str = "released") -> None:
        """Voluntarily release a non-terminal claim."""
        if claim.state == ClaimState.RELEASED:
            return
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
                released = self._adapter.release(claim.lease_id)
            except Exception as exc:
                raise LeaseReleaseFailed(claim.claim_id, claim.lease_id, str(exc)) from exc
            if not released:
                raise LeaseReleaseFailed(
                    claim.claim_id,
                    claim.lease_id,
                    "lease provider did not confirm release",
                )

    # ── invariant checks ───────────────────────────────────────────────────
    def assert_active_has_live_lease(self, claim: TaskClaim) -> None:
        """D2-I5: an ACTIVE claim must have a live Kernel lease."""
        if claim.state != ClaimState.ACTIVE:
            return
        if claim.lease_id is None:
            raise KernelLeaseRequired(claim.claim_id, claim.task_id)

    def current_active_claims(self, agent_id: str) -> int:
        return len(self.active_claims_for_agent(agent_id))
