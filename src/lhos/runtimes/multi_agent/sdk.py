"""Scheduler public SDK facade.

The Agent-facing entrypoint wraps the core MultiAgentScheduler and adds:
  - automatic observe_vpg + reconcile pass after every schedule (so callers
    do not have to wire it themselves)
  - typed Event query surface
  - projection snapshot + rebuild hook for tests and demos
"""

from __future__ import annotations

from typing import Any

from .handoff import (
    OwnershipHandoffIntent,
    OwnershipHandoffResult,
)
from .models import AgentSnapshot, ResourceVector
from .scheduler import MultiAgentScheduler, ScheduleResult


class SchedulerSession:
    """Typed public facade the Demo/test layer talks to."""

    def __init__(self, scheduler: MultiAgentScheduler) -> None:
        self._s = scheduler

    # ── scheduling ─────────────────────────────────────────────────────────
    def schedule_once(self, graph_id: str, **kwargs: Any) -> ScheduleResult:
        return self._s.schedule_once(graph_id, **kwargs)

    def schedule_until_idle(self, graph_id: str, **kwargs: Any) -> list[ScheduleResult]:
        return self._s.schedule_until_idle(graph_id, **kwargs)

    # ── state observation ─────────────────────────────────────────────────
    @property
    def claims(self) -> list[Any]:
        return self._s.claims

    @property
    def attempts(self) -> list[Any]:
        return self._s.attempts

    @property
    def match_log(self) -> list[Any]:
        return self._s.match_log

    @property
    def resource_manager(self) -> Any:
        """Expose the scheduler's logical resource accounting projection.

        The scheduler core owns admission accounting, while this facade is the
        public object returned by :func:`create_scheduler`.  Keeping this
        narrow read/query surface available is useful for reconciliation and
        diagnostics without making the facade a second resource authority.
        """
        return self._s.resource_manager

    def active_claim_for_task(self, task_id: str, graph_id: str | None = None) -> Any | None:
        return self._s.get_claim(task_id, graph_id)

    def attempt_for_claim(self, claim_id: str) -> Any | None:
        return self._s.get_attempt_for_claim(claim_id)

    def bind_attempt_provenance(self, claim_id: str, digest: str) -> bool:
        """Seal the canonical provenance digest for one active attempt."""
        return self._s.bind_attempt_provenance(claim_id, digest)

    def bind_attempt_context_snapshot(
        self,
        claim_id: str,
        *,
        snapshot_id: str,
        manifest_id: str,
        manifest_hash: str,
        working_set_hash: str,
        materialized_hash: str,
    ) -> bool:
        """Seal the exact Context VM identity for one active attempt."""
        return self._s.bind_attempt_context_snapshot(
            claim_id,
            snapshot_id=snapshot_id,
            manifest_id=manifest_id,
            manifest_hash=manifest_hash,
            working_set_hash=working_set_hash,
            materialized_hash=materialized_hash,
        )

    def bind_agent_snapshot(self, claim_id: str, snapshot: AgentSnapshot) -> bool:
        """Bind or monotonically update runtime cognition for one Attempt."""
        return self._s.bind_agent_snapshot(claim_id, snapshot)

    def mark_stale_cognition(
        self,
        claim_id: str,
        reason: str = "stale_cognition",
    ) -> bool:
        """Quarantine an Attempt whose read-set/cognition is obsolete."""
        return self._s.mark_stale_cognition(claim_id, reason)

    def mark_execution_started(self, claim: Any | str) -> Any | None:
        """Promote the claim's attempt to RUNNING."""
        return self._s.mark_execution_started(claim)

    def mark_execution_operationally_succeeded(self, claim: Any | str) -> Any | None:
        """Record executor success without asserting semantic verification."""
        return self._s.mark_execution_operationally_succeeded(claim)

    def renew_claim(
        self,
        claim_id: str,
        ttl: Any | None = None,
        *,
        expected_attempt_id: str | None = None,
        expected_semantic_epoch: int | None = None,
    ) -> bool:
        """Cooperative heartbeat for one active claim lease."""
        return self._s.renew_claim(
            claim_id,
            ttl,
            expected_attempt_id=expected_attempt_id,
            expected_semantic_epoch=expected_semantic_epoch,
        )

    def heartbeat(
        self,
        claim_id: str,
        ttl: Any | None = None,
        *,
        expected_attempt_id: str | None = None,
        expected_semantic_epoch: int | None = None,
    ) -> bool:
        """Alias for :meth:`renew_claim` used by worker integrations."""
        return self.renew_claim(
            claim_id,
            ttl,
            expected_attempt_id=expected_attempt_id,
            expected_semantic_epoch=expected_semantic_epoch,
        )

    def handoff_task(self, graph_id: str, task_id: str, **kwargs: Any) -> Any:
        """Run a bounded exact-claim PREEMPT/REBASE handoff."""
        return self._s.handoff_task(graph_id, task_id, **kwargs)

    def prepare_handoff(self, graph_id: str, task_id: str, **kwargs: Any) -> OwnershipHandoffResult:
        """Durably prepare a bounded ownership-handoff intent."""
        return self._s.prepare_handoff(graph_id, task_id, **kwargs)

    def commit_handoff(
        self,
        intent: OwnershipHandoffIntent | OwnershipHandoffResult | dict[str, Any],
    ) -> OwnershipHandoffResult:
        """Commit a previously prepared handoff through fenced Scheduler gates."""
        if isinstance(intent, dict):
            intent = OwnershipHandoffIntent.model_validate(intent)
        return self._s.commit_handoff(intent)

    def recover_handoff(self, handoff_id: str) -> OwnershipHandoffResult:
        """Recover a durable handoff intent without guessing ownership."""
        return self._s.recover_handoff(handoff_id)

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
        expected_claim_id: str | None = None,
    ) -> bool:
        """Release operational ownership without changing VPG semantics."""
        return self._s.release_task(
            graph_id,
            task_id,
            reason=reason,
            retry=retry,
            expected_claim_id=expected_claim_id,
        )

    def set_resource_capacity(self, pool_id: str, capacity: ResourceVector) -> None:
        self._s.set_resource_capacity(pool_id, capacity)

    def update_registered_resource_capacity(
        self,
        pool_id: str,
        capacity: ResourceVector,
    ) -> ResourceVector:
        """Atomically update Registry + logical allocator for one pool."""

        return self._s.update_registered_resource_capacity(pool_id, capacity)

    def refresh_registry_resources(self) -> None:
        self._s.refresh_registry_resources()

    def retire_agent_process(self, agent_id: str, process_id: str) -> int:
        return self._s.retire_agent_process(agent_id, process_id)

    def close(self) -> None:
        self._s.close()

    # ── reconcile ──────────────────────────────────────────────────────────
    def reconcile(self) -> Any:
        return self._s.reconcile()

    def observe_vpg(self, graph_id: str) -> dict[str, int]:
        return self._s.observe_vpg(graph_id)

    @staticmethod
    def _add_cleanup_note(root_exc: BaseException, message: str) -> None:
        """Attach bounded cleanup diagnostics without replacing ``root_exc``."""

        add_note = getattr(root_exc, "add_note", None)
        if callable(add_note):
            add_note(message[:2048])

    def _record_failed_exact_cleanup(
        self,
        *,
        graph_id: str,
        task_id: str,
        claim_id: str,
        reason: str,
        cleanup_exc: BaseException,
        root_exc: BaseException | None = None,
    ) -> str | None:
        """Persist one failed exact-Claim compensation, best effort.

        Immutable Claim identity is used instead of the task's current owner,
        so a replacement Claim can never be attributed to an older cleanup
        attempt. Marker persistence is secondary recovery work: if it fails,
        the bounded diagnostic is attached to the original exception rather
        than replacing it.
        """

        claim = next(
            (item for item in self._s.claims if item.claim_id == claim_id),
            None,
        )
        attempt = self._s.get_attempt_for_claim(claim_id)
        attempt_id = str(getattr(attempt, "attempt_id", "") or "")
        lease_id = str(getattr(claim, "lease_id", "") or "")
        cleanup_error = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
        try:
            event = self._s.record_cleanup_required(
                graph_id=graph_id,
                task_id=task_id,
                claim_id=claim_id,
                attempt_id=attempt_id,
                lease_id=lease_id,
                reason=reason,
                error=cleanup_error,
            )
        except BaseException as marker_exc:
            diagnostic = (
                "durable cleanup marker write failed for exact "
                f"claim_id={claim_id} task_id={task_id}: "
                f"{type(marker_exc).__name__}: {marker_exc}"
            )
            if root_exc is not None:
                self._add_cleanup_note(root_exc, diagnostic)
            return diagnostic
        return str((getattr(event, "metadata", {}) or {}).get("marker_id", "")) or None

    def run_pass(self, graph_id: str, **kwargs: Any) -> ScheduleResult:
        """schedule_once + observe_vpg + reconcile — a coherent full step.

        ``allowed_task_ids`` may be supplied by an opt-in WHAT/WHEN policy.
        It only filters candidates before the normal Scheduler eligibility,
        resource, Claim, and Lease checks; omitting it preserves legacy
        behavior.  ``expected_graph_version`` is an optional fail-closed
        freshness fence for policy-driven callers.  If the VPG has advanced
        before admission, the returned ``ScheduleResult`` is marked
        ``policy_stale`` and no reconcile side effect is attempted.
        """
        res = self._s.schedule_once(graph_id, **kwargs)
        if getattr(res, "policy_stale", False):
            return res
        expected_version = kwargs.get("expected_graph_version")
        if expected_version is not None:
            observed_version = int(self._s._vpg.current_graph_version(graph_id))
            if observed_version != expected_version:
                # The graph may advance after ``schedule_once`` acquires a
                # claim.  Do not hand that ownership to a worker under a
                # superseded policy snapshot; release only exact identities.
                policy_cleanup_errors: list[str] = []
                for dispatch in tuple(res.dispatched):
                    task_id = str(dispatch.get("task_id", "")).strip()
                    claim_id = str(dispatch.get("claim_id", "")).strip()
                    if not task_id or not claim_id:
                        continue
                    try:
                        self._s.release_task(
                            graph_id,
                            task_id,
                            reason="run_pass_policy_graph_changed",
                            retry=True,
                            expected_claim_id=claim_id,
                        )
                    except BaseException as exc:
                        policy_cleanup_errors.append(f"{claim_id}:{type(exc).__name__}: {exc}")
                        marker_result = self._record_failed_exact_cleanup(
                            graph_id=graph_id,
                            task_id=task_id,
                            claim_id=claim_id,
                            reason="run_pass_policy_graph_changed",
                            cleanup_exc=exc,
                        )
                        if marker_result is not None:
                            policy_cleanup_errors.append(
                                f"{claim_id}:marker_id={marker_result}"
                                if not marker_result.startswith(
                                    "durable cleanup marker write failed"
                                )
                                else marker_result
                            )
                res.dispatched.clear()
                res.idle = True
                res.policy_stale = True
                res.expected_graph_version = int(expected_version)
                res.observed_graph_version = observed_version
                res.policy_stale_reason = (
                    "adaptive policy graph version changed during admission: "
                    f"planned={expected_version}, observed={observed_version}"
                )
                # These diagnostic attributes are intentionally dynamic to
                # preserve the small legacy ScheduleResult surface.
                res.policy_cleanup_required = bool(policy_cleanup_errors)
                res.policy_cleanup_errors = tuple(policy_cleanup_errors)
                return res
        try:
            self._s.observe_vpg(graph_id)
            self._s.reconcile()
        except BaseException as root_exc:
            # ``schedule_once`` has already made each returned Claim/Lease
            # authoritative. If post-admission observation or reconciliation
            # fails, the caller never receives those dispatches and therefore
            # cannot execute or clean them up. Compensate only the exact
            # identities created by this pass; the claim fence prevents stale
            # cleanup from releasing a replacement owner.
            cleanup_errors: list[str] = []
            for dispatch in res.dispatched:
                task_id = str(dispatch.get("task_id", "")).strip()
                claim_id = str(dispatch.get("claim_id", "")).strip()
                if not task_id or not claim_id:
                    continue
                try:
                    self._s.release_task(
                        graph_id,
                        task_id,
                        reason="run_pass_post_admission_failed",
                        retry=True,
                        expected_claim_id=claim_id,
                    )
                except BaseException as cleanup_exc:
                    marker_result = self._record_failed_exact_cleanup(
                        graph_id=graph_id,
                        task_id=task_id,
                        claim_id=claim_id,
                        reason="run_pass_post_admission_failed",
                        cleanup_exc=cleanup_exc,
                        root_exc=root_exc,
                    )
                    cleanup_errors.append(
                        f"claim_id={claim_id} task_id={task_id}: "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                        + (
                            f" marker_id={marker_result}"
                            if marker_result is not None
                            and not marker_result.startswith("durable cleanup marker write failed")
                            else ""
                        )
                    )
            if cleanup_errors:
                note = "run_pass exact-claim compensation was incomplete; " + " | ".join(
                    cleanup_errors
                )
                self._add_cleanup_note(root_exc, note)
            raise
        return res

    # ── events / projection ───────────────────────────────────────────────
    @property
    def events(self) -> list[Any]:
        return list(self._s._events)

    def record_event(self, event: Any) -> Any:
        """Append one already-validated scheduler audit event.

        The core Scheduler remains the only owner of durable event ordering
        and projection transactions.  This hook is intentionally journal-only:
        callers cannot use it to claim work or mutate execution ownership.
        Only semantic-interrupt and Harness-control events are accepted.
        """
        return self._s.record_event(event)

    def record_interrupt_acknowledgement(self, **kwargs: Any) -> Any:
        """Persist bounded cooperative-interrupt acknowledgement metadata."""

        return self._s.record_interrupt_acknowledgement(**kwargs)

    def record_scheduling_epoch(self, **kwargs: Any) -> Any:
        """Persist one adaptive scheduling-epoch audit event.

        Scheduling-epoch records are journal-only proposals: they do not
        acquire/release claims or leases and do not mutate VPG state.  The
        facade forwards to the core scheduler so AgentOS can use the same
        durable ordering and idempotency guarantees as other audit events.
        """

        return self._s.record_scheduling_epoch(**kwargs)

    # ── durable cleanup markers ─────────────────────────────────────────
    @property
    def cleanup_markers(self) -> list[dict[str, Any]]:
        """Return unresolved cancellation/cleanup markers.

        Markers are a read-only projection of the Scheduler journal.  Reading
        them never releases a Claim or Lease; recovery code must call
        :meth:`reconcile_cleanup_markers` after checking authoritative
        ownership.
        """

        return self._s.cleanup_markers

    def record_cleanup_required(self, **kwargs: Any) -> Any:
        """Durably record that one exact Claim cleanup still needs repair."""

        return self._s.record_cleanup_required(**kwargs)

    def record_cleanup_resolution(self, **kwargs: Any) -> Any:
        """Durably close one cleanup marker after ownership is reconciled."""

        return self._s.record_cleanup_resolution(**kwargs)

    def reconcile_cleanup_markers(self) -> list[dict[str, Any]]:
        """Resolve only markers whose exact Claim has no remaining Lease."""

        return self._s.reconcile_cleanup_markers()

    def projection_snapshot(self) -> dict[str, Any]:
        return {
            "claims": [c.model_dump() for c in self._s.claims],
            "attempts": [a.model_dump() for a in self._s.attempts],
            "match_log": [m.model_dump() for m in self._s.match_log],
        }


# ── convenience factory ──────────────────────────────────────────────────────
def create_scheduler(
    registry: Any,
    *,
    vpg: Any,
    process_provider: Any,
    lease_provider: Any,
    capability_provider: Any | None = None,
    **kwargs: Any,
) -> SchedulerSession:
    core = MultiAgentScheduler(
        registry,
        vpg=vpg,
        process_provider=process_provider,
        lease_provider=lease_provider,
        capability_provider=capability_provider,
        **kwargs,
    )
    return SchedulerSession(core)
