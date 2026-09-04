"""Multi-Agent Scheduler — central orchestrator.

Implements:
    - claim-acquisition protocol (Section 18) with GraphVersion race
      re-check (Section 19)
    - schedule_once / schedule_until_idle (Sections 31, 32)
    - projection rebuild + event journal persistence
    - high-level claim lifecycle: PROPOSED -> ACQUIRING -> ACTIVE ->
      COMPLETED / LOST / RELEASED.

Scheduler owns NO resource authority — the Kernel Lease does.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterable, Mapping
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast

from .attempts import AttemptManager
from .durable_state import SchedulerStateCorruption, SchedulerStateStore
from .eligibility import evaluate_eligibility
from .errors import LeaseReleaseFailed
from .handoff import (
    OwnershipHandoffIntent,
    OwnershipHandoffPhase,
    OwnershipHandoffResult,
    OwnershipHandoffStatus,
)
from .lease_adapter import DEFAULT_CLAIM_TTL, claim_resource_uri
from .matching import match_deterministic_best_fit_v1
from .models import (
    TERMINAL_CLAIM_STATES,
    AgentSnapshot,
    ClaimHandoffResult,
    ClaimHandoffStatus,
    ClaimState,
    EligibilityResult,
    MatchDecision,
    ResourceVector,
    ScheduledExecutionAttempt,
    TaskClaim,
    TaskRequirements,
)
from .projections import (
    active_claim_count_by_agent,
)
from .reconciliation import ReconciliationResult
from .requirements import decode_task_requirements
from .resources import AtomicResourceManager, ResourceReservation


def _now() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


# SchedulingEpoch audit metadata is intentionally bounded.  A policy may
# inspect a very large frontier, but persisting every candidate id on every
# epoch would turn the journal into an accidental O(E*V) storage path (and
# make recovery/replay increasingly expensive).  The immutable decision hash
# still identifies the complete policy output; the journal stores only a
# deterministic prefix of the sorted ids for diagnostics.
_MAX_EPOCH_TASK_IDS = 256
_MAX_EPOCH_TASK_ID_LENGTH = 160
_MAX_CLEANUP_TEXT_LENGTH = 512
_MAX_CLEANUP_ID_LENGTH = 160


class ScheduleResult:
    """Outcome of a single schedule pass."""

    def __init__(self) -> None:
        self.dispatched: list[dict[str, Any]] = []
        self.skipped: list[tuple[str, str]] = []  # (task_id, reason)
        self.idle: bool = True
        # Optional control-plane freshness diagnostics.  These fields are
        # additive and deliberately not part of the legacy dispatch payload:
        # an adaptive policy can fail closed when the graph snapshot it was
        # planned against is already superseded, without fabricating a task
        # skip record or acquiring a claim from a newer graph.
        self.policy_stale: bool = False
        self.expected_graph_version: int | None = None
        self.observed_graph_version: int | None = None
        self.policy_stale_reason: str = ""
        self.policy_cleanup_required: bool = False
        self.policy_cleanup_errors: tuple[str, ...] = ()
        # Ordering/locality audit.  ``dispatch_order_applied`` records the
        # ranking an upstream policy asked for, so a pass that dispatched
        # low-value work can be distinguished from one that was never given a
        # ranking.  ``locality_matched`` records the tasks whose selected agent
        # actually held resident reads.
        self.dispatch_order_applied: tuple[str, ...] = ()
        self.locality_matched: tuple[str, ...] = ()

    def mark_dispatched(self, task_id: str, agent_id: str, claim_id: str) -> None:
        self.dispatched.append({"task_id": task_id, "agent_id": agent_id, "claim_id": claim_id})
        self.idle = False


class MultiAgentScheduler:
    """Top-level Scheduler entrypoint.

    Constructed with the AgentRegistry and with injected providers for
    Kernel process/lease/capability authority and the VPG runtime adapter.

    ``vpg`` exposes two required public capabilities:
        vpg.current_graph_version(graph_id) -> int
        vpg.ready_frontier(graph_id)        -> list[TaskDispatchCandidate]
        vpg.task_node_payload(graph_id, task_id) -> dict | None
        vpg.task_validity(graph_id, task_id) -> str | None
    Kernel providers mirror the real Agent OS services but are injected
    so that the Scheduler package never imports the Kernel internals.
    """

    def __init__(
        self,
        registry: Any,
        *,
        vpg: Any,
        process_provider: Any,
        lease_provider: Any,
        capability_provider: Any | None = None,
        lease_ttl: timedelta = DEFAULT_CLAIM_TTL,
        clock: Any = _now,
        idempotent_keys: set[str] | None = None,
        state_store: SchedulerStateStore | None = None,
        state_path: str | None = None,
        resource_manager: AtomicResourceManager | None = None,
    ) -> None:
        from .claims import ClaimManager
        from .lease_adapter import LeaseAdapter

        self._registry = registry
        self._vpg = vpg
        self._process = process_provider
        self._leases = LeaseAdapter(lease_provider, ttl=lease_ttl)
        self._cap = capability_provider
        self._clock = clock
        self._schedule_lock = threading.RLock()
        self._resource_manager_uses_registry = resource_manager is None
        self._resource_manager = resource_manager or AtomicResourceManager()
        self._pending_durable_reservations: list[ResourceReservation] | None = None
        if state_store is not None and state_path is not None:
            raise ValueError("pass either state_store or state_path, not both")
        self._state_store = state_store or (
            SchedulerStateStore(state_path) if state_path is not None else None
        )
        durable = self._state_store.load() if self._state_store is not None else None
        self._idempotent_keys: set[str] = set(
            durable.idempotent_keys if durable is not None else (idempotent_keys or set())
        )

        self._claims_ = ClaimManager(self._leases)
        self._attempts_ = AttemptManager()
        self._claims: list[TaskClaim] = durable.claims if durable is not None else []
        self._attempts: list[ScheduledExecutionAttempt] = (
            durable.attempts if durable is not None else []
        )
        self._match_log: list[MatchDecision] = durable.match_log if durable is not None else []
        # Write-time inverted index: graph_id -> agent_id -> resident read keys.
        # Built lazily on first read (so a journal-recovered Scheduler is correct
        # without replaying binds) and updated incrementally thereafter.
        self._resident_index: dict[str, dict[str, set[str]]] = {}
        self._events: list[Any] = durable.events if durable is not None else []
        # Event ids are part of the durable journal identity.  Keep an
        # in-memory index so idempotent proposal retries do not turn every
        # lifecycle append into an O(E) scan over the complete history.
        self._events_by_id: dict[str, Any] = {}
        for event in self._events:
            event_id = str(getattr(event, "event_id", "") or "")
            if not event_id:
                raise SchedulerStateCorruption("scheduler event has an empty event id")
            if event_id in self._events_by_id:
                raise SchedulerStateCorruption(f"duplicate scheduler event id {event_id!r}")
            self._events_by_id[event_id] = event
        # ClaimManager/AttemptManager are mutable projections used by the
        # lifecycle helpers. Restore their indexes alongside the public lists.
        self._claims_._claims = {claim.claim_id: claim for claim in self._claims}
        self._attempts_._attempts = {attempt.attempt_id: attempt for attempt in self._attempts}
        self._sync_registry_resource_capacities()
        if durable is not None:
            self._restore_active_resource_reservations()

    def _sync_registry_resource_capacities(self) -> None:
        if not self._resource_manager_uses_registry:
            return
        agents = self._registry.list()
        for agent in agents:
            self._resource_manager.set_capacity(agent.agent_id, agent.resource_capacity)
        self._try_restore_pending_resources(
            known_agent_ids={agent.agent_id for agent in agents},
            require_complete=False,
        )

    def _restore_active_resource_reservations(self) -> None:
        reservations: list[ResourceReservation] = []
        seen_reservation_ids: set[str] = set()
        for claim in self._claims:
            if claim.state in TERMINAL_CLAIM_STATES:
                continue
            if claim.resource_reservation_id is None:
                if claim.reserved_resources.is_zero:
                    continue
                raise SchedulerStateCorruption(
                    f"active claim {claim.claim_id!r} has resources but no reservation id"
                )
            if claim.resource_reservation_id in seen_reservation_ids:
                raise SchedulerStateCorruption(
                    f"duplicate durable resource reservation id {claim.resource_reservation_id!r}"
                )
            seen_reservation_ids.add(claim.resource_reservation_id)
            reservations.append(
                ResourceReservation(
                    reservation_id=claim.resource_reservation_id,
                    pool_id=claim.agent_id,
                    owner_id=claim.claim_id,
                    resources=claim.reserved_resources,
                    created_at=claim.activated_at or claim.created_at,
                )
            )
        known_agent_ids = {agent.agent_id for agent in self._registry.list()}
        if self._resource_manager_uses_registry and any(
            reservation.pool_id not in known_agent_ids for reservation in reservations
        ):
            # AgentOS constructs the scheduler before user Agents are
            # registered.  Keep the verified durable projection in memory but
            # defer resource accounting until those capacities are known.
            self._pending_durable_reservations = reservations
            return
        self._restore_reservations(reservations)

    def _try_restore_pending_resources(
        self,
        *,
        known_agent_ids: set[str],
        require_complete: bool,
    ) -> None:
        pending = self._pending_durable_reservations
        if pending is None:
            return
        missing = sorted(
            {
                reservation.pool_id
                for reservation in pending
                if reservation.pool_id not in known_agent_ids
            }
        )
        if missing:
            if require_complete:
                raise SchedulerStateCorruption(
                    "durable resource reservations reference unregistered agent pools: "
                    + ", ".join(missing)
                )
            return
        self._restore_reservations(pending)
        self._pending_durable_reservations = None

    def _restore_reservations(self, reservations: list[ResourceReservation]) -> None:
        try:
            self._resource_manager.restore(reservations)
        except ValueError as exc:
            raise SchedulerStateCorruption(
                "durable active resource reservations are invalid"
            ) from exc

    def refresh_registry_resources(self) -> None:
        """Refresh registry-declared capacities and restore deferred state."""
        with self._schedule_lock:
            self._sync_registry_resource_capacities()

    def retire_agent_process(self, agent_id: str, process_id: str) -> int:
        """Fence claims belonging to a previous process for an agent id.

        Kernel process records are durable logical state, so a fresh AgentOS
        instance must not accidentally treat an old claim as owned by the new
        worker process.  Claims are marked LOST only after any still-present
        Kernel lease is confirmed released.
        """
        from .events import SchedulerEventType, record_event

        with self._schedule_lock:
            retired = 0
            for claim in list(self._claims):
                if (
                    claim.agent_id != agent_id
                    or claim.process_id == process_id
                    or claim.state in TERMINAL_CLAIM_STATES
                ):
                    continue
                lease = self._lease_lookup_for_claim(claim)
                if lease is not None:
                    # A torn durable row may have lost lease_id even though
                    # the authoritative Kernel lease is still present.  Bind
                    # the discovered lease before fencing so old ownership
                    # cannot leak across process replacement.
                    if claim.lease_id is None:
                        from .reconciliation import _bind_claim_lease

                        _bind_claim_lease(claim, lease)
                    lease_id = claim.lease_id
                    if lease_id is None:
                        raise SchedulerStateCorruption(
                            f"authoritative lease lookup returned an unbindable lease "
                            f"for claim {claim.claim_id!r}"
                        )
                    released = self._leases.release(lease_id)
                    if not released:
                        from .errors import LeaseReleaseFailed

                        raise LeaseReleaseFailed(
                            claim.claim_id,
                            lease_id,
                            "previous agent process lease release was not confirmed",
                        )
                self._claims_.mark_lost(claim, reason="agent_process_replaced")
                self._release_claim_resources(claim)
                self._clear_task_idempotency(claim.graph_id, claim.task_id)
                self._record_event(
                    record_event(
                        SchedulerEventType.CLAIM_LOST,
                        graph_id=claim.graph_id,
                        task_id=claim.task_id,
                        agent_id=claim.agent_id,
                        claim_id=claim.claim_id,
                        graph_version=claim.graph_version,
                        reason="agent_process_replaced",
                    )
                )
                retired += 1
            if retired:
                self._persist_state()
            return retired

    def close(self) -> None:
        with self._schedule_lock:
            store, self._state_store = self._state_store, None
            if store is not None:
                store.close()

    def _release_claim_resources(self, claim: TaskClaim) -> None:
        reservation_id = claim.resource_reservation_id
        if reservation_id is not None:
            if self._pending_durable_reservations is not None:
                self._pending_durable_reservations = [
                    reservation
                    for reservation in self._pending_durable_reservations
                    if reservation.reservation_id != reservation_id
                ]
            self._resource_manager.release(reservation_id)

    def _record_event(self, event: Any) -> Any:
        """Append an event to memory and, when configured, durably publish it.

        The snapshot is written in the same SQLite transaction as the event.
        The in-memory append happens only after durable publication succeeds,
        so a storage error never falsely advertises an event as committed.
        """
        existing = self._events_by_id.get(event.event_id)
        if existing is not None:
            if existing.model_dump(mode="json") != event.model_dump(mode="json"):
                raise ValueError(f"conflicting scheduler event id {event.event_id!r}")
            return existing
        if self._state_store is not None:
            self._state_store.append_event(
                event,
                claims=self._claims,
                attempts=self._attempts,
                match_log=self._match_log,
                idempotent_keys=self._idempotent_keys,
            )
        self._events.append(event)
        self._events_by_id[event.event_id] = event
        return event

    def _record_events(self, events: list[Any]) -> list[Any]:
        """Publish a lifecycle event batch with one durable projection."""
        if self._state_store is not None:
            self._state_store.append_events(
                events,
                claims=self._claims,
                attempts=self._attempts,
                match_log=self._match_log,
                idempotent_keys=self._idempotent_keys,
            )
        accepted: list[Any] = []
        for event in events:
            existing = self._events_by_id.get(event.event_id)
            if existing is not None:
                if existing.model_dump(mode="json") != event.model_dump(mode="json"):
                    raise ValueError(f"conflicting scheduler event id {event.event_id!r}")
                continue
            accepted.append(event)
            self._events_by_id[event.event_id] = event
        self._events.extend(accepted)
        return accepted

    def _persist_state(self) -> None:
        """Persist a projection-only mutation when no event is emitted."""
        if self._state_store is not None:
            self._state_store.persist_state(
                claims=self._claims,
                attempts=self._attempts,
                match_log=self._match_log,
                idempotent_keys=self._idempotent_keys,
            )

    def record_event(self, event: Any) -> Any:
        """Durably append an externally-produced scheduler audit event.

        This is deliberately a journal-only hook.  It does not mutate Claims,
        Attempts, leases, resources, or VPG state.  Policy layers (for example
        the semantic-interrupt router) may use it to persist a bounded
        proposal while keeping Scheduler/Kernel ownership authoritative.
        Accepted event types are semantic-interrupt, Harness-control, and
        SchedulingEpoch audit records; lifecycle events remain Scheduler-owned.
        """
        from .events import SchedulerEvent, SchedulerEventType

        if not isinstance(event, SchedulerEvent):
            raise TypeError("event must be a SchedulerEvent")
        if event.event_type not in {
            SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED,
            SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED,
            SchedulerEventType.HARNESS_CONTROL,
            SchedulerEventType.SCHEDULING_EPOCH_PLANNED,
        }:
            raise ValueError(
                "external scheduler audit hook only accepts "
                "semantic interrupt, SchedulingEpoch, or Harness-control events"
            )
        with self._schedule_lock:
            return self._record_event(event)

    def record_scheduling_epoch(
        self,
        *,
        graph_id: str,
        graph_version: int,
        epoch_id: int,
        policy_id: str,
        decision_hash: str,
        projection_hash: str = "",
        candidate_task_ids: Iterable[str] = (),
        selected_task_ids: Iterable[str] = (),
        deferred_task_ids: Iterable[str] = (),
        parallelism_hint: int = 0,
        unavailable: Iterable[Any] = (),
    ) -> Any:
        """Append one bounded, deterministic SchedulingEpoch audit event.

        This helper is deliberately journal-only.  It does not inspect or
        mutate Claims, Leases, resources, or VPG state; the authoritative
        scheduler pass remains a separate operation.  Repeating the same
        epoch identity is idempotent, while a conflicting payload fails closed
        through the normal event identity checks.
        """

        from .events import SchedulerEventType, record_event

        graph = str(graph_id).strip()
        policy = str(policy_id).strip()
        projection = str(projection_hash).strip()
        digest = str(decision_hash).strip().lower()
        if not graph or not policy or not digest:
            raise ValueError("graph_id, policy_id, and decision_hash must be non-empty")
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("decision_hash must be a 64-character SHA-256 hex digest")
        if isinstance(graph_version, bool) or not isinstance(graph_version, int):
            raise TypeError("graph_version must be an integer")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int):
            raise TypeError("epoch_id must be an integer")
        if isinstance(parallelism_hint, bool) or not isinstance(parallelism_hint, int):
            raise TypeError("parallelism_hint must be an integer")
        if graph_version < 0 or epoch_id < 0 or parallelism_hint < 0:
            raise ValueError("graph/version/parallelism values must be non-negative")

        def _ids(values: Iterable[str]) -> tuple[list[str], bool]:
            # Do not retain arbitrarily large/untrusted task identifiers in a
            # durable audit row.  Normalize before sorting so retries are
            # deterministic, then apply the same bound to all three lists.
            normalized: set[str] = set()
            value_truncated = False
            for value in values:
                raw = str(value).strip()
                if not raw:
                    continue
                if len(raw) > _MAX_EPOCH_TASK_ID_LENGTH:
                    value_truncated = True
                normalized.add(raw[:_MAX_EPOCH_TASK_ID_LENGTH])
            ordered = sorted(normalized)
            return (
                ordered[:_MAX_EPOCH_TASK_IDS],
                value_truncated or len(ordered) > _MAX_EPOCH_TASK_IDS,
            )

        def _unavailable(values: Iterable[Any]) -> list[dict[str, str]]:
            bounded: list[dict[str, str]] = []
            for value in values:
                name = str(getattr(value, "name", "") or "").strip()
                reason = str(getattr(value, "reason", "") or "").strip()
                if not name:
                    continue
                bounded.append({"name": name[:120], "reason": reason[:240]})
            return sorted(
                bounded,
                key=lambda item: (item["name"], item["reason"]),
            )[:64]

        candidates, candidates_truncated = _ids(candidate_task_ids)
        selected, selected_truncated = _ids(selected_task_ids)
        deferred, deferred_truncated = _ids(deferred_task_ids)
        task_id_lists_truncated = candidates_truncated or selected_truncated or deferred_truncated
        metadata = {
            "schema_version": "scheduling-epoch.v1",
            "epoch_id": epoch_id,
            "policy_id": policy,
            "projection_hash": projection,
            "candidate_task_ids": candidates,
            "selected_task_ids": selected,
            "deferred_task_ids": deferred,
            "parallelism_hint": parallelism_hint,
            "unavailable": _unavailable(unavailable),
        }
        if task_id_lists_truncated:
            metadata["task_id_lists_truncated"] = True
            metadata["task_id_list_limit"] = _MAX_EPOCH_TASK_IDS
            metadata["task_id_max_length"] = _MAX_EPOCH_TASK_ID_LENGTH
        # Stable identity intentionally excludes wall-clock fields.  This
        # makes retries/restarts idempotent without replaying a second epoch.
        event_key = hashlib.sha256(
            f"{graph}|{graph_version}|{epoch_id}|{policy}|{digest}".encode()
        ).hexdigest()
        event = record_event(
            event_type=SchedulerEventType.SCHEDULING_EPOCH_PLANNED,
            event_id=f"scheduling-epoch-{event_key}",
            graph_id=graph,
            graph_version=graph_version,
            decision_hash=digest,
            reason="adaptive scheduling epoch planned",
            metadata=metadata,
        )
        with self._schedule_lock:
            # ``created_at`` is observational metadata and is intentionally
            # generated afresh on each retry.  Treat an otherwise identical
            # proposal as an idempotent replay instead of routing it through
            # ``_record_event``'s byte-for-byte identity check.  Any semantic
            # difference under the same deterministic event id remains a
            # fail-closed conflict.
            existing = self._events_by_id.get(event.event_id)
            if existing is not None:
                current = existing.model_dump(mode="json")
                proposed = event.model_dump(mode="json")
                current.pop("created_at", None)
                proposed.pop("created_at", None)
                if current != proposed:
                    raise ValueError(f"conflicting scheduler event id {event.event_id!r}")
                return existing
            return self._record_event(event)

    def record_interrupt_acknowledgement(
        self,
        *,
        graph_id: str,
        graph_version: int,
        claim_id: str,
        attempt_id: str = "",
        interrupt_id: str = "",
        action: str,
        status: str = "requested",
        decision_hash: str = "",
        reason: str = "",
    ) -> Any:
        """Append one bounded, idempotent cooperative-interrupt phase event.

        This helper is intentionally narrower than :meth:`record_event`:
        callers provide identities and hashes only, while the Scheduler builds
        a deterministic event id and bounded metadata.  ``status`` records the
        phase (``requested``, ``delivered``, ``observed``, or ``cancelled``);
        it never implies Claim/Lease mutation or forced cancellation.
        """

        from .events import SchedulerEventType, record_event

        graph = str(graph_id).strip()
        claim = str(claim_id).strip()
        normalized_action = str(action).strip().lower()
        normalized_status = str(status).strip().lower()
        if not graph or not claim:
            raise ValueError("graph_id and claim_id must be non-empty")
        if isinstance(graph_version, bool) or not isinstance(graph_version, int):
            raise TypeError("graph_version must be an integer")
        if graph_version < 0:
            raise ValueError("graph_version must be >= 0")
        if normalized_action not in {"preempt", "rebase"}:
            raise ValueError("interrupt acknowledgement action must be preempt or rebase")
        if normalized_status not in {
            "requested",
            "already_requested",
            "delivered",
            "observed",
            "acknowledged",
            "cancelled",
            "non_preemptible",
        }:
            raise ValueError(
                "interrupt phase status must be requested, delivered, observed, "
                "acknowledged, cancelled, or non_preemptible"
            )
        normalized_attempt = str(attempt_id).strip()
        normalized_interrupt = str(interrupt_id).strip()
        normalized_hash = str(decision_hash).strip()
        key = "|".join(
            (
                graph,
                str(graph_version),
                claim,
                normalized_attempt,
                normalized_interrupt,
                normalized_action,
                normalized_status,
                normalized_hash,
            )
        )
        event_id = "semantic-interrupt-ack-" + hashlib.sha256(key.encode()).hexdigest()
        metadata = {
            "schema_version": "semantic-interrupt.v1",
            "action": normalized_action,
            "status": normalized_status,
            "interrupt_id": normalized_interrupt,
        }
        if normalized_attempt:
            metadata["attempt_id"] = normalized_attempt
        if reason:
            # Reasons are bounded diagnostics; never persist prompt/model
            # content through this policy boundary.
            metadata["reason"] = str(reason).strip()[:512]
        event = record_event(
            event_id=event_id,
            event_type=SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED,
            graph_id=graph,
            graph_version=graph_version,
            claim_id=claim,
            attempt_id=normalized_attempt,
            decision_hash=normalized_hash,
            reason="cooperative semantic interrupt acknowledged",
            metadata=metadata,
        )
        with self._schedule_lock:
            # ACK event identities are deterministic.  A retry naturally
            # receives a fresh ``created_at`` value, so compare the bounded
            # semantic payload rather than treating the timestamp difference
            # as a conflicting event.  Any other same-id mutation remains a
            # fail-closed conflict.
            existing = self._events_by_id.get(event_id)
            if existing is not None:
                comparable = (
                    "event_type",
                    "graph_id",
                    "claim_id",
                    "attempt_id",
                    "graph_version",
                    "decision_hash",
                    "reason",
                    "metadata",
                )
                if all(getattr(existing, field) == getattr(event, field) for field in comparable):
                    return existing
                raise ValueError(f"conflicting scheduler event id {event_id!r}")
            return self._record_event(event)

    # ── durable cleanup audit ─────────────────────────────────────────────
    def record_cleanup_required(
        self,
        *,
        graph_id: str,
        task_id: str,
        claim_id: str,
        attempt_id: str = "",
        reason: str = "cleanup failed",
        error: str = "",
        lease_id: str = "",
        cleanup_id: str = "",
    ) -> Any:
        """Durably record that exact Claim cleanup still needs reconciliation.

        This is deliberately an *audit marker*, not a state transition:
        recording it never releases, marks, or retargets a Claim/Lease.  It is
        safe to call from cancellation/error handlers after an exact-fenced
        release attempt failed.  Repeating the same marker is idempotent.
        """

        from .events import SchedulerEventType, record_event

        graph = str(graph_id).strip()
        task = str(task_id).strip()
        claim = str(claim_id).strip()
        attempt = str(attempt_id).strip()
        if not graph or not task or not claim:
            raise ValueError("graph_id, task_id, and claim_id must be non-empty")
        # The optional caller id lets two independent cleanup workflows for
        # the same ownership epoch share or deliberately split a marker.
        supplied_id = str(cleanup_id).strip()
        marker_seed = supplied_id or "|".join((graph, task, claim, attempt))
        marker_id = hashlib.sha256(marker_seed.encode()).hexdigest()
        metadata: dict[str, Any] = {
            "schema_version": "execution-cleanup.v1",
            "status": "required",
            "marker_id": marker_id,
            "cleanup_id": supplied_id[:_MAX_CLEANUP_ID_LENGTH],
            "task_id": task,
            "claim_id": claim,
        }
        if attempt:
            metadata["attempt_id"] = attempt
        if lease_id:
            metadata["lease_id"] = str(lease_id).strip()[:_MAX_CLEANUP_TEXT_LENGTH]
        if error:
            metadata["error"] = str(error).strip()[:_MAX_CLEANUP_TEXT_LENGTH]
        event = record_event(
            event_type=SchedulerEventType.EXECUTION_CLEANUP_REQUIRED,
            event_id=f"execution-cleanup-required-{marker_id}",
            graph_id=graph,
            task_id=task,
            claim_id=claim,
            attempt_id=attempt,
            reason=str(reason).strip()[:_MAX_CLEANUP_TEXT_LENGTH],
            metadata=metadata,
        )
        with self._schedule_lock:
            existing = self._events_by_id.get(event.event_id)
            if existing is not None:
                current = existing.model_dump(mode="json")
                proposed = event.model_dump(mode="json")
                current.pop("created_at", None)
                proposed.pop("created_at", None)
                if current != proposed:
                    raise ValueError(f"conflicting cleanup marker id {marker_id!r}")
                return existing
            return self._record_event(event)

    def record_cleanup_resolution(
        self,
        *,
        marker_id: str,
        graph_id: str = "",
        task_id: str = "",
        claim_id: str = "",
        attempt_id: str = "",
        status: str = "resolved",
        reason: str = "",
    ) -> Any:
        """Append an idempotent resolution for one cleanup marker.

        ``status`` is either ``resolved`` (the exact ownership epoch is no
        longer live) or ``superseded`` (a newer ownership epoch replaced it).
        Like :meth:`record_cleanup_required`, this is journal-only and never
        mutates Claim/Lease state.
        """

        from .events import SchedulerEventType, record_event

        marker = str(marker_id).strip().lower()
        if len(marker) != 64 or any(c not in "0123456789abcdef" for c in marker):
            raise ValueError("marker_id must be a 64-character SHA-256 hex digest")
        normalized_status = str(status).strip().lower()
        if normalized_status not in {"resolved", "superseded"}:
            raise ValueError("cleanup resolution status must be resolved or superseded")
        graph = str(graph_id).strip()
        task = str(task_id).strip()
        claim = str(claim_id).strip()
        attempt = str(attempt_id).strip()
        with self._schedule_lock:
            # A resolution is meaningful only for a marker that was durably
            # requested first.  Resolve omitted correlation fields from the
            # required event, while rejecting a caller that attempts to close
            # a different Claim/attempt under the same marker id.
            required_event = None
            existing_statuses: set[str] = set()
            for prior in self._events:
                prior_metadata = getattr(prior, "metadata", {}) or {}
                prior_marker = str(prior_metadata.get("marker_id", "")).strip().lower()
                if prior_marker != marker:
                    continue
                if prior.event_type == SchedulerEventType.EXECUTION_CLEANUP_REQUIRED:
                    if required_event is not None:
                        raise SchedulerStateCorruption(
                            f"duplicate cleanup-required events for marker {marker!r}"
                        )
                    required_event = prior
                elif prior.event_type == SchedulerEventType.EXECUTION_CLEANUP_RESOLVED:
                    existing_statuses.add("resolved")
                elif prior.event_type == SchedulerEventType.EXECUTION_CLEANUP_SUPERSEDED:
                    existing_statuses.add("superseded")
            if required_event is None:
                raise ValueError(f"unknown cleanup marker {marker!r}")
            if len(existing_statuses) > 1 or (
                existing_statuses and normalized_status not in existing_statuses
            ):
                raise ValueError(
                    f"cleanup marker {marker!r} already closed with {sorted(existing_statuses)!r}"
                )
            expected_fields = {
                "graph_id": str(getattr(required_event, "graph_id", "") or ""),
                "task_id": str(getattr(required_event, "task_id", "") or ""),
                "claim_id": str(getattr(required_event, "claim_id", "") or ""),
                "attempt_id": str(getattr(required_event, "attempt_id", "") or ""),
            }
            supplied_fields = {
                "graph_id": graph,
                "task_id": task,
                "claim_id": claim,
                "attempt_id": attempt,
            }
            for field, supplied in supplied_fields.items():
                expected = expected_fields[field]
                if supplied and supplied != expected:
                    raise ValueError(
                        f"cleanup marker {marker!r} {field} does not match the required event"
                    )
                if not supplied:
                    supplied_fields[field] = expected
            graph = supplied_fields["graph_id"]
            task = supplied_fields["task_id"]
            claim = supplied_fields["claim_id"]
            attempt = supplied_fields["attempt_id"]
        metadata: dict[str, Any] = {
            "schema_version": "execution-cleanup.v1",
            "status": normalized_status,
            "marker_id": marker,
        }
        if reason:
            metadata["reason"] = str(reason).strip()[:_MAX_CLEANUP_TEXT_LENGTH]
        event_type = (
            SchedulerEventType.EXECUTION_CLEANUP_RESOLVED
            if normalized_status == "resolved"
            else SchedulerEventType.EXECUTION_CLEANUP_SUPERSEDED
        )
        normalized_reason = str(reason).strip()[:_MAX_CLEANUP_TEXT_LENGTH]
        event = record_event(
            event_type=event_type,
            event_id=f"execution-cleanup-{normalized_status}-{marker}",
            graph_id=graph,
            task_id=task,
            claim_id=claim,
            attempt_id=attempt,
            reason=normalized_reason,
            metadata=metadata,
        )
        with self._schedule_lock:
            # Re-check the closure set after constructing the event.  Another
            # recovery thread may have closed the marker while bounded
            # metadata was being formatted; never allow concurrent
            # ``resolved`` and ``superseded`` events for one marker.
            opposite_type = (
                SchedulerEventType.EXECUTION_CLEANUP_SUPERSEDED
                if normalized_status == "resolved"
                else SchedulerEventType.EXECUTION_CLEANUP_RESOLVED
            )
            for prior in self._events:
                prior_metadata = getattr(prior, "metadata", {}) or {}
                prior_marker = str(prior_metadata.get("marker_id", "")).strip().lower()
                if prior_marker == marker and prior.event_type == opposite_type:
                    raise ValueError(
                        f"cleanup marker {marker!r} was concurrently closed "
                        f"as {str(opposite_type)!r}"
                    )
            existing = self._events_by_id.get(event.event_id)
            if existing is not None:
                current = existing.model_dump(mode="json")
                proposed = event.model_dump(mode="json")
                current.pop("created_at", None)
                proposed.pop("created_at", None)
                if current != proposed:
                    raise ValueError(f"conflicting cleanup resolution id {marker!r}")
                return existing
            return self._record_event(event)

    def _pending_cleanup_markers_locked(self) -> list[dict[str, Any]]:
        """Project unresolved cleanup markers from the append-only journal."""

        from .events import SchedulerEventType

        required: dict[str, Any] = {}
        closed: set[str] = set()
        for event in self._events:
            metadata = getattr(event, "metadata", {})
            marker = str(metadata.get("marker_id", "")).strip().lower()
            if not marker:
                continue
            if event.event_type is SchedulerEventType.EXECUTION_CLEANUP_REQUIRED:
                required[marker] = event
            elif event.event_type in {
                SchedulerEventType.EXECUTION_CLEANUP_RESOLVED,
                SchedulerEventType.EXECUTION_CLEANUP_SUPERSEDED,
            }:
                closed.add(marker)
        markers: list[dict[str, Any]] = []
        for marker, event in required.items():
            if marker in closed:
                continue
            metadata = dict(getattr(event, "metadata", {}) or {})
            markers.append(
                {
                    "marker_id": marker,
                    "graph_id": str(getattr(event, "graph_id", "") or ""),
                    "task_id": str(getattr(event, "task_id", "") or ""),
                    "claim_id": str(getattr(event, "claim_id", "") or ""),
                    "attempt_id": str(getattr(event, "attempt_id", "") or ""),
                    "reason": str(getattr(event, "reason", "") or ""),
                    "metadata": metadata,
                }
            )
        return sorted(
            markers,
            key=lambda item: (
                item["graph_id"],
                item["task_id"],
                item["claim_id"],
                item["marker_id"],
            ),
        )

    @property
    def cleanup_markers(self) -> list[dict[str, Any]]:
        """Return unresolved durable cleanup markers (read-only projection)."""

        with self._schedule_lock:
            return self._pending_cleanup_markers_locked()

    def reconcile_cleanup_markers(self) -> list[dict[str, Any]]:
        """Audit unresolved markers against the current Claim projection.

        A marker is resolved only when its exact Claim is terminal *and* the
        authoritative lease lookup finds no remaining lease.  Active or
        otherwise unknown ownership stays pending, so this helper cannot
        silently bless a leaked lease.  The returned dictionaries are bounded
        diagnostics suitable for recovery tooling.
        """

        with self._schedule_lock:
            outcomes: list[dict[str, Any]] = []
            for marker in self._pending_cleanup_markers_locked():
                claim = next(
                    (item for item in self._claims if item.claim_id == marker["claim_id"]),
                    None,
                )
                base = {
                    "marker_id": marker["marker_id"],
                    "graph_id": marker["graph_id"],
                    "task_id": marker["task_id"],
                    "claim_id": marker["claim_id"],
                }
                if claim is None:
                    outcomes.append(
                        {
                            **base,
                            "status": "pending",
                            "reason": "claim_not_found",
                        }
                    )
                    continue
                if claim.state not in TERMINAL_CLAIM_STATES:
                    outcomes.append(
                        {
                            **base,
                            "status": "pending",
                            "claim_state": claim.state.value,
                            "reason": "claim_still_non_terminal",
                        }
                    )
                    continue
                try:
                    lease = self._lease_lookup_for_claim(claim)
                except BaseException as exc:
                    outcomes.append(
                        {
                            **base,
                            "status": "pending",
                            "claim_state": claim.state.value,
                            "reason": f"lease_lookup_failed:{type(exc).__name__}",
                        }
                    )
                    continue
                if lease is not None:
                    outcomes.append(
                        {
                            **base,
                            "status": "pending",
                            "claim_state": claim.state.value,
                            "reason": "terminal_claim_has_live_lease",
                        }
                    )
                    continue
                self._record_cleanup_resolution_locked(
                    marker_id=marker["marker_id"],
                    graph_id=marker["graph_id"],
                    task_id=marker["task_id"],
                    claim_id=marker["claim_id"],
                    attempt_id=marker["attempt_id"],
                    status="resolved",
                    reason=f"claim_terminal:{claim.state.value}",
                )
                outcomes.append(
                    {
                        **base,
                        "status": "resolved",
                        "claim_state": claim.state.value,
                        "reason": "claim_terminal_without_live_lease",
                    }
                )
            return outcomes

    def _record_cleanup_resolution_locked(self, **kwargs: Any) -> Any:
        """Lock-held implementation used by :meth:`reconcile_cleanup_markers`."""

        # The public method acquires the same RLock; keeping one implementation
        # avoids a second, subtly different event identity path.
        return self.record_cleanup_resolution(**kwargs)

    # ── public query surface ───────────────────────────────────────────────
    @property
    def claims(self) -> list[TaskClaim]:
        with self._schedule_lock:
            return list(self._claims)

    @property
    def attempts(self) -> list[ScheduledExecutionAttempt]:
        with self._schedule_lock:
            return list(self._attempts)

    @property
    def match_log(self) -> list[MatchDecision]:
        with self._schedule_lock:
            return list(self._match_log)

    @property
    def resource_manager(self) -> AtomicResourceManager:
        return self._resource_manager

    def set_resource_capacity(self, pool_id: str, capacity: ResourceVector) -> None:
        """Update one schedulable pool under the lifecycle lock."""
        with self._schedule_lock:
            self._resource_manager.set_capacity(pool_id, capacity)

    def update_registered_resource_capacity(
        self,
        pool_id: str,
        capacity: ResourceVector,
    ) -> ResourceVector:
        """Atomically update one registered Agent pool and logical allocator.

        Scheduling passes and registry-resource refreshes use the same
        lifecycle lock, so neither can observe the temporary state between
        allocator validation and registry publication.  Capacity below active
        reservations fails before the registry is changed.  A registry failure
        restores both projections before releasing the lock.

        The returned vector is the allocator capacity that was authoritative
        immediately before this update.
        """

        with self._schedule_lock:
            descriptor = self._registry.get(pool_id)
            if descriptor is None:
                raise KeyError(pool_id)
            previous_capacity = self._resource_manager.capacity(pool_id)
            previous_registry_capacity = descriptor.resource_capacity

            # Validate against active reservations first.  While this lock is
            # held no scheduling pass can acquire a new reservation.
            self._resource_manager.set_capacity(pool_id, capacity)
            try:
                self._registry.update(
                    pool_id,
                    resource_capacity=capacity,
                )
            except BaseException as exc:
                rollback_errors: list[BaseException] = []
                try:
                    self._resource_manager.set_capacity(pool_id, previous_capacity)
                except BaseException as rollback_exc:  # pragma: no cover - defensive invariant
                    rollback_errors.append(rollback_exc)
                try:
                    current = self._registry.get(pool_id)
                    if (
                        current is not None
                        and current.resource_capacity != previous_registry_capacity
                    ):
                        self._registry.update(
                            pool_id,
                            resource_capacity=previous_registry_capacity,
                        )
                except BaseException as rollback_exc:  # pragma: no cover - injected failure
                    rollback_errors.append(rollback_exc)
                if rollback_errors:
                    detail = "; ".join(type(item).__name__ for item in rollback_errors)
                    raise RuntimeError(
                        "registered resource-capacity update failed and rollback "
                        f"was incomplete: {detail}"
                    ) from exc
                raise
            return previous_capacity

    def get_claim(self, task_id: str, graph_id: str | None = None) -> TaskClaim | None:
        with self._schedule_lock:
            return self._get_claim_locked(task_id, graph_id)

    def _get_claim_locked(
        self,
        task_id: str,
        graph_id: str | None = None,
    ) -> TaskClaim | None:
        for c in self._claims:
            if (
                c.task_id == task_id
                and (graph_id is None or c.graph_id == graph_id)
                and c.state == ClaimState.ACTIVE
            ):
                return c
        return None

    def get_attempt_for_claim(self, claim_id: str) -> ScheduledExecutionAttempt | None:
        with self._schedule_lock:
            return self._attempts_.latest_attempt_for_claim(claim_id)

    def mark_execution_started(self, claim: TaskClaim | str) -> ScheduledExecutionAttempt | None:
        with self._schedule_lock:
            return self._mark_execution_started_locked(claim)

    def _mark_execution_started_locked(
        self,
        claim: TaskClaim | str,
    ) -> ScheduledExecutionAttempt | None:
        """Promote a dispatched attempt to RUNNING and journal the transition.

        ``claim`` may be a TaskClaim object or claim id so SDK integrations can
        use this method without reaching into the AttemptManager projection.
        Terminal/unknown claims are treated as no-ops and return ``None``.
        """
        from .events import SchedulerEventType, record_event

        claim_obj: TaskClaim | None
        if isinstance(claim, str):
            claim_obj = next((c for c in self._claims if c.claim_id == claim), None)
        else:
            claim_obj = claim
        if claim_obj is None or claim_obj.state != ClaimState.ACTIVE:
            return None
        attempt = self.get_attempt_for_claim(claim_obj.claim_id)
        if attempt is None:
            return None
        if attempt.state.value == "dispatched":
            self._attempts_.mark_running(attempt)
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_STARTED,
                    graph_id=claim_obj.graph_id,
                    task_id=claim_obj.task_id,
                    agent_id=claim_obj.agent_id,
                    claim_id=claim_obj.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim_obj.graph_version,
                )
            )
            return attempt
        # A duplicate start notification is harmless while the worker is
        # already running.  Do not, however, acknowledge terminal or
        # quarantined states as if they had started successfully.
        if attempt.state.value == "running":
            return attempt
        return None

    def mark_execution_operationally_succeeded(
        self,
        claim: TaskClaim | str,
    ) -> ScheduledExecutionAttempt | None:
        with self._schedule_lock:
            return self._mark_execution_operationally_succeeded_locked(claim)

    def _mark_execution_operationally_succeeded_locked(
        self,
        claim: TaskClaim | str,
    ) -> ScheduledExecutionAttempt | None:
        """Record executor success without asserting semantic verification.

        VPG remains the authority for the later semantic-verification
        transition.  This milestone is durable when a state store is
        configured, allowing a restart to distinguish a completed external
        action from an attempt that never reached the executor.
        """
        from .events import SchedulerEventType, record_event

        claim_obj: TaskClaim | None
        if isinstance(claim, str):
            claim_obj = next((c for c in self._claims if c.claim_id == claim), None)
        else:
            claim_obj = claim
        if claim_obj is None or claim_obj.state != ClaimState.ACTIVE:
            return None
        attempt = self.get_attempt_for_claim(claim_obj.claim_id)
        if attempt is None:
            return None
        if attempt.state.value in {"dispatched", "running"}:
            self._attempts_.mark_operationally_succeeded(attempt)
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_OPERATIONALLY_SUCCEEDED,
                    graph_id=claim_obj.graph_id,
                    task_id=claim_obj.task_id,
                    agent_id=claim_obj.agent_id,
                    claim_id=claim_obj.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim_obj.graph_version,
                )
            )
            return attempt
        # Operational success is a one-way lifecycle transition.  In
        # particular, a late duplicate callback must not turn a stale,
        # failed, crashed, or already-verified attempt into a successful one.
        return None

    # ── scheduling pass ─────────────────────────────────────────────────────
    def schedule_once(
        self,
        graph_id: str,
        *,
        max_claims: int | None = None,
        allowed_task_ids: Iterable[str] | None = None,
        expected_graph_version: int | None = None,
        dispatch_order: Iterable[str] | None = None,
        task_read_keys_by_task: Mapping[str, Iterable[str]] | None = None,
    ) -> ScheduleResult:
        if expected_graph_version is not None and (
            isinstance(expected_graph_version, bool)
            or not isinstance(expected_graph_version, int)
            or expected_graph_version < 0
        ):
            raise ValueError("expected_graph_version must be a non-negative integer")
        with self._schedule_lock:
            if expected_graph_version is not None:
                observed = int(self._vpg.current_graph_version(graph_id))
                if observed != expected_graph_version:
                    result = ScheduleResult()
                    result.policy_stale = True
                    result.expected_graph_version = expected_graph_version
                    result.observed_graph_version = observed
                    result.policy_stale_reason = (
                        "adaptive policy graph version is stale: "
                        f"planned={expected_graph_version}, observed={observed}"
                    )
                    return result
            if any(
                claim.graph_id == graph_id
                and claim.state in {ClaimState.PROPOSED, ClaimState.ACQUIRING}
                for claim in self._claims
            ):
                self._reconcile_locked()
            return self._schedule_once_locked(
                graph_id,
                max_claims=max_claims,
                allowed_task_ids=allowed_task_ids,
                expected_graph_version=expected_graph_version,
                dispatch_order=dispatch_order,
                task_read_keys_by_task=task_read_keys_by_task,
            )

    def _schedule_once_locked(
        self,
        graph_id: str,
        *,
        max_claims: int | None = None,
        allowed_task_ids: Iterable[str] | None = None,
        expected_graph_version: int | None = None,
        dispatch_order: Iterable[str] | None = None,
        task_read_keys_by_task: Mapping[str, Iterable[str]] | None = None,
    ) -> ScheduleResult:
        """Single scheduling pass over the VPG ready frontier.

        For each ready task in VPG order:
          1. skip if a valid ACTIVE claim exists
          2. decode TaskRequirements
          3. evaluate eligibility across registered agents
          4. deterministic matching
          5. acquire Kernel exclusive lease (re-checks readiness + version
             before linearizing, per Section 19)
          6. record DispatchResult

        Returns ScheduleResult with dispatched + skipped reasons.
        """
        from .events import SchedulerEventType, record_event

        # ``allowed_task_ids`` is an advisory WHAT/WHEN boundary supplied by
        # an upstream policy engine.  It never bypasses the authoritative VPG
        # readiness, eligibility, resource, Claim, or Lease checks below.
        allowed = (
            None
            if allowed_task_ids is None
            else frozenset(
                str(task_id).strip() for task_id in allowed_task_ids if str(task_id).strip()
            )
        )

        # AgentOS may register agents after constructing the Scheduler.
        self._sync_registry_resource_capacities()

        # Authoritative VPG frontier.
        try:
            frontier = list(self._vpg.ready_frontier(graph_id))
        except Exception:
            # Unknown graph (e.g. pre-demo demarcation) — treat as empty.
            return ScheduleResult()
        if not frontier:
            return ScheduleResult()

        # ``dispatch_order`` is the upstream policy's *ranking*, as distinct
        # from the advisory filter above.  It can never admit an unready task —
        # it only decides who among the authoritative frontier is offered a
        # Claim first.  That is what lets a capacity-bounded pass spend its
        # budget on critical-path work instead of the graph's static
        # (priority, topo_depth, created_in_version, id) order.  The sort is
        # stable, so unranked frontier entries keep their VPG order behind the
        # ranked ones.
        order_rank: dict[str, int] = {}
        for position, requested in enumerate(dispatch_order or ()):
            key = str(requested).strip()
            if key and key not in order_rank:
                order_rank[key] = position
        if order_rank:
            frontier.sort(key=lambda item: order_rank.get(item.task_id, len(order_rank)))

        # Agent-side context residency.  This is the one dispatch input that is
        # not derivable from the graph: which agent has already read what.
        resident_read_keys_by_agent = self._agent_resident_read_keys(graph_id)
        declared_reads_by_task = {
            str(task_key).strip(): tuple(
                str(key).strip() for key in (keys or ()) if str(key).strip()
            )
            for task_key, keys in (task_read_keys_by_task or {}).items()
            if str(task_key).strip()
        }

        current_version = self._vpg.current_graph_version(graph_id)
        if expected_graph_version is not None and current_version != expected_graph_version:
            result = ScheduleResult()
            result.policy_stale = True
            result.expected_graph_version = expected_graph_version
            result.observed_graph_version = int(current_version)
            result.policy_stale_reason = (
                "adaptive policy graph version changed before admission: "
                f"planned={expected_graph_version}, observed={current_version}"
            )
            return result
        active_by_agent = active_claim_count_by_agent(self._claims)
        # Attempt numbering previously rescanned every durable claim for every
        # frontier candidate -- O(candidates x claims) per pass, quadratic over a
        # run, and pure decision overhead that grows with history.  Counted once
        # per pass instead.  Rebuilt each pass rather than maintained across
        # passes, so it cannot go stale as claims change state.
        active_claim_by_task: dict[tuple[str, str], TaskClaim] = {
            (item.graph_id, item.task_id): item
            for item in self._claims
            if item.state == ClaimState.ACTIVE
        }
        attempts_by_task: dict[tuple[str, str], int] = {}
        for prior_claim in self._claims:
            attempt_key = (prior_claim.graph_id, prior_claim.task_id)
            attempts_by_task[attempt_key] = attempts_by_task.get(attempt_key, 0) + 1

        result = ScheduleResult()
        result.dispatch_order_applied = tuple(sorted(order_rank, key=order_rank.get))  # type: ignore[arg-type]
        locality_matched: list[str] = []
        claims_this_pass = 0

        for candidate in frontier:
            task_id = candidate.task_id

            if allowed is not None and task_id not in allowed:
                result.skipped.append((task_id, "adaptive policy deferred"))
                continue

            # Existing active claim: skip (per D2-I4 we never create a 2nd).
            # Pre-dispatch skip check, served from the per-pass index rather than
            # rescanning every durable claim per candidate.  The post-acquire
            # lookup below deliberately stays a live query: it must observe the
            # claim this pass just created, which no pre-built index can contain.
            existing = active_claim_by_task.get((graph_id, task_id))
            if existing is not None:
                result.skipped.append((task_id, f"active claim {existing.claim_id}"))
                continue

            # Bounds.
            if max_claims is not None and claims_this_pass >= max_claims:
                result.skipped.append((task_id, "max_claims bound"))
                continue

            # Decode requirements from the TaskNode metadata.
            payload = self._vpg.task_node_payload(graph_id, task_id)
            if payload is None:
                result.skipped.append((task_id, "task payload missing"))
                continue
            req = decode_task_requirements(task_id, payload)
            semantic_epoch = self._semantic_epoch(payload, current_version)
            attempts_in_epoch = self._attempts_.count_attempts_for_epoch(
                graph_id,
                task_id,
                semantic_epoch,
            )
            if req.max_attempts is not None and attempts_in_epoch >= max(req.max_attempts, 0):
                result.skipped.append(
                    (
                        task_id,
                        f"max_attempts exhausted ({attempts_in_epoch}/{req.max_attempts})",
                    )
                )
                continue

            # Idempotency key: graph+task+version+agent composite is the
            # canonical repeat-scheduling key (Section 30).
            idem_key = self._claim_idempotency_key(
                graph_id,
                task_id,
                current_version,
            )
            if idem_key in self._idempotent_keys:
                result.skipped.append((task_id, "idempotent replay"))
                continue

            # ── eligibility ────────────────────────────────────────────
            eligibility = self._evaluate_eligibility_for_task(
                graph_id,
                current_version,
                req,
                candidate,
                active_by_agent,
            )
            eligible_agents = [e for e in eligibility if e.eligible]
            if not eligible_agents:
                reasons = tuple((e.agent_id, e.reason_text) for e in eligibility)
                result.skipped.append((task_id, f"no eligible agent; evaluated={reasons}"))
                self._record_event(
                    record_event(
                        SchedulerEventType.ELIGIBILITY_EVALUATED,
                        graph_id=graph_id,
                        task_id=task_id,
                        graph_version=current_version,
                        reason="no eligible agent",
                    )
                )
                continue

            agent_pool = [self._registry.get(e.agent_id) for e in eligible_agents]
            agent_pool = [a for a in agent_pool if a is not None]
            if not agent_pool:
                result.skipped.append((task_id, "eligible agents disappeared"))
                continue

            # deterministic best-fit across eligible pool
            decision = match_deterministic_best_fit_v1(
                graph_id=graph_id,
                graph_version=current_version,
                task_id=task_id,
                task_priority=req.priority,
                eligible_agents=agent_pool,
                active_claims_by_agent=active_by_agent,
                preferred_specializations=req.preferred_specializations,
                preferred_agent=req.preferred_agent,
                task_read_keys=declared_reads_by_task.get(task_id, ()),
                resident_read_keys_by_agent=resident_read_keys_by_agent,
            )
            selected_score = next(
                (c for c in decision.candidates if c.agent_id == decision.selected_agent_id),
                None,
            )
            if selected_score is not None and any(
                reason.startswith("context residency") for reason in selected_score.reasons
            ):
                locality_matched.append(task_id)
            self._match_log.append(decision)
            try:
                self._record_event(
                    record_event(
                        SchedulerEventType.MATCH_DECISION_CREATED,
                        graph_id=graph_id,
                        task_id=task_id,
                        agent_id=decision.selected_agent_id,
                        graph_version=current_version,
                        decision_hash=decision.decision_hash,
                        reason=f"selected {decision.selected_agent_id}",
                    )
                )
            except Exception:
                self._match_log.remove(decision)
                raise

            # ── acquire exclusive kernel lease ───────────────────────────
            attempt_number = attempts_by_task.get((graph_id, task_id), 0)
            acquired = self._acquire_claim(
                graph_id=graph_id,
                task_id=task_id,
                graph_version=current_version,
                claim_id=f"claim-{graph_id}-{task_id}-{current_version}-{attempt_number}",
                agent_id=decision.selected_agent_id,
                attempt_number=attempt_number,
                semantic_epoch=semantic_epoch,
                resources=req.resources,
            )
            if not acquired:
                result.skipped.append((task_id, "claim race lost / kernel refused lease"))
                continue

            claims_this_pass += 1
            existing = self.get_claim(task_id, graph_id)
            claim_id = existing.claim_id if existing is not None else ""
            result.mark_dispatched(
                task_id,
                decision.selected_agent_id,
                claim_id,
            )
            active_by_agent[decision.selected_agent_id] = (
                active_by_agent.get(decision.selected_agent_id, 0) + 1
            )
            self._idependent_mark_idempotent(idem_key)

        result.locality_matched = tuple(locality_matched)
        return result

    def _live_graph_version(self, graph_id: str) -> int | None:
        """Current VPG version, or ``None`` when it cannot be observed.

        Stamped onto terminal attempt events so a pure projection can compare
        the version an attempt was dispatched under against the version in
        effect when it finished.  ``None`` rather than a guess: reporting a
        fabricated version would make superseded work look current.
        """

        try:
            return int(self._vpg.current_graph_version(graph_id))
        except Exception:
            return None

    @staticmethod
    def _snapshot_read_keys(snapshot: Any) -> set[str]:
        """Both identity forms a snapshot's read-set can be matched under.

        Provenance keeps the raw ``resource_uri`` while graph-side declarations
        are often normalized to a bare artifact id.  Recording both forms lets
        residency and declarations intersect without a translation table that
        could silently drift out of step.
        """

        keys: set[str] = set()
        for binding in snapshot.read_set:
            identity = binding.identity
            if identity is not None:
                keys.add(identity)
            if binding.artifact_id:
                keys.add(binding.artifact_id)
        return keys

    def _index_agent_residency(self, graph_id: str, snapshot: Any) -> None:
        """Fold one bound snapshot into the residency index (write-time)."""

        index = self._resident_index.get(graph_id)
        if index is None:
            # Nothing has read the index for this graph yet; the lazy build on
            # first read will pick this snapshot up from the durable attempts.
            return
        index.setdefault(snapshot.agent_id, set()).update(self._snapshot_read_keys(snapshot))

    def _agent_resident_read_keys(self, graph_id: str) -> dict[str, tuple[str, ...]]:
        """Resource identities each agent has already read, per durable snapshot.

        Context residency is path-dependent agent state, so it cannot be
        recovered from the graph.  It is recovered instead from the read-sets
        already committed to ``AgentSnapshot``s -- the same evidence commit-time
        read-set validation trusts -- which keeps dispatch and validation
        reading one authority rather than two.

        Maintained as a write-time inverted index rather than rebuilt per
        scheduling pass.  Rebuilding scanned every durable attempt on every
        pass, which is O(attempts) per dispatch and therefore quadratic over a
        run -- pure decision overhead that grows with history and competes with
        the scheduling gain it exists to find.  The index is built lazily on
        first read so a Scheduler recovered from its journal is correct without
        replaying binds, then updated incrementally as snapshots bind.
        """

        index = self._resident_index.get(graph_id)
        if index is None:
            index = {}
            for attempt in self._attempts_.all_attempts():
                if attempt.graph_id != graph_id:
                    continue
                snapshot = attempt.agent_snapshot
                if snapshot is None:
                    continue
                index.setdefault(snapshot.agent_id, set()).update(
                    self._snapshot_read_keys(snapshot)
                )
            self._resident_index[graph_id] = index
        return {agent_id: tuple(sorted(keys)) for agent_id, keys in index.items()}

    def schedule_until_idle(
        self,
        graph_id: str,
        *,
        max_dispatches: int = 1000,
        max_claims_per_pass: int | None = None,
    ) -> list[ScheduleResult]:
        """Repeatedly schedule until no more dispatches are possible.

        Safety-bound by max_dispatches; never an infinite daemon loop
        (Section 32).
        """
        out: list[ScheduleResult] = []
        total = 0
        for _ in range(max_dispatches + 1):
            res = self.schedule_once(graph_id, max_claims=max_claims_per_pass)
            out.append(res)
            if not res.dispatched:
                break
            total += len(res.dispatched)
            if total >= max_dispatches:
                break
        return out

    # ── claim acquisition protocol (Section 18) ────────────────────────────
    def _acquire_claim(
        self,
        *,
        graph_id: str,
        task_id: str,
        graph_version: int,
        claim_id: str,
        agent_id: str,
        attempt_number: int = 0,
        semantic_epoch: int = 0,
        resources: ResourceVector | None = None,
        handoff_id: str | None = None,
    ) -> bool:
        from .events import SchedulerEventType, record_event

        agent = self._registry.get(agent_id)
        if agent is None:
            return False

        # Re-check GraphVersion — stale readiness proof cannot linearize ownership.
        current = self._vpg.current_graph_version(graph_id)
        if current != graph_version:
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_REJECTED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    graph_version=current,
                    reason=f"graph version race (used {graph_version}, now {current})",
                )
            )
            return False

        # Re-check that the task is STILL in the ready frontier at the
        # current version.
        frontier = self._vpg.ready_frontier(graph_id)
        if not any(c.task_id == task_id for c in frontier):
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_REJECTED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    graph_version=current,
                    reason="task no longer in ready frontier",
                )
            )
            return False

        # Re-check agent process liveness + state.
        proc = self._process.get(agent.process_id)
        if proc is None:
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_REJECTED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    graph_version=current,
                    reason="agent process missing",
                )
            )
            return False
        state = getattr(proc, "state", None)
        if state in ("exited", "failed"):
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_REJECTED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    graph_version=current,
                    reason=f"agent process in terminal state {state!r}",
                )
            )
            return False

        requested_resources = resources or ResourceVector()
        reservation = self._resource_manager.try_reserve(
            pool_id=agent_id,
            owner_id=claim_id,
            request=requested_resources,
        )
        if reservation is None:
            shortages = requested_resources.shortages(self._resource_manager.available(agent_id))
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_REJECTED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    graph_version=current,
                    reason=f"resources no longer available: {shortages}",
                )
            )
            return False

        resource = claim_resource_uri(graph_id, task_id)
        claim = self._claims_.propose(
            claim_id=claim_id,
            graph_id=graph_id,
            graph_version=current,
            task_id=task_id,
            agent_id=agent_id,
            process_id=agent.process_id,
            lease_resource=resource,
            attempt_number=attempt_number,
        )
        claim.resource_reservation_id = reservation.reservation_id
        claim.reserved_resources = requested_resources
        claim.handoff_id = str(handoff_id).strip() if handoff_id else None
        self._claims_.mark_acquiring(claim)
        # The projection row must exist before the proposal event is
        # committed, otherwise restart recovery cannot reconcile it.
        self._claims.append(claim)
        try:
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_PROPOSED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    claim_id=claim.claim_id,
                    graph_version=current,
                )
            )
        except Exception:
            self._claims.remove(claim)
            self._claims_._claims.pop(claim.claim_id, None)
            self._release_claim_resources(claim)
            raise

        try:
            lease_acquired = self._claims_.try_acquire_lease(claim)
        except Exception as lease_exc:
            # A malformed lease response may have acquired Kernel ownership
            # before its fencing data was rejected.  ClaimManager leaves such
            # a row ACQUIRING when compensation release was not confirmed so
            # reconciliation can retry/reclaim the authoritative lease.  Do
            # not turn that row into terminal REJECTED (which reconciliation
            # intentionally skips), and retain the logical resource
            # reservation until ownership cleanup is confirmed.
            cleanup_pending = (
                isinstance(lease_exc, LeaseReleaseFailed)
                and claim.state == ClaimState.ACQUIRING
                and claim.lease_id is not None
            )
            if cleanup_pending:
                claim.released_at = None
                claim.reason = (
                    f"{claim.reason or 'kernel_lease_acquisition_failed'}; "
                    "scheduler_cleanup_pending"
                )
            else:
                claim.state = ClaimState.REJECTED
                claim.reason = f"kernel_lease_acquisition_failed: {lease_exc}"
                claim.released_at = self._clock()
                self._release_claim_resources(claim)
            try:
                self._record_event(
                    record_event(
                        SchedulerEventType.CLAIM_REJECTED,
                        graph_id=graph_id,
                        task_id=task_id,
                        agent_id=agent_id,
                        claim_id=claim.claim_id,
                        graph_version=current,
                        reason=claim.reason,
                        claim_state=claim.state.value,
                        metadata={"cleanup_pending": cleanup_pending},
                    )
                )
            except Exception as persist_exc:
                raise persist_exc from lease_exc
            raise
        if lease_acquired:
            attempt = self._attempts_.start_attempt(
                attempt_id=f"attempt-{graph_id}-{task_id}-{attempt_number}",
                graph_id=graph_id,
                graph_version=current,
                semantic_epoch=semantic_epoch,
                task_id=task_id,
                claim_id=claim.claim_id,
                agent_id=agent_id,
                process_id=agent.process_id,
                attempt_number=attempt_number,
            )
            self._attempts.append(attempt)
            lease_event = record_event(
                SchedulerEventType.CLAIM_LEASE_ACQUIRED,
                graph_id=graph_id,
                task_id=task_id,
                agent_id=agent_id,
                claim_id=claim.claim_id,
                graph_version=current,
                reason=claim.reason or "",
            )
            dispatch_event = record_event(
                SchedulerEventType.EXECUTION_DISPATCHED,
                graph_id=graph_id,
                task_id=task_id,
                agent_id=agent_id,
                claim_id=claim.claim_id,
                attempt_id=attempt.attempt_id,
                graph_version=current,
                metadata={
                    "attempt_number": attempt_number,
                    "semantic_epoch": semantic_epoch,
                },
            )
            try:
                self._record_events([lease_event, dispatch_event])
            except Exception as persist_exc:
                try:
                    self._claims_.release(
                        claim,
                        reason="scheduler_state_persistence_failed_after_lease",
                    )
                except Exception as cleanup_exc:
                    raise cleanup_exc from persist_exc
                self._attempts.remove(attempt)
                self._attempts_._attempts.pop(attempt.attempt_id, None)
                self._release_claim_resources(claim)
                with suppress(Exception):
                    self._persist_state()
                raise
            return True

        # Persist the terminal lease-refusal state as a durable decision.
        self._release_claim_resources(claim)
        self._record_event(
            record_event(
                SchedulerEventType.CLAIM_REJECTED,
                graph_id=graph_id,
                task_id=task_id,
                agent_id=agent_id,
                claim_id=claim.claim_id,
                graph_version=current,
                reason=claim.reason or "kernel_exclusive_lease_refused",
            )
        )
        return False

    # ── claim lifecycle transitions (Scheduler-initiated) ──────────────────
    def mark_task_completed(self, claim: TaskClaim) -> None:
        with self._schedule_lock:
            self._mark_task_completed_locked(claim)

    def _mark_task_completed_locked(self, claim: TaskClaim) -> None:
        from .events import SchedulerEventType, record_event

        if claim.state != ClaimState.ACTIVE:
            return
        attempt = self.get_attempt_for_claim(claim.claim_id)
        if attempt is None:
            return
        # Semantic completion is only legal after an operational success (or
        # when replaying an already semantically verified attempt).  A VPG
        # ``verified`` observation alone must never promote a dispatched,
        # running, failed, crashed, or stale attempt.
        if attempt.state.value not in {
            "succeeded_operationally",
            "verified_semantically",
        }:
            return
        if attempt.state.value == "succeeded_operationally":
            if not self._attempts_.mark_semantically_verified(attempt):
                # AttemptManager is the final lifecycle authority.  Keep the
                # claim ACTIVE if a concurrent/late mutation invalidated the
                # transition between the state check and promotion.
                return
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_SEMANTICALLY_VERIFIED,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
                    metadata={
                        # The graph version in effect when this attempt reached a
                        # terminal state, as distinct from the dispatch version
                        # above.  Without both numbers on the same journal there
                        # is no way to tell that an attempt kept computing after
                        # the graph had already moved on, because the two
                        # journals share no total order.
                        "live_graph_version": self._live_graph_version(claim.graph_id),
                    },
                )
            )
        self._claims_.complete(claim)
        try:
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_COMPLETED,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    reason="vpg task verified",
                )
            )
        finally:
            self._release_claim_resources(claim)

    def _evidence_matches_active_attempt(self, claim: TaskClaim) -> bool:
        """Require semantic evidence to belong to this claim/attempt epoch.

        The optional VPG adapter method keeps legacy test doubles and older
        integrations source-compatible.  When present (the built-in SDK
        facade), absence of a complete binding fails closed.
        """
        attempt = self.get_attempt_for_claim(claim.claim_id)
        if attempt is None:
            return False
        if attempt.state.value not in {
            "succeeded_operationally",
            "verified_semantically",
        }:
            return False
        lookup = getattr(self._vpg, "task_evidence_bindings", None)
        if not callable(lookup):
            return True
        try:
            bindings = lookup(claim.graph_id, claim.task_id)
        except Exception:
            return False
        for binding in bindings or ():
            # A malformed adapter row is untrusted input.  Do not let an
            # AttributeError escape reconciliation or accidentally treat a
            # partially decoded row as ownership evidence.
            if not hasattr(binding, "get"):
                continue
            if (
                binding.get("claim_id") != claim.claim_id
                or binding.get("attempt_id") != attempt.attempt_id
                or binding.get("semantic_epoch") != attempt.semantic_epoch
            ):
                continue
            # If the executor sealed a provenance/coverage digest on this
            # attempt, semantic Evidence must carry the exact same digest.
            # This prevents a verifier from attaching a structurally valid
            # Evidence record produced from a different observation trace.
            expected_digest = getattr(attempt, "provenance_digest", None)
            if expected_digest is not None and (
                "provenance_digest" not in binding
                or binding.get("provenance_digest") != expected_digest
            ):
                continue
            expected_context_snapshot = getattr(attempt, "context_snapshot_id", None)
            if expected_context_snapshot is not None and (
                binding.get("context_snapshot_id") != expected_context_snapshot
                or binding.get("context_manifest_id")
                != getattr(attempt, "context_manifest_id", None)
                or binding.get("context_manifest_hash")
                != getattr(attempt, "context_manifest_hash", None)
                or binding.get("context_working_set_hash")
                != getattr(attempt, "context_working_set_hash", None)
                or binding.get("context_materialized_hash")
                != getattr(attempt, "context_materialized_hash", None)
            ):
                continue
            # New built-in VPG facades expose the lease generation captured
            # when Evidence was committed.  If the field is present, it must
            # match the active claim exactly; a missing value is fail-closed.
            # Legacy adapters may omit the field entirely and retain the
            # three-field compatibility gate above.
            if "lease_fencing_token" in binding:
                token = getattr(claim, "lease_fencing_token", None)
                if token is None or binding.get("lease_fencing_token") != token:
                    continue
            return True
        return False

    def bind_attempt_provenance(self, claim_id: str, digest: str) -> bool:
        """Seal the canonical provenance digest for an ACTIVE claim attempt.

        Binding is performed under the scheduler lifecycle lock and is
        idempotent for the same digest.  A missing/terminal claim, missing
        attempt, or attempted replacement of an already-sealed digest fails
        closed and returns ``False``; malformed digests still raise the
        ``ValueError`` supplied by :class:`AttemptManager`.
        """
        with self._schedule_lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None or claim.state != ClaimState.ACTIVE:
                return False
            attempt = self.get_attempt_for_claim(claim_id)
            if attempt is None or attempt.claim_id != claim_id:
                return False
            bound = self._attempts_.bind_provenance_digest(attempt, digest)
            if not bound:
                return False
            self._persist_state()
            return True

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
        """Seal the Context VM materialization for an ACTIVE claim attempt."""

        with self._schedule_lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None or claim.state != ClaimState.ACTIVE:
                return False
            attempt = self.get_attempt_for_claim(claim_id)
            if attempt is None or attempt.claim_id != claim_id:
                return False
            bound = self._attempts_.bind_context_snapshot(
                attempt,
                snapshot_id=snapshot_id,
                manifest_id=manifest_id,
                manifest_hash=manifest_hash,
                working_set_hash=working_set_hash,
                materialized_hash=materialized_hash,
            )
            if not bound:
                return False
            self._persist_state()
            return True

    def bind_agent_snapshot(
        self,
        claim_id: str,
        snapshot: AgentSnapshot,
    ) -> bool:
        """Bind/update an AgentSnapshot to the exact active Attempt.

        The AttemptManager performs identity and monotonicity checks. This
        wrapper adds the Scheduler ownership gate and publishes a compact
        durable event containing only the snapshot fingerprint.
        """

        from .events import SchedulerEventType, record_event

        with self._schedule_lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None or claim.state != ClaimState.ACTIVE:
                return False
            attempt = self._attempts_.latest_attempt_for_claim(claim_id)
            if attempt is None or attempt.claim_id != claim_id:
                return False
            existing = attempt.agent_snapshot
            if existing is not None and existing == snapshot:
                # Exact replay is a true no-op: do not append another audit
                # event merely because an idempotent caller retried.
                return True
            if existing is None:
                bound = self._attempts_.bind_agent_snapshot(attempt, snapshot)
            else:
                bound = self._attempts_.update_agent_snapshot(
                    attempt,
                    snapshot,
                    expected_fingerprint=existing.fingerprint(),
                )
            if not bound:
                return False
            self._index_agent_residency(claim.graph_id, snapshot)
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_SNAPSHOT_BOUND,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
                    reason="agent_snapshot_bound",
                    metadata={
                        "snapshot_fingerprint": snapshot.fingerprint(),
                        "snapshot_state": snapshot.state.value,
                        "progress": snapshot.progress,
                        "read_count": len(snapshot.read_set),
                        "write_count": len(snapshot.write_set),
                    },
                )
            )
            return True

    def mark_stale_cognition(
        self,
        claim_id: str,
        reason: str = "stale_cognition",
    ) -> bool:
        """Quarantine a live Attempt whose bound cognition is obsolete."""

        from .events import SchedulerEventType, record_event

        normalized_reason = str(reason).strip() or "stale_cognition"
        with self._schedule_lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None or claim.state != ClaimState.ACTIVE:
                return False
            attempt = self._attempts_.latest_attempt_for_claim(claim_id)
            if attempt is None or attempt.claim_id != claim_id:
                return False
            if attempt.state.value == "stale_cognition":
                return attempt.error == normalized_reason
            # Read-set validation often runs immediately after the executor
            # reports operational success, but before semantic Evidence is
            # committed.  That boundary is still subject to cognition
            # change: an operationally successful result based on an
            # obsolete input must be quarantined rather than treated as a
            # normal failure (or, worse, allowed to close the Goal).
            #
            # A semantically verified attempt is immutable and can never be
            # rolled back through this API.  All earlier execution states may
            # be marked stale, including ``succeeded_operationally``.
            if attempt.state.value not in {
                "dispatched",
                "running",
                "succeeded_operationally",
            }:
                return False
            self._attempts_.mark_stale_cognition(attempt, error=normalized_reason)
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_STALE_COGNITION,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
                    claim_state=claim.state.value,
                    reason=normalized_reason,
                    metadata={
                        "semantic_epoch": attempt.semantic_epoch,
                        "live_graph_version": self._live_graph_version(claim.graph_id),
                        "snapshot_fingerprint": (
                            attempt.agent_snapshot.fingerprint()
                            if attempt.agent_snapshot is not None
                            else None
                        ),
                    },
                )
            )
            return True

    def renew_claim(
        self,
        claim_id: str,
        ttl: timedelta | None = None,
        *,
        expected_attempt_id: str | None = None,
        expected_semantic_epoch: int | None = None,
    ) -> bool:
        """Renew the Kernel lease for one ACTIVE claim.

        Renewal is a cooperative heartbeat, not a semantic progress commit.
        The operation validates the claim and (optionally) the exact attempt
        identity/epoch while holding the scheduler lock, then delegates the
        atomic lease update to the Kernel-backed provider.  Any ownership
        mismatch or provider refusal fails closed and leaves the claim ACTIVE
        but unextended; the next reconciliation pass will fence it.
        """
        with self._schedule_lock:
            claim = next((c for c in self._claims if c.claim_id == claim_id), None)
            if claim is None or claim.state != ClaimState.ACTIVE:
                return False
            attempt = self.get_attempt_for_claim(claim_id)
            if attempt is None:
                return False
            if expected_attempt_id is not None and attempt.attempt_id != expected_attempt_id:
                return False
            if (
                expected_semantic_epoch is not None
                and attempt.semantic_epoch != expected_semantic_epoch
            ):
                return False
            lease = self._claims_.renew_claim(claim, ttl)
            if lease is None:
                return False
            from .events import SchedulerEventType, record_event

            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_LEASE_RENEWED,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
                    claim_state=claim.state.value,
                    reason="kernel_lease_renewed",
                    metadata={
                        "lease_id": claim.lease_id,
                        "lease_fencing_token": claim.lease_fencing_token,
                        "lease_expires_at": (
                            claim.lease_expires_at.isoformat()
                            if claim.lease_expires_at is not None
                            else None
                        ),
                    },
                )
            )
            return True

    # ── bounded ownership-handoff intent protocol ─────────────────────────
    def _handoff_intent_events_locked(
        self,
        handoff_id: str,
    ) -> list[Any]:
        """Return durable intent events for one handoff id in journal order."""

        from .events import SchedulerEventType

        return [
            event
            for event in self._events
            if getattr(event, "metadata", {}).get("handoff_id") == handoff_id
            and getattr(event, "event_type", None)
            in {
                SchedulerEventType.TASK_HANDOFF_PREPARED,
                SchedulerEventType.TASK_HANDOFF_COMMITTING,
                SchedulerEventType.TASK_HANDOFF_COMMITTED,
                SchedulerEventType.TASK_HANDOFF_RECOVERY,
            }
        ]

    @staticmethod
    def _handoff_intent_from_event(event: Any) -> OwnershipHandoffIntent:
        raw = (getattr(event, "metadata", {}) or {}).get("intent")
        if not isinstance(raw, dict):
            raise ValueError("handoff event is missing its durable intent")
        return OwnershipHandoffIntent.model_validate(raw)

    def _handoff_existing_locked(
        self,
        intent: OwnershipHandoffIntent,
    ) -> OwnershipHandoffResult | None:
        """Decode an already persisted intent/result without mutating state."""

        from .events import SchedulerEventType

        events = self._handoff_intent_events_locked(intent.handoff_id)
        if not events:
            return None
        first_intent = self._handoff_intent_from_event(events[0])
        if first_intent.fingerprint() != intent.fingerprint():
            return OwnershipHandoffResult(
                intent=intent,
                phase=OwnershipHandoffPhase.FAILED_CLOSED,
                status=OwnershipHandoffStatus.REFUSED,
                reason="handoff_id is already bound to a different durable intent",
            )
        latest = events[-1]
        latest_type = getattr(latest, "event_type", None)
        metadata = getattr(latest, "metadata", {}) or {}
        if latest_type is SchedulerEventType.TASK_HANDOFF_COMMITTED:
            raw_result = metadata.get("claim_result")
            claim_result = None
            if isinstance(raw_result, dict):
                claim_result = ClaimHandoffResult.model_validate(raw_result)
            return OwnershipHandoffResult(
                intent=intent,
                phase=OwnershipHandoffPhase.COMMITTED,
                status=OwnershipHandoffStatus.REPLAYED,
                source_lease_released=True,
                replacement_claim_id=(claim_result.replacement_claim_id if claim_result else None),
                replacement_attempt_id=(
                    claim_result.replacement_attempt_id if claim_result else None
                ),
                claim_result=claim_result,
                reason="handoff commit already durable",
            )
        if latest_type is SchedulerEventType.TASK_HANDOFF_RECOVERY:
            raw_phase = str(metadata.get("phase", OwnershipHandoffPhase.FAILED_CLOSED.value))
            try:
                phase = OwnershipHandoffPhase(raw_phase)
            except ValueError:
                phase = OwnershipHandoffPhase.FAILED_CLOSED
            raw_status = str(metadata.get("status", OwnershipHandoffStatus.IN_DOUBT.value))
            try:
                status = OwnershipHandoffStatus(raw_status)
            except ValueError:
                status = OwnershipHandoffStatus.IN_DOUBT
            return OwnershipHandoffResult(
                intent=intent,
                phase=phase,
                status=status,
                source_lease_released=metadata.get("source_lease_released"),
                reason=str(metadata.get("reason", "handoff recovery is durable")),
            )
        if latest_type is SchedulerEventType.TASK_HANDOFF_COMMITTING:
            return OwnershipHandoffResult(
                intent=intent,
                phase=OwnershipHandoffPhase.COMMITTING,
                status=OwnershipHandoffStatus.COMMITTING,
                reason="handoff commit intent is durable; commit/recovery is required",
            )
        return OwnershipHandoffResult(
            intent=intent,
            phase=OwnershipHandoffPhase.PREPARED,
            status=OwnershipHandoffStatus.PREPARED,
            reason="handoff intent is prepared",
        )

    def prepare_handoff(
        self,
        graph_id: str,
        task_id: str,
        *,
        source_claim_id: str,
        replacement_agent_id: str,
        action: str = "rebase",
        reason: str = "semantic handoff",
        handoff_id: str | None = None,
        expected_attempt_id: str,
        expected_semantic_epoch: int,
    ) -> OwnershipHandoffResult:
        """Durably prepare an exact ownership-handoff intent.

        Preparation does not release a lease, mutate Claims, call a Harness,
        or mutate VPG.  It is safe to retry and gives crash recovery a durable
        witness before the release-first compatibility path is entered.
        """

        graph = str(graph_id).strip()
        task = str(task_id).strip()
        source_id = str(source_claim_id).strip()
        replacement_agent = str(replacement_agent_id).strip()
        normalized_action = str(action).strip().lower()
        normalized_reason = str(reason).strip() or "semantic handoff"
        if normalized_action not in {"preempt", "rebase"}:
            raise ValueError("handoff action must be preempt or rebase")
        if not graph or not task or not source_id or not replacement_agent:
            raise ValueError(
                "graph_id, task_id, source_claim_id, and replacement_agent_id must be non-empty"
            )
        if not str(expected_attempt_id).strip():
            raise ValueError("expected_attempt_id must be non-empty")
        if (
            isinstance(expected_semantic_epoch, bool)
            or not isinstance(expected_semantic_epoch, int)
            or expected_semantic_epoch < 0
        ):
            raise ValueError("expected_semantic_epoch must be a non-negative integer")
        handoff = str(handoff_id or "").strip()
        if not handoff:
            handoff = f"handoff:{graph}:{task}:{source_id}:{replacement_agent}:{normalized_action}"

        with self._schedule_lock:
            source = next(
                (
                    claim
                    for claim in self._claims
                    if claim.claim_id == source_id
                    and claim.graph_id == graph
                    and claim.task_id == task
                ),
                None,
            )
            if source is None:
                # A prior durable intent may still be recoverable after a
                # projection reload.  Build a minimal identity only when an
                # existing event proves the original intent.
                existing_events = self._handoff_intent_events_locked(handoff)
                if existing_events:
                    prior = self._handoff_intent_from_event(existing_events[0])
                    requested = prior.model_copy(
                        update={
                            "graph_id": graph,
                            "task_id": task,
                            "source_claim_id": source_id,
                            "replacement_agent_id": replacement_agent,
                            "action": normalized_action,
                        }
                    )
                    existing = self._handoff_existing_locked(requested)
                    if existing is not None:
                        return existing
                raise ValueError("source Claim was not found for handoff preparation")
            if source.state is not ClaimState.ACTIVE:
                raise ValueError("source Claim must be ACTIVE for handoff preparation")
            attempt = self._attempts_.latest_attempt_for_claim(source.claim_id)
            if attempt is None:
                raise ValueError("source Claim has no execution Attempt")
            if attempt.attempt_id != str(expected_attempt_id).strip():
                raise ValueError("source Attempt identity mismatch")
            if attempt.semantic_epoch != expected_semantic_epoch:
                raise ValueError("source semantic epoch mismatch")
            if not source.lease_id:
                raise ValueError("source Claim has no live lease identity")
            intent = OwnershipHandoffIntent(
                handoff_id=handoff,
                graph_id=graph,
                task_id=task,
                source_claim_id=source.claim_id,
                source_attempt_id=attempt.attempt_id,
                source_agent_id=source.agent_id,
                replacement_agent_id=replacement_agent,
                source_graph_version=source.graph_version,
                source_semantic_epoch=attempt.semantic_epoch,
                source_fencing_token=source.lease_fencing_token,
                source_lease_id=source.lease_id,
                action=cast(Literal["preempt", "rebase"], normalized_action),
                reason=normalized_reason,
            )
            existing = self._handoff_existing_locked(intent)
            if existing is not None:
                return existing
            from .events import SchedulerEventType, record_event

            self._record_event(
                record_event(
                    event_type=SchedulerEventType.TASK_HANDOFF_PREPARED,
                    graph_id=graph,
                    task_id=task,
                    agent_id=replacement_agent,
                    claim_id=source.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=source.graph_version,
                    reason=normalized_reason,
                    metadata={
                        "schema_version": intent.schema_version,
                        "handoff_id": handoff,
                        "phase": OwnershipHandoffPhase.PREPARED.value,
                        "intent_fingerprint": intent.fingerprint(),
                        "intent": intent.model_dump(mode="json"),
                    },
                )
            )
            return OwnershipHandoffResult(
                intent=intent,
                phase=OwnershipHandoffPhase.PREPARED,
                status=OwnershipHandoffStatus.PREPARED,
                source_lease_released=False,
                reason="handoff intent prepared durably",
            )

    def commit_handoff(
        self,
        intent: OwnershipHandoffIntent | OwnershipHandoffResult,
    ) -> OwnershipHandoffResult:
        """Commit a prepared intent through the existing fenced handoff path.

        This is a bounded compatibility transaction: the durable COMMITTING
        marker is written before the release-first path, and the COMMITTED
        marker is written only after replacement admission succeeds.  A crash
        between those markers is recoverable but remains ``IN_DOUBT``.
        """

        if isinstance(intent, OwnershipHandoffResult):
            intent = intent.intent
        if not isinstance(intent, OwnershipHandoffIntent):
            intent = OwnershipHandoffIntent.model_validate(intent)
        with self._schedule_lock:
            existing = self._handoff_existing_locked(intent)
            if existing is not None and existing.status in {
                OwnershipHandoffStatus.REPLAYED,
                OwnershipHandoffStatus.COMMITTED,
                OwnershipHandoffStatus.FAILED_CLOSED,
                OwnershipHandoffStatus.IN_DOUBT,
                OwnershipHandoffStatus.ABORTED,
            }:
                return existing
            if existing is None:
                # Require an explicit prepare witness.  Do not silently create
                # a transaction at commit time.
                return OwnershipHandoffResult(
                    intent=intent,
                    phase=OwnershipHandoffPhase.FAILED_CLOSED,
                    status=OwnershipHandoffStatus.REFUSED,
                    reason="handoff commit requires a durable PREPARED intent",
                )
            from .events import SchedulerEventType, record_event

            self._record_event(
                record_event(
                    event_type=SchedulerEventType.TASK_HANDOFF_COMMITTING,
                    graph_id=intent.graph_id,
                    task_id=intent.task_id,
                    agent_id=intent.replacement_agent_id,
                    claim_id=intent.source_claim_id,
                    attempt_id=intent.source_attempt_id,
                    graph_version=intent.source_graph_version,
                    reason=intent.reason,
                    metadata={
                        "schema_version": intent.schema_version,
                        "handoff_id": intent.handoff_id,
                        "phase": OwnershipHandoffPhase.COMMITTING.value,
                        "intent_fingerprint": intent.fingerprint(),
                        "intent": intent.model_dump(mode="json"),
                    },
                )
            )
            try:
                claim_result = self.handoff_task(
                    intent.graph_id,
                    intent.task_id,
                    source_claim_id=intent.source_claim_id,
                    replacement_agent_id=intent.replacement_agent_id,
                    action=intent.action,
                    reason=intent.reason,
                    handoff_id=intent.handoff_id,
                    expected_attempt_id=intent.source_attempt_id,
                    expected_semantic_epoch=intent.source_semantic_epoch,
                )
            except Exception as exc:
                result = OwnershipHandoffResult(
                    intent=intent,
                    phase=OwnershipHandoffPhase.FAILED_CLOSED,
                    status=OwnershipHandoffStatus.FAILED_CLOSED,
                    source_lease_released=None,
                    reason=f"handoff commit raised: {type(exc).__name__}: {exc}",
                )
                self._record_event(
                    record_event(
                        event_type=SchedulerEventType.TASK_HANDOFF_RECOVERY,
                        graph_id=intent.graph_id,
                        task_id=intent.task_id,
                        agent_id=intent.replacement_agent_id,
                        claim_id=intent.source_claim_id,
                        attempt_id=intent.source_attempt_id,
                        graph_version=intent.source_graph_version,
                        reason=result.reason,
                        metadata={
                            "schema_version": intent.schema_version,
                            "handoff_id": intent.handoff_id,
                            "phase": result.phase.value,
                            "status": result.status.value,
                            "source_lease_released": None,
                            "intent_fingerprint": intent.fingerprint(),
                            "intent": intent.model_dump(mode="json"),
                        },
                    )
                )
                return result
            status = (
                OwnershipHandoffStatus.COMMITTED
                if claim_result.status
                in {ClaimHandoffStatus.TRANSFERRED, ClaimHandoffStatus.REPLAYED}
                else OwnershipHandoffStatus.FAILED_CLOSED
                if claim_result.status is ClaimHandoffStatus.FAILED_CLOSED
                else OwnershipHandoffStatus.REFUSED
            )
            phase = (
                OwnershipHandoffPhase.COMMITTED
                if status is OwnershipHandoffStatus.COMMITTED
                else OwnershipHandoffPhase.FAILED_CLOSED
                if status is OwnershipHandoffStatus.FAILED_CLOSED
                else OwnershipHandoffPhase.ABORTED
            )
            result = OwnershipHandoffResult(
                intent=intent,
                phase=phase,
                status=status,
                source_lease_released=claim_result.status
                in {
                    ClaimHandoffStatus.TRANSFERRED,
                    ClaimHandoffStatus.REPLAYED,
                }
                or (
                    claim_result.status is ClaimHandoffStatus.FAILED_CLOSED
                    and "source lease release" not in claim_result.reason
                ),
                replacement_claim_id=claim_result.replacement_claim_id,
                replacement_attempt_id=claim_result.replacement_attempt_id,
                claim_result=claim_result,
                reason=claim_result.reason,
            )
            event_type = (
                SchedulerEventType.TASK_HANDOFF_COMMITTED
                if phase is OwnershipHandoffPhase.COMMITTED
                else SchedulerEventType.TASK_HANDOFF_RECOVERY
            )
            self._record_event(
                record_event(
                    event_type=event_type,
                    graph_id=intent.graph_id,
                    task_id=intent.task_id,
                    agent_id=intent.replacement_agent_id,
                    claim_id=intent.source_claim_id,
                    attempt_id=intent.source_attempt_id,
                    graph_version=intent.source_graph_version,
                    reason=result.reason,
                    metadata={
                        "schema_version": intent.schema_version,
                        "handoff_id": intent.handoff_id,
                        "phase": phase.value,
                        "status": status.value,
                        "source_lease_released": result.source_lease_released,
                        "intent_fingerprint": intent.fingerprint(),
                        "intent": intent.model_dump(mode="json"),
                        "claim_result": (
                            claim_result.model_dump(mode="json")
                            if phase is OwnershipHandoffPhase.COMMITTED
                            else None
                        ),
                    },
                )
            )
            return result

    def recover_handoff(self, handoff_id: str) -> OwnershipHandoffResult:
        """Recover one interrupted intent without guessing ownership.

        A prepared-but-never-committing intent is safely aborted.  A
        COMMITTING intent is committed only when a matching replacement Claim
        is already durable; otherwise the method returns ``IN_DOUBT`` or
        ``FAILED_CLOSED`` and leaves all live ownership untouched.
        """

        handoff = str(handoff_id).strip()
        if not handoff:
            raise ValueError("handoff_id must be non-empty")
        with self._schedule_lock:
            events = self._handoff_intent_events_locked(handoff)
            if not events:
                raise ValueError("handoff intent was not found")
            intent = self._handoff_intent_from_event(events[0])
            existing = self._handoff_existing_locked(intent)
            if existing is None:
                raise ValueError("handoff intent history is incomplete")
            if existing.status in {
                OwnershipHandoffStatus.REPLAYED,
                OwnershipHandoffStatus.COMMITTED,
                OwnershipHandoffStatus.ABORTED,
                OwnershipHandoffStatus.FAILED_CLOSED,
                OwnershipHandoffStatus.IN_DOUBT,
            }:
                return existing
            from .events import SchedulerEventType, record_event

            # No COMMITTING marker means no ownership mutation was authorized.
            if existing.phase is OwnershipHandoffPhase.PREPARED:
                result = OwnershipHandoffResult(
                    intent=intent,
                    phase=OwnershipHandoffPhase.ABORTED,
                    status=OwnershipHandoffStatus.ABORTED,
                    source_lease_released=False,
                    reason="prepared handoff aborted during recovery",
                )
                self._record_event(
                    record_event(
                        event_type=SchedulerEventType.TASK_HANDOFF_RECOVERY,
                        graph_id=intent.graph_id,
                        task_id=intent.task_id,
                        agent_id=intent.replacement_agent_id,
                        claim_id=intent.source_claim_id,
                        attempt_id=intent.source_attempt_id,
                        graph_version=intent.source_graph_version,
                        reason=result.reason,
                        metadata={
                            "schema_version": intent.schema_version,
                            "handoff_id": intent.handoff_id,
                            "phase": result.phase.value,
                            "status": result.status.value,
                            "source_lease_released": False,
                            "intent_fingerprint": intent.fingerprint(),
                            "intent": intent.model_dump(mode="json"),
                        },
                    )
                )
                return result

            replacement = next(
                (
                    claim
                    for claim in self._claims
                    if claim.handoff_id == intent.handoff_id
                    and claim.graph_id == intent.graph_id
                    and claim.task_id == intent.task_id
                ),
                None,
            )
            if replacement is not None and replacement.state in {
                ClaimState.ACTIVE,
                ClaimState.COMPLETED,
            }:
                attempt = self._attempts_.latest_attempt_for_claim(replacement.claim_id)
                claim_result = ClaimHandoffResult(
                    handoff_id=intent.handoff_id,
                    status=ClaimHandoffStatus.REPLAYED,
                    graph_id=intent.graph_id,
                    task_id=intent.task_id,
                    source_claim_id=intent.source_claim_id,
                    source_attempt_id=intent.source_attempt_id,
                    replacement_agent_id=intent.replacement_agent_id,
                    replacement_claim_id=replacement.claim_id,
                    replacement_attempt_id=attempt.attempt_id if attempt else None,
                    source_fencing_token=intent.source_fencing_token,
                    replacement_fencing_token=replacement.lease_fencing_token,
                    action=intent.action,
                    reason="replacement Claim proves handoff commit",
                )
                result = OwnershipHandoffResult(
                    intent=intent,
                    phase=OwnershipHandoffPhase.COMMITTED,
                    status=OwnershipHandoffStatus.COMMITTED,
                    source_lease_released=True,
                    replacement_claim_id=replacement.claim_id,
                    replacement_attempt_id=attempt.attempt_id if attempt else None,
                    claim_result=claim_result,
                    reason=claim_result.reason,
                )
                self._record_event(
                    record_event(
                        event_type=SchedulerEventType.TASK_HANDOFF_COMMITTED,
                        graph_id=intent.graph_id,
                        task_id=intent.task_id,
                        agent_id=intent.replacement_agent_id,
                        claim_id=intent.source_claim_id,
                        attempt_id=intent.source_attempt_id,
                        graph_version=intent.source_graph_version,
                        reason=result.reason,
                        metadata={
                            "schema_version": intent.schema_version,
                            "handoff_id": intent.handoff_id,
                            "phase": result.phase.value,
                            "status": result.status.value,
                            "source_lease_released": True,
                            "intent_fingerprint": intent.fingerprint(),
                            "intent": intent.model_dump(mode="json"),
                            "claim_result": claim_result.model_dump(mode="json"),
                        },
                    )
                )
                return result

            source = next(
                (claim for claim in self._claims if claim.claim_id == intent.source_claim_id),
                None,
            )
            if source is not None and source.state is ClaimState.ACTIVE:
                lease = self._lease_lookup_for_claim(source)
                if lease is not None:
                    result = OwnershipHandoffResult(
                        intent=intent,
                        phase=OwnershipHandoffPhase.ABORTED,
                        status=OwnershipHandoffStatus.ABORTED,
                        source_lease_released=False,
                        reason="COMMITTING intent found with intact source lease; aborted",
                    )
                else:
                    result = OwnershipHandoffResult(
                        intent=intent,
                        phase=OwnershipHandoffPhase.FAILED_CLOSED,
                        status=OwnershipHandoffStatus.IN_DOUBT,
                        source_lease_released=None,
                        reason="source Claim is ACTIVE but its Kernel lease is missing",
                    )
            else:
                result = OwnershipHandoffResult(
                    intent=intent,
                    phase=OwnershipHandoffPhase.FAILED_CLOSED,
                    status=OwnershipHandoffStatus.IN_DOUBT,
                    source_lease_released=None,
                    reason="COMMITTING intent has neither a live source nor replacement Claim",
                )
            self._record_event(
                record_event(
                    event_type=SchedulerEventType.TASK_HANDOFF_RECOVERY,
                    graph_id=intent.graph_id,
                    task_id=intent.task_id,
                    agent_id=intent.replacement_agent_id,
                    claim_id=intent.source_claim_id,
                    attempt_id=intent.source_attempt_id,
                    graph_version=intent.source_graph_version,
                    reason=result.reason,
                    metadata={
                        "schema_version": intent.schema_version,
                        "handoff_id": intent.handoff_id,
                        "phase": result.phase.value,
                        "status": result.status.value,
                        "source_lease_released": result.source_lease_released,
                        "intent_fingerprint": intent.fingerprint(),
                        "intent": intent.model_dump(mode="json"),
                    },
                )
            )
            return result

    def handoff_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        source_claim_id: str,
        replacement_agent_id: str,
        action: str = "rebase",
        reason: str = "semantic handoff",
        handoff_id: str | None = None,
        expected_attempt_id: str,
        expected_semantic_epoch: int,
    ) -> ClaimHandoffResult:
        """Perform a bounded, exact-identity claim handoff.

        This is intentionally **not** a cross-service transaction.  The
        source Kernel lease is released first, then a replacement claim is
        admitted through the normal VPG/eligibility/resource/lease gates.  If
        replacement admission fails, the method returns ``FAILED_CLOSED``:
        there is no active source or replacement claim, and an old worker can
        never release a future claim because all release paths are fenced by
        ``source_claim_id``.
        """
        from .events import SchedulerEventType, record_event

        graph = str(graph_id).strip()
        task = str(task_id).strip()
        source_id = str(source_claim_id).strip()
        replacement_agent = str(replacement_agent_id).strip()
        normalized_action = str(action).strip().lower()
        normalized_reason = str(reason).strip() or "semantic handoff"
        if normalized_action not in {"preempt", "rebase"}:
            raise ValueError("handoff action must be preempt or rebase")
        if not graph or not task or not source_id or not replacement_agent:
            raise ValueError(
                "graph_id, task_id, source_claim_id, and replacement_agent_id must be non-empty"
            )
        if not str(expected_attempt_id).strip():
            raise ValueError("expected_attempt_id must be non-empty")
        if (
            isinstance(expected_semantic_epoch, bool)
            or not isinstance(expected_semantic_epoch, int)
            or expected_semantic_epoch < 0
        ):
            raise ValueError("expected_semantic_epoch must be a non-negative integer")
        if handoff_id is None:
            handoff_id = (
                f"handoff:{graph}:{task}:{source_id}:{replacement_agent}:{normalized_action}"
            )
        handoff = str(handoff_id).strip()
        if not handoff:
            raise ValueError("handoff_id must be non-empty")

        with self._schedule_lock:
            # ``handoff_id`` is a durable idempotency identity and therefore
            # must not be reused across graph/task scopes.  Check both the
            # claim projection and reassignment journal before touching the
            # source lease.  This also covers a torn release-first handoff
            # which has an audit event but no replacement Claim yet.
            handoff_claims = [claim for claim in self._claims if claim.handoff_id == handoff]
            foreign_claim = next(
                (
                    claim
                    for claim in handoff_claims
                    if claim.graph_id != graph or claim.task_id != task
                ),
                None,
            )
            if foreign_claim is not None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=str(expected_attempt_id),
                    replacement_agent_id=replacement_agent,
                    replacement_claim_id=foreign_claim.claim_id,
                    action=normalized_action,
                    reason=(
                        "handoff idempotency key is already bound to another "
                        f"graph/task ({foreign_claim.graph_id}/{foreign_claim.task_id})"
                    ),
                )
            if len(handoff_claims) > 1:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=str(expected_attempt_id),
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason=("handoff idempotency key has duplicate Claim projection rows"),
                )
            from .events import SchedulerEventType

            foreign_event = next(
                (
                    event
                    for event in self._events
                    if getattr(event, "event_type", None)
                    in {
                        SchedulerEventType.TASK_REASSIGNMENT_STARTED,
                        SchedulerEventType.TASK_REASSIGNED,
                    }
                    and getattr(event, "metadata", {}).get("handoff_id") == handoff
                    and (
                        getattr(event, "graph_id", "") != graph
                        or getattr(event, "task_id", "") != task
                    )
                ),
                None,
            )
            if foreign_event is not None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=str(expected_attempt_id),
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason=(
                        "handoff idempotency key is already bound to another "
                        "graph/task in the reassignment journal"
                    ),
                )
            same_scope_event = next(
                (
                    event
                    for event in self._events
                    if getattr(event, "event_type", None)
                    in {
                        SchedulerEventType.TASK_REASSIGNMENT_STARTED,
                        SchedulerEventType.TASK_REASSIGNED,
                    }
                    and getattr(event, "metadata", {}).get("handoff_id") == handoff
                    and getattr(event, "graph_id", "") == graph
                    and getattr(event, "task_id", "") == task
                ),
                None,
            )
            if same_scope_event is not None and not handoff_claims:
                # A durable reassignment record without its replacement Claim
                # is a torn release-first handoff.  Never retry by releasing
                # whichever source happens to be active now; reconciliation
                # must first determine the authoritative ownership outcome.
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=str(expected_attempt_id),
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason=(
                        "handoff reassignment record exists without a replacement Claim projection"
                    ),
                )

            # Idempotent replay: a replacement created by this handoff is the
            # only authoritative replay witness. Never return a different
            # claim merely because the source task has since been reassigned.
            existing_replacement = handoff_claims[0] if handoff_claims else None
            if existing_replacement is not None:
                # ``handoff_id`` is an idempotency key, not an ownership
                # capability.  Reusing it with a different source/attempt,
                # replacement agent, or action must not silently replay the
                # old transfer: that could make a stale caller believe it
                # owns a different handoff.  Recover the original request
                # identity from the durable reassignment audit event before
                # returning REPLAYED/FAILED_CLOSED.
                recorded_started = next(
                    (
                        event
                        for event in reversed(self._events)
                        if getattr(event, "event_type", None)
                        is SchedulerEventType.TASK_REASSIGNMENT_STARTED
                        and getattr(event, "metadata", {}).get("handoff_id") == handoff
                    ),
                    None,
                )
                recorded_finished = next(
                    (
                        event
                        for event in reversed(self._events)
                        if getattr(event, "event_type", None) is SchedulerEventType.TASK_REASSIGNED
                        and getattr(event, "metadata", {}).get("handoff_id") == handoff
                    ),
                    None,
                )
                if recorded_started is None:
                    # A replacement projection without its immutable
                    # reassignment-start record is not enough to authenticate
                    # a replay request.  Do not guess from the handoff id or
                    # replacement row; fail closed and leave ownership
                    # untouched so reconciliation can inspect the torn state.
                    return ClaimHandoffResult(
                        handoff_id=handoff,
                        status=ClaimHandoffStatus.FAILED_CLOSED,
                        graph_id=graph,
                        task_id=task,
                        source_claim_id=source_id,
                        source_attempt_id=str(expected_attempt_id),
                        replacement_agent_id=replacement_agent,
                        replacement_claim_id=existing_replacement.claim_id,
                        action=normalized_action,
                        reason=(
                            "handoff replacement exists without an authoritative "
                            "reassignment-start record"
                        ),
                    )
                recorded_source_claim = (
                    str(getattr(recorded_started, "claim_id", "") or "")
                    if recorded_started is not None
                    else ""
                )
                recorded_source_attempt = (
                    str(getattr(recorded_started, "attempt_id", "") or "")
                    if recorded_started is not None
                    else ""
                )
                recorded_agent = (
                    str(getattr(recorded_started, "agent_id", "") or "")
                    if recorded_started is not None
                    else str(getattr(existing_replacement, "agent_id", "") or "")
                )
                recorded_action = ""
                recorded_semantic_epoch: int | None = None
                for event in (recorded_started, recorded_finished):
                    if event is not None:
                        event_metadata = getattr(event, "metadata", {}) or {}
                        candidate_action = (
                            str(event_metadata.get("action", "") or "").strip().lower()
                        )
                        if candidate_action:
                            recorded_action = candidate_action
                        candidate_epoch = event_metadata.get("source_semantic_epoch")
                        if isinstance(candidate_epoch, int) and not isinstance(
                            candidate_epoch, bool
                        ):
                            recorded_semantic_epoch = candidate_epoch
                replay_conflicts: list[str] = []
                if recorded_source_claim and recorded_source_claim != source_id:
                    replay_conflicts.append("source_claim_id")
                if recorded_source_attempt and recorded_source_attempt != str(expected_attempt_id):
                    replay_conflicts.append("expected_attempt_id")
                if recorded_agent and recorded_agent != replacement_agent:
                    replay_conflicts.append("replacement_agent_id")
                if recorded_action and recorded_action != normalized_action:
                    replay_conflicts.append("action")
                if (
                    recorded_semantic_epoch is not None
                    and recorded_semantic_epoch != expected_semantic_epoch
                ):
                    replay_conflicts.append("expected_semantic_epoch")
                if replay_conflicts:
                    return ClaimHandoffResult(
                        handoff_id=handoff,
                        status=ClaimHandoffStatus.REFUSED,
                        graph_id=graph,
                        task_id=task,
                        source_claim_id=source_id,
                        source_attempt_id=str(expected_attempt_id),
                        replacement_agent_id=replacement_agent,
                        replacement_claim_id=existing_replacement.claim_id,
                        action=normalized_action,
                        reason=(
                            "handoff idempotency key already bound to a different "
                            "request: " + ", ".join(replay_conflicts)
                        ),
                    )
                replacement_attempt = self._attempts_.latest_attempt_for_claim(
                    existing_replacement.claim_id
                )
                if existing_replacement.state not in {
                    ClaimState.ACTIVE,
                    ClaimState.COMPLETED,
                }:
                    return ClaimHandoffResult(
                        handoff_id=handoff,
                        status=ClaimHandoffStatus.FAILED_CLOSED,
                        graph_id=graph,
                        task_id=task,
                        source_claim_id=source_id,
                        source_attempt_id=expected_attempt_id or "",
                        replacement_agent_id=existing_replacement.agent_id,
                        replacement_claim_id=existing_replacement.claim_id,
                        replacement_attempt_id=(
                            replacement_attempt.attempt_id if replacement_attempt else None
                        ),
                        replacement_fencing_token=existing_replacement.lease_fencing_token,
                        action=normalized_action,
                        reason="handoff replacement exists but is not live",
                    )
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REPLAYED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=expected_attempt_id or "",
                    replacement_agent_id=existing_replacement.agent_id,
                    replacement_claim_id=existing_replacement.claim_id,
                    replacement_attempt_id=(
                        replacement_attempt.attempt_id if replacement_attempt else None
                    ),
                    source_fencing_token=None,
                    replacement_fencing_token=existing_replacement.lease_fencing_token,
                    action=normalized_action,
                    reason="handoff already materialized",
                )

            source = next(
                (
                    claim
                    for claim in self._claims
                    if claim.claim_id == source_id
                    and claim.graph_id == graph
                    and claim.task_id == task
                ),
                None,
            )
            if source is None or source.state != ClaimState.ACTIVE:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=expected_attempt_id or "",
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="source claim is not ACTIVE",
                )
            attempt = self._attempts_.latest_attempt_for_claim(source.claim_id)
            if attempt is None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id="",
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="source claim has no execution attempt",
                )
            if expected_attempt_id is not None and attempt.attempt_id != expected_attempt_id:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="source attempt identity mismatch",
                )
            if (
                expected_semantic_epoch is not None
                and attempt.semantic_epoch != expected_semantic_epoch
            ):
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="source semantic epoch mismatch",
                )
            if attempt.state.value not in {
                "dispatched",
                "running",
                "succeeded_operationally",
                "stale_cognition",
            }:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason=f"source attempt is not handoffable ({attempt.state.value})",
                )
            if not source.lease_id:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="source ACTIVE claim has no lease id",
                )
            replacement = self._registry.get(replacement_agent)
            if replacement is None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="replacement agent is not registered",
                )

            # Validate the replacement against the same typed eligibility
            # contract used by schedule_once *before* releasing the source.
            # This prevents an obvious capability/concurrency rejection from
            # destructively preempting useful work.
            current_version = self._vpg.current_graph_version(graph)
            try:
                frontier = list(self._vpg.ready_frontier(graph))
            except Exception:
                frontier = []
            candidate = next((item for item in frontier if item.task_id == task), None)
            if candidate is None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="task is not currently READY/repair-ready",
                )
            payload = self._vpg.task_node_payload(graph, task)
            if payload is None:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="task payload missing",
                )
            req = decode_task_requirements(task, payload)
            active_by_agent = active_claim_count_by_agent(self._claims)
            if replacement_agent == source.agent_id:
                active_by_agent[replacement_agent] = max(
                    0,
                    active_by_agent.get(replacement_agent, 0) - 1,
                )
            eligibility = evaluate_eligibility(
                replacement,
                task,
                graph,
                current_version,
                task_kind=req.task_kind,
                required_specializations=req.required_specializations,
                required_tools=req.required_tools,
                required_capabilities=req.required_capabilities,
                readiness_version=getattr(
                    getattr(candidate, "readiness_proof", None),
                    "graph_version",
                    current_version,
                ),
                active_claims_for_agent=active_by_agent.get(replacement_agent, 0),
                process_state=getattr(
                    self._process.get(replacement.process_id),
                    "state",
                    None,
                ),
                process_exists=self._process.get(replacement.process_id) is not None,
                capability_checker=self._cap,
            )
            if not eligibility.eligible:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.REFUSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    action=normalized_action,
                    reason="replacement agent ineligible: " + "; ".join(eligibility.reasons),
                )
            if replacement_agent != source.agent_id:
                shortages = self._resource_manager.shortages(
                    replacement_agent,
                    req.resources,
                )
                if shortages:
                    return ClaimHandoffResult(
                        handoff_id=handoff,
                        status=ClaimHandoffStatus.REFUSED,
                        graph_id=graph,
                        task_id=task,
                        source_claim_id=source_id,
                        source_attempt_id=attempt.attempt_id,
                        replacement_agent_id=replacement_agent,
                        action=normalized_action,
                        reason=f"replacement resources unavailable: {shortages}",
                    )

            source_token = source.lease_fencing_token
            # Release the exact authoritative lease before mutating the
            # projection. A refusal/error leaves the source claim untouched.
            try:
                released = self._leases.release(source.lease_id)
            except Exception as exc:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    source_fencing_token=source_token,
                    action=normalized_action,
                    reason=f"source lease release failed: {exc}",
                )
            if not released:
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    source_fencing_token=source_token,
                    action=normalized_action,
                    reason="source lease release was not confirmed",
                )

            # The source epoch is now fenced at the Kernel. Mark it terminal
            # only after release succeeds; old callbacks remain unable to
            # release a replacement because they carry source_claim_id.
            if normalized_action == "rebase":
                self._attempts_.mark_stale_cognition(attempt, error=normalized_reason)
            else:
                self._attempts_.mark_preempted(attempt, error=normalized_reason)
            source.state = ClaimState.RELEASED
            source.released_at = self._clock()
            source.reason = normalized_reason
            self._release_claim_resources(source)
            self._record_event(
                record_event(
                    SchedulerEventType.TASK_REASSIGNMENT_STARTED,
                    graph_id=graph,
                    task_id=task,
                    agent_id=replacement_agent,
                    claim_id=source_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=source.graph_version,
                    reason=normalized_reason,
                    metadata={
                        "handoff_id": handoff,
                        "action": normalized_action,
                        "source_semantic_epoch": attempt.semantic_epoch,
                        "source_fencing_token": source_token,
                    },
                )
            )
            # Admission is deliberately delegated to the normal fenced path.
            self._clear_task_idempotency(graph, task)
            replacement_claim_id = f"claim-{handoff}"
            attempt_number = sum(
                1 for claim in self._claims if claim.graph_id == graph and claim.task_id == task
            )
            try:
                admitted = self._acquire_claim(
                    graph_id=graph,
                    task_id=task,
                    graph_version=current_version,
                    claim_id=replacement_claim_id,
                    agent_id=replacement_agent,
                    attempt_number=attempt_number,
                    semantic_epoch=self._semantic_epoch(payload, current_version),
                    resources=req.resources,
                    handoff_id=handoff,
                )
            except Exception as exc:
                # The source epoch is already fenced. Never surface an
                # ambiguous exception as if ownership were still active.
                self._record_event(
                    record_event(
                        SchedulerEventType.TASK_REASSIGNED,
                        graph_id=graph,
                        task_id=task,
                        agent_id=replacement_agent,
                        claim_id=source_id,
                        graph_version=current_version,
                        reason="handoff failed closed: replacement raised",
                        metadata={
                            "handoff_id": handoff,
                            "action": normalized_action,
                            "error": str(exc)[:512],
                        },
                    )
                )
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    source_fencing_token=source_token,
                    action=normalized_action,
                    reason=f"replacement admission raised: {exc}",
                )
            if not admitted:
                self._record_event(
                    record_event(
                        SchedulerEventType.TASK_REASSIGNED,
                        graph_id=graph,
                        task_id=task,
                        agent_id=replacement_agent,
                        claim_id=source_id,
                        graph_version=current_version,
                        reason="handoff failed closed: replacement admission refused",
                        metadata={"handoff_id": handoff, "action": normalized_action},
                    )
                )
                return ClaimHandoffResult(
                    handoff_id=handoff,
                    status=ClaimHandoffStatus.FAILED_CLOSED,
                    graph_id=graph,
                    task_id=task,
                    source_claim_id=source_id,
                    source_attempt_id=attempt.attempt_id,
                    replacement_agent_id=replacement_agent,
                    source_fencing_token=source_token,
                    action=normalized_action,
                    reason="replacement admission refused",
                )
            new_claim = next(
                (claim for claim in self._claims if claim.claim_id == replacement_claim_id),
                None,
            )
            new_attempt = self._attempts_.latest_attempt_for_claim(replacement_claim_id)
            self._record_event(
                record_event(
                    SchedulerEventType.TASK_REASSIGNED,
                    graph_id=graph,
                    task_id=task,
                    agent_id=replacement_agent,
                    claim_id=replacement_claim_id,
                    attempt_id=new_attempt.attempt_id if new_attempt else "",
                    graph_version=current_version,
                    reason="handoff replacement admitted",
                    metadata={
                        "handoff_id": handoff,
                        "action": normalized_action,
                        "source_claim_id": source_id,
                    },
                )
            )
            return ClaimHandoffResult(
                handoff_id=handoff,
                status=ClaimHandoffStatus.TRANSFERRED,
                graph_id=graph,
                task_id=task,
                source_claim_id=source_id,
                source_attempt_id=attempt.attempt_id,
                replacement_agent_id=replacement_agent,
                replacement_claim_id=new_claim.claim_id if new_claim else replacement_claim_id,
                replacement_attempt_id=new_attempt.attempt_id if new_attempt else None,
                source_fencing_token=source_token,
                replacement_fencing_token=(new_claim.lease_fencing_token if new_claim else None),
                action=normalized_action,
                reason="replacement admitted",
            )

    def release_claim(self, claim: TaskClaim, reason: str = "released") -> None:
        with self._schedule_lock:
            self._release_claim_locked(claim, reason=reason)

    def _release_claim_locked(self, claim: TaskClaim, reason: str = "released") -> None:
        from .events import SchedulerEventType, record_event

        if claim.state in {ClaimState.RELEASED, ClaimState.LOST, ClaimState.COMPLETED}:
            return
        self._claims_.release(claim, reason=reason)
        try:
            self._record_event(
                record_event(
                    SchedulerEventType.CLAIM_RELEASED,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    reason=reason,
                )
            )
        finally:
            self._release_claim_resources(claim)

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
        expected_claim_id: str | None = None,
    ) -> bool:
        """Release Kernel-backed ownership after one operational attempt.

        VPG validity is deliberately untouched.  If the task is still present
        in the authoritative ready frontier, a later scheduling pass may issue
        a new claim and acquire a new Kernel lease.  When
        ``expected_claim_id`` is supplied, lookup, identity validation, and
        release occur under one lock so stale workers cannot release a
        replacement claim.
        """
        with self._schedule_lock:
            return self._release_task_locked(
                graph_id,
                task_id,
                reason=reason,
                retry=retry,
                expected_claim_id=expected_claim_id,
            )

    def _release_task_locked(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str,
        retry: bool,
        expected_claim_id: str | None,
    ) -> bool:
        from .events import SchedulerEventType, record_event

        claim = self._get_claim_locked(task_id, graph_id)
        if expected_claim_id is not None and (claim is None or claim.claim_id != expected_claim_id):
            return False
        released = False
        if claim is not None and claim.graph_id == graph_id:
            attempt = self.get_attempt_for_claim(claim.claim_id)
            if attempt is not None:
                preserve_stale = attempt.state.value == "stale_cognition"
                requested_stale = str(reason).strip().lower().startswith("stale_cognition")
                if not preserve_stale and requested_stale:
                    self._attempts_.mark_stale_cognition(attempt, error=reason)
                    preserve_stale = True
                elif not preserve_stale:
                    self._attempts_.mark_failed(attempt, error=reason)
                self._record_event(
                    record_event(
                        (
                            SchedulerEventType.EXECUTION_STALE_COGNITION
                            if preserve_stale
                            else SchedulerEventType.EXECUTION_FAILED
                        ),
                        graph_id=graph_id,
                        task_id=task_id,
                        agent_id=claim.agent_id,
                        claim_id=claim.claim_id,
                        attempt_id=attempt.attempt_id,
                        graph_version=claim.graph_version,
                        reason=reason,
                        metadata=({"preserved_stale_cognition": True} if preserve_stale else {}),
                    )
                )
            self._release_claim_locked(claim, reason=reason)
            released = True
        if retry:
            self._clear_task_idempotency(graph_id, task_id)
        return released

    # ── VPG observation hook ───────────────────────────────────────────────
    def observe_vpg(self, graph_id: str) -> dict[str, int]:
        """Poll VPG state and derive scheduler-side state transitions:
          - Task VERIFIED  -> owning ACTIVE claim COMPLETED.
        Returns a tally of derived transitions.
        """
        with self._schedule_lock:
            return self._observe_vpg_locked(graph_id)

    def _observe_vpg_locked(self, graph_id: str) -> dict[str, int]:
        tally: dict[str, int] = {"claims_completed": 0}
        for claim in list(self._claims):
            if claim.graph_id != graph_id:
                continue
            if claim.state != ClaimState.ACTIVE:
                continue
            validity = self._vpg.task_validity(graph_id, claim.task_id)
            if validity == "verified" and self._evidence_matches_active_attempt(claim):
                self._mark_task_completed_locked(claim)
                tally["claims_completed"] += 1
        return tally

    # ── reconciliation (Section 27) ────────────────────────────────────────
    def reconcile(self) -> ReconciliationResult:
        """Run reconciliation between Scheduler projection and authoritative
        Kernel + VPG state."""
        with self._schedule_lock:
            return self._reconcile_locked()

    def _reconcile_locked(self) -> ReconciliationResult:
        from .reconciliation import reconcile as _reconcile

        states_before = {claim.claim_id: claim.state for claim in self._claims}

        result = _reconcile(
            self._claims,
            self._attempts,
            lease_is_live=self._leases.is_lease_active,
            process_is_alive=self._process_is_alive,
            vpg_task_verified=self._vpg_task_verified,
            vpg_task_stale=self._vpg_task_stale,
            lease_lookup=self._lease_lookup_for_claim,
            release_lease=self._leases.release,
            clock_now=self._clock,
        )
        # Dispatch idempotency protects a live/finished ownership epoch, not a
        # failed one.  Once authoritative reconciliation moves a claim to
        # LOST, allow the still-ready task to acquire a fresh claim.
        for claim in self._claims:
            if (
                claim.state == ClaimState.LOST
                and states_before.get(claim.claim_id) != ClaimState.LOST
            ):
                self._release_claim_resources(claim)
                self._clear_task_idempotency(claim.graph_id, claim.task_id)
            elif (
                claim.state == ClaimState.COMPLETED
                and states_before.get(claim.claim_id) != ClaimState.COMPLETED
            ):
                self._release_claim_resources(claim)
        # Reconciliation mutates claims/attempts without emitting a scheduler
        # lifecycle event for every repair. Publish the resulting projection
        # so a restart observes the repaired state.
        self._persist_state()
        return result

    # ── administrative helpers ─────────────────────────────────────────────
    def _evaluate_eligibility_for_task(
        self,
        graph_id: str,
        graph_version: int,
        req: TaskRequirements,
        candidate: Any,
        active_by_agent: dict[str, int],
    ) -> list[EligibilityResult]:
        out: list[EligibilityResult] = []
        for agent in self._registry.list():
            proc = self._process.get(agent.process_id)
            exists = proc is not None
            state = getattr(proc, "state", None) if proc is not None else None
            result = evaluate_eligibility(
                agent,
                req.task_id,
                graph_id,
                graph_version,
                task_kind=req.task_kind,
                required_specializations=req.required_specializations,
                required_tools=req.required_tools,
                required_capabilities=req.required_capabilities,
                readiness_version=candidate.readiness_proof.graph_version,
                active_claims_for_agent=active_by_agent.get(agent.agent_id, 0),
                process_state=state,
                process_exists=exists,
                capability_checker=self._cap,
            )
            shortages = self._resource_manager.shortages(agent.agent_id, req.resources)
            if result.eligible and shortages:
                detail = ", ".join(f"{name}={amount}" for name, amount in shortages.items())
                result = result.model_copy(
                    update={
                        "eligible": False,
                        "reasons": (*result.reasons, f"insufficient resources: {detail}"),
                    }
                )
            out.append(result)
        return out

    def _process_is_alive(self, pid: str) -> bool:
        proc = self._process.get(pid)
        if proc is None:
            return False
        return getattr(proc, "state", None) not in ("exited", "failed")

    def _vpg_task_verified(
        self,
        graph_id: str,
        task_id: str,
        claim: TaskClaim | None = None,
    ) -> bool:
        """Return whether VPG verification belongs to the current claim.

        Reconciliation invokes this callback with a ``TaskClaim`` witness
        (see :func:`_invoke_task_predicate`).  A task can be ``verified`` in
        VPG because an older worker/attempt produced the evidence; that must
        not complete a newer ACTIVE claim.  The optional
        ``task_evidence_bindings`` adapter is implemented by the built-in
        VPG facade and is therefore treated as an authority boundary:
        absence of a matching claim/attempt/epoch binding fails closed.

        The two-argument form remains source-compatible for legacy callers
        that only ask about semantic task validity.
        """
        if self._vpg.task_validity(graph_id, task_id) != "verified":
            return False
        if claim is None:
            return True
        return self._evidence_matches_active_attempt(claim)

    def _vpg_task_stale(
        self,
        graph_id: str,
        task_id: str,
        claim: TaskClaim | None = None,
    ) -> bool:
        """Return whether a claim is invalidated by the current STALE epoch.

        STALE tasks can legitimately be in the ready frontier for repair.
        Such a newly dispatched repair claim must survive reconciliation;
        only ownership from an older semantic epoch is obsolete.
        """
        if self._vpg.task_validity(graph_id, task_id) != "stale":
            return False
        if claim is None:
            return True
        payload = self._vpg.task_node_payload(graph_id, task_id) or {}
        metadata = payload.get("metadata")
        stale_at = metadata.get("__stale_at_version") if isinstance(metadata, dict) else None
        if not isinstance(stale_at, int):
            # Legacy/plain STALE signals have no repair-epoch witness, so
            # conservatively reclaim the active ownership.
            return True
        attempt = self.get_attempt_for_claim(claim.claim_id)
        claim_epoch = attempt.semantic_epoch if attempt is not None else claim.graph_version
        return claim_epoch < stale_at

    def _lease_lookup_for_claim(self, claim: TaskClaim) -> Any | None:
        leases = self._leases.list_for_task(claim.graph_id, claim.task_id)
        if claim.lease_id is None:
            for lease in leases:
                if getattr(lease, "owner_pid", None) == claim.process_id:
                    return lease
            return None
        for lease in leases:
            if lease.lease_id == claim.lease_id:
                return lease
        return None

    @staticmethod
    def _claim_idempotency_key(graph_id: str, task_id: str, graph_version: int) -> str:
        return f"{graph_id}:{task_id}:v{graph_version}"

    @staticmethod
    def _semantic_epoch(payload: dict[str, Any], graph_version: int) -> int:
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            return graph_version
        stale_at = metadata.get("__stale_at_version")
        created_at = payload.get("created_in_version")
        if isinstance(stale_at, int):
            return stale_at
        if isinstance(created_at, int):
            return created_at
        return graph_version

    def _idependent_mark_idempotent(self, key: str) -> None:
        self._idempotent_keys.add(key)
        self._persist_state()

    def _clear_task_idempotency(self, graph_id: str, task_id: str) -> None:
        prefix = f"{graph_id}:{task_id}:v"
        self._idempotent_keys = {key for key in self._idempotent_keys if not key.startswith(prefix)}
        self._persist_state()
