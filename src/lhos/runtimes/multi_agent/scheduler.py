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

import threading
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

from .attempts import AttemptManager
from .durable_state import SchedulerStateCorruption, SchedulerStateStore
from .eligibility import evaluate_eligibility
from .lease_adapter import DEFAULT_CLAIM_TTL, claim_resource_uri
from .matching import match_deterministic_best_fit_v1
from .models import (
    TERMINAL_CLAIM_STATES,
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


class ScheduleResult:
    """Outcome of a single schedule pass."""

    def __init__(self) -> None:
        self.dispatched: list[dict[str, Any]] = []
        self.skipped: list[tuple[str, str]] = []  # (task_id, reason)
        self.idle: bool = True

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
        self._events: list[Any] = durable.events if durable is not None else []
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
        if self._state_store is not None:
            self._state_store.append_event(
                event,
                claims=self._claims,
                attempts=self._attempts,
                match_log=self._match_log,
                idempotent_keys=self._idempotent_keys,
            )
        self._events.append(event)
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
        self._events.extend(events)
        return events

    def _persist_state(self) -> None:
        """Persist a projection-only mutation when no event is emitted."""
        if self._state_store is not None:
            self._state_store.persist_state(
                claims=self._claims,
                attempts=self._attempts,
                match_log=self._match_log,
                idempotent_keys=self._idempotent_keys,
            )

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

    # ── scheduling pass ─────────────────────────────────────────────────────
    def schedule_once(
        self,
        graph_id: str,
        *,
        max_claims: int | None = None,
    ) -> ScheduleResult:
        with self._schedule_lock:
            if any(
                claim.graph_id == graph_id
                and claim.state in {ClaimState.PROPOSED, ClaimState.ACQUIRING}
                for claim in self._claims
            ):
                self._reconcile_locked()
            return self._schedule_once_locked(graph_id, max_claims=max_claims)

    def _schedule_once_locked(
        self,
        graph_id: str,
        *,
        max_claims: int | None = None,
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

        current_version = self._vpg.current_graph_version(graph_id)
        active_by_agent = active_claim_count_by_agent(self._claims)

        result = ScheduleResult()
        claims_this_pass = 0

        for candidate in frontier:
            task_id = candidate.task_id

            # Existing active claim: skip (per D2-I4 we never create a 2nd).
            existing = self.get_claim(task_id, graph_id)
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
            )
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
            attempt_number = sum(
                1
                for claim in self._claims
                if claim.graph_id == graph_id and claim.task_id == task_id
            )
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

        return result

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
        if attempt is not None:
            if attempt.state.value not in {
                "succeeded_operationally",
                "verified_semantically",
            }:
                self._attempts_.mark_operationally_succeeded(attempt)
            self._attempts_.mark_semantically_verified(attempt)
            self._record_event(
                record_event(
                    SchedulerEventType.EXECUTION_SEMANTICALLY_VERIFIED,
                    graph_id=claim.graph_id,
                    task_id=claim.task_id,
                    agent_id=claim.agent_id,
                    claim_id=claim.claim_id,
                    attempt_id=attempt.attempt_id,
                    graph_version=claim.graph_version,
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
                self._attempts_.mark_failed(attempt, error=reason)
                self._record_event(
                    record_event(
                        SchedulerEventType.EXECUTION_FAILED,
                        graph_id=graph_id,
                        task_id=task_id,
                        agent_id=claim.agent_id,
                        claim_id=claim.claim_id,
                        attempt_id=attempt.attempt_id,
                        graph_version=claim.graph_version,
                        reason=reason,
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
            if validity == "verified":
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

    def _vpg_task_verified(self, graph_id: str, task_id: str) -> bool:
        return bool(self._vpg.task_validity(graph_id, task_id) == "verified")

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
