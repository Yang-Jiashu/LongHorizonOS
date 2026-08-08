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

from datetime import UTC, datetime, timedelta
from typing import Any

from .eligibility import evaluate_eligibility
from .lease_adapter import DEFAULT_CLAIM_TTL, claim_resource_uri
from .matching import match_deterministic_best_fit_v1
from .models import (
    ClaimState,
    EligibilityResult,
    MatchDecision,
    ScheduledExecutionAttempt,
    TaskClaim,
    TaskRequirements,
)
from .projections import (
    active_claim_count_by_agent,
)
from .reconciliation import ReconciliationResult
from .requirements import decode_task_requirements


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
        self.dispatched.append(
            {"task_id": task_id, "agent_id": agent_id, "claim_id": claim_id}
        )
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
    ) -> None:
        from .claims import ClaimManager
        from .lease_adapter import LeaseAdapter

        self._registry = registry
        self._vpg = vpg
        self._process = process_provider
        self._leases = LeaseAdapter(lease_provider, ttl=lease_ttl)
        self._cap = capability_provider
        self._clock = clock
        self._idempotent_keys: set[str] = set(idempotent_keys or set())

        self._claims_ = ClaimManager(self._leases)
        self._claims: list[TaskClaim] = []
        self._attempts: list[ScheduledExecutionAttempt] = []
        self._match_log: list[MatchDecision] = []
        self._events: list[Any] = []

    # ── public query surface ───────────────────────────────────────────────
    @property
    def claims(self) -> list[TaskClaim]:
        return list(self._claims)

    @property
    def attempts(self) -> list[ScheduledExecutionAttempt]:
        return list(self._attempts)

    @property
    def match_log(self) -> list[MatchDecision]:
        return list(self._match_log)

    def get_claim(self, task_id: str) -> TaskClaim | None:
        for c in self._claims:
            if c.task_id == task_id and c.state == ClaimState.ACTIVE:
                return c
        return None

    # ── scheduling pass ─────────────────────────────────────────────────────
    def schedule_once(
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
            existing = self.get_claim(task_id)
            if existing is not None:
                result.skipped.append(
                    (task_id, f"active claim {existing.claim_id}")
                )
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

            # Idempotency key: graph+task+version+agent composite is the
            # canonical repeat-scheduling key (Section 30).
            idem_key = self._claim_idempotency_key(
                graph_id, task_id, current_version,
            )
            if idem_key in self._idempotent_keys:
                result.skipped.append((task_id, "idempotent replay"))
                continue

            # ── eligibility ────────────────────────────────────────────
            eligibility = self._evaluate_eligibility_for_task(
                graph_id, current_version, req, candidate, active_by_agent,
            )
            eligible_agents = [
                e for e in eligibility if e.eligible
            ]
            if not eligible_agents:
                reasons = tuple(
                    (e.agent_id, e.reason_text) for e in eligibility
                )
                result.skipped.append(
                    (task_id, f"no eligible agent; evaluated={reasons}")
                )
                self._events.append(
                    record_event(
                        SchedulerEventType.ELIGIBILITY_EVALUATED,
                        graph_id=graph_id,
                        task_id=task_id,
                        graph_version=current_version,
                        reason="no eligible agent",
                    )
                )
                continue

            agent_pool = [
                self._registry.get(e.agent_id) for e in eligible_agents
            ]
            agent_pool = [a for a in agent_pool if a is not None]
            if not agent_pool:
                result.skipped.append(
                    (task_id, "eligible agents disappeared")
                )
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
            )
            self._match_log.append(decision)
            self._events.append(
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

            # ── acquire exclusive kernel lease ───────────────────────────
            acquired = self._acquire_claim(
                graph_id=graph_id,
                task_id=task_id,
                graph_version=current_version,
                claim_id=f"claim-{task_id}-{current_version}",
                agent_id=decision.selected_agent_id,
            )
            if not acquired:
                result.skipped.append(
                    (task_id, "claim race lost / kernel refused lease")
                )
                continue

            claims_this_pass += 1
            existing = self.get_claim(task_id)
            claim_id = existing.claim_id if existing is not None else ""
            result.mark_dispatched(
                task_id, decision.selected_agent_id, claim_id,
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
            res = self.schedule_once(
                graph_id, max_claims=max_claims_per_pass
            )
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
    ) -> bool:
        from .events import SchedulerEventType, record_event

        agent = self._registry.get(agent_id)
        if agent is None:
            return False

        # Re-check GraphVersion — stale readiness proof cannot linearize ownership.
        current = self._vpg.current_graph_version(graph_id)
        if current != graph_version:
            self._events.append(
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
            self._events.append(
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
            self._events.append(
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
            self._events.append(
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

        resource = claim_resource_uri(graph_id, task_id)
        claim = self._claims_.propose(
            claim_id=claim_id,
            graph_id=graph_id,
            graph_version=current,
            task_id=task_id,
            agent_id=agent_id,
            process_id=agent.process_id,
            lease_resource=resource,
        )
        self._claims_.mark_acquiring(claim)
        self._events.append(
            record_event(
                SchedulerEventType.CLAIM_PROPOSED,
                graph_id=graph_id,
                task_id=task_id,
                agent_id=agent_id,
                claim_id=claim.claim_id,
                graph_version=current,
            )
        )
        self._claims.append(claim)

        if self._claims_.try_acquire_lease(claim):
            self._events.append(
                record_event(
                    SchedulerEventType.CLAIM_LEASE_ACQUIRED,
                    graph_id=graph_id,
                    task_id=task_id,
                    agent_id=agent_id,
                    claim_id=claim.claim_id,
                    graph_version=current,
                    reason=claim.reason or "",
                )
            )
            return True

        return False

    # ── claim lifecycle transitions (Scheduler-initiated) ──────────────────
    def mark_task_completed(self, claim: TaskClaim) -> None:
        from .events import SchedulerEventType, record_event

        if claim.state != ClaimState.ACTIVE:
            return
        self._claims_.complete(claim)
        self._events.append(
            record_event(
                SchedulerEventType.CLAIM_COMPLETED,
                graph_id=claim.graph_id,
                task_id=claim.task_id,
                agent_id=claim.agent_id,
                claim_id=claim.claim_id,
                reason="vpg task verified",
            )
        )

    def release_claim(self, claim: TaskClaim, reason: str = "released") -> None:
        from .events import SchedulerEventType, record_event

        if claim.state in {ClaimState.RELEASED, ClaimState.LOST, ClaimState.COMPLETED}:
            return
        self._claims_.release(claim, reason=reason)
        self._events.append(
            record_event(
                SchedulerEventType.CLAIM_RELEASED,
                graph_id=claim.graph_id,
                task_id=claim.task_id,
                agent_id=claim.agent_id,
                claim_id=claim.claim_id,
                reason=reason,
            )
        )

    # ── VPG observation hook ───────────────────────────────────────────────
    def observe_vpg(self, graph_id: str) -> dict[str, int]:
        """Poll VPG state and derive scheduler-side state transitions:
          - Task VERIFIED  -> owning ACTIVE claim COMPLETED.
        Returns a tally of derived transitions.
        """
        tally: dict[str, int] = {"claims_completed": 0}
        for claim in list(self._claims):
            if claim.state != ClaimState.ACTIVE:
                continue
            validity = self._vpg.task_validity(graph_id, claim.task_id)
            if validity == "verified":
                self.mark_task_completed(claim)
                tally["claims_completed"] += 1
        return tally

    # ── reconciliation (Section 27) ────────────────────────────────────────
    def reconcile(self) -> ReconciliationResult:
        """Run reconciliation between Scheduler projection and authoritative
        Kernel + VPG state."""
        from .reconciliation import reconcile as _reconcile

        for claim in self._claims:
            if claim.state == ClaimState.ACTIVE and claim.lease_id is None:
                # Self-repair: an ACTIVE claim without a lease_id leaks
                # ownership and must be LOST.
                self._claims_.mark_lost(claim, reason="active_without_lease")

        return _reconcile(
            self._claims,
            self._attempts,
            lease_is_live=self._leases.is_lease_active,
            process_is_alive=self._process_is_alive,
            vpg_task_verified=self._vpg_task_verified,
            vpg_task_stale=self._vpg_task_stale,
            lease_lookup=self._lease_lookup_for_claim,
            release_lease=self._leases.release,
        )

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
            out.append(result)
        return out

    def _process_is_alive(self, pid: str) -> bool:
        proc = self._process.get(pid)
        if proc is None:
            return False
        return getattr(proc, "state", None) not in ("exited", "failed")

    def _vpg_task_verified(self, graph_id: str, task_id: str) -> bool:
        return bool(self._vpg.task_validity(graph_id, task_id) == "verified")

    def _vpg_task_stale(self, graph_id: str, task_id: str) -> bool:
        return bool(self._vpg.task_validity(graph_id, task_id) == "stale")

    def _lease_lookup_for_claim(self, claim: TaskClaim) -> Any | None:
        leases = self._leases.list_for_task(claim.graph_id, claim.task_id)
        for lease in leases:
            if lease.lease_id == claim.lease_id:
                return lease
        return None

    @staticmethod
    def _claim_idempotency_key(
        graph_id: str, task_id: str, graph_version: int
    ) -> str:
        return f"{graph_id}:{task_id}:v{graph_version}"

    def _idependent_mark_idempotent(self, key: str) -> None:
        self._idempotent_keys.add(key)
