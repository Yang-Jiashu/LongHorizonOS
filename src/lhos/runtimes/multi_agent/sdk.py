"""Scheduler public SDK facade.

The Agent-facing entrypoint wraps the core MultiAgentScheduler and adds:
  - automatic observe_vpg + reconcile pass after every schedule (so callers
    do not have to wire it themselves)
  - typed Event query surface
  - projection snapshot + rebuild hook for tests and demos
"""

from __future__ import annotations

from typing import Any

from .models import ResourceVector
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

    def active_claim_for_task(self, task_id: str, graph_id: str | None = None) -> Any | None:
        return self._s.get_claim(task_id, graph_id)

    def attempt_for_claim(self, claim_id: str) -> Any | None:
        return self._s.get_attempt_for_claim(claim_id)

    def mark_execution_started(self, claim: Any | str) -> Any | None:
        """Promote the claim's attempt to RUNNING."""
        return self._s.mark_execution_started(claim)

    def mark_execution_operationally_succeeded(self, claim: Any | str) -> Any | None:
        """Record executor success without asserting semantic verification."""
        return self._s.mark_execution_operationally_succeeded(claim)

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

    def run_pass(self, graph_id: str, **kwargs: Any) -> ScheduleResult:
        """schedule_once + observe_vpg + reconcile — a coherent full step."""
        res = self._s.schedule_once(graph_id, **kwargs)
        self._s.observe_vpg(graph_id)
        self._s.reconcile()
        return res

    # ── events / projection ───────────────────────────────────────────────
    @property
    def events(self) -> list[Any]:
        return list(self._s._events)

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
