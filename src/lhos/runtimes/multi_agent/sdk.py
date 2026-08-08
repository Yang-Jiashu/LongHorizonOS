"""Scheduler public SDK facade.

The Agent-facing entrypoint wraps the core MultiAgentScheduler and adds:
  - automatic observe_vpg + reconcile pass after every schedule (so callers
    do not have to wire it themselves)
  - typed Event query surface
  - projection snapshot + rebuild hook for tests and demos
"""

from __future__ import annotations

from typing import Any

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

    def active_claim_for_task(self, task_id: str) -> Any | None:
        return self._s.get_claim(task_id)

    # ── reconcile ──────────────────────────────────────────────────────────
    def reconcile(self) -> Any:
        return self._s.reconcile()

    def observe_vpg(self, graph_id: str) -> dict[str, int]:
        return self._s.observe_vpg(graph_id)

    def run_pass(self, graph_id: str) -> ScheduleResult:
        """schedule_once + observe_vpg + reconcile — a coherent full step."""
        res = self._s.schedule_once(graph_id)
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
