"""Caller-owned polling loop for workspace-driven scheduling epochs.

The authority-backed :class:`WorkspaceObservationWatcher` already provides a
single ``poll -> reconcile -> semantic interrupt`` pass.  This module supplies
the missing lifecycle seam: repeatedly invoke the existing watcher through an
``EventDrivenSupervisor`` at explicit, bounded intervals.

The loop never starts a thread, daemon, or hidden asyncio task.  Awaiting
``run`` owns the loop until one of these caller-visible boundaries is reached:

* ``max_steps`` polls have completed;
* the supervisor reaches a terminal state;
* an optional ``asyncio.Event`` requests a stop; or
* the caller cancels the coroutine.

This remains a single-process, explicit-file polling primitive.  It is not a
universal filesystem/API/world watcher.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .errors import ConfigurationError
from .event_supervisor import (
    EventDrivenSupervisor,
    SupervisorState,
    SupervisorStepResult,
)


class WorkspaceWatchLoopStopReason(StrEnum):
    """Why one bounded polling run returned."""

    MAX_STEPS = "max_steps"
    STOP_EVENT = "stop_event"
    SUPERVISOR_TERMINAL = "supervisor_terminal"
    NO_WORK = "no_work"
    ZERO_BUDGET = "zero_budget"


@dataclass(frozen=True, slots=True)
class WorkspaceWatchLoopResult:
    """Auditable result of one caller-owned polling run."""

    steps: tuple[SupervisorStepResult, ...]
    stop_reason: WorkspaceWatchLoopStopReason
    supervisor_state: SupervisorState
    polls_completed: int

    @property
    def complete(self) -> bool:
        return self.supervisor_state is SupervisorState.CLOSED

    @property
    def stopped_by_caller(self) -> bool:
        return self.stop_reason is WorkspaceWatchLoopStopReason.STOP_EVENT

    def as_dict(self) -> dict[str, Any]:
        return {
            "steps": [item.as_dict() for item in self.steps],
            "stop_reason": self.stop_reason.value,
            "supervisor_state": self.supervisor_state.value,
            "polls_completed": self.polls_completed,
            "complete": self.complete,
            "stopped_by_caller": self.stopped_by_caller,
        }


class WorkspaceWatchLoop:
    """Boundedly poll an existing watcher through an event supervisor.

    ``supervisor`` must already have a configured
    ``WorkspaceObservationWatcher``.  Every iteration invokes exactly one
    ``supervisor.step(poll_workspace=True, ...)`` call.  Sleeping happens only
    between completed steps, so ``max_steps=1`` never waits unnecessarily.
    """

    def __init__(
        self,
        supervisor: EventDrivenSupervisor,
        *,
        poll_interval: float = 1.0,
    ) -> None:
        if not isinstance(supervisor, EventDrivenSupervisor):
            raise TypeError("supervisor must be an EventDrivenSupervisor")
        if supervisor.watcher is None:
            raise ConfigurationError("supervisor must be configured with a workspace watcher")
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or poll_interval < 0
        ):
            raise ConfigurationError("poll_interval must be a non-negative number of seconds")
        self._supervisor = supervisor
        self._poll_interval = float(poll_interval)
        self._running = False

    @property
    def supervisor(self) -> EventDrivenSupervisor:
        return self._supervisor

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    @property
    def running(self) -> bool:
        return self._running

    async def run(
        self,
        *,
        max_steps: int,
        stop_event: asyncio.Event | None = None,
        execute: bool = True,
        stop_supervisor: bool = True,
    ) -> WorkspaceWatchLoopResult:
        """Poll until a declared boundary and return the complete transcript.

        ``stop_event`` is checked before every poll and during the interval
        wait.  If it becomes set while waiting, the next workspace poll is
        skipped.  By default the loop calls ``supervisor.stop("workspace
        watch loop stop event")`` so lifecycle cleanup is explicit and
        observable; set ``stop_supervisor=False`` when the caller intends to
        resume the same supervisor manually.
        """

        if isinstance(max_steps, bool) or not isinstance(max_steps, int):
            raise ConfigurationError("max_steps must be an integer")
        if max_steps < 0:
            raise ConfigurationError("max_steps must be >= 0")
        if stop_event is not None and not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event or None")
        if not isinstance(execute, bool):
            raise ConfigurationError("execute must be a boolean")
        if not isinstance(stop_supervisor, bool):
            raise ConfigurationError("stop_supervisor must be a boolean")
        if self._running:
            raise RuntimeError("workspace watch loop is already running")

        if max_steps == 0:
            return WorkspaceWatchLoopResult(
                steps=(),
                stop_reason=WorkspaceWatchLoopStopReason.ZERO_BUDGET,
                supervisor_state=self._supervisor.state,
                polls_completed=0,
            )

        self._running = True
        steps: list[SupervisorStepResult] = []
        stop_reason = WorkspaceWatchLoopStopReason.MAX_STEPS
        try:
            for index in range(max_steps):
                if stop_event is not None and stop_event.is_set():
                    stop_reason = WorkspaceWatchLoopStopReason.STOP_EVENT
                    if stop_supervisor and not self._supervisor.terminal:
                        self._supervisor.stop("workspace watch loop stop event")
                    break
                if self._supervisor.terminal:
                    stop_reason = WorkspaceWatchLoopStopReason.SUPERVISOR_TERMINAL
                    break

                step = await self._supervisor.step(
                    poll_workspace=True,
                    execute=execute,
                )
                steps.append(step)
                if self._supervisor.terminal:
                    stop_reason = WorkspaceWatchLoopStopReason.SUPERVISOR_TERMINAL
                    break
                # ``NO_WORK`` is not a terminal condition for a watch loop:
                # a later poll may observe an external file mutation and
                # reopen useful computation.
                if index + 1 < max_steps:
                    stopped = await self._wait_for_next_poll(stop_event)
                    if stopped:
                        stop_reason = WorkspaceWatchLoopStopReason.STOP_EVENT
                        if stop_supervisor and not self._supervisor.terminal:
                            self._supervisor.stop("workspace watch loop stop event")
                        break
        finally:
            self._running = False

        return WorkspaceWatchLoopResult(
            steps=tuple(steps),
            stop_reason=stop_reason,
            supervisor_state=self._supervisor.state,
            polls_completed=len(steps),
        )

    async def _wait_for_next_poll(
        self,
        stop_event: asyncio.Event | None,
    ) -> bool:
        if stop_event is None:
            if self._poll_interval:
                await asyncio.sleep(self._poll_interval)
            return False
        if stop_event.is_set():
            return True
        if self._poll_interval == 0:
            await asyncio.sleep(0)
            return stop_event.is_set()
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=self._poll_interval,
            )
        except TimeoutError:
            return False
        return True


__all__ = [
    "WorkspaceWatchLoop",
    "WorkspaceWatchLoopResult",
    "WorkspaceWatchLoopStopReason",
]
