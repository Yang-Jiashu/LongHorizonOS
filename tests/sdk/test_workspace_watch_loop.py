"""Focused tests for the caller-owned workspace watch loop."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from lhos.sdk import (
    AgentCognitionState,
    EventDrivenSupervisor,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
    SupervisorState,
    WorkspaceWatchLoop,
    WorkspaceWatchLoopStopReason,
)


def _state(*, version: int = 1, closed: bool = False) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=version,
            projection_hash=f"{version:064d}",
            graph_closed=closed,
            goal_closed=closed,
            ready_frontier=() if closed else ("task-a",),
            repair_ready_frontier=(),
            verified_task_ids=("task-a",) if closed else (),
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=() if closed else ("task-a",),
        ),
        agent_cognition=AgentCognitionState(available=True, current_attempts=()),
        context={"available": True},
        resources=ResourceRuntimeState(available=True, pools=(), active_claims=()),
    )


class _FakeOS:
    def __init__(self) -> None:
        self.state = _state()
        self.executions = 0

    def runtime_state(self, _goal: object) -> GlobalRuntimeState:
        return self.state

    async def execute_online_epoch(self, _goal: object, **_kwargs: object) -> object:
        self.executions += 1
        self.state = _state(
            version=self.state.progress.graph_version + 1,
            closed=True,
        )
        return SimpleNamespace(
            goal_state="closed",
            failures=[],
            verified=["task-a"],
            meta={"online_epoch": {"actual_dispatched_task_ids": ("task-a",)}},
        )


class _PollWatcher:
    """Small adapter exposing the existing watcher protocol."""

    def __init__(self) -> None:
        self.polls = 0

    def poll_and_reconcile(self, **_kwargs: object) -> object:
        self.polls += 1
        return None


def _supervisor(*, max_epochs: int = 10) -> tuple[EventDrivenSupervisor, _PollWatcher]:
    fake = _FakeOS()
    watcher = _PollWatcher()
    supervisor = EventDrivenSupervisor(
        fake,
        "goal",
        watcher=watcher,
        max_epochs=max_epochs,
        poll_workspace=True,
    )
    return supervisor, watcher


@pytest.mark.asyncio
async def test_watch_loop_polls_with_explicit_max_steps_and_no_hidden_daemon() -> None:
    supervisor, watcher = _supervisor()
    loop = WorkspaceWatchLoop(supervisor, poll_interval=0)

    result = await loop.run(max_steps=3, execute=False)

    assert result.stop_reason is WorkspaceWatchLoopStopReason.MAX_STEPS
    assert result.polls_completed == 3
    assert watcher.polls == 3
    assert supervisor.state is SupervisorState.RUNNING
    assert loop.running is False


@pytest.mark.asyncio
async def test_watch_loop_stop_event_stops_before_next_poll_and_cleans_up() -> None:
    supervisor, watcher = _supervisor()
    loop = WorkspaceWatchLoop(supervisor, poll_interval=0)
    stop_event = asyncio.Event()

    async def set_stop() -> None:
        await asyncio.sleep(0)
        stop_event.set()

    setter = asyncio.create_task(set_stop())
    result = await loop.run(
        max_steps=10,
        stop_event=stop_event,
        execute=False,
    )
    await setter

    assert result.stop_reason is WorkspaceWatchLoopStopReason.STOP_EVENT
    assert result.polls_completed <= 2
    assert watcher.polls == result.polls_completed
    assert supervisor.state is SupervisorState.STOPPED
    assert loop.running is False


@pytest.mark.asyncio
async def test_watch_loop_respects_zero_budget_without_polling() -> None:
    supervisor, watcher = _supervisor()
    loop = WorkspaceWatchLoop(supervisor, poll_interval=0)

    result = await loop.run(max_steps=0, execute=False)

    assert result.stop_reason is WorkspaceWatchLoopStopReason.ZERO_BUDGET
    assert result.polls_completed == 0
    assert watcher.polls == 0
    assert supervisor.state is SupervisorState.CREATED


@pytest.mark.asyncio
async def test_watch_loop_stops_when_supervisor_reaches_closed_goal() -> None:
    supervisor, watcher = _supervisor(max_epochs=2)
    loop = WorkspaceWatchLoop(supervisor, poll_interval=0)

    result = await loop.run(max_steps=5, execute=True)

    assert result.stop_reason is WorkspaceWatchLoopStopReason.SUPERVISOR_TERMINAL
    assert result.complete
    assert result.polls_completed == 1
    assert watcher.polls == 1
    assert supervisor.state is SupervisorState.CLOSED


def test_watch_loop_requires_configured_workspace_watcher() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)

    with pytest.raises(Exception, match="workspace watcher"):
        WorkspaceWatchLoop(supervisor)
