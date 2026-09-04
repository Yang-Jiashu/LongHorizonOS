"""Focused tests for the bounded caller-owned event supervisor."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lhos.sdk import (
    Agent,
    AgentCognitionState,
    AgentOS,
    ConfigurationError,
    EventDrivenSupervisor,
    GlobalRuntimeState,
    ProgressSemanticState,
    ResourceRuntimeState,
    SemanticInterrupt,
    SemanticInterruptKind,
    SupervisorEventKind,
    SupervisorState,
    SupervisorStepStatus,
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
        self.execution_kwargs: list[dict[str, object]] = []
        self.planned: list[tuple[object, ...]] = []

    def runtime_state(self, _goal: object) -> GlobalRuntimeState:
        return self.state

    async def execute_online_epoch(self, _goal: object, **kwargs: object) -> object:
        self.executions += 1
        self.execution_kwargs.append(dict(kwargs))
        self.state = _state(version=self.state.progress.graph_version + 1, closed=True)
        return SimpleNamespace(
            goal_state="closed",
            failures=[],
            verified=["task-a"],
            meta={"online_epoch": {"actual_dispatched_task_ids": ("task-a",)}},
        )

    def plan_interrupts(self, _goal: object, interrupts: object, **_kwargs: object) -> object:
        self.planned.append(tuple(interrupts))
        return SimpleNamespace(decisions=(), graph_version=self.state.progress.graph_version)


class _CountingWatcher:
    """Minimal watcher adapter used to assert terminal-step inertness."""

    def __init__(self, fake: _FakeOS) -> None:
        self.fake = fake
        self.polls = 0

    def poll_and_reconcile(self, **_kwargs: object) -> object:
        self.polls += 1
        # A real watcher would return WorkspaceWatchPoll.  Returning ``None``
        # is sufficient for this lifecycle test and keeps it independent of
        # workspace/provenance setup.
        return None


@pytest.mark.asyncio
async def test_supervisor_start_step_executes_one_bounded_epoch_and_closes() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=2)

    started = supervisor.start()
    assert started.state is SupervisorState.RUNNING
    result = await supervisor.step()

    assert result.status is SupervisorStepStatus.CLOSED
    assert result.goal_closed
    assert supervisor.state is SupervisorState.CLOSED
    assert fake.executions == 1
    assert result.graph_version == 2


@pytest.mark.asyncio
async def test_supervisor_forwards_resource_aware_epoch_configuration() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(
        fake,
        "goal",
        max_epochs=1,
        max_concurrency=3,
        max_dispatches_per_epoch=2,
        max_parallelism=2,
        resource_aware=True,
        persist_epoch=False,
    )

    result = await supervisor.step()

    assert result.status is SupervisorStepStatus.CLOSED
    assert fake.execution_kwargs == [
        {
            "max_concurrency": 3,
            "max_dispatches": 2,
            "max_parallelism": 2,
            "resource_aware": True,
            "conflict_graph": None,
            "persist_epoch": False,
        }
    ]


@pytest.mark.asyncio
async def test_closed_supervisor_does_not_poll_watcher_or_reopen_after_external_change() -> None:
    fake = _FakeOS()
    watcher = _CountingWatcher(fake)
    supervisor = EventDrivenSupervisor(
        fake,
        "goal",
        watcher=watcher,
        max_epochs=2,
        poll_workspace=True,
    )

    first = await supervisor.step()
    assert first.status is SupervisorStepStatus.CLOSED
    assert watcher.polls == 1

    # Simulate another authority reporting an open/repaired projection after
    # this supervisor has already committed its terminal CLOSED lifecycle.
    fake.state = _state(version=99, closed=False)
    second = await supervisor.step(poll_workspace=True)

    assert second.status is SupervisorStepStatus.CLOSED
    assert supervisor.state is SupervisorState.CLOSED
    assert second.graph_version == 2
    assert second.goal_closed
    assert watcher.polls == 1
    assert fake.executions == 1


@pytest.mark.asyncio
async def test_goal_closed_before_first_epoch_skips_watcher_reconciliation() -> None:
    fake = _FakeOS()
    watcher = _CountingWatcher(fake)
    supervisor = EventDrivenSupervisor(
        fake,
        "goal",
        watcher=watcher,
        max_epochs=2,
        poll_workspace=True,
    )
    supervisor.start()
    fake.state = _state(version=2, closed=True)

    result = await supervisor.step(poll_workspace=True)

    assert result.status is SupervisorStepStatus.CLOSED
    assert supervisor.state is SupervisorState.CLOSED
    assert watcher.polls == 0
    assert fake.executions == 0


@pytest.mark.asyncio
async def test_observation_only_step_publishes_goal_closure_immediately() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=2)
    supervisor.start()
    fake.state = _state(version=2, closed=True)

    result = await supervisor.step(execute=False)

    assert result.status is SupervisorStepStatus.CLOSED
    assert supervisor.state is SupervisorState.CLOSED
    assert supervisor.snapshot.goal_state == "closed"


@pytest.mark.asyncio
async def test_supervisor_observation_only_does_not_execute() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=2)

    result = await supervisor.step(execute=False)

    assert result.status is SupervisorStepStatus.OBSERVED
    assert fake.executions == 0
    assert supervisor.state is SupervisorState.RUNNING


@pytest.mark.asyncio
async def test_supervisor_stop_event_is_caller_owned_and_idempotent() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=2)

    first = await supervisor.step(events=({"kind": "stop", "reason": "operator"},))
    second = await supervisor.step()

    assert first.status is SupervisorStepStatus.STOPPED
    assert first.accepted_event_ids
    assert second.status is SupervisorStepStatus.STOPPED
    assert supervisor.snapshot.stop_reason == "operator"
    assert fake.executions == 0


@pytest.mark.asyncio
async def test_failed_closed_terminal_step_preserves_failure_status() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)
    supervisor.start()
    supervisor._fail_closed("simulated terminal failure")

    result = await supervisor.step()

    assert result.status is SupervisorStepStatus.FAILED_CLOSED
    assert result.supervisor_state is SupervisorState.FAILED_CLOSED
    assert supervisor.snapshot.last_error == "simulated terminal failure"
    assert fake.executions == 0


@pytest.mark.asyncio
async def test_supervisor_interrupt_event_is_planned_before_execution() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)
    interrupt = SemanticInterrupt(
        interrupt_id="interrupt-1",
        graph_id="graph",
        graph_version=1,
        kind=SemanticInterruptKind.REQUIREMENT_CHANGED,
        reason="requirement changed",
        affected_task_ids=("task-a",),
    )

    result = await supervisor.step(events=(interrupt,), execute=False)

    assert result.status is SupervisorStepStatus.OBSERVED
    assert result.accepted_event_ids == ("interrupt-1",)
    assert fake.planned == [(interrupt,)]


def test_supervisor_submit_duplicate_is_idempotent_and_conflict_fails_closed() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)
    event = supervisor.submit({"kind": "tick", "event_id": "tick-1"})
    replay = supervisor.submit({"kind": "tick", "event_id": "tick-1"})

    assert event == replay
    assert len(supervisor.pending_events) == 1

    with pytest.raises(Exception, match="conflicting duplicate"):
        supervisor.submit({"kind": "tick", "event_id": "tick-1", "reason": "different"})
    assert supervisor.state is SupervisorState.FAILED_CLOSED


def test_agent_os_event_supervisor_factory_is_inert_and_requires_compiled_goal() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = os_.goal("factory-goal")
        os_.add_agent(Agent("worker"))
        goal.task("task-a", agent="worker")
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.event_supervisor(goal)
        goal.compile(os_)
        supervisor = os_.event_supervisor(goal, max_epochs=1)
        assert isinstance(supervisor, EventDrivenSupervisor)
        assert supervisor.state is SupervisorState.CREATED
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_supervisor_rejects_stale_event_version_fail_closed() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)
    stale = {
        "kind": SupervisorEventKind.TICK.value,
        "event_id": "stale-tick",
        "graph_id": "graph",
        "graph_version": 99,
    }

    result = await supervisor.step(events=(stale,), execute=False)

    assert result.status is SupervisorStepStatus.EVENT_REJECTED
    assert result.rejected_event_ids == ("stale-tick",)
    assert supervisor.state is SupervisorState.FAILED_CLOSED
    assert fake.executions == 0


@pytest.mark.asyncio
async def test_supervisor_run_is_bounded_and_returns_transcript() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=3)

    run = await supervisor.run(max_steps=1)

    assert len(run.steps) == 1
    assert run.complete
    assert run.final_snapshot.state is SupervisorState.CLOSED
    assert run.as_dict()["complete"] is True


@pytest.mark.asyncio
async def test_supervisor_async_iterator_yields_one_terminal_observation() -> None:
    fake = _FakeOS()
    supervisor = EventDrivenSupervisor(fake, "goal", max_epochs=1)

    statuses = [item.status async for item in supervisor]

    assert statuses == [SupervisorStepStatus.CLOSED]
    assert supervisor.state is SupervisorState.CLOSED
