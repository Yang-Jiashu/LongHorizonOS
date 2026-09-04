"""Focused tests for the explicit bounded multi-epoch execution loop."""

from __future__ import annotations

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    OnlineExecutionLoopResult,
    VerificationOutcome,
)


def _passed(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


@pytest.mark.asyncio
async def test_execute_online_epochs_reobserves_and_stops_on_closed_goal() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("online-epochs-sequential")
        goal.task("first", agent="worker", verify=lambda: _passed("first"))
        goal.task("second", agent="worker", verify=lambda: _passed("second"))

        result = await os_.execute_online_epochs(
            goal,
            max_epochs=5,
            max_concurrency=1,
            max_dispatches_per_epoch=1,
        )

        assert isinstance(result, OnlineExecutionLoopResult)
        assert result.goal_closed is True
        assert result.complete is True
        assert result.stop_reason == "goal_closed"
        assert len(result.epochs) == 2
        assert result.final_result is result.epochs[-1]
        assert executed == ["first", "second"]
        assert result.total_dispatches == 2
        assert [item.meta["online_epoch"]["max_dispatches"] for item in result.epochs] == [
            1,
            1,
        ]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epochs_respects_max_epochs_and_zero_budget() -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda task_id: executed.append(task_id),
                specializations=("python",),
            )
        )
        goal = Goal("online-epochs-bound")
        goal.task("first", agent="worker", verify=lambda: _passed("first"))
        goal.task("second", agent="worker", verify=lambda: _passed("second"))

        bounded = await os_.execute_online_epochs(
            goal,
            max_epochs=1,
            max_dispatches_per_epoch=1,
        )
        assert bounded.stop_reason == "max_epochs"
        assert len(bounded.epochs) == 1
        assert bounded.goal_closed is False
        assert executed == ["first"]

        zero = await os_.execute_online_epochs(
            goal,
            max_epochs=0,
            max_dispatches_per_epoch=0,
        )
        assert zero.stop_reason == "max_epochs"
        assert zero.epochs == ()
        assert zero.final_result is not None
        assert zero.final_result.goal_state == "open"
        assert executed == ["first"]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epochs_stops_fail_closed_on_epoch_failure() -> None:
    executed: list[str] = []

    async def fail(task_id: str) -> None:
        executed.append(task_id)
        raise RuntimeError("boom")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=fail, specializations=("python",)))
        goal = Goal("online-epochs-failure")
        goal.task("task", agent="worker", verify=lambda: _passed("task"))

        result = await os_.execute_online_epochs(
            goal,
            max_epochs=4,
            max_dispatches_per_epoch=1,
        )

        assert result.stop_reason == "epoch_failed"
        assert len(result.epochs) == 1
        assert result.goal_closed is False
        assert result.epochs[0].failures
        assert executed == ["task"]
    finally:
        os_.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_epochs": -1}, "max_epochs must be >= 0"),
        ({"max_epochs": True}, "max_epochs must be an integer"),
        ({"max_concurrency": 0}, "max_concurrency must be >= 1"),
        (
            {"max_dispatches_per_epoch": -1},
            "max_dispatches_per_epoch must be >= 0",
        ),
        ({"stop_when_closed": "yes"}, "stop_when_closed must be a boolean"),
    ],
)
async def test_execute_online_epochs_validates_bounds(kwargs, message: str) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("online-epochs-validation")
        with pytest.raises(ConfigurationError, match=message):
            await os_.execute_online_epochs(goal, **kwargs)
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_execute_online_epochs_reports_resource_policy_configuration() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=lambda _task_id: None))
        goal = Goal("online-epochs-resource-config")
        goal.task("task", agent="worker", verify=lambda: _passed("task"))

        result = await os_.execute_online_epochs(
            goal,
            max_epochs=1,
            max_concurrency=1,
            max_parallelism=1,
            resource_aware=True,
            persist_epoch=False,
        )

        assert result.goal_closed
        assert result.max_parallelism == 1
        assert result.resource_aware is True
        assert result.as_dict()["max_parallelism"] == 1
        assert result.as_dict()["resource_aware"] is True
        assert result.epochs[0].meta["online_epoch"]["resource_aware"] is True
    finally:
        os_.close()
