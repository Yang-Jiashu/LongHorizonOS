"""Self-driving kernel-loop tests.

The claim under test is narrow and is the one the system previously could not
make: given a Goal, the runtime observes, plans, dispatches and re-evaluates
*by itself* until the Goal is VERIFIED -- without the caller pumping a step per
epoch -- and it always stops for an explicit, bounded reason.
"""

from __future__ import annotations

import pytest

from lhos.sdk import Agent, AgentOS, ConfigurationError, Goal, VerificationOutcome
from lhos.sdk.kernel_loop import drive_goal_to_closure


def _pass(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=f"out-{task_id}",
        version=1,
        content=f"{task_id}:verified",
    )


def _chain_goal(goal_id: str, length: int = 3, *, agent: str = "worker") -> Goal:
    goal = Goal(goal_id)
    previous = None
    for index in range(1, length + 1):
        task_id = f"t{index}"
        previous = goal.task(
            task_id,
            agent=agent,
            depends_on=(previous,) if previous is not None else (),
            verify=lambda task_id=task_id: _pass(task_id),
        )
    return goal


async def test_kernel_loop_drives_goal_to_verified_closure_without_caller_stepping() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = _chain_goal("kernel-loop-closure", length=4)

        result = await drive_goal_to_closure(os_, goal, max_epochs=32)

        assert result.goal_closed is True
        assert result.stop_reason == "goal_closed"
        assert executed == ["t1", "t2", "t3", "t4"]
        assert set(result.verified_task_ids) == {"t1", "t2", "t3", "t4"}
        # The whole point: more than one epoch happened inside a single call.
        assert result.epochs_executed > 1
        assert result.idle_polls_used == 0
    finally:
        os_.close()


async def test_kernel_loop_stops_bounded_when_no_work_is_admissible() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("kernel-loop-starved")
        goal.task(
            "unreachable",
            agent="worker",
            required_specializations=("rust",),
            verify=lambda: _pass("unreachable"),
        )

        result = await drive_goal_to_closure(os_, goal, max_epochs=8)

        assert result.goal_closed is False
        assert result.stop_reason in {"no_admissible_work", "max_epochs"}
        assert result.epochs_executed <= 8
        assert result.progressed is False
    finally:
        os_.close()


async def test_kernel_loop_honours_the_epoch_budget() -> None:
    async def execute(_task_id: str) -> None:
        return None

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = _chain_goal("kernel-loop-bounded", length=5)

        result = await drive_goal_to_closure(os_, goal, max_epochs=2)

        assert result.epochs_executed == 2
        assert result.goal_closed is False
        # The supervisor reports its own budget first, which is the more precise
        # reason; the loop's own ceiling is the fallback.
        assert result.stop_reason in {"budget_exhausted", "max_epochs"}
    finally:
        os_.close()


async def test_idle_polling_without_a_change_source_is_rejected() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = _chain_goal("kernel-loop-idle-config", length=1)

        with pytest.raises(ConfigurationError, match="idle_polls"):
            await drive_goal_to_closure(os_, goal, idle_polls=3)
    finally:
        os_.close()


async def test_kernel_loop_rejects_a_non_positive_epoch_budget() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = _chain_goal("kernel-loop-zero-epochs", length=1)

        with pytest.raises(ConfigurationError, match="max_epochs"):
            await drive_goal_to_closure(os_, goal, max_epochs=0)
    finally:
        os_.close()


async def test_kernel_loop_transcript_records_each_epoch() -> None:
    async def execute(_task_id: str) -> None:
        return None

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = _chain_goal("kernel-loop-transcript", length=3)

        result = await drive_goal_to_closure(os_, goal, max_epochs=16)

        assert len(result.steps) == result.epochs_executed
        assert [step.step_id for step in result.steps] == list(range(1, result.epochs_executed + 1))
        assert tuple(result.dispatched_task_ids) == ("t1", "t2", "t3")
        # Every executing epoch must carry the ranking it dispatched under, so a
        # low-value dispatch can be told apart from an unranked one.
        assert result.dispatch_orders
    finally:
        os_.close()


async def test_adaptive_parallelism_lets_the_loop_choose_its_own_degree() -> None:
    """The degree becomes a per-epoch decision instead of a caller constant."""

    async def execute(_task_id: str) -> None:
        return None

    os_ = AgentOS(":memory:")
    try:
        for index in range(1, 4):
            os_.add_agent(Agent(f"worker-{index}", executor=execute, specializations=("python",)))
        goal = Goal("kernel-loop-adaptive-degree")
        for index in range(1, 5):
            goal.task(
                f"t{index}",
                agent="worker-1",
                inputs=(f"in-{index}",),
                outputs=(f"out-{index}",),
                verify=lambda index=index: _pass(f"t{index}"),
            )

        result = await drive_goal_to_closure(
            os_,
            goal,
            max_epochs=16,
            max_concurrency=3,
            max_parallelism=3,
            adaptive_parallelism=True,
        )

        assert result.goal_closed is True
        assert result.parallelism_decisions, "no degree was decided"
        for decision in result.parallelism_decisions:
            # The ceiling is a bound, never exceeded, and every choice explains itself.
            assert 0 <= decision.chosen_degree <= 3
            assert decision.degree_reason
    finally:
        os_.close()


async def test_adaptive_parallelism_is_off_by_default() -> None:
    async def execute(_task_id: str) -> None:
        return None

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = _chain_goal("kernel-loop-degree-default", length=2)

        result = await drive_goal_to_closure(os_, goal, max_epochs=8)

        assert result.goal_closed is True
        assert result.parallelism_decisions == ()
    finally:
        os_.close()


async def test_adaptive_parallelism_rejects_a_non_boolean() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = _chain_goal("kernel-loop-degree-config", length=1)

        with pytest.raises(ConfigurationError, match="adaptive_parallelism"):
            await drive_goal_to_closure(os_, goal, adaptive_parallelism="yes")  # type: ignore[arg-type]
    finally:
        os_.close()
