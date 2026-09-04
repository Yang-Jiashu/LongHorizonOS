"""Async-path measured wall-clock.

The sync execution path timed its executor; the async path did not, so an async
executor that reported no token counters was recorded as costing nothing.  That
is the failure mode this covers: a task must not look free merely because its
executor was silent.  The span is measured by the SDK's own dispatcher with a
monotonic clock, so it is observed rather than declared.
"""

from __future__ import annotations

import asyncio

from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeBudgetLimits,
    Goal,
    TaskComputeEstimate,
    VerificationOutcome,
)


def _pass(task_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=task_id,
        version=1,
        content=f"{task_id}-v1",
    )


def _estimate(task_id: str) -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id=task_id,
        verified_progress_units=1,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=1,
        estimated_tokens=1,
        estimated_wall_time_ms=1,
        known=True,
    )


def _measured_wall_times(os_: AgentOS) -> list[int]:
    aggregate = os_.usage_ledger.aggregate()
    return [int(aggregate.measured.wall_time_ms)]


async def test_silent_async_executor_still_records_measured_wall_clock() -> None:
    """No counters supplied, yet the attempt must not be recorded as free."""

    async def execute(_task_id: str) -> None:
        # Real elapsed time, no reported tokens or cost.
        await asyncio.sleep(0.05)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("async-measured-span")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        await os_.run_async(
            goal,
            max_dispatches=1,
            max_concurrency=1,
            adaptive=True,
            budget_aware=True,
            budget_estimates={"task": _estimate("task")},
            budget_limits=ComputeBudgetLimits(max_tokens=8, max_wall_time_ms=600_000),
            automatic_rebase=False,
        )

        measured = os_.usage_ledger.aggregate().measured
        assert measured.wall_time_ms > 0, "async executor span was not measured"
        # ~50ms of real sleep; a generous ceiling keeps this robust on a busy host
        # while still failing if the span were a declared constant.
        assert measured.wall_time_ms < 60_000
    finally:
        os_.close()


async def test_a_faster_async_executor_measures_a_smaller_span() -> None:
    """The span tracks real elapsed time rather than a fixed declared value."""

    async def slow(_task_id: str) -> None:
        await asyncio.sleep(0.12)

    async def quick(_task_id: str) -> None:
        await asyncio.sleep(0)

    spans: list[int] = []
    for executor in (slow, quick):
        os_ = AgentOS(":memory:")
        try:
            os_.add_agent(Agent("worker", executor=executor, specializations=("python",)))
            goal = Goal(f"async-span-{executor.__name__}")
            goal.task("task", agent="worker", verify=lambda: _pass("task"))
            await os_.run_async(
                goal,
                max_dispatches=1,
                max_concurrency=1,
                adaptive=True,
                budget_aware=True,
                budget_estimates={"task": _estimate("task")},
                budget_limits=ComputeBudgetLimits(max_tokens=8, max_wall_time_ms=600_000),
                automatic_rebase=False,
            )
            spans.append(int(os_.usage_ledger.aggregate().measured.wall_time_ms))
        finally:
            os_.close()

    slow_span, quick_span = spans
    assert slow_span > quick_span
