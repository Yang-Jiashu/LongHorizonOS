"""External Harness usage must enter the AgentOS measured-cost ledger."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from lhos.integrations.harness import DeepSeekTraceSummary, HarnessUsage
from lhos.runtimes.multi_agent import AttemptState
from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeBudgetLimits,
    Goal,
    TaskComputeEstimate,
    VerificationOutcome,
)
from lhos.sdk.errors import ExecutionError
from lhos.sdk.os import _extract_measured_computation_cost


def _outcome() -> SimpleNamespace:
    return SimpleNamespace(
        trace=DeepSeekTraceSummary(
            usage=HarnessUsage(
                uncached_input_tokens=11,
                output_tokens=3,
                cache_read_tokens=7,
                cache_write_tokens=2,
                model_calls=1,
                tool_calls=2,
                wall_time_ms=123,
            )
        )
    )


def _estimate() -> TaskComputeEstimate:
    return TaskComputeEstimate(
        task_id="task",
        verified_progress_units=1,
        success_basis_points=10_000,
        input_stability_basis_points=10_000,
        normalized_cost_units=1,
        estimated_tokens=100,
        estimated_wall_time_ms=1000,
        known=True,
    )


def test_deepseek_trace_usage_maps_to_computation_cost() -> None:
    cost = _extract_measured_computation_cost(_outcome())

    assert cost is not None
    assert cost.input_tokens == 11
    assert cost.output_tokens == 3
    assert cost.cached_input_tokens == 9
    assert cost.model_calls == 1
    assert cost.tool_calls == 2
    assert cost.elapsed_ms == 123


async def test_successful_harness_usage_is_measured_before_verification() -> None:
    async def execute(_context, _task_id):
        return _outcome()

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
            )
        )
        goal = Goal("harness-usage-success")
        goal.task(
            "task",
            agent="worker",
            executor_api="context_v1",
            verify=lambda: VerificationOutcome(
                passed=False,
                artifact_id="artifact",
                version=1,
                content="not committed",
            ),
            max_attempts=1,
        )

        await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
            adaptive=True,
            budget_aware=True,
            budget_estimates={"task": _estimate()},
            budget_limits=ComputeBudgetLimits(
                max_tokens=10_000,
                max_wall_time_ms=60_000,
            ),
            automatic_rebase=False,
        )

        measured = os_.usage_ledger.aggregate().measured
        assert measured.tokens == 14
        assert measured.context_tokens == 9
        assert measured.wall_time_ms >= 0
    finally:
        os_.close()


async def test_failed_harness_partial_usage_is_not_lost() -> None:
    async def execute(_context, _task_id):
        error = ExecutionError("provider failed after consuming tokens")
        error.partial_outcome = _outcome()
        raise error

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
            )
        )
        goal = Goal("harness-usage-failure")
        goal.task(
            "task",
            agent="worker",
            executor_api="context_v1",
            verify=lambda: VerificationOutcome(
                passed=True,
                artifact_id="artifact",
                version=1,
                content="must not run",
            ),
            max_attempts=1,
        )

        await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
            adaptive=True,
            budget_aware=True,
            budget_estimates={"task": _estimate()},
            budget_limits=ComputeBudgetLimits(
                max_tokens=10_000,
                max_wall_time_ms=60_000,
            ),
            automatic_rebase=False,
        )

        measured = os_.usage_ledger.aggregate().measured
        assert measured.tokens == 14
        assert measured.context_tokens == 9
    finally:
        os_.close()


async def test_preempted_harness_partial_usage_is_not_lost() -> None:
    started = asyncio.Event()

    async def execute(context, _task_id):
        token = context.cancellation_token
        token.attach_partial_outcome(_outcome())
        started.set()
        await token.wait()
        token.raise_if_cancelled()

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                executor_api="context_v1",
            )
        )
        goal = Goal("harness-usage-preempt")
        goal.task(
            "task",
            agent="worker",
            executor_api="context_v1",
            verify=lambda: VerificationOutcome(
                passed=True,
                artifact_id="artifact",
                version=1,
                content="must not commit",
            ),
            max_attempts=1,
        )
        running = asyncio.create_task(
            os_.run_async(
                goal,
                max_dispatches=1,
                max_steps=1,
                max_concurrency=1,
                adaptive=True,
                budget_aware=True,
                budget_estimates={"task": _estimate()},
                budget_limits=ComputeBudgetLimits(
                    max_tokens=10_000,
                    max_wall_time_ms=60_000,
                ),
                automatic_rebase=False,
            )
        )
        await started.wait()
        for _ in range(200):
            attempt = next(
                (
                    item
                    for item in os_.scheduler.attempts
                    if item.task_id == "task" and item.state is AttemptState.RUNNING
                ),
                None,
            )
            if attempt is not None:
                break
            await asyncio.sleep(0.005)
        else:
            raise AssertionError("running attempt was not observed")

        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="preempt",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="usage-preempt",
        )
        assert delivery.accepted
        await running

        measured = os_.usage_ledger.aggregate().measured
        assert measured.tokens == 14
        assert measured.context_tokens == 9
    finally:
        os_.close()
