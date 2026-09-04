"""Streaming dispatch (event-driven work-conserving refill) tests.

Verifies the opt-in streaming_dispatch path: pool.run_streaming + per-completion
frontier refill.  The default (non-streaming) path is covered by test_adaptive_run.
"""

from __future__ import annotations

import asyncio

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    VerificationOutcome,
)


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


def test_streaming_dispatch_requires_adaptive() -> None:
    """streaming_dispatch=True without adaptive=True must fail closed."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("streaming-requires-adaptive")
    goal.task("t1", agent="worker", verify=lambda: _pass("t1"))
    with pytest.raises(ConfigurationError, match="streaming_dispatch requires adaptive"):
        asyncio.run(os_.run_async(goal, max_dispatches=2, streaming_dispatch=True))


def test_streaming_dispatch_must_be_bool() -> None:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("streaming-bool-check")
    goal.task("t1", agent="worker", verify=lambda: _pass("t1"))
    with pytest.raises(ConfigurationError, match="streaming_dispatch must be a boolean"):
        asyncio.run(os_.run_async(goal, max_dispatches=2, adaptive=True, streaming_dispatch="yes"))


def test_streaming_dispatch_basic_completion() -> None:
    """Streaming mode must complete tasks correctly (same correctness as batch)."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("streaming-basic")
    goal.task("first", agent="worker", verify=lambda: _pass("first"))
    goal.task("second", agent="worker", verify=lambda: _pass("second"))

    result = asyncio.run(os_.run_async(
        goal,
        max_dispatches=4,
        max_steps=8,
        max_concurrency=2,
        adaptive=True,
        streaming_dispatch=True,
    ))

    assert result.goal_state == "closed"
    assert sorted(result.verified) == ["first", "second"]
    assert result.meta["streaming_dispatch"] is True
    assert isinstance(result.meta["streaming_refills"], tuple)


def test_streaming_dispatch_refills_under_low_concurrency() -> None:
    """With max_concurrency=1 and 3 independent tasks, completion should trigger refill.

    In batch mode each epoch dispatches 1 task and waits for it.  In streaming mode
    the first task's completion should immediately refill the next ready task,
    so we expect at least one recorded refill event.
    """
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("streaming-refill")
    goal.task("a", agent="worker", verify=lambda: _pass("a"))
    goal.task("b", agent="worker", verify=lambda: _pass("b"))
    goal.task("c", agent="worker", verify=lambda: _pass("c"))

    result = asyncio.run(os_.run_async(
        goal,
        max_dispatches=6,
        max_steps=10,
        max_concurrency=1,
        adaptive=True,
        streaming_dispatch=True,
    ))

    assert result.goal_state == "closed"
    assert sorted(result.verified) == ["a", "b", "c"]
    refills = result.meta["streaming_refills"]
    # With 3 tasks and concurrency=1, at least 2 refills should happen
    # (after first and second task complete).
    assert len(refills) >= 1, f"expected at least 1 refill, got {len(refills)}: {refills}"
    for refill in refills:
        assert "triggered_by" in refill
        assert "refilled_task_ids" in refill
        assert len(refill["refilled_task_ids"]) >= 1


def test_streaming_dispatch_no_refill_when_disabled() -> None:
    """Default (non-streaming) mode must not record streaming refills."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",)))
    goal = Goal("streaming-off")
    goal.task("x", agent="worker", verify=lambda: _pass("x"))

    result = asyncio.run(os_.run_async(
        goal,
        max_dispatches=2,
        adaptive=True,
        streaming_dispatch=False,
    ))

    assert result.goal_state == "closed"
    assert result.meta["streaming_dispatch"] is False
    assert result.meta["streaming_refills"] == ()
