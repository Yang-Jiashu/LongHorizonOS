"""Error isolation and recovery tests.

Verifies that a single task failure does not cascade to unrelated tasks,
and that only the affected subgraph is recomputed (invalidation cone).
"""
from __future__ import annotations

import asyncio

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    Goal,
    VerificationOutcome,
)


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True, artifact_id=artifact_id, version=1, content=f"{artifact_id}-v1"
    )


def _fail(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=False, artifact_id=artifact_id, version=0, content=f"{artifact_id}-failed"
    )


def test_failure_isolated_to_single_task() -> None:
    """A failing task must not block independent tasks."""
    call_count = {"a": 0, "b": 0, "c": 0}

    async def executor(task_id: str) -> object:
        call_count[task_id] += 1
        if task_id == "a" and call_count["a"] == 1:
            return _fail("a")  # fail first time
        return _pass(task_id)

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",), executor=executor))
    goal = Goal("error-isolation")
    a = goal.task("a", agent="worker", outputs=["art-a"], max_attempts=2)
    goal.task("b", agent="worker", depends_on=(a,), outputs=["art-b"])
    goal.task("c", agent="worker", outputs=["art-c"])

    result = asyncio.run(os_.run_async(
        goal, max_dispatches=10, max_steps=15, max_concurrency=2, max_parallelism=2,
        adaptive=True,
    ))

    # c should complete regardless of a's failure
    assert "c" in result.verified
    # a should eventually pass on retry
    assert "a" in result.verified
    # b should pass after a succeeds
    assert "b" in result.verified
    # a was called twice (fail + retry)
    assert call_count["a"] == 2, f"a called {call_count['a']} times, expected 2"
    # b and c each called once
    assert call_count["b"] == 1, f"b called {call_count['b']} times"
    assert call_count["c"] == 1, f"c called {call_count['c']} times"


def test_downstream_invalidation_after_failure() -> None:
    """When a task fails and is retried, downstream tasks must wait for the retry."""
    call_count = {"a": 0, "b": 0}

    async def executor(task_id: str) -> object:
        call_count[task_id] += 1
        if task_id == "a" and call_count["a"] == 1:
            return _fail("a")
        return _pass(task_id)

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",), executor=executor))
    goal = Goal("downstream-invalid")
    a = goal.task("a", agent="worker", outputs=["art-a"], max_attempts=2)
    goal.task("b", agent="worker", depends_on=(a,), outputs=["art-b"])

    result = asyncio.run(os_.run_async(
        goal, max_dispatches=8, max_steps=12, max_concurrency=2, max_parallelism=2,
        adaptive=True,
    ))

    assert result.goal_state == "closed"
    assert "a" in result.verified
    assert "b" in result.verified
    # b should only be called once (after a's successful retry)
    assert call_count["b"] == 1, f"b called {call_count['b']} times, should not run on failed a"


def test_independent_task_unaffected_by_failure() -> None:
    """An independent task must complete even when another task permanently fails."""
    async def executor(task_id: str) -> object:
        if task_id == "bad":
            return _fail("bad")
        return _pass(task_id)

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker", specializations=("python",), executor=executor))
    goal = Goal("perm-failure")
    goal.task("bad", agent="worker", outputs=["art-bad"], max_attempts=1)
    goal.task("good", agent="worker", outputs=["art-good"])

    result = asyncio.run(os_.run_async(
        goal, max_dispatches=6, max_steps=10, max_concurrency=2, max_parallelism=2,
        adaptive=True,
    ))

    # good task should complete despite bad task failing
    assert "good" in result.verified
    # bad task should be in failures
    assert any("bad" in f for f in result.failures)
