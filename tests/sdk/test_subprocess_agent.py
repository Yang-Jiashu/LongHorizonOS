"""Command-backed Agent tests: preemption that stops real computation.

The distinction being tested is the one the system could not previously make.
Cooperative cancellation of an arbitrary in-process callable discards a *result*
while the work keeps running and its cost keeps accruing.  A command-backed
Agent's work lives in a child process the SDK owns, so a preemption ends the
computation itself.
"""

from __future__ import annotations

import asyncio
import sys
import time
from types import SimpleNamespace

import pytest

from lhos.runtimes.multi_agent.worker_pool import (
    CooperativeCancellationToken,
    CooperativeInterrupt,
)
from lhos.sdk import Agent, AgentOS, ConfigurationError, Goal, VerificationOutcome
from lhos.sdk.errors import ExecutionError
from lhos.sdk.kernel_loop import drive_goal_to_closure
from lhos.sdk.subprocess_agent import subprocess_task_executor


def _ctx(token: CooperativeCancellationToken | None = None) -> SimpleNamespace:
    return SimpleNamespace(cancellation_token=token)


def _sleeper(seconds: float) -> list[str]:
    return [sys.executable, "-c", f"import time; time.sleep({seconds})"]


async def test_command_backed_agent_completes_and_verifies_end_to_end() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "runner",
                executor=subprocess_task_executor(
                    [sys.executable, "-c", "import sys; print(sys.argv[-1])"],
                    timeout_seconds=30,
                ),
                executor_api="context_v1",
                specializations=("python",),
            )
        )
        goal = Goal("subprocess-agent-closure")
        goal.task(
            "build",
            agent="runner",
            executor_api="context_v1",
            verify=lambda _ctx: VerificationOutcome(
                passed=True,
                artifact_id="out-build",
                version=1,
                content="build:verified",
            ),
        )

        result = await drive_goal_to_closure(os_, goal, max_epochs=8)

        assert result.goal_closed is True
        assert result.verified_task_ids == ("build",)
    finally:
        os_.close()


async def test_preemption_kills_a_child_that_would_otherwise_keep_running(tmp_path) -> None:
    """A pending interrupt must end the computation, not just drop its answer.

    Raising promptly proves nothing on its own -- an executor that abandons a
    still-running child also returns promptly, while the child keeps burning
    cost.  So the child is told to write a marker only after a delay: if the
    marker never appears, the computation genuinely stopped.
    """

    marker = tmp_path / "child-kept-running.txt"
    execute = subprocess_task_executor(
        [
            sys.executable,
            "-c",
            "import time,pathlib,sys; time.sleep(1.0); "
            "pathlib.Path(sys.argv[-2]).write_text('alive', encoding='utf-8')",
            str(marker),
        ],
        poll_seconds=0.01,
    )
    token = CooperativeCancellationToken(claim_id="claim-test")
    token.request(interrupt_id="i-1", action="preempt", reason="stale_input")

    started = time.monotonic()
    with pytest.raises(CooperativeInterrupt):
        await execute(_ctx(token), "long-task")
    assert time.monotonic() - started < 10
    assert token.observed is True

    # Outlive the child's own delay; a survivor would have written by now.
    await asyncio.sleep(2.0)
    assert not marker.exists(), "child survived preemption and kept computing"


async def test_timeout_kills_the_child_and_reports_a_timeout() -> None:
    execute = subprocess_task_executor(_sleeper(30), timeout_seconds=0.3, poll_seconds=0.01)

    started = time.monotonic()
    with pytest.raises(ExecutionError, match="timeout"):
        await execute(_ctx(), "slow-task")
    assert time.monotonic() - started < 10


async def test_non_zero_exit_is_a_failure_not_a_preemption() -> None:
    execute = subprocess_task_executor(
        [sys.executable, "-c", "import sys; sys.exit(3)"],
        poll_seconds=0.01,
    )

    with pytest.raises(ExecutionError, match="exited with code 3"):
        await execute(_ctx(), "failing-task")


async def test_uncancelled_child_runs_to_completion() -> None:
    execute = subprocess_task_executor(_sleeper(0.05), poll_seconds=0.01)
    token = CooperativeCancellationToken(claim_id="claim-test")

    await execute(_ctx(token), "quick-task")

    assert token.request_pending is False


async def test_command_is_validated() -> None:
    with pytest.raises(ConfigurationError, match="timeout_seconds"):
        subprocess_task_executor([sys.executable], timeout_seconds=0)
    with pytest.raises(ConfigurationError, match="poll_seconds"):
        subprocess_task_executor([sys.executable], poll_seconds=0)

    execute = subprocess_task_executor(lambda _task_id: [])
    with pytest.raises(ConfigurationError, match="non-empty argv"):
        await execute(_ctx(), "bad")


async def test_callable_command_receives_the_task_id() -> None:
    seen: list[str] = []

    def build(task_id: str) -> list[str]:
        seen.append(task_id)
        return [sys.executable, "-c", "pass"]

    execute = subprocess_task_executor(build, poll_seconds=0.01)
    await execute(_ctx(), "task-42")

    assert seen == ["task-42"]


async def test_concurrent_children_do_not_interfere() -> None:
    execute = subprocess_task_executor(_sleeper(0.05), poll_seconds=0.01)

    await asyncio.gather(*(execute(_ctx(), f"t{index}") for index in range(4)))


async def test_usage_attributes_a_preemption_distinctly_from_a_teardown() -> None:
    """An audit must tell "we stopped it" apart from "we tore it down"."""

    seen: dict[str, dict] = {}
    execute = subprocess_task_executor(
        _sleeper(30),
        poll_seconds=0.01,
        on_usage=lambda task_id, usage: seen.__setitem__(task_id, usage),
    )
    token = CooperativeCancellationToken(claim_id="claim-test")
    token.request(interrupt_id="i-2", action="preempt", reason="stale_input")

    with pytest.raises(CooperativeInterrupt):
        await execute(_ctx(token), "preempted")

    assert seen["preempted"]["terminated_by"] == "semantic_interrupt"
    assert seen["preempted"]["wall_time_ms"] >= 0


async def test_usage_reports_measured_wall_clock_for_a_completed_child() -> None:
    seen: dict[str, dict] = {}
    execute = subprocess_task_executor(
        _sleeper(0.05),
        poll_seconds=0.01,
        on_usage=lambda task_id, usage: seen.__setitem__(task_id, usage),
    )

    await execute(_ctx(), "done")

    usage = seen["done"]
    assert usage["exit_code"] == 0
    assert usage["terminated_by"] is None
    assert usage["wall_time_ms"] > 0
