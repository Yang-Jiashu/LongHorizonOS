"""Semantic-preemption tests for ``AgentOS.run_async(preempt_superseded=...)``.

These exercise the one behaviour a static plan provably cannot imitate: while a
batch is in flight, a sibling commit advances the graph and supersedes a
still-running peer's declared input, and the execution loop delivers a
cooperative interrupt to *exactly* that peer attempt.  The tests prove the
interrupt was actually delivered (durable ack events + the attempt's terminal
state), that a peer whose inputs stay current is never touched, and that with
the flag off behaviour is unchanged.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import Any

import pytest

from lhos.runtimes.multi_agent import AttemptState, InterruptDeliveryStatus
from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome


def _pass(artifact_id: str, version: int = 1) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=version,
        content=f"{artifact_id}-v{version}",
    )


async def _wait_for_attempt_record(os_: AgentOS, *, task_id: str) -> Any:
    """Wait until any durable attempt record exists for ``task_id``."""

    for _ in range(400):
        for attempt in os_.scheduler.attempts:
            if attempt.task_id == task_id:
                return attempt
        await asyncio.sleep(0.005)
    raise AssertionError(f"attempt for {task_id!r} was not observed")


async def _wait_for_running_attempt(os_: AgentOS, *, task_id: str) -> Any:
    for _ in range(400):
        for attempt in os_.scheduler.attempts:
            if attempt.task_id == task_id and attempt.state is AttemptState.RUNNING:
                return attempt
        await asyncio.sleep(0.005)
    raise AssertionError(f"running attempt for {task_id!r} was not observed")


def _interrupt_acks(os_: AgentOS, *, claim_id: str) -> list[Any]:
    return [
        event
        for event in os_.scheduler.events
        if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
        and event.claim_id == claim_id
    ]


async def _wait_for_interrupt_ack(os_: AgentOS, *, claim_id: str) -> list[Any]:
    for _ in range(400):
        acks = _interrupt_acks(os_, claim_id=claim_id)
        if acks:
            return acks
        await asyncio.sleep(0.005)
    raise AssertionError(f"no semantic interrupt ack for claim {claim_id!r}")


def _final_attempt(os_: AgentOS, *, attempt_id: str) -> Any:
    return next(item for item in os_.scheduler.attempts if item.attempt_id == attempt_id)


@pytest.mark.asyncio
async def test_preempt_superseded_interrupts_only_the_stale_peer() -> None:
    """A running peer whose declared input is superseded mid-batch is
    interrupted and does not commit; a peer with current inputs is untouched."""

    started_super = asyncio.Event()
    started_clean = asyncio.Event()
    release_clean = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:
        # Pre-seed the artifacts the peers declare so their dispatch-time
        # freshness baseline is a real version (1), not "unknown".
        os_._facts.add_version("shared", 1, "shared-v1")
        os_._facts.add_version("other", 1, "other-v1")

        async def executor(ctx: Any, task_id: str) -> None:
            if task_id == "bump":
                # Only commit once both peers are parked (their cooperative
                # tokens are registered), so the superseding commit is strictly
                # mid-flight relative to them.
                await asyncio.wait_for(started_super.wait(), timeout=5)
                await asyncio.wait_for(started_clean.wait(), timeout=5)
                return
            if task_id == "super":
                started_super.set()
                token = ctx.cancellation_token
                assert token is not None
                await token.wait()
                token.raise_if_cancelled()
                return
            if task_id == "clean":
                started_clean.set()
                await release_clean.wait()
                return

        os_.add_agent(
            Agent(
                "worker",
                executor=executor,
                executor_api="context_v1",
                max_concurrency=3,
            )
        )
        goal = Goal("preempt-superseded")
        # "bump" produces shared@2 (supersedes "super"'s declared input).
        goal.task(
            "bump", agent="worker", executor_api="context_v1", verify=lambda _c: _pass("shared", 2)
        )
        # "super" reads the now-stale "shared"; it must be interrupted.
        goal.task(
            "super",
            agent="worker",
            executor_api="context_v1",
            inputs=["shared"],
            verify=lambda _c: _pass("super-out"),
        )
        # "clean" reads "other" which never changes; it must NOT be interrupted.
        goal.task(
            "clean",
            agent="worker",
            executor_api="context_v1",
            inputs=["other"],
            verify=lambda _c: _pass("clean-out"),
        )

        running = asyncio.create_task(
            os_.run_async(
                goal,
                max_dispatches=3,
                max_steps=1,
                max_concurrency=3,
                automatic_rebase=False,
                preempt_superseded=True,
            )
        )
        await asyncio.wait_for(started_super.wait(), timeout=5)
        await asyncio.wait_for(started_clean.wait(), timeout=5)

        super_attempt = await _wait_for_attempt_record(os_, task_id="super")
        clean_attempt = await _wait_for_running_attempt(os_, task_id="clean")

        # Mutation proof: the loop actually delivered the interrupt to super's
        # exact claim (durable ack event), not merely that the run finished.
        acks = await _wait_for_interrupt_ack(os_, claim_id=super_attempt.claim_id)
        assert any(
            str(event.metadata.get("reason", "")).startswith("preempt_superseded") for event in acks
        )

        # The still-current peer must not have been interrupted while it runs.
        assert _interrupt_acks(os_, claim_id=clean_attempt.claim_id) == []
        assert not running.done()

        release_clean.set()
        result = await asyncio.wait_for(running, timeout=5)

        # Stale peer stopped without committing Evidence.
        assert "super" not in result.verified
        assert _final_attempt(os_, attempt_id=super_attempt.attempt_id).state is not (
            AttemptState.VERIFIED_SEMANTICALLY
        )
        # Valid work preserved: the superseding task and the current peer commit.
        assert "bump" in result.verified
        assert "clean" in result.verified
        assert _interrupt_acks(os_, claim_id=clean_attempt.claim_id) == []
        assert _final_attempt(os_, attempt_id=clean_attempt.attempt_id).state is (
            AttemptState.VERIFIED_SEMANTICALLY
        )
    finally:
        release_clean.set()
        if not running.done():
            running.cancel()
            with suppress(asyncio.CancelledError):
                await running
        os_.close()


@pytest.mark.asyncio
async def test_preempt_superseded_flag_off_leaves_behaviour_unchanged() -> None:
    """With the flag off, the identical situation delivers no interrupt and the
    declared-stale peer commits exactly as before."""

    started_super = asyncio.Event()
    started_clean = asyncio.Event()
    release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:
        os_._facts.add_version("shared", 1, "shared-v1")
        os_._facts.add_version("other", 1, "other-v1")

        async def executor(ctx: Any, task_id: str) -> None:
            del ctx
            if task_id == "bump":
                await asyncio.wait_for(started_super.wait(), timeout=5)
                await asyncio.wait_for(started_clean.wait(), timeout=5)
                return
            if task_id == "super":
                started_super.set()
                await release.wait()
                return
            if task_id == "clean":
                started_clean.set()
                await release.wait()
                return

        os_.add_agent(
            Agent(
                "worker",
                executor=executor,
                executor_api="context_v1",
                max_concurrency=3,
            )
        )
        goal = Goal("preempt-superseded-off")
        goal.task(
            "bump", agent="worker", executor_api="context_v1", verify=lambda _c: _pass("shared", 2)
        )
        goal.task(
            "super",
            agent="worker",
            executor_api="context_v1",
            inputs=["shared"],
            verify=lambda _c: _pass("super-out"),
        )
        goal.task(
            "clean",
            agent="worker",
            executor_api="context_v1",
            inputs=["other"],
            verify=lambda _c: _pass("clean-out"),
        )

        running = asyncio.create_task(
            os_.run_async(
                goal,
                max_dispatches=3,
                max_steps=1,
                max_concurrency=3,
                automatic_rebase=False,
                # preempt_superseded defaults to False.
            )
        )
        await asyncio.wait_for(started_super.wait(), timeout=5)
        await asyncio.wait_for(started_clean.wait(), timeout=5)
        super_attempt = await _wait_for_attempt_record(os_, task_id="super")
        release.set()
        result = await asyncio.wait_for(running, timeout=5)

        # No interrupt was delivered to anyone: baseline behaviour is identical.
        assert not any(
            event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
            for event in os_.scheduler.events
        )
        # The declared-stale peer commits (it did not observe an interrupt).
        assert "super" in result.verified
        assert _final_attempt(os_, attempt_id=super_attempt.attempt_id).state is (
            AttemptState.VERIFIED_SEMANTICALLY
        )
        assert result.goal_state == "closed"
    finally:
        release.set()
        if not running.done():
            running.cancel()
            with suppress(asyncio.CancelledError):
                await running
        os_.close()


@pytest.mark.asyncio
async def test_preempt_superseded_delivery_fences_exact_identity() -> None:
    """The opt-in ``superseded_from_graph_version`` path never relaxes the
    claim/attempt/epoch identity fences and refuses when nothing advanced."""

    started = asyncio.Event()
    os_ = AgentOS(":memory:")
    running: asyncio.Task[Any] | None = None
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            del task_id
            started.set()
            token = ctx.cancellation_token
            assert token is not None
            await token.wait()
            token.raise_if_cancelled()

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("preempt-superseded-fences")
        goal.task("task-a", agent="worker", executor_api="context_v1", verify=lambda _c: _pass("a"))

        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=5)
        attempt = await _wait_for_running_attempt(os_, task_id="task-a")
        v = attempt.graph_version
        common = {
            "claim_id": attempt.claim_id,
            "task_id": attempt.task_id,
            "attempt_id": attempt.attempt_id,
            "action": "preempt",
            "expected_semantic_epoch": attempt.semantic_epoch,
        }

        # Observed version does not match the attempt's basis -> refuse.
        mismatched = os_.deliver_interrupt(goal, **common, superseded_from_graph_version=v + 5)
        assert mismatched.status is InterruptDeliveryStatus.STALE_GRAPH

        # Correct observed version but the graph never advanced -> refuse
        # (nothing was superseded).
        not_advanced = os_.deliver_interrupt(goal, **common, superseded_from_graph_version=v)
        assert not_advanced.status is InterruptDeliveryStatus.STALE_GRAPH

        original = os_._vpg_surface.current_graph_version
        os_._vpg_surface.current_graph_version = lambda _gid: v + 1
        try:
            # Graph advanced past the observed basis, but the epoch fence still
            # applies on the opt-in path.
            stale_epoch = os_.deliver_interrupt(
                goal,
                claim_id=attempt.claim_id,
                task_id=attempt.task_id,
                attempt_id=attempt.attempt_id,
                action="preempt",
                expected_semantic_epoch=attempt.semantic_epoch + 1,
                superseded_from_graph_version=v,
            )
            assert stale_epoch.status is InterruptDeliveryStatus.STALE_EPOCH

            # Wrong attempt identity still refused on the opt-in path.
            wrong_attempt = os_.deliver_interrupt(
                goal,
                claim_id=attempt.claim_id,
                task_id=attempt.task_id,
                attempt_id="attempt-does-not-match",
                action="preempt",
                expected_semantic_epoch=attempt.semantic_epoch,
                superseded_from_graph_version=v,
            )
            assert wrong_attempt.status is InterruptDeliveryStatus.IDENTITY_MISMATCH

            # Exact identity + observed basis + real advance -> accepted.
            accepted = os_.deliver_interrupt(goal, **common, superseded_from_graph_version=v)
            assert accepted.accepted
            assert accepted.preemptible is True
        finally:
            os_._vpg_surface.current_graph_version = original

        result = await asyncio.wait_for(running, timeout=5)
        assert "task-a" not in result.verified
        assert _final_attempt(os_, attempt_id=attempt.attempt_id).state is not (
            AttemptState.VERIFIED_SEMANTICALLY
        )
    finally:
        if running is not None and not running.done():
            running.cancel()
            with suppress(asyncio.CancelledError):
                await running
        os_.close()
