"""End-to-end semantic-interrupt tests for the public AgentOS async runtime.

These tests intentionally exercise the SDK composition root rather than the
worker-pool unit API.  A semantic interrupt must be checked against the exact
graph/task/claim/attempt identity, reach a cooperative ``context_v1`` callback,
and prevent the interrupted attempt from publishing VERIFIED evidence.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest

from lhos.runtimes.multi_agent import AttemptState, InterruptDeliveryStatus
from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome


def _pass(artifact_id: str = "artifact") -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


async def _wait_for_attempt(os_: AgentOS, *, task_id: str) -> Any:
    """Wait until the scheduler has a live running attempt for ``task_id``."""

    for _ in range(200):
        for attempt in os_.scheduler.attempts:
            if attempt.task_id == task_id and attempt.state is AttemptState.RUNNING:
                return attempt
        await asyncio.sleep(0.005)
    raise AssertionError(f"running attempt for {task_id!r} was not observed")


async def _wait_for_attempt_record(os_: AgentOS, *, task_id: str) -> Any:
    """Wait until any durable attempt record exists, regardless of state."""

    for _ in range(200):
        for attempt in os_.scheduler.attempts:
            if attempt.task_id == task_id:
                return attempt
        await asyncio.sleep(0.005)
    raise AssertionError(f"attempt for {task_id!r} was not observed")


async def _cooperative_executor(ctx: Any, task_id: str, started: asyncio.Event) -> None:
    del task_id
    started.set()
    # The SDK binds the worker token to ExecutionContext before invoking the
    # callback.  Waiting on the token makes observation explicit and durable.
    token = ctx.cancellation_token
    assert token is not None
    await token.wait()
    token.raise_if_cancelled()


@pytest.mark.asyncio
async def test_sdk_cooperative_interrupt_is_observed_and_not_verified() -> None:
    started = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda ctx, task_id: _cooperative_executor(ctx, task_id, started),
                executor_api="context_v1",
            )
        )
        goal = Goal("sdk-interrupt")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass("should-not-commit"),
        )

        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None

        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="rebase",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-1",
            decision_hash="a" * 64,
            reason="artifact changed",
        )
        assert delivery.status in {
            InterruptDeliveryStatus.REQUESTED,
            InterruptDeliveryStatus.ALREADY_REQUESTED,
            InterruptDeliveryStatus.DELIVERED,
            InterruptDeliveryStatus.ALREADY_DELIVERED,
        }
        assert delivery.preemptible is True

        result = await asyncio.wait_for(running, timeout=3)
        assert "task-a" not in result.verified
        assert result.goal_state != "closed"
        assert any("task-a" in failure for failure in result.failures)
        final_attempt = next(
            item for item in os_.scheduler.attempts if item.attempt_id == attempt.attempt_id
        )
        assert final_attempt.state is not AttemptState.VERIFIED_SEMANTICALLY
        assert any(
            event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
            and event.claim_id == attempt.claim_id
            for event in os_.scheduler.events
        )
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_sdk_ignored_cooperative_interrupt_quarantines_late_completion() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def ignored_executor(ctx: Any, task_id: str) -> None:
            del ctx, task_id
            started.set()
            await release.wait()

        os_.add_agent(
            Agent(
                "worker",
                executor=ignored_executor,
                executor_api="context_v1",
            )
        )
        goal = Goal("sdk-interrupt-ignored")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass("late"),
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")
        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="preempt",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-ignored",
        )
        assert delivery.preemptible is True
        # The callback deliberately ignores the token; completion must still
        # be quarantined at the worker operational-success fence.
        release.set()
        result = await asyncio.wait_for(running, timeout=3)
        assert "task-a" not in result.verified
        final_attempt = next(
            item for item in os_.scheduler.attempts if item.attempt_id == attempt.attempt_id
        )
        assert final_attempt.state is not AttemptState.VERIFIED_SEMANTICALLY
        assert final_attempt.error == "semantic_interrupt_unobserved:preempt"
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_sdk_legacy_callback_reports_non_preemptible_and_can_finish() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def legacy_executor(task_id: str) -> None:
            del task_id
            started.set()
            await release.wait()

        os_.add_agent(Agent("worker", executor=legacy_executor))
        goal = Goal("sdk-interrupt-legacy")
        goal.task("task-a", agent="worker", verify=lambda: _pass("legacy"))
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")
        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="preempt",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-legacy",
        )
        assert delivery.status is InterruptDeliveryStatus.NON_PREEMPTIBLE
        assert delivery.preemptible is False
        release.set()
        result = await asyncio.wait_for(running, timeout=3)
        assert result.goal_state == "closed"
        assert "task-a" in result.verified
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_sdk_interrupt_rejects_stale_graph_epoch_and_identity() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            del ctx, task_id
            started.set()
            await release.wait()

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("sdk-interrupt-fences")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass(),
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")
        common = {
            "claim_id": attempt.claim_id,
            "task_id": attempt.task_id,
            "attempt_id": attempt.attempt_id,
            "action": "preempt",
        }

        stale_graph = os_.deliver_interrupt(
            goal,
            **common,
            expected_graph_version=attempt.graph_version + 1,
            expected_semantic_epoch=attempt.semantic_epoch,
        )
        assert stale_graph.status is InterruptDeliveryStatus.STALE_GRAPH

        stale_epoch = os_.deliver_interrupt(
            goal,
            **common,
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch + 1,
        )
        assert stale_epoch.status is InterruptDeliveryStatus.STALE_EPOCH

        wrong_claim = os_.deliver_interrupt(
            goal,
            **{**common, "claim_id": "claim-does-not-match"},
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
        )
        assert wrong_claim.status is InterruptDeliveryStatus.IDENTITY_MISMATCH

        wrong_task = os_.deliver_interrupt(
            goal,
            **{**common, "task_id": "task-does-not-match"},
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
        )
        assert wrong_task.status is InterruptDeliveryStatus.IDENTITY_MISMATCH

        wrong_attempt = os_.deliver_interrupt(
            goal,
            **{**common, "attempt_id": "attempt-does-not-match"},
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
        )
        assert wrong_attempt.status is InterruptDeliveryStatus.IDENTITY_MISMATCH

        # A rejected request must not stop the live callback.
        assert not running.done()
        release.set()
        result = await asyncio.wait_for(running, timeout=3)
        assert result.goal_state == "closed"
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_sdk_interrupt_rejects_attempt_from_currently_advanced_graph_without_optional_fence() -> (
    None
):
    """The attempt snapshot is fenced even when the caller omits a version."""

    started = asyncio.Event()
    release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            del ctx, task_id
            started.set()
            await release.wait()

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("sdk-interrupt-unconditional-graph-fence")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass(),
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")

        # Simulate an externally observed semantic graph update after this
        # attempt started.  The caller intentionally supplies no optional
        # expected_graph_version; the attempt's own snapshot must still fence
        # delivery.
        current = attempt.graph_version + 1
        original = os_._vpg_surface.current_graph_version
        os_._vpg_surface.current_graph_version = lambda _gid: current
        try:
            delivery = os_.deliver_interrupt(
                goal,
                claim_id=attempt.claim_id,
                task_id=attempt.task_id,
                attempt_id=attempt.attempt_id,
                action="rebase",
                expected_semantic_epoch=attempt.semantic_epoch,
            )
        finally:
            os_._vpg_surface.current_graph_version = original

        assert delivery.status is InterruptDeliveryStatus.STALE_GRAPH
        assert delivery.graph_version == current
        assert not running.done()
        release.set()
        result = await asyncio.wait_for(running, timeout=3)
        assert result.goal_state == "closed"
    finally:
        release.set()
        os_.close()


@pytest.mark.asyncio
async def test_sdk_interrupt_during_verifier_is_fenced_before_semantic_commit() -> None:
    """A request after operational success must still invalidate the commit."""

    executor_done = asyncio.Event()
    verifier_started = asyncio.Event()
    verifier_release = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            del ctx, task_id
            executor_done.set()

        async def verifier(ctx: Any) -> VerificationOutcome:
            del ctx
            verifier_started.set()
            await verifier_release.wait()
            return _pass("verifier-race")

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("sdk-interrupt-verifier-race")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=verifier,
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(executor_done.wait(), timeout=2)
        await asyncio.wait_for(verifier_started.wait(), timeout=2)
        attempt = await _wait_for_attempt_record(os_, task_id="task-a")
        for _ in range(200):
            current = next(
                item for item in os_.scheduler.attempts if item.attempt_id == attempt.attempt_id
            )
            if current.state is AttemptState.SUCCEEDED_OPERATIONALLY:
                break
            await asyncio.sleep(0.005)
        else:
            raise AssertionError("verifier did not observe operational success")

        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="rebase",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-verifier-race",
            reason="input changed during verification",
        )
        assert delivery.accepted
        verifier_release.set()
        result = await asyncio.wait_for(running, timeout=3)

        assert "task-a" not in result.verified
        final_attempt = next(
            item for item in os_.scheduler.attempts if item.attempt_id == attempt.attempt_id
        )
        assert final_attempt.state is not AttemptState.VERIFIED_SEMANTICALLY
    finally:
        verifier_release.set()
        os_.close()


@pytest.mark.asyncio
async def test_sdk_verifier_observed_interrupt_is_stale_cognition_not_verifier_failure() -> None:
    """A verifier that explicitly observes a token cannot publish a failure retry."""

    executor_done = asyncio.Event()
    verifier_started = asyncio.Event()
    os_ = AgentOS(":memory:")
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            del ctx, task_id
            executor_done.set()

        async def verifier(ctx: Any) -> VerificationOutcome:
            verifier_started.set()
            token = ctx.cancellation_token
            assert token is not None
            await token.wait()
            ctx.raise_if_interrupted()
            raise AssertionError("raise_if_interrupted must not return after a request")

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("sdk-interrupt-verifier-observed")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=verifier,
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(executor_done.wait(), timeout=2)
        await asyncio.wait_for(verifier_started.wait(), timeout=2)
        attempt = await _wait_for_attempt_record(os_, task_id="task-a")
        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="rebase",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-verifier-observed",
        )
        assert delivery.accepted

        result = await asyncio.wait_for(running, timeout=3)
        assert "task-a" not in result.verified
        final_attempt = next(
            item for item in os_.scheduler.attempts if item.attempt_id == attempt.attempt_id
        )
        assert final_attempt.state is AttemptState.STALE_COGNITION
        assert final_attempt.error is not None
        assert "semantic_interrupt" in final_attempt.error
        assert "verifier_failed" not in final_attempt.error
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_sdk_interrupt_routes_to_exact_pool_when_same_graph_runs_concurrently() -> None:
    """Independent run_async callers must not overwrite each other's pool."""

    started: dict[str, asyncio.Event] = {
        "task-a": asyncio.Event(),
        "task-b": asyncio.Event(),
    }
    run_one: asyncio.Task[Any] | None = None
    run_two: asyncio.Task[Any] | None = None
    os_ = AgentOS(":memory:")
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            started[task_id].set()
            token = ctx.cancellation_token
            assert token is not None
            await token.wait()
            ctx.raise_if_interrupted()

        os_.add_agent(
            Agent(
                "worker",
                executor=executor,
                executor_api="context_v1",
                max_concurrency=2,
            )
        )
        goal = Goal("sdk-interrupt-concurrent-runs")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass("a"),
        )
        goal.task(
            "task-b",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass("b"),
        )

        run_one = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started["task-a"].wait(), timeout=2)
        attempt_a = await _wait_for_attempt(os_, task_id="task-a")

        run_two = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started["task-b"].wait(), timeout=2)
        attempt_b = await _wait_for_attempt(os_, task_id="task-b")
        assert attempt_a.claim_id != attempt_b.claim_id

        # Interrupting A must address run one's pool, not the most recently
        # registered pool for the shared graph.  Once run one cleans up, B
        # must remain routable through run two's pool.
        delivery_a = os_.deliver_interrupt(
            goal,
            claim_id=attempt_a.claim_id,
            task_id=attempt_a.task_id,
            attempt_id=attempt_a.attempt_id,
            action="rebase",
            expected_graph_version=attempt_a.graph_version,
            expected_semantic_epoch=attempt_a.semantic_epoch,
            interrupt_id="sdk-int-concurrent-a",
        )
        assert delivery_a.accepted
        result_one = await asyncio.wait_for(run_one, timeout=3)
        assert "task-a" not in result_one.verified

        delivery_b = os_.deliver_interrupt(
            goal,
            claim_id=attempt_b.claim_id,
            task_id=attempt_b.task_id,
            attempt_id=attempt_b.attempt_id,
            action="preempt",
            expected_graph_version=attempt_b.graph_version,
            expected_semantic_epoch=attempt_b.semantic_epoch,
            interrupt_id="sdk-int-concurrent-b",
        )
        assert delivery_b.accepted
        result_two = await asyncio.wait_for(run_two, timeout=3)
        assert "task-b" not in result_two.verified
    finally:
        if run_one is not None and not run_one.done():
            run_one.cancel()
            with suppress(asyncio.CancelledError):
                await run_one
        if run_two is not None and not run_two.done():
            run_two.cancel()
            with suppress(asyncio.CancelledError):
                await run_two
        os_.close()


@pytest.mark.asyncio
async def test_sdk_interrupt_ack_events_survive_reopen(tmp_path: Path) -> None:
    db = tmp_path / "sdk-interrupt-events.sqlite"
    started = asyncio.Event()
    os_ = AgentOS(str(db))
    try:

        async def executor(ctx: Any, task_id: str) -> None:
            started.set()
            await ctx.cancellation_token.wait()
            del task_id
            ctx.raise_if_interrupted()

        os_.add_agent(Agent("worker", executor=executor, executor_api="context_v1"))
        goal = Goal("sdk-interrupt-durable")
        goal.task(
            "task-a",
            agent="worker",
            executor_api="context_v1",
            verify=lambda _ctx: _pass(),
        )
        running = asyncio.create_task(
            os_.run_async(goal, max_dispatches=1, max_steps=1, max_concurrency=1)
        )
        await asyncio.wait_for(started.wait(), timeout=2)
        attempt = await _wait_for_attempt(os_, task_id="task-a")
        delivery = os_.deliver_interrupt(
            goal,
            claim_id=attempt.claim_id,
            task_id=attempt.task_id,
            attempt_id=attempt.attempt_id,
            action="rebase",
            expected_graph_version=attempt.graph_version,
            expected_semantic_epoch=attempt.semantic_epoch,
            interrupt_id="sdk-int-durable",
            decision_hash="b" * 64,
        )
        assert delivery.preemptible is True
        result = await asyncio.wait_for(running, timeout=3)
        assert result.goal_state != "closed"
    finally:
        os_.close()

    reopened = AgentOS(str(db))
    try:
        acknowledgements = [
            event
            for event in reopened.scheduler.events
            if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
            and event.claim_id == attempt.claim_id
        ]
        assert acknowledgements
        statuses = {event.metadata.get("status") for event in acknowledgements}
        assert "requested" in statuses
        assert statuses & {"delivered", "observed", "cancelled"}
    finally:
        reopened.close()
