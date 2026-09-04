"""Deterministic concurrency tests for the asynchronous worker runtime."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from lhos.runtimes.multi_agent import (
    AsyncWorkerPool,
    AttemptState,
    InterruptDeliveryStatus,
    WorkerJob,
    WorkerStatus,
)
from lhos.runtimes.multi_agent.worker_pool import CooperativeInterrupt


@dataclass
class _Result:
    attempt_id: str
    dispatched: bool = True
    error: str | None = None


class _Attempt:
    def __init__(self, attempt_id: str) -> None:
        self.attempt_id = attempt_id
        self.state = AttemptState.DISPATCHED


class _Lifecycle:
    def __init__(self) -> None:
        self.attempts: dict[str, _Attempt] = {}
        self.started: list[str] = []
        self.succeeded: list[str] = []
        self.released: list[tuple[str, str, str, bool]] = []

    def mark_execution_started(self, claim_id: str) -> _Attempt | None:
        attempt = self.attempts.get(claim_id)
        if attempt is None:
            attempt = _Attempt(f"attempt-{claim_id}")
            self.attempts[claim_id] = attempt
        attempt.state = AttemptState.RUNNING
        self.started.append(claim_id)
        return attempt

    def mark_execution_operationally_succeeded(self, claim_id: str) -> _Attempt | None:
        attempt = self.attempts.get(claim_id)
        if attempt is None:
            return None
        attempt.state = AttemptState.SUCCEEDED_OPERATIONALLY
        self.succeeded.append(claim_id)
        return attempt

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
    ) -> None:
        self.released.append((graph_id, task_id, reason, retry))


class _GateDispatcher:
    """Blocks every dispatched job until the test releases its task gate."""

    def __init__(self) -> None:
        self.entered: asyncio.Queue[str] = asyncio.Queue()
        self.gates: dict[str, asyncio.Event] = {}
        self.active = 0
        self.peak = 0
        self.active_by_agent: dict[str, int] = {}
        self.peak_by_agent: dict[str, int] = {}

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _Result:
        self.active += 1
        self.peak = max(self.peak, self.active)
        agent_active = self.active_by_agent.get(agent_id, 0) + 1
        self.active_by_agent[agent_id] = agent_active
        self.peak_by_agent[agent_id] = max(
            agent_active,
            self.peak_by_agent.get(agent_id, 0),
        )
        await self.entered.put(task_id)
        gate = self.gates.setdefault(task_id, asyncio.Event())
        try:
            await gate.wait()
            failure = execution_spec.get("raise")
            if failure:
                raise RuntimeError(str(failure))
            if execution_spec.get("reject"):
                return _Result(
                    attempt_id=f"dispatcher-{claim_id}",
                    dispatched=False,
                    error="not accepted",
                )
            return _Result(attempt_id=f"dispatcher-{claim_id}")
        finally:
            self.active -= 1
            agent_active = self.active_by_agent[agent_id] - 1
            if agent_active:
                self.active_by_agent[agent_id] = agent_active
            else:
                self.active_by_agent.pop(agent_id, None)

    async def next_entered(self) -> str:
        return await asyncio.wait_for(self.entered.get(), timeout=1)

    def release(self, task_id: str) -> None:
        self.gates.setdefault(task_id, asyncio.Event()).set()


class _SlowDispatcher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release_event = asyncio.Event()

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _Result:
        self.started.set()
        await self.release_event.wait()
        return _Result(attempt_id=f"dispatcher-{claim_id}")


class _CooperativeDispatcher:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.acknowledged = asyncio.Event()

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
        cancellation_token: Any,
    ) -> _Result:
        del agent_id, task_id, task_kind, execution_spec
        self.started.set()
        try:
            await cancellation_token.wait()
            cancellation_token.raise_if_cancelled()
        except CooperativeInterrupt:
            self.acknowledged.set()
            raise
        raise AssertionError("cooperative token wait returned without cancellation")


class _HeartbeatCompletionRaceDispatcher:
    """Finish dispatch only after a periodic heartbeat is in flight."""

    def __init__(self, heartbeat_started: asyncio.Event, dispatch_done: asyncio.Event) -> None:
        self.heartbeat_started = heartbeat_started
        self.dispatch_done = dispatch_done
        self.started = asyncio.Event()

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _Result:
        del agent_id, task_id, task_kind, execution_spec
        self.started.set()
        await self.heartbeat_started.wait()
        self.dispatch_done.set()
        return _Result(attempt_id=f"dispatcher-{claim_id}")


class _FailDispatcher:
    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _Result:
        raise RuntimeError("stale worker failed")


class _HeartbeatLifecycle(_Lifecycle):
    def __init__(self, *, heartbeat_result: bool = True) -> None:
        super().__init__()
        self.heartbeat_calls: list[str] = []
        self.heartbeat_result = heartbeat_result

    def heartbeat(self, claim_id: str, **kwargs: Any) -> bool:
        self.heartbeat_calls.append(claim_id)
        return self.heartbeat_result


class _ReplacementClaimLifecycle(_Lifecycle):
    """Models a replacement claim created before stale-worker cleanup runs."""

    def __init__(self) -> None:
        super().__init__()
        self.current_claim_id = "replacement-claim"
        self.release_calls: list[tuple[str, str | None]] = []
        self.released_claims: list[str] = []

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
        expected_claim_id: str | None = None,
    ) -> bool:
        del graph_id, task_id, reason, retry
        self.release_calls.append((self.current_claim_id, expected_claim_id))
        # An unfenced stale release would incorrectly release the replacement.
        if expected_claim_id is None or expected_claim_id == self.current_claim_id:
            self.released_claims.append(self.current_claim_id)
            return True
        return False


class _OperationalFenceLifecycle(_Lifecycle):
    """Simulate a semantic interrupt winning before operational completion."""

    def mark_execution_operationally_succeeded(self, claim_id: str) -> _Attempt | None:
        attempt = self.attempts.get(claim_id)
        if attempt is not None:
            attempt.state = AttemptState.STALE_COGNITION
        return None


class _BrokenHeartbeatPool(AsyncWorkerPool):
    """Adversarial pool whose heartbeat loop exits without publishing failure."""

    @staticmethod
    async def _heartbeat_loop(
        source: Any,
        interval: float,
        failure: asyncio.Future[BaseException],
        claim_id: str,
    ) -> None:
        return


def _job(
    n: int,
    *,
    agent: str = "agent-a",
    units: int = 1,
    spec: dict[str, Any] | None = None,
) -> WorkerJob:
    return WorkerJob(
        graph_id="graph-1",
        task_id=f"task-{n}",
        claim_id=f"claim-{n}",
        agent_id=agent,
        task_kind="test",
        execution_spec=spec or {},
        capacity_units=units,
    )


async def test_pool_executes_real_overlap_but_never_exceeds_global_capacity():
    dispatcher = _GateDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=2,
    )

    running = asyncio.create_task(pool.run([_job(0), _job(1), _job(2), _job(3)]))
    first_wave = {await dispatcher.next_entered(), await dispatcher.next_entered()}
    assert first_wave == {"task-0", "task-1"}
    assert dispatcher.peak == 2
    assert pool.active_jobs == 2
    assert pool.capacity_snapshot()["global"] == {
        "limit": 2,
        "in_use": 2,
        "available": 0,
    }

    # A third dispatch cannot enter while both execution slots are occupied.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(dispatcher.entered.get(), timeout=0.02)

    for task_id in first_wave:
        dispatcher.release(task_id)
    second_wave = {await dispatcher.next_entered(), await dispatcher.next_entered()}
    assert second_wave == {"task-2", "task-3"}
    assert dispatcher.peak == 2
    for task_id in second_wave:
        dispatcher.release(task_id)

    outcomes = await asyncio.wait_for(running, timeout=1)
    assert [outcome.task_id for outcome in outcomes] == [
        "task-0",
        "task-1",
        "task-2",
        "task-3",
    ]
    assert all(outcome.status == WorkerStatus.SUCCEEDED for outcome in outcomes)
    assert pool.active_jobs == 0
    assert pool.active_capacity_units == 0
    assert pool.capacity_snapshot()["global"]["available"] == 2


async def test_agent_capacity_is_enforced_without_serializing_other_agents():
    dispatcher = _GateDispatcher()
    pool = AsyncWorkerPool(
        dispatcher,
        max_concurrency=3,
        agent_concurrency={"agent-a": 1, "agent-b": 2},
    )
    jobs = [
        _job(0, agent="agent-a"),
        _job(1, agent="agent-a"),
        _job(2, agent="agent-b"),
        _job(3, agent="agent-b"),
    ]
    running = asyncio.create_task(pool.run(jobs))

    first_wave = {await dispatcher.next_entered() for _ in range(3)}
    assert first_wave == {"task-0", "task-2", "task-3"}
    assert dispatcher.peak_by_agent == {"agent-a": 1, "agent-b": 2}
    assert pool.active_by_agent == {"agent-a": 1, "agent-b": 2}

    # Release one B slot: A's second task remains blocked by A's own limit.
    dispatcher.release("task-2")
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(dispatcher.entered.get(), timeout=0.02)

    dispatcher.release("task-0")
    assert await dispatcher.next_entered() == "task-1"
    dispatcher.release("task-1")
    dispatcher.release("task-3")
    outcomes = await asyncio.wait_for(running, timeout=1)
    assert all(outcome.ok for outcome in outcomes)
    assert dispatcher.peak_by_agent == {"agent-a": 1, "agent-b": 2}


async def test_weighted_capacity_reserves_units_and_never_overcommits():
    dispatcher = _GateDispatcher()
    pool = AsyncWorkerPool(dispatcher, max_concurrency=3)
    running = asyncio.create_task(pool.run([_job(0, units=2), _job(1, units=2)]))

    assert await dispatcher.next_entered() == "task-0"
    assert pool.active_capacity_units == 2
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(dispatcher.entered.get(), timeout=0.02)

    dispatcher.release("task-0")
    assert await dispatcher.next_entered() == "task-1"
    assert pool.active_capacity_units == 2
    dispatcher.release("task-1")
    outcomes = await asyncio.wait_for(running, timeout=1)
    assert all(outcome.ok for outcome in outcomes)
    assert pool.capacity_snapshot()["global"]["available"] == 3


async def test_weighted_capacity_is_atomic_under_three_competing_jobs():
    dispatcher = _GateDispatcher()
    pool = AsyncWorkerPool(dispatcher, max_concurrency=3)
    running = asyncio.create_task(
        pool.run(
            [
                _job(0, units=2),
                _job(1, units=2),
                _job(2, units=2),
            ]
        )
    )

    first = await dispatcher.next_entered()
    assert first == "task-0"
    assert pool.active_capacity_units == 2
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(dispatcher.entered.get(), timeout=0.02)

    dispatcher.release(first)
    second = await dispatcher.next_entered()
    dispatcher.release(second)
    third = await dispatcher.next_entered()
    dispatcher.release(third)

    outcomes = await asyncio.wait_for(running, timeout=1)
    assert [outcome.status for outcome in outcomes] == [
        WorkerStatus.SUCCEEDED,
        WorkerStatus.SUCCEEDED,
        WorkerStatus.SUCCEEDED,
    ]
    assert dispatcher.peak == 1
    assert pool.capacity_snapshot()["global"]["available"] == 3


async def test_failure_releases_claim_and_capacity_then_next_job_runs():
    dispatcher = _GateDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
    )
    running = asyncio.create_task(
        pool.run(
            [
                _job(0, spec={"raise": "boom"}),
                _job(1),
            ]
        )
    )

    assert await dispatcher.next_entered() == "task-0"
    dispatcher.release("task-0")
    assert await dispatcher.next_entered() == "task-1"
    dispatcher.release("task-1")
    failed, succeeded = await asyncio.wait_for(running, timeout=1)

    assert failed.status == WorkerStatus.FAILED
    assert failed.error == "RuntimeError: boom"
    assert succeeded.status == WorkerStatus.SUCCEEDED
    assert lifecycle.released == [
        ("graph-1", "task-0", "execution_failed", True),
    ]
    assert lifecycle.succeeded == ["claim-1"]
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_stale_worker_failure_does_not_release_replacement_claim():
    dispatcher = _FailDispatcher()
    lifecycle = _ReplacementClaimLifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)

    [outcome] = await asyncio.wait_for(pool.run([_job(0)]), timeout=1)

    assert outcome.status == WorkerStatus.FAILED
    assert lifecycle.release_calls == [("replacement-claim", "claim-0")]
    assert lifecycle.released_claims == []
    assert pool.active_jobs == 0
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_dispatch_rejection_releases_claim_and_does_not_mark_success():
    dispatcher = _GateDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)
    running = asyncio.create_task(pool.run([_job(0, spec={"reject": True})]))

    assert await dispatcher.next_entered() == "task-0"
    dispatcher.release("task-0")
    [outcome] = await asyncio.wait_for(running, timeout=1)

    assert outcome.status == WorkerStatus.REJECTED
    assert lifecycle.succeeded == []
    assert lifecycle.released == [
        ("graph-1", "task-0", "dispatch_rejected", True),
    ]


async def test_cancelling_pool_waits_for_claim_and_capacity_cleanup():
    dispatcher = _GateDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)
    running = asyncio.create_task(pool.run([_job(0), _job(1)]))

    assert await dispatcher.next_entered() == "task-0"
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert lifecycle.released == [
        ("graph-1", "task-0", "worker_cancelled", True),
        ("graph-1", "task-1", "worker_cancelled", True),
    ]
    assert pool.active_jobs == 0
    assert pool.active_capacity_units == 0
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_cooperative_interrupt_routes_token_and_fenced_cleanup():
    dispatcher = _CooperativeDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)
    running = asyncio.create_task(pool.run([_job(0)]))
    await dispatcher.started.wait()

    delivery = pool.request_interrupt(
        "claim-0",
        action="rebase",
        interrupt_id="interrupt-0",
        reason="artifact changed",
    )
    assert delivery.status is InterruptDeliveryStatus.REQUESTED
    assert delivery.accepted
    assert (await asyncio.wait_for(running, timeout=1))[0].status is WorkerStatus.CANCELLED
    assert dispatcher.acknowledged.is_set()
    assert lifecycle.released == [
        ("graph-1", "task-0", "semantic_interrupt:rebase", True),
    ]
    assert pool.active_jobs == 0
    assert pool.request_interrupt("claim-0").status is InterruptDeliveryStatus.NOT_RUNNING


async def test_legacy_dispatcher_does_not_receive_interrupt_keyword():
    dispatcher = _SlowDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)
    running = asyncio.create_task(pool.run([_job(0)]))
    await dispatcher.started.wait()
    delivery = pool.request_interrupt("claim-0", action="preempt")
    assert delivery.status is InterruptDeliveryStatus.REQUESTED
    dispatcher.release_event.set()
    [outcome] = await asyncio.wait_for(running, timeout=1)
    # The legacy dispatcher ignores the token, so it completes normally; the
    # pool still never injects a new keyword into its call signature.
    assert outcome.status is WorkerStatus.SUCCEEDED


def test_interrupt_acknowledgement_is_bounded_durable_and_idempotent(tmp_path):
    from lhos.runtimes.multi_agent.durable_state import SchedulerStateStore
    from lhos.runtimes.multi_agent.events import SchedulerEventType
    from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler

    db = tmp_path / "interrupt-ack.sqlite"
    vpg = FakeVPG()
    scheduler = fake_scheduler(
        {"agent-a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
        fake_vpg=vpg,
        state_path=str(db),
    )
    first = scheduler.record_interrupt_acknowledgement(
        graph_id=vpg.graph_id,
        graph_version=0,
        claim_id="claim-0",
        attempt_id="attempt-0",
        interrupt_id="interrupt-0",
        action="rebase",
        status="requested",
        decision_hash="a" * 64,
        reason="bounded reason",
    )
    second = scheduler.record_interrupt_acknowledgement(
        graph_id=vpg.graph_id,
        graph_version=0,
        claim_id="claim-0",
        attempt_id="attempt-0",
        interrupt_id="interrupt-0",
        action="rebase",
        status="requested",
        decision_hash="a" * 64,
        reason="bounded reason",
    )
    assert first.event_id == second.event_id
    assert (
        len(
            [
                event
                for event in scheduler.events
                if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
            ]
        )
        == 1
    )
    scheduler.close()
    store = SchedulerStateStore(db)
    try:
        state = store.load()
        ack = next(
            event
            for event in state.events
            if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_ACKNOWLEDGED
        )
        assert ack.metadata["action"] == "rebase"
        assert ack.metadata["status"] == "requested"
        assert ack.metadata["reason"] == "bounded reason"
        assert len(ack.metadata) <= 6
    finally:
        store.close()


async def test_operational_success_is_not_semantic_verification():
    dispatcher = _GateDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=1)
    running = asyncio.create_task(pool.run([_job(0)]))

    assert await dispatcher.next_entered() == "task-0"
    dispatcher.release("task-0")
    [outcome] = await asyncio.wait_for(running, timeout=1)

    assert outcome.ok
    attempt = lifecycle.attempts["claim-0"]
    assert attempt.state == AttemptState.SUCCEEDED_OPERATIONALLY
    assert attempt.state != AttemptState.VERIFIED_SEMANTICALLY
    assert lifecycle.released == []


async def test_operational_fence_skips_success_callback_and_releases_claim():
    """A stale/terminal lifecycle result must not publish semantic success."""

    dispatcher = _GateDispatcher()
    lifecycle = _OperationalFenceLifecycle()
    success_callbacks: list[str] = []

    async def on_success(job: WorkerJob, _result: Any) -> None:
        success_callbacks.append(job.claim_id)

    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        on_success=on_success,
    )
    running = asyncio.create_task(pool.run([_job(0)]))

    assert await dispatcher.next_entered() == "task-0"
    dispatcher.release("task-0")
    [outcome] = await asyncio.wait_for(running, timeout=1)

    assert outcome.status == WorkerStatus.FAILED
    assert outcome.error is not None
    assert "no longer admits operational completion" in outcome.error
    assert success_callbacks == []
    assert lifecycle.attempts["claim-0"].state == AttemptState.STALE_COGNITION
    assert lifecycle.released == [
        ("graph-1", "task-0", "execution_failed", True),
    ]
    assert pool.capacity_snapshot()["global"]["available"] == 1


def test_worker_job_validates_capacity_and_can_consume_schedule_result_mapping():
    job = WorkerJob.from_dispatch(
        {
            "task_id": "task-x",
            "claim_id": "claim-x",
            "agent_id": "agent-x",
        },
        graph_id="graph-x",
        execution_spec={"prompt": "safe copy"},
    )
    assert job.graph_id == "graph-x"
    assert job.execution_spec == {"prompt": "safe copy"}
    with pytest.raises(ValueError, match="capacity_units must be >= 1"):
        WorkerJob(
            task_id="task",
            claim_id="claim",
            agent_id="agent",
            capacity_units=0,
        )


async def test_duplicate_claim_is_rejected_before_second_dispatch():
    dispatcher = _GateDispatcher()
    job = _job(0)
    pool = AsyncWorkerPool(dispatcher, max_concurrency=2)
    running = asyncio.create_task(pool.run([job, job]))

    assert await dispatcher.next_entered() == "task-0"
    dispatcher.release("task-0")
    first, duplicate = await asyncio.wait_for(running, timeout=1)
    assert first.ok
    assert duplicate.status == WorkerStatus.REJECTED
    assert duplicate.error == "duplicate claim_id submitted to worker pool"


async def test_pool_runs_cooperative_heartbeat_during_long_dispatch():
    dispatcher = _SlowDispatcher()
    lifecycle = _HeartbeatLifecycle()
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        heartbeat_interval=0.005,
    )
    running = asyncio.create_task(pool.run([_job(0)]))
    await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
    for _ in range(3):
        await asyncio.sleep(0.008)
        if lifecycle.heartbeat_calls:
            break
    assert lifecycle.heartbeat_calls == ["claim-0"] or lifecycle.heartbeat_calls
    dispatcher.release_event.set()
    [outcome] = await asyncio.wait_for(running, timeout=1)
    assert outcome.status == WorkerStatus.SUCCEEDED
    assert lifecycle.released == []


async def test_heartbeat_rejection_fails_job_and_releases_claim():
    dispatcher = _SlowDispatcher()
    lifecycle = _HeartbeatLifecycle(heartbeat_result=False)
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        heartbeat_interval=0.001,
    )
    running = asyncio.create_task(pool.run([_job(0)]))
    [outcome] = await asyncio.wait_for(running, timeout=1)
    assert outcome.status == WorkerStatus.FAILED
    assert outcome.error is not None and "heartbeat rejected" in outcome.error
    assert lifecycle.released == [
        ("graph-1", "task-0", "heartbeat_failed", True),
    ]
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_immediate_heartbeat_rejection_fails_before_first_interval():
    """The first renewal is immediate, not delayed by the configured period."""
    dispatcher = _SlowDispatcher()
    lifecycle = _HeartbeatLifecycle(heartbeat_result=False)
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        # A long period makes a delayed-first-heartbeat implementation hang.
        heartbeat_interval=60.0,
    )

    [outcome] = await asyncio.wait_for(pool.run([_job(0)]), timeout=1)

    assert outcome.status == WorkerStatus.FAILED
    assert outcome.error is not None and "heartbeat rejected" in outcome.error
    assert lifecycle.heartbeat_calls == ["claim-0"]
    # The initial renewal is completed before user-side dispatch starts.
    assert not dispatcher.started.is_set()
    assert lifecycle.released == [
        ("graph-1", "task-0", "heartbeat_failed", True),
    ]
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_dispatch_completion_does_not_swallow_inflight_heartbeat_rejection():
    """A heartbeat awaiting a provider response wins over dispatch success."""
    heartbeat_started = asyncio.Event()
    dispatch_done = asyncio.Event()
    dispatcher = _HeartbeatCompletionRaceDispatcher(heartbeat_started, dispatch_done)
    lifecycle = _Lifecycle()
    calls = 0

    async def heartbeat(_job: WorkerJob, _attempt_id: str) -> bool:
        nonlocal calls
        calls += 1
        if calls == 1:
            return True
        heartbeat_started.set()
        await dispatch_done.wait()
        return False

    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        heartbeat_interval=0.001,
        heartbeat_callback=heartbeat,
    )

    [outcome] = await asyncio.wait_for(pool.run([_job(0)]), timeout=1)

    assert outcome.status == WorkerStatus.FAILED
    assert outcome.error is not None and "heartbeat rejected" in outcome.error
    assert calls >= 2
    assert lifecycle.released == [
        ("graph-1", "task-0", "heartbeat_failed", True),
    ]
    assert pool.capacity_snapshot()["global"]["available"] == 1


async def test_cancelling_heartbeat_dispatch_cleans_up_heartbeat_and_capacity():
    dispatcher = _SlowDispatcher()
    lifecycle = _HeartbeatLifecycle()
    pool = AsyncWorkerPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        heartbeat_interval=0.001,
    )
    running = asyncio.create_task(pool.run([_job(0)]))
    await asyncio.wait_for(dispatcher.started.wait(), timeout=1)
    await asyncio.sleep(0.004)
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    assert pool.active_jobs == 0
    assert pool.active_capacity_units == 0
    assert pool.capacity_snapshot()["global"]["available"] == 1
    assert lifecycle.released == [
        ("graph-1", "task-0", "worker_cancelled", True),
    ]


async def test_unexpected_heartbeat_loop_exit_fails_closed_and_cleans_up():
    dispatcher = _SlowDispatcher()
    lifecycle = _HeartbeatLifecycle()
    pool = _BrokenHeartbeatPool(
        dispatcher,
        scheduler=lifecycle,
        max_concurrency=1,
        heartbeat_interval=0.001,
    )

    [outcome] = await asyncio.wait_for(pool.run([_job(0)]), timeout=1)

    assert outcome.status == WorkerStatus.FAILED
    assert outcome.error is not None
    assert "heartbeat loop exited unexpectedly" in outcome.error
    assert lifecycle.released == [
        ("graph-1", "task-0", "heartbeat_failed", True),
    ]
    assert pool.active_jobs == 0
    assert pool.active_capacity_units == 0
    assert pool.capacity_snapshot()["global"]["available"] == 1
