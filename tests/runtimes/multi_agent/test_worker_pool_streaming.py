"""``run_streaming`` refills a freed slot without waiting for the batch to drain.

``run`` plans a fixed set of jobs and returns only when all of them finish, so a
short task cannot hand its execution slot to anything until the slowest task in
its batch completes. Measured on a 42-task workload that left 26-39% of the
configured concurrency unusable (``artifacts/barrier-cost-20260819.json``), and
the waste was *larger* for the better scheduler, because prioritising the
critical path leaves a queue of runnable-but-deferred cheap work that a barrier
cannot serve.

``run_streaming`` yields outcomes in completion order and accepts ``submit``
while running, so the caller can admit the next task the instant a slot frees.
Admission policy deliberately stays with the caller: deciding what is *safe* to
run next needs the conflict graph, which the pool cannot see.

These tests cover the pool half only. Wiring it into ``run_async`` is separate.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from lhos.runtimes.multi_agent.worker_pool import (
    AsyncWorkerPool,
    WorkerJob,
    WorkerPoolError,
    WorkerStatus,
)
from tests.runtimes.multi_agent.test_async_worker_pool import _Lifecycle, _Result

pytestmark = pytest.mark.asyncio


class _ControlledDispatcher:
    """Each claim finishes only when its own event is set."""

    def __init__(self) -> None:
        self.gates: dict[str, asyncio.Event] = {}
        self.started: list[str] = []
        self.concurrent = 0
        self.peak_concurrent = 0

    def gate(self, claim_id: str) -> asyncio.Event:
        return self.gates.setdefault(claim_id, asyncio.Event())

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> _Result:
        self.started.append(claim_id)
        self.concurrent += 1
        self.peak_concurrent = max(self.peak_concurrent, self.concurrent)
        try:
            await self.gate(claim_id).wait()
        finally:
            self.concurrent -= 1
        return _Result(attempt_id=f"dispatcher-{claim_id}")


def _job(n: int, *, agent: str = "agent-a") -> WorkerJob:
    return WorkerJob(
        graph_id="graph-1",
        task_id=f"task-{n}",
        claim_id=f"claim-{n}",
        agent_id=agent,
        task_kind="test",
        execution_spec={},
    )


def _pool(dispatcher: Any, *, limit: int) -> AsyncWorkerPool:
    return AsyncWorkerPool(dispatcher, scheduler=_Lifecycle(), max_concurrency=limit)


async def test_a_freed_slot_is_refilled_before_the_straggler_finishes() -> None:
    """The regression this whole change exists for."""

    dispatcher = _ControlledDispatcher()
    pool = _pool(dispatcher, limit=2)

    completed: list[str] = []
    refilled = False
    stream = pool.run_streaming([_job(1), _job(2)])
    # Let both start, then finish only the short one.
    dispatcher.gate("claim-1").set()
    async for outcome in stream:
        completed.append(outcome.job.claim_id)
        if not refilled:
            refilled = True
            # claim-2 is still running: its slot was never freed.
            assert "claim-2" not in completed
            pool.submit(_job(3))
            dispatcher.gate("claim-3").set()
        elif outcome.job.claim_id == "claim-3":
            # The refill ran and finished while the straggler was still in flight.
            assert "claim-2" not in completed
            dispatcher.gate("claim-2").set()

    assert completed == ["claim-1", "claim-3", "claim-2"]
    assert dispatcher.started == ["claim-1", "claim-2", "claim-3"]


async def test_streaming_never_exceeds_the_configured_concurrency() -> None:
    """Refilling must respect capacity, or it just oversubscribes the host."""

    dispatcher = _ControlledDispatcher()
    pool = _pool(dispatcher, limit=2)

    stream = pool.run_streaming([_job(index) for index in range(1, 4)])
    for index in range(1, 4):
        dispatcher.gate(f"claim-{index}").set()
    seen = 0
    async for outcome in stream:
        seen += 1
        assert outcome.status is WorkerStatus.SUCCEEDED
        if seen == 1:
            pool.submit(_job(9))
            dispatcher.gate("claim-9").set()

    assert seen == 4
    assert dispatcher.peak_concurrent <= 2


async def test_every_submitted_job_is_reported_exactly_once() -> None:
    dispatcher = _ControlledDispatcher()
    pool = _pool(dispatcher, limit=3)

    stream = pool.run_streaming([_job(1)])
    dispatcher.gate("claim-1").set()
    reported: list[str] = []
    async for outcome in stream:
        reported.append(outcome.job.claim_id)
        if len(reported) == 1:
            for index in (2, 3):
                pool.submit(_job(index))
                dispatcher.gate(f"claim-{index}").set()

    assert sorted(reported) == ["claim-1", "claim-2", "claim-3"]
    assert len(reported) == len(set(reported))


async def test_duplicate_claim_is_rejected_like_the_batch_path() -> None:
    """``run`` rejects duplicates and leaves the active claim alone; so must this."""

    dispatcher = _ControlledDispatcher()
    pool = _pool(dispatcher, limit=2)

    stream = pool.run_streaming([_job(1)])
    dispatcher.gate("claim-1").set()
    statuses: list[WorkerStatus] = []
    submitted_duplicate = False
    async for outcome in stream:
        statuses.append(outcome.status)
        if not submitted_duplicate:
            submitted_duplicate = True
            pool.submit(_job(1))  # same claim_id as one already admitted

    assert WorkerStatus.REJECTED in statuses
    # The duplicate must never reach the dispatcher.
    assert dispatcher.started.count("claim-1") == 1


async def test_submit_outside_a_streaming_pass_is_refused() -> None:
    """Silently dropping the job would lose work with no signal."""

    pool = _pool(_ControlledDispatcher(), limit=1)
    with pytest.raises(WorkerPoolError, match="requires an active run_streaming"):
        pool.submit(_job(1))


async def test_two_concurrent_streaming_passes_are_refused() -> None:
    """Two passes would share one task set and one claim set."""

    dispatcher = _ControlledDispatcher()
    pool = _pool(dispatcher, limit=2)
    stream = pool.run_streaming([_job(1)])
    dispatcher.gate("claim-1").set()
    async for _outcome in stream:
        with pytest.raises(WorkerPoolError, match="already active"):
            second = pool.run_streaming([_job(2)])
            await second.__anext__()


async def test_an_empty_pass_completes_immediately() -> None:
    pool = _pool(_ControlledDispatcher(), limit=2)
    outcomes = [outcome async for outcome in pool.run_streaming([])]
    assert outcomes == []
    # The pass must have released its flag so a later pass can start.
    assert [outcome async for outcome in pool.run_streaming([])] == []


async def test_cancellation_awaits_children_before_propagating() -> None:
    """Claim release and capacity return must complete before cancel escapes."""

    dispatcher = _ControlledDispatcher()
    lifecycle = _Lifecycle()
    pool = AsyncWorkerPool(dispatcher, scheduler=lifecycle, max_concurrency=2)

    async def drive() -> None:
        async for _outcome in pool.run_streaming([_job(1), _job(2)]):
            pass

    task = asyncio.create_task(drive())
    while len(dispatcher.started) < 2:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert dispatcher.concurrent == 0
    assert lifecycle.released
