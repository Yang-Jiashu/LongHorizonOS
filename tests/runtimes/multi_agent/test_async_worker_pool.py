"""Deterministic concurrency tests for the asynchronous worker runtime."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from lhos.runtimes.multi_agent import (
    AsyncWorkerPool,
    AttemptState,
    WorkerJob,
    WorkerStatus,
)


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
