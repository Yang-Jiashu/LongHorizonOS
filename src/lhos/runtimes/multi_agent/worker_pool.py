"""Capacity-bounded asynchronous execution for scheduled Agent claims.

The multi-agent scheduler decides *who owns* a task.  ``AsyncWorkerPool``
decides *when the owned task is allowed to execute* and drives the injected
dispatcher without blocking the event loop.

This module deliberately keeps the resource model small and explicit:

* ``max_concurrency`` is a global execution-slot limit.
* ``agent_concurrency`` optionally adds a per-agent slot limit.
* ``capacity_units`` on a job reserves more than one slot when a task is
  known to be heavier than a normal unit task.

These limits are execution capacity, not a claim authority.  Kernel-backed
leases and the Scheduler remain authoritative for ownership.  A failed or
cancelled execution therefore releases its claim through the injected
Scheduler before its execution slots are returned.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol


class WorkerPoolError(RuntimeError):
    """Base error for the asynchronous worker pool."""


class CapacityRequestTooLarge(WorkerPoolError):
    """Raised when one job requests more units than a configured limit."""


class DispatchRejected(WorkerPoolError):
    """Raised when an injected dispatcher returns ``dispatched=False``."""


class WorkerLifecycle(Protocol):
    """Minimal public Scheduler lifecycle required by the pool."""

    def mark_execution_started(self, claim: str) -> Any: ...

    def mark_execution_operationally_succeeded(self, claim: str) -> Any: ...

    def release_task(
        self,
        graph_id: str,
        task_id: str,
        *,
        reason: str = "execution_failed",
        retry: bool = True,
    ) -> Any: ...


class AsyncDispatcher(Protocol):
    """Dispatcher shape accepted by :class:`AsyncWorkerPool`."""

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> Any: ...


Callback = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class WorkerJob:
    """One scheduler dispatch record normalized for asynchronous execution."""

    task_id: str
    claim_id: str
    agent_id: str
    task_kind: str = ""
    execution_spec: Mapping[str, Any] = field(default_factory=dict)
    capacity_units: int = 1
    graph_id: str = ""

    def __post_init__(self) -> None:
        for name in ("task_id", "claim_id", "agent_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if isinstance(self.capacity_units, bool) or not isinstance(self.capacity_units, int):
            raise TypeError("capacity_units must be an integer")
        if self.capacity_units < 1:
            raise ValueError("capacity_units must be >= 1")
        if not isinstance(self.execution_spec, Mapping):
            raise TypeError("execution_spec must be a mapping")
        # Freeze the top-level input boundary.  A fresh dict is passed to the
        # dispatcher for every call, so a dispatcher cannot mutate caller data.
        object.__setattr__(self, "execution_spec", dict(self.execution_spec))

    @classmethod
    def from_dispatch(
        cls,
        dispatch: WorkerJob | Mapping[str, Any],
        *,
        task_kind: str = "",
        execution_spec: Mapping[str, Any] | None = None,
        graph_id: str = "",
    ) -> WorkerJob:
        """Build a job from ``ScheduleResult.dispatched`` or an existing job."""
        if isinstance(dispatch, cls):
            return dispatch
        if not isinstance(dispatch, Mapping):
            raise TypeError("dispatch must be a WorkerJob or mapping")
        spec = execution_spec
        if spec is None:
            raw_spec = dispatch.get("execution_spec", {})
            spec = raw_spec if isinstance(raw_spec, Mapping) else {}
        return cls(
            task_id=str(dispatch.get("task_id", "")),
            claim_id=str(dispatch.get("claim_id", "")),
            agent_id=str(dispatch.get("agent_id", "")),
            task_kind=str(dispatch.get("task_kind", task_kind) or ""),
            execution_spec=spec,
            capacity_units=dispatch.get("capacity_units", 1),
            graph_id=str(dispatch.get("graph_id", graph_id) or ""),
        )


class WorkerStatus(StrEnum):
    """Terminal state reported for one pool job."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class WorkerOutcome:
    """Deterministic result of one asynchronous worker job."""

    job: WorkerJob
    status: WorkerStatus
    attempt_id: str = ""
    error: str | None = None
    started: bool = False
    dispatch_result: Any | None = None

    @property
    def task_id(self) -> str:
        return self.job.task_id

    @property
    def claim_id(self) -> str:
        return self.job.claim_id

    @property
    def ok(self) -> bool:
        return self.status == WorkerStatus.SUCCEEDED


class _UnitLimiter:
    """FIFO weighted limiter that grants each request atomically.

    A multi-unit request never holds a partial allocation while waiting for
    the remainder.  That removes the weighted-semaphore deadlock where several
    jobs each acquire one unit and then all wait forever for their final unit.
    Release is synchronous so cancellation cleanup cannot strand capacity.
    """

    def __init__(self, limit: int, *, name: str) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise TypeError(f"{name} must be an integer")
        if limit < 1:
            raise ValueError(f"{name} must be >= 1")
        self.limit = limit
        self.name = name
        self._available = limit
        self._in_use = 0
        self._waiters: deque[tuple[int, asyncio.Future[None]]] = deque()

    async def acquire(self, amount: int) -> None:
        if amount > self.limit:
            raise CapacityRequestTooLarge(
                f"job requests {amount} {self.name} units; limit is {self.limit}"
            )
        if amount < 1:
            raise ValueError(f"{self.name} request must be >= 1")

        if not self._waiters and amount <= self._available:
            self._grant(amount)
            return

        future = asyncio.get_running_loop().create_future()
        waiter = (amount, future)
        self._waiters.append(waiter)
        self._drain_waiters()
        try:
            await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                self.release(amount)
            else:
                with suppress(ValueError):
                    self._waiters.remove(waiter)
                future.cancel()
                self._drain_waiters()
            raise

    def release(self, amount: int) -> None:
        if amount < 1:
            return
        if amount > self._in_use:
            raise RuntimeError(f"{self.name} limiter released more units than acquired")
        self._in_use -= amount
        self._available += amount
        self._drain_waiters()

    def _grant(self, amount: int) -> None:
        self._available -= amount
        self._in_use += amount

    def _drain_waiters(self) -> None:
        while self._waiters:
            amount, future = self._waiters[0]
            if future.cancelled():
                self._waiters.popleft()
                continue
            if amount > self._available:
                return
            self._waiters.popleft()
            self._grant(amount)
            future.set_result(None)

    @property
    def in_use(self) -> int:
        return self._in_use

    @property
    def available(self) -> int:
        return self._available


def _exception_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__


async def _await_if_needed(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class AsyncWorkerPool:
    """Run scheduler-dispatched jobs concurrently under explicit capacities.

    ``run`` preserves input order in its returned outcomes, while execution
    itself is concurrent.  Dispatcher failures are converted to an outcome
    rather than escaping and cancelling unrelated jobs.
    """

    def __init__(
        self,
        dispatcher: AsyncDispatcher,
        *,
        scheduler: WorkerLifecycle | None = None,
        max_concurrency: int = 1,
        agent_concurrency: Mapping[str, int] | None = None,
        on_success: Callback | None = None,
        on_failure: Callback | None = None,
    ) -> None:
        if not hasattr(dispatcher, "dispatch") or not callable(dispatcher.dispatch):
            raise TypeError("dispatcher must provide an async dispatch(...) method")
        self._dispatcher = dispatcher
        self._scheduler = scheduler
        self._global = _UnitLimiter(max_concurrency, name="global capacity")
        self._agent_limiters = {
            agent_id: _UnitLimiter(limit, name=f"agent {agent_id} capacity")
            for agent_id, limit in (agent_concurrency or {}).items()
        }
        self._on_success = on_success
        self._on_failure = on_failure
        self._active_jobs = 0
        self._active_by_agent: dict[str, int] = {}
        self._active_units = 0

    @property
    def active_jobs(self) -> int:
        """Number of jobs that have acquired all required capacities."""
        return self._active_jobs

    @property
    def active_by_agent(self) -> dict[str, int]:
        return dict(self._active_by_agent)

    @property
    def active_capacity_units(self) -> int:
        return self._active_units

    def capacity_snapshot(self) -> dict[str, Any]:
        """Return an audit-friendly, read-only capacity snapshot."""
        return {
            "global": {
                "limit": self._global.limit,
                "in_use": self._global.in_use,
                "available": self._global.available,
            },
            "agents": {
                agent_id: {
                    "limit": limiter.limit,
                    "in_use": limiter.in_use,
                    "available": limiter.available,
                }
                for agent_id, limiter in sorted(self._agent_limiters.items())
            },
        }

    async def run(
        self,
        jobs: Iterable[WorkerJob | Mapping[str, Any]],
    ) -> list[WorkerOutcome]:
        """Execute all jobs and return outcomes in submission order.

        Duplicate claim IDs are rejected before dispatching.  The existing
        active claim is intentionally left untouched for a caller that
        accidentally submitted the same dispatch twice.
        """
        normalized = [WorkerJob.from_dispatch(job) for job in jobs]
        seen_claims: set[str] = set()
        tasks: list[asyncio.Task[WorkerOutcome]] = []
        for job in normalized:
            if job.claim_id in seen_claims:
                tasks.append(
                    asyncio.create_task(
                        self._duplicate_outcome(job),
                        name=f"lhos-worker-rejected-{job.claim_id}",
                    )
                )
                continue
            seen_claims.add(job.claim_id)
            tasks.append(
                asyncio.create_task(
                    self._execute(job),
                    name=f"lhos-worker-{job.agent_id}-{job.task_id}",
                )
            )
        if not tasks:
            return []
        try:
            return list(await asyncio.gather(*tasks))
        except asyncio.CancelledError:
            # ``_execute`` owns per-job cleanup.  Await all children so claim
            # release and capacity return complete before propagating cancel.
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    async def _duplicate_outcome(self, job: WorkerJob) -> WorkerOutcome:
        return WorkerOutcome(
            job=job,
            status=WorkerStatus.REJECTED,
            error="duplicate claim_id submitted to worker pool",
        )

    async def _execute(self, job: WorkerJob) -> WorkerOutcome:
        token: tuple[_UnitLimiter, _UnitLimiter | None, int] | None = None
        started = False
        attempt_id = ""
        try:
            token = await self._acquire_capacity(job)
            self._mark_active(job, delta=1)

            if self._scheduler is not None:
                started_attempt = await _await_if_needed(
                    self._scheduler.mark_execution_started(job.claim_id)
                )
                if started_attempt is None:
                    raise WorkerPoolError(f"unknown or terminal claim {job.claim_id}")
                started = True
                attempt_id = str(getattr(started_attempt, "attempt_id", "") or "")

            dispatch_result = await self._dispatcher.dispatch(
                agent_id=job.agent_id,
                task_id=job.task_id,
                task_kind=job.task_kind,
                claim_id=job.claim_id,
                execution_spec=dict(job.execution_spec),
            )
            if not bool(getattr(dispatch_result, "dispatched", False)):
                error = getattr(dispatch_result, "error", None) or "dispatcher rejected execution"
                raise DispatchRejected(str(error))
            if not attempt_id:
                attempt_id = str(getattr(dispatch_result, "attempt_id", "") or "")

            if self._scheduler is not None:
                await _await_if_needed(
                    self._scheduler.mark_execution_operationally_succeeded(job.claim_id)
                )
            if self._on_success is not None:
                await _await_if_needed(self._on_success(job, dispatch_result))
            return WorkerOutcome(
                job=job,
                status=WorkerStatus.SUCCEEDED,
                attempt_id=attempt_id,
                started=started,
                dispatch_result=dispatch_result,
            )
        except asyncio.CancelledError:
            await self._release_after_failure(job, reason="worker_cancelled")
            raise
        except BaseException as exc:
            reason = (
                "dispatch_rejected" if isinstance(exc, DispatchRejected) else "execution_failed"
            )
            outcome = WorkerOutcome(
                job=job,
                status=(
                    WorkerStatus.REJECTED
                    if isinstance(exc, DispatchRejected)
                    else WorkerStatus.FAILED
                ),
                attempt_id=attempt_id,
                error=_exception_text(exc),
                started=started,
            )
            await self._release_after_failure(job, reason=reason)
            if self._on_failure is not None:
                # A monitoring hook must not strand a claim or hide the
                # dispatch failure that caused this outcome.
                with suppress(BaseException):
                    await _await_if_needed(self._on_failure(job, outcome))
            return outcome
        finally:
            if token is not None:
                self._release_capacity(job, token)
                self._mark_active(job, delta=-1)

    async def _acquire_capacity(
        self,
        job: WorkerJob,
    ) -> tuple[_UnitLimiter, _UnitLimiter | None, int]:
        agent_limiter = self._agent_limiters.get(job.agent_id)
        # Acquire the narrower per-agent budget first.  Holding a global
        # unit while waiting for an agent unit would let an earlier queued
        # job hoard global capacity and block unrelated agents (a classic
        # multi-resource admission deadlock).
        if agent_limiter is not None:
            await agent_limiter.acquire(job.capacity_units)
        try:
            await self._global.acquire(job.capacity_units)
        except BaseException:
            if agent_limiter is not None:
                agent_limiter.release(job.capacity_units)
            raise
        return self._global, agent_limiter, job.capacity_units

    def _release_capacity(
        self,
        job: WorkerJob,
        token: tuple[_UnitLimiter, _UnitLimiter | None, int],
    ) -> None:
        global_limiter, agent_limiter, units = token
        if agent_limiter is not None:
            agent_limiter.release(units)
        global_limiter.release(units)

    def _mark_active(self, job: WorkerJob, *, delta: int) -> None:
        self._active_jobs += delta
        self._active_units += delta * job.capacity_units
        current = self._active_by_agent.get(job.agent_id, 0) + delta
        if current:
            self._active_by_agent[job.agent_id] = current
        else:
            self._active_by_agent.pop(job.agent_id, None)
        if self._active_jobs < 0 or self._active_units < 0:
            raise RuntimeError("worker pool active capacity invariant violated")

    async def _release_after_failure(self, job: WorkerJob, *, reason: str) -> None:
        if self._scheduler is None:
            return
        # The pool reports the original execution failure.  Reconciliation
        # remains responsible for repairing a provider-side release error.
        with suppress(BaseException):
            await _await_if_needed(
                self._scheduler.release_task(
                    job.graph_id,
                    job.task_id,
                    reason=reason,
                    retry=True,
                )
            )
