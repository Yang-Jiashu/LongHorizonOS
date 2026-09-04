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
import math
import threading
from collections import deque
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from enum import StrEnum
from typing import Any, Protocol, cast


class WorkerPoolError(RuntimeError):
    """Base error for the asynchronous worker pool."""


class CapacityRequestTooLarge(WorkerPoolError):
    """Raised when one job requests more units than a configured limit."""


class DispatchRejected(WorkerPoolError):
    """Raised when an injected dispatcher returns ``dispatched=False``."""


class HeartbeatFailed(WorkerPoolError):
    """Raised when a cooperative claim heartbeat cannot be renewed.

    The worker pool treats a failed heartbeat as an execution failure and
    releases the scheduler claim.  Cancellation remains cooperative: a
    dispatcher which ignores ``CancelledError`` cannot be forcibly killed by
    this in-process pool.
    """


class CooperativeInterrupt(WorkerPoolError):
    """Raised when a dispatcher explicitly observes a cancellation token."""

    def __init__(self, token: CooperativeCancellationToken) -> None:
        self.token = token
        action = token.action or "cancel"
        reason = token.reason or "semantic interrupt requested"
        super().__init__(f"cooperative interrupt for claim {token.claim_id!r}: {action} ({reason})")


class UnobservedCooperativeInterrupt(CooperativeInterrupt):
    """Quarantine a completion that raced an unobserved interrupt request."""


class CooperativeCancellationToken:
    """A one-way, cooperative cancellation signal for one running claim.

    The token never mutates Scheduler claims or Kernel leases.  A dispatcher
    must explicitly accept the ``cancellation_token=`` keyword and poll
    :meth:`is_cancelled`, await :meth:`wait`, or call
    :meth:`raise_if_cancelled`.  If user code ignores the token, the pool
    cannot forcibly terminate it; completion is quarantined after the
    dispatcher returns so it cannot cross the operational-success fence.
    """

    __slots__ = (
        "_action",
        "_decision_hash",
        "_delivered",
        "_event",
        "_interrupt_id",
        "_lock",
        "_loop",
        "_observation_callback",
        "_observed",
        "_partial_outcome",
        "_reason",
        "_requested",
        "claim_id",
    )

    def __init__(
        self,
        claim_id: str,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        observation_callback: Callable[[], None] | None = None,
    ) -> None:
        normalized = str(claim_id).strip()
        if not normalized:
            raise ValueError("claim_id must be non-empty")
        self.claim_id = normalized
        self._event = asyncio.Event()
        self._loop = loop
        self._lock = threading.Lock()
        self._requested = False
        self._delivered = False
        self._observed = False
        self._partial_outcome: Any | None = None
        self._interrupt_id = ""
        self._action = ""
        self._reason = ""
        self._decision_hash = ""
        self._observation_callback = observation_callback

    @property
    def is_cancelled(self) -> bool:
        """Whether an interrupt is pending, acknowledging it when observed."""

        requested = self.request_pending
        if requested:
            self._mark_observed()
        return requested

    @property
    def cancelled(self) -> bool:
        """Alias for :attr:`is_cancelled` used by common cancellation APIs."""

        return self.is_cancelled

    @property
    def request_pending(self) -> bool:
        """Whether cancellation was requested without implying observation."""

        with self._lock:
            return self._requested

    @property
    def delivered(self) -> bool:
        with self._lock:
            return self._delivered

    @property
    def observed(self) -> bool:
        with self._lock:
            return self._observed

    @property
    def interrupt_id(self) -> str:
        with self._lock:
            return self._interrupt_id

    @property
    def action(self) -> str:
        with self._lock:
            return self._action

    @property
    def reason(self) -> str:
        with self._lock:
            return self._reason

    @property
    def decision_hash(self) -> str:
        with self._lock:
            return self._decision_hash

    @property
    def partial_outcome(self) -> Any | None:
        """Structured measured work retained when cancellation interrupts an executor."""

        with self._lock:
            return self._partial_outcome

    def attach_partial_outcome(self, value: Any) -> None:
        """Attach one executor-owned partial result before acknowledging cancel."""

        with self._lock:
            self._partial_outcome = value

    def request(
        self,
        *,
        action: str = "preempt",
        interrupt_id: str = "",
        reason: str = "semantic interrupt requested",
        decision_hash: str = "",
    ) -> bool:
        """Set the one-way signal and return whether this call changed state."""

        normalized_action = str(action).strip().lower()
        normalized_id = str(interrupt_id).strip()
        normalized_reason = str(reason).strip() or "semantic interrupt requested"
        normalized_hash = str(decision_hash).strip().lower()
        if not normalized_action:
            raise ValueError("interrupt action must be non-empty")
        with self._lock:
            if self._requested:
                return False
            self._requested = True
            self._interrupt_id = normalized_id
            self._action = normalized_action
            self._reason = normalized_reason
            self._decision_hash = normalized_hash
        self._set_event_threadsafe()
        return True

    async def wait(self) -> None:
        """Wait until cancellation is requested and acknowledge observation."""

        await self._event.wait()
        self._mark_observed()

    def raise_if_cancelled(self) -> None:
        """Raise :class:`CooperativeInterrupt` when the signal is set."""

        if self.request_pending:
            self._mark_observed()
            raise CooperativeInterrupt(self)

    def _set_event_threadsafe(self) -> None:
        loop = self._loop
        if loop is None:
            self._event.set()
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is loop:
            self._event.set()
            return
        loop.call_soon_threadsafe(self._event.set)

    def _mark_delivered(self) -> bool:
        with self._lock:
            if self._delivered:
                return False
            self._delivered = True
            return True

    def _mark_observed(self) -> bool:
        callback: Callable[[], None] | None
        with self._lock:
            if not self._requested or self._observed:
                return False
            self._observed = True
            callback = self._observation_callback
        if callback is not None:
            callback()
        return True


class InterruptDeliveryStatus(StrEnum):
    """Result status for one cooperative interrupt request."""

    REQUESTED = "requested"
    ALREADY_REQUESTED = "already_requested"
    DELIVERED = "delivered"
    ALREADY_DELIVERED = "already_delivered"
    NOT_RUNNING = "not_running"
    # Compatibility alias: older callers only understood REQUESTED.  The
    # ``preemptible`` field on InterruptDelivery carries the stronger
    # distinction without changing the wire value.
    NON_PREEMPTIBLE = "requested"
    STALE_GRAPH = "stale_graph"
    STALE_EPOCH = "stale_epoch"
    IDENTITY_MISMATCH = "identity_mismatch"
    NOT_ACTIONABLE = "not_actionable"
    UNSUPPORTED_ACTION = "unsupported_action"


class InterruptTransitionPhase(StrEnum):
    """Auditable phases of one cooperative semantic interrupt."""

    REQUESTED = "requested"
    DELIVERED = "delivered"
    OBSERVED = "observed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class InterruptDelivery:
    """Bounded result of routing one interrupt to a running pool job."""

    claim_id: str
    status: InterruptDeliveryStatus
    graph_id: str = ""
    graph_version: int | None = None
    task_id: str = ""
    attempt_id: str = ""
    semantic_epoch: int | None = None
    action: str = ""
    interrupt_id: str = ""
    decision_hash: str = ""
    reason: str = ""
    preemptible: bool = True
    delivered: bool = False
    observed: bool = False

    @property
    def accepted(self) -> bool:
        return self.preemptible and self.status in {
            InterruptDeliveryStatus.REQUESTED,
            InterruptDeliveryStatus.ALREADY_REQUESTED,
            InterruptDeliveryStatus.DELIVERED,
            InterruptDeliveryStatus.ALREADY_DELIVERED,
        }

    @property
    def acknowledged(self) -> bool:
        """True only after the executor explicitly observed the token."""

        return self.observed


@dataclass(frozen=True, slots=True)
class InterruptTransition:
    """One exact-identity transition emitted by the cooperative worker path."""

    phase: InterruptTransitionPhase
    graph_id: str
    graph_version: int | None
    task_id: str
    claim_id: str
    attempt_id: str
    semantic_epoch: int | None
    action: str
    interrupt_id: str
    decision_hash: str
    reason: str
    observed: bool


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
        expected_claim_id: str | None = None,
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
        **kwargs: Any,
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
    graph_version: int | None = None

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
        raw_graph_version: Any = dispatch.get("graph_version")
        return cls(
            task_id=str(dispatch.get("task_id", "")),
            claim_id=str(dispatch.get("claim_id", "")),
            agent_id=str(dispatch.get("agent_id", "")),
            task_kind=str(dispatch.get("task_kind", task_kind) or ""),
            execution_spec=spec,
            capacity_units=dispatch.get("capacity_units", 1),
            graph_id=str(dispatch.get("graph_id", graph_id) or ""),
            graph_version=(
                None if raw_graph_version is None else int(cast(Any, raw_graph_version))
            ),
        )


# Kept after ``WorkerJob`` so tools evaluating annotations do not need to
# resolve a forward reference through a local alias.
HeartbeatCallback = Callable[["WorkerJob", str], Any]
InterruptTransitionCallback = Callable[[InterruptTransition], Any]


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
    interrupt_id: str = ""
    interrupt_action: str = ""
    interrupt_observed: bool = False

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
        heartbeat_interval: float | timedelta | None = None,
        heartbeat_callback: HeartbeatCallback | None = None,
        heartbeat: HeartbeatCallback | None = None,
        on_interrupt_transition: InterruptTransitionCallback | None = None,
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
        if heartbeat_callback is not None and heartbeat is not None:
            raise ValueError("provide only one of heartbeat_callback or heartbeat")
        if heartbeat is not None:
            heartbeat_callback = heartbeat
        if heartbeat_callback is not None and heartbeat_interval is None:
            raise ValueError("heartbeat_interval is required when a heartbeat callback is set")
        self._heartbeat_interval = self._normalize_heartbeat_interval(heartbeat_interval)
        self._heartbeat_callback = heartbeat_callback
        self._on_interrupt_transition = on_interrupt_transition
        self._active_jobs = 0
        self._active_by_agent: dict[str, int] = {}
        self._active_units = 0
        self._interrupt_lock = threading.RLock()
        self._interrupt_tokens: dict[str, CooperativeCancellationToken] = {}
        self._active_attempts: dict[str, tuple[WorkerJob, str, int | None]] = {}
        self._interrupt_capabilities: dict[str, bool] = {}
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._dispatcher_accepts_interrupt_token = self._supports_interrupt_token(dispatcher)
        # Streaming-run state.  ``run`` is unchanged and does not use these.
        self._streaming = False
        self._stream_tasks: set[asyncio.Task[WorkerOutcome]] = set()
        self._stream_claims: set[str] = set()

    def submit(self, job: WorkerJob | Mapping[str, Any]) -> None:
        """Admit one more job into a ``run_streaming`` pass already in flight.

        This exists so a caller can refill capacity the moment a slot frees
        instead of waiting for a whole batch to drain.  ``run`` plans a fixed set
        of jobs up front, so a short task cannot hand its slot to anything until
        the slowest task in its batch finishes; measured on a 42-task workload
        that left 26-39% of the configured concurrency unusable.

        Admission policy stays with the caller.  The pool owns capacity and
        completion notification only, so it never decides what is safe to run
        next -- that judgement needs the conflict graph, which the pool cannot
        see.
        """

        if not self._streaming:
            raise WorkerPoolError("submit() requires an active run_streaming(...) pass")
        normalized = WorkerJob.from_dispatch(job)
        if normalized.claim_id in self._stream_claims:
            # Same contract as ``run``: reject the duplicate and leave the
            # already-active claim untouched.
            task = asyncio.create_task(
                self._duplicate_outcome(normalized),
                name=f"lhos-worker-rejected-{normalized.claim_id}",
            )
        else:
            self._stream_claims.add(normalized.claim_id)
            task = asyncio.create_task(
                self._execute(normalized),
                name=f"lhos-worker-{normalized.agent_id}-{normalized.task_id}",
            )
        self._stream_tasks.add(task)

    async def run_streaming(
        self,
        jobs: Iterable[WorkerJob | Mapping[str, Any]],
    ) -> AsyncIterator[WorkerOutcome]:
        """Yield outcomes as they complete, allowing ``submit`` while running.

        Unlike ``run``, outcomes arrive in **completion** order rather than
        submission order -- arriving early is the entire point, because the
        consumer uses each completion to decide what to admit next.

        The pass ends when no work remains, so a consumer that submits from
        inside the loop keeps it alive.  Cancellation still awaits every child
        before propagating, so claim release and capacity return complete first.
        """

        if self._streaming:
            raise WorkerPoolError("run_streaming(...) is already active on this pool")
        self._event_loop = asyncio.get_running_loop()
        self._streaming = True
        self._stream_tasks = set()
        self._stream_claims = set()
        try:
            for job in jobs:
                self.submit(job)
            while self._stream_tasks:
                done, _pending = await asyncio.wait(
                    self._stream_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                self._stream_tasks.difference_update(done)
                # Several jobs can finish in one wait; order them by task name so
                # a simultaneous completion is reported deterministically.
                for task in sorted(done, key=lambda item: item.get_name()):
                    yield task.result()
        except asyncio.CancelledError:
            for task in self._stream_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self._stream_tasks, return_exceptions=True)
            raise
        finally:
            self._streaming = False
            self._stream_tasks = set()
            self._stream_claims = set()

    @staticmethod
    def _supports_interrupt_token(dispatcher: Any) -> bool:
        """Return whether ``dispatch`` explicitly names the token keyword.

        A dispatcher accepting only ``**kwargs`` is intentionally not treated
        as opted in: passing a new keyword to an unreviewed adapter could
        silently alter user behavior.  The built-in SDK dispatcher declares
        the parameter explicitly.
        """

        try:
            parameters = inspect.signature(dispatcher.dispatch).parameters
        except (TypeError, ValueError):
            return False
        parameter = parameters.get("cancellation_token")
        return parameter is not None and parameter.kind in {
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        }

    def _supports_interrupt_for_job(self, job: WorkerJob) -> bool:
        checker = getattr(self._dispatcher, "supports_interrupt_token", None)
        if callable(checker):
            try:
                return bool(checker(job))
            except Exception:
                return False
        return self._dispatcher_accepts_interrupt_token

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

    @property
    def interrupt_tokens(self) -> dict[str, CooperativeCancellationToken]:
        """Read-only copy of tokens for currently running jobs."""

        with self._interrupt_lock:
            return dict(self._interrupt_tokens)

    def request_interrupt(
        self,
        claim_id: str,
        *,
        action: str = "preempt",
        interrupt_id: str = "",
        reason: str = "semantic interrupt requested",
        decision_hash: str = "",
    ) -> InterruptDelivery:
        """Request cooperative cancellation for one currently running claim.

        This method only sets an in-process event.  It does not release a
        Claim, transfer a Lease, or cancel an asyncio task.  A dispatcher must
        explicitly opt into ``cancellation_token=`` and observe the signal.
        """

        normalized_claim = str(claim_id).strip()
        normalized_action = str(action).strip().lower()
        if normalized_action not in {"preempt", "rebase"}:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.UNSUPPORTED_ACTION,
                action=normalized_action,
                interrupt_id=str(interrupt_id).strip(),
                decision_hash=str(decision_hash).strip().lower(),
                reason=str(reason).strip(),
            )
        with self._interrupt_lock:
            token = self._interrupt_tokens.get(normalized_claim)
            active = self._active_attempts.get(normalized_claim)
            if token is None or active is None:
                return InterruptDelivery(
                    claim_id=normalized_claim,
                    status=InterruptDeliveryStatus.NOT_RUNNING,
                    action=normalized_action,
                    interrupt_id=str(interrupt_id).strip(),
                    decision_hash=str(decision_hash).strip().lower(),
                    reason=str(reason).strip(),
                )
            job, attempt_id, semantic_epoch = active
            preemptible = self._interrupt_capabilities.get(normalized_claim, False)
            changed = token.request(
                action=normalized_action,
                interrupt_id=interrupt_id,
                reason=reason,
                decision_hash=decision_hash,
            )
        self._emit_interrupt_transition(
            InterruptTransition(
                phase=InterruptTransitionPhase.REQUESTED,
                graph_id=job.graph_id,
                graph_version=job.graph_version,
                task_id=job.task_id,
                claim_id=normalized_claim,
                attempt_id=attempt_id,
                semantic_epoch=semantic_epoch,
                action=token.action,
                interrupt_id=token.interrupt_id,
                decision_hash=token.decision_hash,
                reason=token.reason,
                observed=token.observed,
            )
        )
        if preemptible:
            with self._interrupt_lock:
                token._mark_delivered()
            self._emit_interrupt_transition(
                InterruptTransition(
                    phase=InterruptTransitionPhase.DELIVERED,
                    graph_id=job.graph_id,
                    graph_version=job.graph_version,
                    task_id=job.task_id,
                    claim_id=normalized_claim,
                    attempt_id=attempt_id,
                    semantic_epoch=semantic_epoch,
                    action=token.action,
                    interrupt_id=token.interrupt_id,
                    decision_hash=token.decision_hash,
                    reason=token.reason,
                    observed=token.observed,
                )
            )
        if not preemptible:
            return InterruptDelivery(
                claim_id=normalized_claim,
                status=InterruptDeliveryStatus.NON_PREEMPTIBLE,
                graph_id=job.graph_id,
                task_id=job.task_id,
                attempt_id=attempt_id,
                semantic_epoch=semantic_epoch,
                action=token.action,
                interrupt_id=token.interrupt_id,
                decision_hash=token.decision_hash,
                reason=token.reason,
                preemptible=False,
                delivered=False,
                observed=False,
            )
        return InterruptDelivery(
            claim_id=normalized_claim,
            status=(
                InterruptDeliveryStatus.REQUESTED
                if changed
                else InterruptDeliveryStatus.ALREADY_REQUESTED
            ),
            graph_id=job.graph_id,
            graph_version=job.graph_version,
            task_id=job.task_id,
            attempt_id=attempt_id,
            semantic_epoch=semantic_epoch,
            action=token.action,
            interrupt_id=token.interrupt_id,
            decision_hash=token.decision_hash,
            reason=token.reason,
            preemptible=True,
            delivered=token.delivered,
            observed=token.observed,
        )

    async def run(
        self,
        jobs: Iterable[WorkerJob | Mapping[str, Any]],
    ) -> list[WorkerOutcome]:
        """Execute all jobs and return outcomes in submission order.

        Duplicate claim IDs are rejected before dispatching.  The existing
        active claim is intentionally left untouched for a caller that
        accidentally submitted the same dispatch twice.
        """
        self._event_loop = asyncio.get_running_loop()
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
        semantic_epoch: int | None = None
        interrupt_token: CooperativeCancellationToken | None = None
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
                raw_epoch = getattr(started_attempt, "semantic_epoch", None)
                if isinstance(raw_epoch, int) and not isinstance(raw_epoch, bool):
                    semantic_epoch = raw_epoch
            with self._interrupt_lock:
                self._active_attempts[job.claim_id] = (job, attempt_id, semantic_epoch)
                self._interrupt_capabilities[job.claim_id] = self._supports_interrupt_for_job(job)
            owner_loop = asyncio.get_running_loop()

            def notify_interrupt_observed() -> None:
                # ``call_soon_threadsafe`` returns an asyncio.Handle, but the
                # token callback is deliberately side-effect-only.
                owner_loop.call_soon_threadsafe(
                    self._emit_interrupt_transition,
                    self._interrupt_transition(
                        InterruptTransitionPhase.OBSERVED,
                        job,
                        attempt_id=attempt_id,
                        semantic_epoch=semantic_epoch,
                        token=interrupt_token,
                        observed=True,
                    ),
                )

            interrupt_token = CooperativeCancellationToken(
                job.claim_id,
                loop=owner_loop,
                observation_callback=notify_interrupt_observed,
            )
            with self._interrupt_lock:
                self._interrupt_tokens[job.claim_id] = interrupt_token

            dispatch_result = await self._dispatch_with_heartbeat(
                job,
                attempt_id=attempt_id,
                semantic_epoch=semantic_epoch,
                cancellation_token=interrupt_token,
            )
            # Only a dispatcher that explicitly opted into the token can
            # have observed the request.  For that path, re-check before the
            # operational-success fence so a late cooperative request cannot
            # race semantic commit.  Legacy dispatchers intentionally ignore
            # the token and retain their historical completion semantics.
            interrupt_capable = self._interrupt_capabilities.get(job.claim_id, False)
            if interrupt_capable and interrupt_token.request_pending:
                if interrupt_token.observed:
                    raise CooperativeInterrupt(interrupt_token)
                raise UnobservedCooperativeInterrupt(interrupt_token)
            if not bool(getattr(dispatch_result, "dispatched", False)):
                error = getattr(dispatch_result, "error", None) or "dispatcher rejected execution"
                raise DispatchRejected(str(error))
            if not attempt_id:
                attempt_id = str(getattr(dispatch_result, "attempt_id", "") or "")

            if self._scheduler is not None:
                operational_attempt = await _await_if_needed(
                    self._scheduler.mark_execution_operationally_succeeded(job.claim_id)
                )
                # The dispatcher may finish after a semantic interrupt,
                # lease loss, or claim reassignment.  A ``None`` lifecycle
                # result is an explicit ownership fence failure; never invoke
                # ``on_success`` (which may publish semantic Evidence) for a
                # result that did not cross the Scheduler's operational
                # success boundary.
                if operational_attempt is None or operational_attempt is False:
                    raise WorkerPoolError(
                        f"claim {job.claim_id!r} no longer admits operational completion"
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
            if isinstance(exc, HeartbeatFailed):
                reason = "heartbeat_failed"
            if isinstance(exc, UnobservedCooperativeInterrupt):
                reason = f"semantic_interrupt_unobserved:{exc.token.action or 'preempt'}"
            elif isinstance(exc, CooperativeInterrupt):
                action = exc.token.action or "preempt"
                reason = f"semantic_interrupt:{action}"
            partial_outcome = (
                exc.token.partial_outcome
                if isinstance(exc, CooperativeInterrupt)
                else getattr(exc, "partial_outcome", None)
            )
            outcome = WorkerOutcome(
                job=job,
                status=(
                    WorkerStatus.REJECTED
                    if isinstance(exc, DispatchRejected)
                    else WorkerStatus.CANCELLED
                    if isinstance(exc, CooperativeInterrupt)
                    else WorkerStatus.FAILED
                ),
                attempt_id=attempt_id,
                error=_exception_text(exc),
                started=started,
                dispatch_result=(
                    {
                        "interrupt_acknowledged": (
                            exc.token.observed if isinstance(exc, CooperativeInterrupt) else False
                        ),
                        "interrupt_observed": exc.token.observed,
                        "interrupt_id": exc.token.interrupt_id,
                        "interrupt_action": exc.token.action,
                        "partial_outcome": partial_outcome,
                    }
                    if isinstance(exc, CooperativeInterrupt)
                    else partial_outcome
                ),
                interrupt_id=(
                    exc.token.interrupt_id if isinstance(exc, CooperativeInterrupt) else ""
                ),
                interrupt_action=(
                    exc.token.action if isinstance(exc, CooperativeInterrupt) else ""
                ),
                interrupt_observed=(
                    exc.token.observed if isinstance(exc, CooperativeInterrupt) else False
                ),
            )
            if isinstance(exc, CooperativeInterrupt):
                self._emit_interrupt_transition(
                    self._interrupt_transition(
                        InterruptTransitionPhase.CANCELLED,
                        job,
                        attempt_id=attempt_id,
                        semantic_epoch=semantic_epoch,
                        token=exc.token,
                        observed=exc.token.observed,
                    )
                )
            await self._release_after_failure(job, reason=reason)
            if self._on_failure is not None:
                # A monitoring hook must not strand a claim or hide the
                # dispatch failure that caused this outcome.
                with suppress(BaseException):
                    await _await_if_needed(self._on_failure(job, outcome))
            return outcome
        finally:
            with self._interrupt_lock:
                self._interrupt_tokens.pop(job.claim_id, None)
                self._active_attempts.pop(job.claim_id, None)
                self._interrupt_capabilities.pop(job.claim_id, None)
            if token is not None:
                self._release_capacity(job, token)
                self._mark_active(job, delta=-1)

    def _interrupt_transition(
        self,
        phase: InterruptTransitionPhase,
        job: WorkerJob,
        *,
        attempt_id: str,
        semantic_epoch: int | None,
        token: CooperativeCancellationToken | None,
        observed: bool,
    ) -> InterruptTransition:
        if token is None:
            raise WorkerPoolError("interrupt transition requires a cancellation token")
        return InterruptTransition(
            phase=phase,
            graph_id=job.graph_id,
            graph_version=job.graph_version,
            task_id=job.task_id,
            claim_id=job.claim_id,
            attempt_id=attempt_id,
            semantic_epoch=semantic_epoch,
            action=token.action,
            interrupt_id=token.interrupt_id,
            decision_hash=token.decision_hash,
            reason=token.reason,
            observed=observed,
        )

    def _emit_interrupt_transition(self, transition: InterruptTransition) -> None:
        callback = self._on_interrupt_transition
        if callback is None:
            return
        owner_loop = self._event_loop
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if owner_loop is not None and running_loop is not owner_loop:
            owner_loop.call_soon_threadsafe(self._emit_interrupt_transition, transition)
            return
        result = callback(transition)
        if inspect.isawaitable(result):
            if running_loop is None:
                raise WorkerPoolError(
                    "async interrupt transition callback requires a running event loop"
                )
            # ``inspect.isawaitable`` intentionally accepts Futures and other
            # awaitables in addition to bare coroutines; ``cast`` documents
            # the narrow create_task contract without changing runtime
            # behavior for the existing coroutine callbacks.
            running_loop.create_task(cast(Coroutine[Any, Any, Any], result))

    @staticmethod
    def _normalize_heartbeat_interval(
        value: float | timedelta | None,
    ) -> float | None:
        if value is None:
            return None
        seconds = value.total_seconds() if isinstance(value, timedelta) else value
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
            raise TypeError("heartbeat_interval must be a number or timedelta")
        if not math.isfinite(float(seconds)) or seconds <= 0:
            raise ValueError("heartbeat_interval must be finite and positive")
        return float(seconds)

    def _heartbeat_source(
        self,
        job: WorkerJob,
        *,
        attempt_id: str,
        semantic_epoch: int | None,
    ) -> Callable[[], Any] | None:
        """Resolve a callback or Scheduler heartbeat hook.

        The scheduler hook is deliberately discovered at runtime so older
        lifecycle test doubles remain compatible when heartbeat is disabled.
        """

        if self._heartbeat_callback is not None:
            callback = self._heartbeat_callback
            return lambda: callback(job, attempt_id)
        if self._scheduler is None:
            return None
        candidate = getattr(self._scheduler, "heartbeat", None)
        if not callable(candidate):
            candidate = getattr(self._scheduler, "renew_claim", None)
        if not callable(candidate):
            return None
        hook: Callable[..., Any] = candidate

        def invoke() -> Any:
            # SchedulerSession exposes these optional fencing arguments.  Use
            # signature inspection for small legacy adapters that accept only
            # ``claim_id`` (or ``claim_id, ttl``).
            kwargs: dict[str, Any] = {}
            try:
                params = inspect.signature(hook).parameters
            except (TypeError, ValueError):
                params = None
            if params is not None and "expected_attempt_id" in params:
                kwargs["expected_attempt_id"] = attempt_id or None
            if params is not None and "expected_semantic_epoch" in params:
                kwargs["expected_semantic_epoch"] = semantic_epoch
            return hook(job.claim_id, **kwargs)

        return invoke

    async def _dispatch_with_heartbeat(
        self,
        job: WorkerJob,
        *,
        attempt_id: str,
        semantic_epoch: int | None,
        cancellation_token: CooperativeCancellationToken | None = None,
    ) -> Any:
        """Dispatch one job while running an optional cooperative heartbeat."""

        interval = self._heartbeat_interval
        if interval is None:
            # Preserve the historical direct-await cancellation semantics when
            # heartbeats are disabled (the default compatibility path).
            return await self._dispatch_once(job, cancellation_token=cancellation_token)

        source = self._heartbeat_source(
            job,
            attempt_id=attempt_id,
            semantic_epoch=semantic_epoch,
        )
        if source is None:
            raise HeartbeatFailed(
                "heartbeat_interval configured but no heartbeat callback or "
                "scheduler heartbeat/renew_claim hook is available"
            )

        # Establish/renew ownership before starting any user-side work.  A
        # short dispatch could otherwise finish before the first periodic
        # heartbeat and appear successful without ever proving that its lease
        # was still valid.
        try:
            initial_result = await _await_if_needed(source())
            if initial_result is False:
                raise HeartbeatFailed(f"heartbeat rejected for claim {job.claim_id!r}")
        except asyncio.CancelledError:
            raise
        except HeartbeatFailed:
            raise
        except BaseException as exc:
            raise HeartbeatFailed(
                f"heartbeat failed for claim {job.claim_id!r}: {_exception_text(exc)}"
            ) from exc

        # Serialize periodic heartbeat callbacks with the dispatch-completion
        # decision.  If a callback is already awaiting a provider response
        # when dispatch finishes, completion must wait for that callback rather
        # than cancelling it in ``finally`` and accidentally reporting success.
        # The lock is not held while the heartbeat loop sleeps, so short jobs
        # do not incur an extra interval delay.
        heartbeat_lock = asyncio.Lock()
        heartbeat_errors: list[BaseException] = []

        async def tracked_source() -> Any:
            async with heartbeat_lock:
                try:
                    result = await _await_if_needed(source())
                    if result is False:
                        error = HeartbeatFailed(f"heartbeat rejected for claim {job.claim_id!r}")
                        if not heartbeat_errors:
                            heartbeat_errors.append(error)
                        raise error
                    return result
                except asyncio.CancelledError:
                    raise
                except HeartbeatFailed as exc:
                    if not heartbeat_errors:
                        heartbeat_errors.append(exc)
                    raise
                except BaseException as exc:
                    error = HeartbeatFailed(
                        f"heartbeat failed for claim {job.claim_id!r}: {_exception_text(exc)}"
                    )
                    if not heartbeat_errors:
                        heartbeat_errors.append(error)
                    raise error from exc

        dispatch_task = asyncio.create_task(
            self._dispatch_once(job, cancellation_token=cancellation_token),
            name=f"lhos-dispatch-{job.agent_id}-{job.task_id}",
        )

        loop = asyncio.get_running_loop()
        heartbeat_failure: asyncio.Future[BaseException] = loop.create_future()
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(tracked_source, interval, heartbeat_failure, job.claim_id),
            name=f"lhos-heartbeat-{job.claim_id}",
        )
        try:
            done, _ = await asyncio.wait(
                # Include the loop task itself.  A heartbeat implementation
                # must run until dispatch completes or it reports a failure;
                # if it exits for any other reason we fail closed instead of
                # silently allowing a long-running claim to outlive its lease.
                {dispatch_task, heartbeat_failure, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            # Prefer a heartbeat failure if both tasks completed in the same
            # event-loop turn; fail closed rather than reporting success after
            # ownership was lost.
            if heartbeat_failure in done:
                failure = heartbeat_failure.result()
                dispatch_task.cancel()
                await asyncio.gather(dispatch_task, return_exceptions=True)
                raise failure

            if heartbeat_task in done:
                # ``_heartbeat_loop`` normally publishes a HeartbeatFailed
                # through ``heartbeat_failure`` before returning.  Treat any
                # bare return or unexpected exception as a fail-closed lease
                # loss; otherwise an accidentally broken heartbeat loop could
                # report dispatch success without renewal.
                try:
                    heartbeat_task.result()
                except asyncio.CancelledError as exc:
                    failure = HeartbeatFailed(
                        f"heartbeat loop cancelled unexpectedly for claim {job.claim_id!r}"
                    )
                    failure.__cause__ = exc
                except BaseException as exc:
                    failure = (
                        exc
                        if isinstance(exc, HeartbeatFailed)
                        else HeartbeatFailed(
                            f"heartbeat loop failed for claim {job.claim_id!r}: "
                            f"{_exception_text(exc)}"
                        )
                    )
                else:
                    failure = HeartbeatFailed(
                        f"heartbeat loop exited unexpectedly for claim {job.claim_id!r}"
                    )
                dispatch_task.cancel()
                await asyncio.gather(dispatch_task, return_exceptions=True)
                raise failure

            result = await dispatch_task
            # Acquiring the same lock used by ``tracked_source`` waits for an
            # in-flight callback, while preventing a fresh callback from
            # starting before the heartbeat task is cancelled in ``finally``.
            # This closes the dispatch/heartbeat completion race without
            # waiting for the loop's next periodic sleep.
            async with heartbeat_lock:
                if heartbeat_errors:
                    raise heartbeat_errors[0]
                if heartbeat_failure.done():
                    raise heartbeat_failure.result()
            return result
        finally:
            if not heartbeat_task.done():
                heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            if not dispatch_task.done():
                dispatch_task.cancel()
                await asyncio.gather(dispatch_task, return_exceptions=True)

    async def _dispatch_once(
        self,
        job: WorkerJob,
        *,
        cancellation_token: CooperativeCancellationToken | None,
    ) -> Any:
        """Invoke the dispatcher, passing a token only after explicit opt-in."""

        kwargs: dict[str, Any] = {
            "agent_id": job.agent_id,
            "task_id": job.task_id,
            "task_kind": job.task_kind,
            "claim_id": job.claim_id,
            "execution_spec": dict(job.execution_spec),
        }
        if self._supports_interrupt_for_job(job):
            kwargs["cancellation_token"] = cancellation_token
        return await self._dispatcher.dispatch(**kwargs)

    @staticmethod
    async def _heartbeat_loop(
        source: Callable[[], Any],
        interval: float,
        failure: asyncio.Future[BaseException],
        claim_id: str,
    ) -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                result = await _await_if_needed(source())
                if result is False:
                    raise HeartbeatFailed(f"heartbeat rejected for claim {claim_id!r}")
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                error = (
                    exc
                    if isinstance(exc, HeartbeatFailed)
                    else HeartbeatFailed(
                        f"heartbeat failed for claim {claim_id!r}: {_exception_text(exc)}"
                    )
                )
                if not failure.done():
                    failure.set_result(error)
                return

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
        # When the scheduler supports claim fencing, pass the exact claim ID
        # that this worker owned.  Without this guard, a stale worker whose
        # dispatch/heartbeat fails after reassignment could release the newer
        # claim for the same graph/task.
        with suppress(BaseException):
            release = self._scheduler.release_task
            kwargs: dict[str, Any] = {
                "reason": reason,
                "retry": True,
            }
            try:
                params = inspect.signature(release).parameters
                supports_fence = "expected_claim_id" in params or any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in params.values()
                )
            except (TypeError, ValueError):
                supports_fence = True
            if supports_fence:
                kwargs["expected_claim_id"] = job.claim_id
            await _await_if_needed(release(job.graph_id, job.task_id, **kwargs))
