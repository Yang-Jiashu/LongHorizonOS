"""Harness session control protocol for long-running Agent computation.

A Harness owns the execution loop for one Agent session.  LongHorizonOS owns
the global decision about whether that session should start, continue,
checkpoint, rebase, or preempt.  This module defines the narrow control
boundary between those responsibilities.

The protocol is intentionally independent from ``AgentOS`` dispatch internals.
It does not create Claims or Leases and a Harness result is never semantic
Evidence.  Callers must still pass the normal Scheduler/Kernel ownership and
VPG verification gates before committing work.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any, Final, Literal, Protocol, runtime_checkable
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictInt,
    field_validator,
    model_validator,
)

from .errors import ConfigurationError

HARNESS_SESSION_SCHEMA_VERSION: Final[Literal["harness-session.v1"]] = "harness-session.v1"


def _uuid() -> str:
    return uuid4().hex


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HarnessOperation(StrEnum):
    """Control operations an OS may request from a Harness session."""

    START = "start"
    CONTINUE = "continue"
    CHECKPOINT = "checkpoint"
    REBASE = "rebase"
    PREEMPT = "preempt"


class HarnessSessionState(StrEnum):
    """Observable lifecycle state of one Harness session."""

    CREATED = "created"
    RUNNING = "running"
    CHECKPOINTED = "checkpointed"
    PREEMPTED = "preempted"
    COMPLETED = "completed"


class HarnessResultStatus(StrEnum):
    """Whether a control request changed the Harness session."""

    APPLIED = "applied"
    UNSUPPORTED = "unsupported"
    REJECTED = "rejected"
    FAILED = "failed"


class HarnessSessionIdentity(_FrozenModel):
    """Exact ownership and semantic basis of a Harness session.

    ``session_id`` is the Harness-local stable identity.  Claim and Attempt
    identities fence it to one Scheduler execution owner.  Graph version and
    semantic epoch describe the computation basis and change after a
    successful ``REBASE``.
    """

    schema_version: Literal["harness-session.v1"] = HARNESS_SESSION_SCHEMA_VERSION
    session_id: str = Field(default_factory=_uuid, min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    semantic_epoch: StrictInt = Field(default=0, ge=0)
    task_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    claim_id: str = Field(min_length=1)
    attempt_id: str = Field(min_length=1)

    @field_validator(
        "session_id",
        "graph_id",
        "task_id",
        "agent_id",
        "claim_id",
        "attempt_id",
        mode="before",
    )
    @classmethod
    def _non_empty_identity(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Harness session identity fields must be non-empty")
        return normalized

    @field_validator("graph_version", "semantic_epoch")
    @classmethod
    def _real_integer(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("Harness graph version and semantic epoch must be integers")
        return value


class HarnessCapabilities(_FrozenModel):
    """Concrete operations and guarantees offered by one Harness adapter."""

    schema_version: Literal["harness-session.v1"] = HARNESS_SESSION_SCHEMA_VERSION
    operations: tuple[HarnessOperation, ...]
    checkpoint_scope: Literal["none", "session"] = "none"
    preemption_mode: Literal["none", "cooperative"] = "none"
    rebase_mode: Literal["none", "in_place"] = "none"

    @field_validator("operations", mode="before")
    @classmethod
    def _normalize_operations(cls, value: Any) -> tuple[HarnessOperation, ...]:
        if value is None:
            return ()
        if isinstance(value, (str, HarnessOperation)):
            value = (value,)
        normalized = {HarnessOperation(item) for item in value}
        return tuple(sorted(normalized, key=lambda operation: operation.value))

    @model_validator(mode="after")
    def _modes_match_operations(self) -> HarnessCapabilities:
        operations = set(self.operations)
        if HarnessOperation.START not in operations:
            raise ValueError("a Harness session must declare START support")
        if (HarnessOperation.CHECKPOINT in operations) != (self.checkpoint_scope != "none"):
            raise ValueError("CHECKPOINT support must match checkpoint_scope")
        if (HarnessOperation.PREEMPT in operations) != (self.preemption_mode != "none"):
            raise ValueError("PREEMPT support must match preemption_mode")
        if (HarnessOperation.REBASE in operations) != (self.rebase_mode != "none"):
            raise ValueError("REBASE support must match rebase_mode")
        return self

    def supports(self, operation: HarnessOperation | str) -> bool:
        """Return whether this adapter explicitly implements ``operation``."""

        try:
            normalized = HarnessOperation(operation)
        except ValueError:
            return False
        return normalized in self.operations


class HarnessSessionSnapshot(_FrozenModel):
    """Read-only point-in-time state returned at the control boundary."""

    schema_version: Literal["harness-session.v1"] = HARNESS_SESSION_SCHEMA_VERSION
    identity: HarnessSessionIdentity
    state: HarnessSessionState = HarnessSessionState.CREATED
    revision: StrictInt = Field(default=0, ge=0)
    checkpoint_id: str | None = None
    progress: float = Field(default=0.0, ge=0.0, le=1.0)
    last_request_id: str | None = None

    @field_validator("revision")
    @classmethod
    def _revision_integer(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("Harness revision must be an integer")
        return value

    @field_validator("checkpoint_id", "last_request_id", mode="before")
    @classmethod
    def _normalize_optional_identity(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class HarnessControlRequest(_FrozenModel):
    """One idempotent OS-to-Harness control request.

    The request carries the exact current session identity and revision.  A
    stale request therefore cannot accidentally control a replacement Claim,
    Attempt, or rebased cognition epoch.
    """

    schema_version: Literal["harness-session.v1"] = HARNESS_SESSION_SCHEMA_VERSION
    request_id: str = Field(default_factory=_uuid, min_length=1)
    operation: HarnessOperation
    session: HarnessSessionIdentity
    expected_revision: StrictInt = Field(ge=0)
    expected_checkpoint_id: str | None = None
    target_graph_version: StrictInt | None = Field(default=None, ge=0)
    target_semantic_epoch: StrictInt | None = Field(default=None, ge=0)
    reason: str = ""
    payload: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("request_id", mode="before")
    @classmethod
    def _request_id_non_empty(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("request_id must be non-empty")
        return normalized

    @field_validator("reason", mode="before")
    @classmethod
    def _normalize_reason(cls, value: Any) -> str:
        return str(value).strip()

    @field_validator("expected_checkpoint_id", mode="before")
    @classmethod
    def _normalize_checkpoint_id(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator(
        "expected_revision",
        "target_graph_version",
        "target_semantic_epoch",
    )
    @classmethod
    def _request_integers(cls, value: int | None) -> int | None:
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise TypeError("Harness request revisions and epochs must be integers")
        return value

    @model_validator(mode="after")
    def _rebase_target_is_explicit(self) -> HarnessControlRequest:
        has_target = self.target_graph_version is not None or self.target_semantic_epoch is not None
        if self.operation is HarnessOperation.REBASE and not has_target:
            raise ValueError("REBASE requires a target graph version or semantic epoch")
        if self.operation is not HarnessOperation.REBASE and has_target:
            raise ValueError("only REBASE may carry a target graph version or semantic epoch")
        return self

    def fingerprint(self) -> str:
        """Return the canonical request digest used for idempotency checks."""

        payload = json.dumps(
            self.model_dump(mode="json"),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


class HarnessHookOutcome(_FrozenModel):
    """Acknowledgement produced by an operation-specific Harness hook.

    ``completed`` only reports that the Harness loop ended operationally.  It
    does not imply VPG verification or Goal closure.
    """

    completed: bool = False
    checkpoint_id: str | None = None
    progress: float | None = Field(default=None, ge=0.0, le=1.0)
    output: Any = None
    details: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("checkpoint_id", mode="before")
    @classmethod
    def _checkpoint_non_empty(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class HarnessControlResult(_FrozenModel):
    """Typed result of applying or rejecting one Harness control request."""

    schema_version: Literal["harness-session.v1"] = HARNESS_SESSION_SCHEMA_VERSION
    request_id: str = Field(min_length=1)
    operation: HarnessOperation
    status: HarnessResultStatus
    before: HarnessSessionSnapshot
    after: HarnessSessionSnapshot
    message: str = ""
    output: Any = None
    details: dict[str, JsonValue] = Field(default_factory=dict)

    @property
    def applied(self) -> bool:
        return self.status is HarnessResultStatus.APPLIED


@runtime_checkable
class HarnessSessionAdapter(Protocol):
    """Execution-unit contract managed by the LongHorizonOS control plane."""

    @property
    def capabilities(self) -> HarnessCapabilities: ...

    @property
    def snapshot(self) -> HarnessSessionSnapshot: ...

    async def control(self, request: HarnessControlRequest) -> HarnessControlResult: ...


HarnessHook = Callable[
    [HarnessControlRequest, HarnessSessionSnapshot],
    HarnessHookOutcome | Any | Awaitable[HarnessHookOutcome | Any],
]


_ALLOWED_STATES: dict[HarnessOperation, frozenset[HarnessSessionState]] = {
    HarnessOperation.START: frozenset({HarnessSessionState.CREATED}),
    HarnessOperation.CONTINUE: frozenset(
        {HarnessSessionState.RUNNING, HarnessSessionState.CHECKPOINTED}
    ),
    HarnessOperation.CHECKPOINT: frozenset({HarnessSessionState.RUNNING}),
    HarnessOperation.REBASE: frozenset(
        {HarnessSessionState.RUNNING, HarnessSessionState.CHECKPOINTED}
    ),
    HarnessOperation.PREEMPT: frozenset(
        {HarnessSessionState.RUNNING, HarnessSessionState.CHECKPOINTED}
    ),
}


class CallableHarnessAdapter:
    """In-process Harness session adapter with explicit lifecycle hooks.

    There are two construction modes:

    * ``executor=...`` wraps an existing zero/one-argument Agent executor as a
      one-shot Harness.  It declares only ``START`` and becomes ``COMPLETED``
      when the callable returns.
    * ``start=...`` plus optional operation hooks implements a stateful Harness
      control adapter.  Hooks accept ``(request, snapshot)`` and acknowledge
      each control request with :class:`HarnessHookOutcome`.

    This adapter stores state and recent idempotency results in memory.  It is
    not a durable session store and cannot kill an uncooperative Python
    callable.
    """

    def __init__(
        self,
        identity: HarnessSessionIdentity,
        executor: Callable[..., Any] | None = None,
        *,
        start: HarnessHook | None = None,
        continue_handler: HarnessHook | None = None,
        checkpoint: HarnessHook | None = None,
        rebase: HarnessHook | None = None,
        preempt: HarnessHook | None = None,
        max_cached_requests: int = 1024,
    ) -> None:
        if not isinstance(identity, HarnessSessionIdentity):
            raise ConfigurationError("identity must be a HarnessSessionIdentity")
        if (executor is None) == (start is None):
            raise ConfigurationError("configure exactly one of executor= or start=")
        stateful_hooks = (continue_handler, checkpoint, rebase, preempt)
        if executor is not None and any(hook is not None for hook in stateful_hooks):
            raise ConfigurationError(
                "a legacy executor is one-shot and cannot declare stateful Harness operations"
            )
        if isinstance(max_cached_requests, bool) or not isinstance(max_cached_requests, int):
            raise ConfigurationError("max_cached_requests must be an integer")
        if max_cached_requests < 1:
            raise ConfigurationError("max_cached_requests must be >= 1")

        self._legacy_executor = executor
        self._hooks: dict[HarnessOperation, HarnessHook] = {}
        if start is not None:
            self._hooks[HarnessOperation.START] = start
        for operation, hook in (
            (HarnessOperation.CONTINUE, continue_handler),
            (HarnessOperation.CHECKPOINT, checkpoint),
            (HarnessOperation.REBASE, rebase),
            (HarnessOperation.PREEMPT, preempt),
        ):
            if hook is not None:
                self._hooks[operation] = hook

        operations = {HarnessOperation.START, *self._hooks}
        self._capabilities = HarnessCapabilities(
            operations=tuple(operations),
            checkpoint_scope=("session" if HarnessOperation.CHECKPOINT in operations else "none"),
            preemption_mode=("cooperative" if HarnessOperation.PREEMPT in operations else "none"),
            rebase_mode=("in_place" if HarnessOperation.REBASE in operations else "none"),
        )
        self._snapshot = HarnessSessionSnapshot(identity=identity)
        self._max_cached_requests = max_cached_requests
        self._request_results: OrderedDict[str, tuple[str, HarnessControlResult]] = OrderedDict()
        self._lock = asyncio.Lock()

    @property
    def capabilities(self) -> HarnessCapabilities:
        return self._capabilities

    @property
    def snapshot(self) -> HarnessSessionSnapshot:
        return self._snapshot

    def restore_durable_snapshot(self, snapshot: HarnessSessionSnapshot) -> None:
        """Restore a previously journaled session projection without hooks.

        This is a deliberately explicit recovery hook for the AgentOS bridge.
        It restores *logical Harness metadata only*; it does not resume an
        arbitrary Python callback, recreate model/tool state, or claim any
        Scheduler/Kernel ownership.  The bridge calls it only after validating
        the durable ``HARNESS_CONTROL`` event chain for this session.

        A caller cannot move a session backwards.  Replacing a snapshot at the
        same revision is allowed only when it is byte-identical, which makes a
        repeated reopen idempotent and fail-closed on conflicting state.
        """

        if not isinstance(snapshot, HarnessSessionSnapshot):
            raise ConfigurationError("durable Harness restore requires a HarnessSessionSnapshot")
        current = self._snapshot
        if snapshot.identity != current.identity:
            raise ConfigurationError(
                "durable Harness restore cannot change the exact session identity"
            )
        if snapshot.revision < current.revision:
            raise ConfigurationError(
                "durable Harness restore cannot move the session revision backwards"
            )
        if snapshot.revision == current.revision:
            if snapshot != current:
                raise ConfigurationError(
                    "durable Harness restore conflicts with the current snapshot"
                )
            return
        # A restored projection supersedes any in-memory request cache.  The
        # AgentOS bridge replays durable request ids from its journal, so
        # retaining cache entries from a pre-restart object could otherwise
        # return a stale result for a different session revision.
        self._request_results.clear()
        self._snapshot = snapshot

    def make_request(
        self,
        operation: HarnessOperation | str,
        *,
        request_id: str | None = None,
        reason: str = "",
        target_graph_version: int | None = None,
        target_semantic_epoch: int | None = None,
        payload: dict[str, JsonValue] | None = None,
    ) -> HarnessControlRequest:
        """Build a request fenced to the adapter's current session snapshot."""

        normalized = HarnessOperation(operation)
        expected_checkpoint_id = (
            self._snapshot.checkpoint_id
            if normalized in {HarnessOperation.CONTINUE, HarnessOperation.REBASE}
            and self._snapshot.state is HarnessSessionState.CHECKPOINTED
            else None
        )
        return HarnessControlRequest(
            request_id=request_id or _uuid(),
            operation=normalized,
            session=self._snapshot.identity,
            expected_revision=self._snapshot.revision,
            expected_checkpoint_id=expected_checkpoint_id,
            target_graph_version=target_graph_version,
            target_semantic_epoch=target_semantic_epoch,
            reason=reason,
            payload=payload or {},
        )

    async def control(self, request: HarnessControlRequest) -> HarnessControlResult:
        """Validate, apply, and acknowledge one control request atomically."""

        if not isinstance(request, HarnessControlRequest):
            request = HarnessControlRequest.model_validate(request)
        fingerprint = request.fingerprint()
        async with self._lock:
            replay = self._request_results.get(request.request_id)
            if replay is not None:
                cached_fingerprint, cached_result = replay
                if cached_fingerprint == fingerprint:
                    self._request_results.move_to_end(request.request_id)
                    return cached_result
                return self._result(
                    request,
                    HarnessResultStatus.REJECTED,
                    "request_id was already used for a different control request",
                )

            rejected = self._validate_request(request)
            if rejected is not None:
                result = self._result(request, *rejected)
                self._remember(request, fingerprint, result)
                return result

            before = self._snapshot
            try:
                outcome = await self._invoke(request, before)
                after = self._transition(request, before, outcome)
            except Exception as exc:
                result = self._result(
                    request,
                    HarnessResultStatus.FAILED,
                    f"Harness {request.operation.value} failed: {type(exc).__name__}: {exc}",
                    before=before,
                )
                self._remember(request, fingerprint, result)
                return result

            self._snapshot = after
            result = HarnessControlResult(
                request_id=request.request_id,
                operation=request.operation,
                status=HarnessResultStatus.APPLIED,
                before=before,
                after=after,
                message=f"{request.operation.value} acknowledged",
                output=outcome.output,
                details=outcome.details,
            )
            self._remember(request, fingerprint, result)
            return result

    def control_sync(self, request: HarnessControlRequest) -> HarnessControlResult:
        """Synchronous convenience wrapper for callers outside an event loop."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.control(request))
        raise RuntimeError("control_sync cannot run inside an active event loop; await control()")

    def _validate_request(
        self,
        request: HarnessControlRequest,
    ) -> tuple[HarnessResultStatus, str] | None:
        current = self._snapshot
        if request.session != current.identity:
            return (
                HarnessResultStatus.REJECTED,
                "session identity does not match the current Harness owner and cognition basis",
            )
        if request.expected_revision != current.revision:
            return (
                HarnessResultStatus.REJECTED,
                f"expected revision {request.expected_revision} does not match "
                f"current revision {current.revision}",
            )
        if not self._capabilities.supports(request.operation):
            return (
                HarnessResultStatus.UNSUPPORTED,
                f"Harness does not declare {request.operation.value} support",
            )
        if current.state not in _ALLOWED_STATES[request.operation]:
            return (
                HarnessResultStatus.REJECTED,
                f"{request.operation.value} is invalid while session is {current.state.value}",
            )

        if (
            request.operation in {HarnessOperation.CONTINUE, HarnessOperation.REBASE}
            and current.state is HarnessSessionState.CHECKPOINTED
            and request.expected_checkpoint_id != current.checkpoint_id
        ):
            return (
                HarnessResultStatus.REJECTED,
                "expected checkpoint identity does not match the current session checkpoint",
            )
        if (
            request.operation is HarnessOperation.CONTINUE
            and current.state is HarnessSessionState.RUNNING
            and request.expected_checkpoint_id is not None
        ):
            return (
                HarnessResultStatus.REJECTED,
                "a running session cannot continue from an unrelated checkpoint",
            )
        if request.operation is HarnessOperation.REBASE:
            target_graph = (
                current.identity.graph_version
                if request.target_graph_version is None
                else request.target_graph_version
            )
            target_epoch = (
                current.identity.semantic_epoch
                if request.target_semantic_epoch is None
                else request.target_semantic_epoch
            )
            if (
                target_graph < current.identity.graph_version
                or target_epoch < current.identity.semantic_epoch
                or (
                    target_graph == current.identity.graph_version
                    and target_epoch == current.identity.semantic_epoch
                )
            ):
                return (
                    HarnessResultStatus.REJECTED,
                    "REBASE target must advance graph version or semantic epoch "
                    "without moving either backwards",
                )
        return None

    async def _invoke(
        self,
        request: HarnessControlRequest,
        before: HarnessSessionSnapshot,
    ) -> HarnessHookOutcome:
        if request.operation is HarnessOperation.START and self._legacy_executor is not None:
            output = await _invoke_legacy_executor(self._legacy_executor, before.identity.task_id)
            return HarnessHookOutcome(completed=True, progress=1.0, output=output)

        hook = self._hooks[request.operation]
        raw = await _invoke_hook(hook, request, before)
        return raw if isinstance(raw, HarnessHookOutcome) else HarnessHookOutcome(output=raw)

    def _transition(
        self,
        request: HarnessControlRequest,
        before: HarnessSessionSnapshot,
        outcome: HarnessHookOutcome,
    ) -> HarnessSessionSnapshot:
        operation = request.operation
        checkpoint_id: str | None = None
        if operation is HarnessOperation.CHECKPOINT:
            if outcome.completed:
                raise ValueError("CHECKPOINT cannot also report completed=True")
            if not outcome.checkpoint_id:
                raise ValueError("CHECKPOINT must acknowledge a non-empty checkpoint_id")
            state = HarnessSessionState.CHECKPOINTED
            checkpoint_id = outcome.checkpoint_id
        elif operation is HarnessOperation.PREEMPT:
            if outcome.completed:
                raise ValueError("PREEMPT cannot also report completed=True")
            state = HarnessSessionState.PREEMPTED
            checkpoint_id = outcome.checkpoint_id
        else:
            if outcome.checkpoint_id is not None:
                raise ValueError(
                    f"{operation.value} cannot publish a checkpoint; use CHECKPOINT or PREEMPT"
                )
            state = (
                HarnessSessionState.COMPLETED if outcome.completed else HarnessSessionState.RUNNING
            )

        identity = before.identity
        if operation is HarnessOperation.REBASE:
            identity = identity.model_copy(
                update={
                    "graph_version": (
                        identity.graph_version
                        if request.target_graph_version is None
                        else request.target_graph_version
                    ),
                    "semantic_epoch": (
                        identity.semantic_epoch
                        if request.target_semantic_epoch is None
                        else request.target_semantic_epoch
                    ),
                }
            )

        progress = before.progress if outcome.progress is None else outcome.progress
        if state is HarnessSessionState.COMPLETED:
            progress = 1.0
        return HarnessSessionSnapshot(
            identity=identity,
            state=state,
            revision=before.revision + 1,
            checkpoint_id=checkpoint_id,
            progress=progress,
            last_request_id=request.request_id,
        )

    def _result(
        self,
        request: HarnessControlRequest,
        status: HarnessResultStatus,
        message: str,
        *,
        before: HarnessSessionSnapshot | None = None,
    ) -> HarnessControlResult:
        snapshot = before or self._snapshot
        return HarnessControlResult(
            request_id=request.request_id,
            operation=request.operation,
            status=status,
            before=snapshot,
            after=snapshot,
            message=message,
        )

    def _remember(
        self,
        request: HarnessControlRequest,
        fingerprint: str,
        result: HarnessControlResult,
    ) -> None:
        self._request_results[request.request_id] = (fingerprint, result)
        self._request_results.move_to_end(request.request_id)
        while len(self._request_results) > self._max_cached_requests:
            self._request_results.popitem(last=False)


async def _invoke_hook(
    hook: HarnessHook,
    request: HarnessControlRequest,
    snapshot: HarnessSessionSnapshot,
) -> HarnessHookOutcome | Any:
    if _is_async_callable(hook):
        result = hook(request, snapshot)
    else:
        result = await asyncio.to_thread(hook, request, snapshot)
    return await result if inspect.isawaitable(result) else result


async def _invoke_legacy_executor(executor: Callable[..., Any], task_id: str) -> Any:
    """Invoke the documented legacy ``executor(task_id)`` or ``executor()`` form."""

    args: tuple[Any, ...]
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        args = (task_id,)
    else:
        try:
            signature.bind(task_id)
        except TypeError as one_arg_error:
            try:
                signature.bind()
            except TypeError:
                raise ConfigurationError(
                    "legacy Harness executor must accept (task_id) or no arguments",
                    cause=one_arg_error,
                ) from one_arg_error
            args = ()
        else:
            args = (task_id,)

    if _is_async_callable(executor):
        result = executor(*args)
    else:
        result = await asyncio.to_thread(executor, *args)
    return await result if inspect.isawaitable(result) else result


def _is_async_callable(value: Any) -> bool:
    if inspect.iscoroutinefunction(value):
        return True
    if not callable(value):
        return False
    return inspect.iscoroutinefunction(type(value).__call__)


__all__ = [
    "HARNESS_SESSION_SCHEMA_VERSION",
    "CallableHarnessAdapter",
    "HarnessCapabilities",
    "HarnessControlRequest",
    "HarnessControlResult",
    "HarnessHook",
    "HarnessHookOutcome",
    "HarnessOperation",
    "HarnessResultStatus",
    "HarnessSessionAdapter",
    "HarnessSessionIdentity",
    "HarnessSessionSnapshot",
    "HarnessSessionState",
]
