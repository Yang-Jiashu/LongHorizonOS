"""Bounded caller-owned event-driven execution supervisor.

This module is the small bridge between the existing one-shot online epoch
API and a real *control loop*.  It deliberately does not start a thread,
daemon, watcher task, or hidden retry loop.  The embedding application owns
the lifecycle and explicitly calls ``start -> step -> ... -> stop`` (or uses
the async iterator).

The supervisor consumes two kinds of input:

* explicit :class:`SemanticInterrupt`/workspace observations supplied by the
  caller; and
* an optional :class:`WorkspaceObservationWatcher` that is polled at an
  explicit step boundary.

After observing and routing the events, one bounded
``AgentOS.execute_online_epoch`` is run against a freshly observed graph.  A
failed observation, graph/version mismatch, blocked watcher route, or failed
execution never falls through to stale work: the supervisor enters a
terminal ``FAILED_CLOSED`` state and returns an auditable step result.

This is intentionally a *bounded vertical slice*, not an always-on service or
an atomic Scheduler/Kernel/Harness transaction.  Those stronger guarantees
remain owned by the existing authorities and are documented as open work.
"""

from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, model_validator

from .errors import ConfigurationError
from .semantic_interrupt import SemanticInterrupt

EVENT_SUPERVISOR_SCHEMA_VERSION: Final[Literal["event-supervisor.v1"]] = "event-supervisor.v1"
EVENT_SUPERVISOR_POLICY_ID: Final[str] = "bounded-caller-event-supervisor.v1"


def _uuid() -> str:
    return uuid4().hex


def _canonical(value: Any) -> Any:
    """Return a bounded JSON-compatible representation for stable hashes."""

    if isinstance(value, BaseModel):
        return _canonical(value.model_dump(mode="json"))
    if isinstance(value, StrEnum):
        return value.value
    if hasattr(value, "as_dict") and callable(value.as_dict):
        try:
            return _canonical(value.as_dict())
        except Exception:
            pass
    if hasattr(value, "__dataclass_fields__"):
        try:
            from dataclasses import asdict

            return _canonical(asdict(value))
        except Exception:
            pass
    if isinstance(value, Mapping):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_canonical(item) for item in value]
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _hash(value: Any) -> str:
    payload = json.dumps(
        _canonical(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _bounded_error(exc: BaseException, limit: int = 240) -> str:
    text = f"{type(exc).__name__}: {exc}".replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else type(exc).__name__


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)


class SupervisorState(StrEnum):
    """Lifecycle state of a caller-owned supervisor."""

    CREATED = "created"
    RUNNING = "running"
    STOPPED = "stopped"
    CLOSED = "closed"
    FAILED_CLOSED = "failed_closed"
    BUDGET_EXHAUSTED = "budget_exhausted"


class SupervisorEventKind(StrEnum):
    """Event types accepted at an explicit supervisor step boundary."""

    INTERRUPT = "interrupt"
    WORKSPACE_POLL = "workspace_poll"
    WORKSPACE_CHANGE = "workspace_change"
    TICK = "tick"
    STOP = "stop"


class SupervisorStepStatus(StrEnum):
    """Outcome of one explicit supervisor step."""

    OBSERVED = "observed"
    EXECUTED = "executed"
    NO_WORK = "no_work"
    CLOSED = "closed"
    STOPPED = "stopped"
    BUDGET_EXHAUSTED = "budget_exhausted"
    EVENT_REJECTED = "event_rejected"
    FAILED_CLOSED = "failed_closed"


class SupervisorEvent(_FrozenModel):
    """A bounded event envelope.

    ``payload`` is intentionally opaque to the journal-facing DTO.  The
    supervisor only accepts known payload classes at the boundary and stores
    the envelope in memory; it never serializes prompts, model output, or
    arbitrary Context.
    """

    schema_version: Literal["event-supervisor.v1"] = EVENT_SUPERVISOR_SCHEMA_VERSION
    event_id: str = Field(default_factory=_uuid, min_length=1)
    kind: SupervisorEventKind
    graph_id: str = ""
    graph_version: StrictInt | None = Field(default=None, ge=0)
    payload: Any = None
    reason: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_event(self) -> SupervisorEvent:
        if not self.event_id.strip():
            raise ValueError("event_id must be non-empty")
        if self.kind is SupervisorEventKind.STOP and self.payload not in (None, ""):
            # A STOP payload is not needed and can accidentally carry an
            # unbounded object.  Keep the boundary small and deterministic.
            raise ValueError("stop events cannot carry a payload")
        return self

    def fingerprint(self) -> str:
        return _hash(
            {
                "schema_version": self.schema_version,
                "event_id": self.event_id,
                "kind": self.kind,
                "graph_id": self.graph_id,
                "graph_version": self.graph_version,
                "payload": self.payload,
                "reason": self.reason,
                "metadata": self.metadata,
            }
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "event_id": self.event_id,
            "kind": self.kind.value,
            "graph_id": self.graph_id,
            "graph_version": self.graph_version,
            "payload": _canonical(self.payload),
            "reason": self.reason,
            "metadata": _canonical(self.metadata),
        }


class SupervisorSnapshot(_FrozenModel):
    """Read-only lifecycle and graph projection."""

    schema_version: Literal["event-supervisor.v1"] = EVENT_SUPERVISOR_SCHEMA_VERSION
    supervisor_id: str = Field(min_length=1)
    goal_id: str = Field(min_length=1)
    state: SupervisorState
    graph_id: str = ""
    graph_version: StrictInt = Field(ge=0)
    goal_state: str = "open"
    epochs_attempted: StrictInt = Field(ge=0)
    pending_event_count: StrictInt = Field(ge=0)
    stop_reason: str = ""
    last_error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class SupervisorStepResult(_FrozenModel):
    """Immutable transcript of one ``step`` call."""

    schema_version: Literal["event-supervisor.v1"] = EVENT_SUPERVISOR_SCHEMA_VERSION
    supervisor_id: str = Field(min_length=1)
    step_id: StrictInt = Field(ge=0)
    epoch_id: StrictInt = Field(ge=0)
    status: SupervisorStepStatus
    supervisor_state: SupervisorState
    goal_id: str = Field(min_length=1)
    graph_id: str = ""
    graph_version: StrictInt = Field(ge=0)
    event_ids: tuple[str, ...] = ()
    accepted_event_ids: tuple[str, ...] = ()
    rejected_event_ids: tuple[str, ...] = ()
    watcher_polled: StrictBool = False
    workspace_route: Any = None
    workspace_poll: Any = None
    interrupt_epoch: Any = None
    execution_result: Any = None
    state_before_hash: str = Field(min_length=64, max_length=64)
    state_after_hash: str = Field(min_length=64, max_length=64)
    reason: str = ""
    result_hash: str = Field(min_length=64, max_length=64)

    @property
    def goal_closed(self) -> bool:
        result = self.execution_result
        if result is not None and getattr(result, "goal_state", None) == "closed":
            return True
        return self.status is SupervisorStepStatus.CLOSED

    @property
    def progressed(self) -> bool:
        result = self.execution_result
        if result is None:
            return False
        online = getattr(result, "meta", {}).get("online_epoch", {})
        dispatched = online.get("actual_dispatched_task_ids", ())
        return bool(dispatched) or bool(getattr(result, "verified", ()))

    def as_dict(self) -> dict[str, Any]:
        # ``workspace_*`` and ``execution_result`` are intentionally typed as
        # opaque adapter values.  Dump in Python mode first, then canonicalize
        # those fields so a fake/third-party result object cannot make the
        # audit projection fail serialization.
        payload = self.model_dump(mode="python")
        payload = {key: _canonical(value) for key, value in payload.items()}
        payload["workspace_route"] = _canonical(self.workspace_route)
        payload["workspace_poll"] = _canonical(self.workspace_poll)
        payload["interrupt_epoch"] = _canonical(self.interrupt_epoch)
        payload["execution_result"] = _canonical(self.execution_result)
        return payload


@dataclass(frozen=True)
class SupervisorRunResult:
    """Bounded transcript returned by :meth:`EventDrivenSupervisor.run`."""

    goal_id: str
    steps: tuple[SupervisorStepResult, ...]
    final_snapshot: SupervisorSnapshot
    stop_reason: str

    @property
    def complete(self) -> bool:
        return self.final_snapshot.state is SupervisorState.CLOSED

    @property
    def failed_closed(self) -> bool:
        return self.final_snapshot.state is SupervisorState.FAILED_CLOSED

    def as_dict(self) -> dict[str, Any]:
        return {
            "goal_id": self.goal_id,
            "steps": [item.as_dict() for item in self.steps],
            "final_snapshot": self.final_snapshot.as_dict(),
            "stop_reason": self.stop_reason,
            "complete": self.complete,
            "failed_closed": self.failed_closed,
        }


def _event_from_value(value: Any) -> SupervisorEvent:
    """Normalize supported event inputs without broadening the event scope."""

    if isinstance(value, SupervisorEvent):
        return value
    if isinstance(value, SemanticInterrupt):
        return SupervisorEvent(
            event_id=value.interrupt_id,
            kind=SupervisorEventKind.INTERRUPT,
            graph_id=value.graph_id,
            graph_version=value.graph_version,
            payload=value,
            reason=value.reason,
        )

    # Imports are local to avoid a module import cycle during SDK bootstrap.
    try:
        from .watchers import WorkspaceObservationChange, WorkspaceWatchPoll

        if isinstance(value, WorkspaceWatchPoll):
            event_id = _hash(
                {
                    "kind": SupervisorEventKind.WORKSPACE_POLL,
                    "goal_id": value.goal_id,
                    "graph_id": value.graph_id,
                    "changes": value.changes,
                    "interrupts": value.interrupts,
                    "epoch": value.epoch,
                }
            )
            graph_version = None if value.epoch is None else int(value.epoch.graph_version)
            return SupervisorEvent(
                event_id=event_id,
                kind=SupervisorEventKind.WORKSPACE_POLL,
                graph_id=value.graph_id,
                graph_version=graph_version,
                payload=value,
                reason="workspace poll",
            )
        if isinstance(value, WorkspaceObservationChange):
            event_id = _hash(
                {
                    "kind": SupervisorEventKind.WORKSPACE_CHANGE,
                    "goal_id": value.goal_id,
                    "artifact_id": value.artifact_id,
                    "resource_uri": value.resource_uri,
                    "kind_value": value.kind,
                    "previous": value.previous_observation,
                    "observation": value.observation,
                }
            )
            return SupervisorEvent(
                event_id=event_id,
                kind=SupervisorEventKind.WORKSPACE_CHANGE,
                graph_id=value.graph_id,
                payload=value,
                reason="workspace observation",
            )
    except ImportError:
        pass

    if isinstance(value, Mapping):
        raw = dict(value)
        kind_raw = raw.pop("kind", raw.pop("event_kind", raw.pop("type", None)))
        if kind_raw is None:
            raise ConfigurationError("event mapping requires kind/event_kind/type")
        try:
            kind = SupervisorEventKind(str(kind_raw).strip().lower())
        except ValueError as exc:
            raise ConfigurationError(f"unsupported supervisor event kind: {kind_raw!r}") from exc
        payload = raw.pop("payload", raw.pop("event", None))
        if kind is SupervisorEventKind.INTERRUPT and payload is None:
            payload = raw.pop("interrupt", None)
        if kind is SupervisorEventKind.STOP:
            payload = None
        event_id = str(raw.pop("event_id", "")).strip()
        if not event_id:
            event_id = _hash({"kind": kind, "payload": payload, "metadata": raw})
        return SupervisorEvent(
            event_id=event_id,
            kind=kind,
            graph_id=str(raw.pop("graph_id", "") or "").strip(),
            graph_version=raw.pop("graph_version", None),
            payload=payload,
            reason=str(raw.pop("reason", "") or "").strip(),
            metadata=raw,
        )
    raise ConfigurationError(f"unsupported supervisor event type: {type(value).__name__}")


class EventDrivenSupervisor:
    """Explicit, bounded event-driven execution supervisor.

    The supervisor is intentionally *caller-owned*.  ``start`` and ``stop``
    are synchronous lifecycle operations; ``step``/``run`` are async because
    they may invoke the existing ``AgentOS.execute_online_epoch`` path.
    No method creates an implicit thread or daemon.
    """

    def __init__(
        self,
        agent_os: Any,
        goal: Any,
        *,
        watcher: Any | None = None,
        max_epochs: int = 1,
        max_concurrency: int = 1,
        max_dispatches_per_epoch: int | None = None,
        max_parallelism: int | None = None,
        resource_aware: bool = False,
        conflict_graph: Any | None = None,
        persist_epoch: bool = True,
        poll_workspace: bool | None = None,
        route_workspace: bool = False,
        reconcile_after_delivery: bool = True,
        deliver_interrupts: bool = False,
        max_pending_events: int = 1024,
        supervisor_id: str | None = None,
        initial_epoch: int = 0,
    ) -> None:
        if agent_os is None:
            raise TypeError("agent_os is required")
        if not callable(getattr(agent_os, "runtime_state", None)):
            raise TypeError("agent_os must expose runtime_state")
        if not callable(getattr(agent_os, "execute_online_epoch", None)):
            raise TypeError("agent_os must expose execute_online_epoch")
        goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not goal_id:
            raise ConfigurationError("goal must identify a non-empty goal_id")
        _validate_non_negative_int("max_epochs", max_epochs)
        _validate_positive_int("max_concurrency", max_concurrency)
        if max_dispatches_per_epoch is not None:
            _validate_non_negative_int("max_dispatches_per_epoch", max_dispatches_per_epoch)
        if max_parallelism is not None:
            _validate_positive_int("max_parallelism", max_parallelism)
        _validate_non_negative_int("max_pending_events", max_pending_events)
        _validate_non_negative_int("initial_epoch", initial_epoch)
        if not isinstance(resource_aware, bool):
            raise ConfigurationError("resource_aware must be a boolean")
        if not isinstance(persist_epoch, bool):
            raise ConfigurationError("persist_epoch must be a boolean")
        if not isinstance(route_workspace, bool):
            raise ConfigurationError("route_workspace must be a boolean")
        if not isinstance(reconcile_after_delivery, bool):
            raise ConfigurationError("reconcile_after_delivery must be a boolean")
        if not isinstance(deliver_interrupts, bool):
            raise ConfigurationError("deliver_interrupts must be a boolean")
        if poll_workspace is not None and not isinstance(poll_workspace, bool):
            raise ConfigurationError("poll_workspace must be a boolean or None")
        if watcher is not None and not (
            callable(getattr(watcher, "poll_and_reconcile", None))
            or callable(getattr(watcher, "poll_and_route", None))
        ):
            raise ConfigurationError("watcher must expose poll_and_reconcile or poll_and_route")

        self._agent_os = agent_os
        self._goal = goal
        self._goal_id = goal_id
        self._watcher = watcher
        self._max_epochs = max_epochs
        self._max_concurrency = max_concurrency
        self._max_dispatches_per_epoch = max_dispatches_per_epoch
        self._max_parallelism = max_parallelism
        self._resource_aware = resource_aware
        self._conflict_graph = conflict_graph
        self._persist_epoch = persist_epoch
        self._poll_workspace = watcher is not None if poll_workspace is None else poll_workspace
        self._route_workspace = route_workspace
        self._reconcile_after_delivery = reconcile_after_delivery
        self._deliver_interrupts = deliver_interrupts
        self._max_pending_events = max_pending_events
        self._supervisor_id = str(supervisor_id or _uuid()).strip()
        if not self._supervisor_id:
            raise ConfigurationError("supervisor_id must be non-empty")
        self._next_epoch = initial_epoch
        self._step_id = 0
        self._epochs_attempted = 0
        self._state = SupervisorState.CREATED
        self._stop_reason = ""
        self._last_error: str | None = None
        self._graph_id = ""
        self._graph_version = 0
        self._goal_state = "open"
        self._pending: deque[SupervisorEvent] = deque()
        self._seen_events: dict[str, str] = {}
        # Keep the last authoritative runtime projection so terminal calls
        # can return an auditable result without observing the world again.
        # In particular, a supervisor that has already reached CLOSED must
        # not let a later external mutation (or a watcher poll) reopen its
        # lifecycle state.
        self._last_runtime_state: Any | None = None
        self._last_snapshot: SupervisorSnapshot | None = None
        self._aiter_started = False

    @property
    def supervisor_id(self) -> str:
        return self._supervisor_id

    @property
    def watcher(self) -> Any | None:
        """Return the caller-supplied workspace watcher, if configured.

        The supervisor remains caller-owned; exposing the immutable reference
        lets bounded adapters compose with it without reaching into private
        implementation state.
        """

        return self._watcher

    @property
    def state(self) -> SupervisorState:
        return self._state

    @property
    def pending_events(self) -> tuple[SupervisorEvent, ...]:
        return tuple(self._pending)

    @property
    def snapshot(self) -> SupervisorSnapshot:
        return self._make_snapshot()

    @property
    def started(self) -> bool:
        return self._state is not SupervisorState.CREATED

    @property
    def terminal(self) -> bool:
        return self._state in {
            SupervisorState.STOPPED,
            SupervisorState.CLOSED,
            SupervisorState.FAILED_CLOSED,
            SupervisorState.BUDGET_EXHAUSTED,
        }

    @property
    def max_parallelism(self) -> int | None:
        """Parallelism ceiling handed to the next executed epoch."""

        return self._max_parallelism

    def set_max_parallelism(self, max_parallelism: int | None) -> None:
        """Set the degree the next epoch will execute with.

        Exposed because parallelism is a per-epoch *decision*, not a property of
        the supervisor: a driver that observes rising conflict or contention must
        be able to lower the degree between steps.  Poking the private attribute
        instead would leave the next reader believing the degree is immutable.
        """

        if max_parallelism is not None:
            _validate_positive_int("max_parallelism", max_parallelism)
        self._max_parallelism = max_parallelism

    def start(self) -> SupervisorSnapshot:
        """Observe the initial graph and enter ``RUNNING`` exactly once."""

        if self._state is not SupervisorState.CREATED:
            return self._make_snapshot()
        try:
            state = self._observe()
            self._graph_id = str(state.graph_id)
            self._graph_version = int(state.progress.graph_version)
            self._goal_state = "closed" if state.progress.goal_closed else "open"
            if state.progress.goal_closed:
                self._state = SupervisorState.CLOSED
                self._stop_reason = "goal_closed"
            elif self._max_epochs == 0:
                self._state = SupervisorState.BUDGET_EXHAUSTED
                self._stop_reason = "max_epochs"
            else:
                self._state = SupervisorState.RUNNING
        except Exception as exc:
            self._fail_closed(_bounded_error(exc))
        return self._make_snapshot()

    def stop(self, reason: str = "caller_stop") -> SupervisorSnapshot:
        """Stop explicitly; repeated calls are idempotent."""

        normalized = str(reason or "caller_stop").strip() or "caller_stop"
        if not self.terminal:
            self._state = SupervisorState.STOPPED
            self._stop_reason = normalized
        return self._make_snapshot()

    request_stop = stop

    def submit(self, event: Any) -> SupervisorEvent:
        """Queue one event, preserving idempotency and bounded memory."""

        normalized = _event_from_value(event)
        fingerprint = normalized.fingerprint()
        previous = self._seen_events.get(normalized.event_id)
        if previous is not None:
            if previous != fingerprint:
                self._fail_closed(f"conflicting duplicate event_id={normalized.event_id!r}")
                raise ConfigurationError(f"conflicting duplicate event_id={normalized.event_id!r}")
            return normalized
        if len(self._pending) >= self._max_pending_events:
            self._fail_closed("pending event budget exhausted")
            raise ConfigurationError("pending event budget exhausted")
        if self._graph_id and normalized.graph_id and normalized.graph_id != self._graph_id:
            self._fail_closed(
                f"event graph mismatch: expected={self._graph_id}, observed={normalized.graph_id}"
            )
            raise ConfigurationError("event graph mismatch")
        self._seen_events[normalized.event_id] = fingerprint
        self._pending.append(normalized)
        return normalized

    def submit_many(self, events: Iterable[Any]) -> tuple[SupervisorEvent, ...]:
        return tuple(self.submit(item) for item in events)

    async def step(
        self,
        events: Iterable[Any] = (),
        *,
        event_proposals: Iterable[Any] | None = None,
        poll_workspace: bool | None = None,
        execute: bool = True,
    ) -> SupervisorStepResult:
        """Run one explicit observe -> event -> bounded epoch pass."""

        if event_proposals is not None:
            if tuple(events):
                raise ConfigurationError("pass either events or event_proposals, not both")
            events = event_proposals
        if not isinstance(execute, bool):
            raise ConfigurationError("execute must be a boolean")
        if poll_workspace is not None and not isinstance(poll_workspace, bool):
            raise ConfigurationError("poll_workspace must be a boolean or None")

        if self._state is SupervisorState.CREATED:
            self.start()
        epoch_id = self._next_epoch
        self._next_epoch += 1
        self._step_id += 1

        # Terminal lifecycle states are monotonic and caller-owned.  Do not
        # call runtime_state, poll a watcher, reconcile observations, or
        # consume queued events after termination.  Besides avoiding side
        # effects, this preserves the CLOSED projection if another authority
        # later reports a reopened/open graph.
        if self.terminal:
            state_before = self._terminal_runtime_state()
            return self._result(
                status=(
                    SupervisorStepStatus.CLOSED
                    if self._state is SupervisorState.CLOSED
                    else SupervisorStepStatus.BUDGET_EXHAUSTED
                    if self._state is SupervisorState.BUDGET_EXHAUSTED
                    else SupervisorStepStatus.FAILED_CLOSED
                    if self._state is SupervisorState.FAILED_CLOSED
                    else SupervisorStepStatus.STOPPED
                ),
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_before,
                reason=self._stop_reason or self._state.value,
            )

        state_before = self._safe_observe()

        # Another caller may have closed the Goal since ``start()``.  Mark
        # this supervisor terminal before polling/reconciling a watcher;
        # otherwise a terminal pass could mutate observation baselines or
        # reopen semantic work after closure.
        if not self.terminal and bool(getattr(state_before.progress, "goal_closed", False)):
            self._state = SupervisorState.CLOSED
            self._goal_state = "closed"
            self._stop_reason = "goal_closed"

        if self.terminal:
            return self._result(
                status=(
                    SupervisorStepStatus.CLOSED
                    if self._state is SupervisorState.CLOSED
                    else SupervisorStepStatus.BUDGET_EXHAUSTED
                    if self._state is SupervisorState.BUDGET_EXHAUSTED
                    else SupervisorStepStatus.FAILED_CLOSED
                    if self._state is SupervisorState.FAILED_CLOSED
                    else SupervisorStepStatus.STOPPED
                ),
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_before,
                reason=self._stop_reason or self._state.value,
            )

        if self._epochs_attempted >= self._max_epochs:
            self._state = SupervisorState.BUDGET_EXHAUSTED
            self._stop_reason = "max_epochs"
            return self._result(
                status=SupervisorStepStatus.BUDGET_EXHAUSTED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_before,
                reason="max_epochs",
            )

        pending_events: tuple[SupervisorEvent, ...] = ()
        event_ids: tuple[str, ...] = ()
        accepted: list[str] = []
        rejected: list[str] = []
        workspace_poll: Any = None
        workspace_route: Any = None
        interrupt_epoch: Any = None

        try:
            # Normalize/queue supplied events inside the fail-closed boundary.
            # A conflicting duplicate or a graph-identity mismatch therefore
            # becomes a typed EVENT_REJECTED result instead of escaping as an
            # unstructured exception from ``step``.
            supplied = tuple(self.submit(item) for item in events)
            queued = tuple(self._pending)
            self._pending.clear()
            pending_events = _dedupe_events((*queued, *supplied))
            event_ids = tuple(item.event_id for item in pending_events)
            should_poll = self._poll_workspace if poll_workspace is None else poll_workspace
            if should_poll and self._watcher is not None:
                workspace_poll, workspace_route = self._poll_watcher(epoch_id)
                if workspace_route is not None:
                    workspace_poll = getattr(workspace_route, "poll", workspace_poll)
                    if getattr(workspace_route, "blocked", ()):
                        raise ConfigurationError(
                            "workspace route contained blocked control requests"
                        )
                if workspace_poll is not None:
                    watcher_event = _event_from_value(workspace_poll)
                    pending_events = _dedupe_events((*pending_events, watcher_event))
                    event_ids = tuple(item.event_id for item in pending_events)

            for event in pending_events:
                if event.kind is SupervisorEventKind.STOP:
                    self.stop(event.reason or "stop_event")
                    accepted.append(event.event_id)
                    continue
                current = self._safe_observe()
                self._check_event_version(event, current)
                if event.kind is SupervisorEventKind.WORKSPACE_CHANGE:
                    if self._watcher is None:
                        raise ConfigurationError(
                            "workspace change event requires a configured watcher"
                        )
                    _route = getattr(self._watcher, "route_observation", None)
                    if not callable(_route):
                        raise ConfigurationError(
                            "configured watcher cannot route workspace changes"
                        )
                    routed = _route(
                        (event.payload,),
                        epoch_id=epoch_id,
                        persist=self._persist_epoch,
                        reconcile_after_delivery=self._reconcile_after_delivery,
                    )
                    workspace_route = routed
                    workspace_poll = getattr(routed, "poll", workspace_poll)
                    if getattr(routed, "blocked", ()):
                        raise ConfigurationError(
                            "workspace change route contained blocked requests"
                        )
                elif event.kind is SupervisorEventKind.WORKSPACE_POLL:
                    workspace_poll = event.payload
                    if getattr(workspace_poll, "epoch", None) is not None:
                        interrupt_epoch = workspace_poll.epoch
                elif event.kind is SupervisorEventKind.INTERRUPT:
                    interrupt = event.payload
                    if not isinstance(interrupt, SemanticInterrupt):
                        interrupt = SemanticInterrupt.model_validate(interrupt)
                    interrupt_epoch = self._agent_os.plan_interrupts(
                        self._goal,
                        (interrupt,),
                        epoch_id=epoch_id,
                        persist=self._persist_epoch,
                    )
                    if self._deliver_interrupts:
                        self._deliver_interrupt_epoch(interrupt_epoch, current)
                elif event.kind is SupervisorEventKind.TICK:
                    pass
                else:
                    raise ConfigurationError(f"unsupported event kind: {event.kind.value}")
                accepted.append(event.event_id)
        except Exception as exc:
            rejected.extend(
                item.event_id for item in pending_events if item.event_id not in accepted
            )
            # Re-queue only events not yet accepted; terminal fail-closed state
            # retains their identities for audit but never silently retries.
            self._fail_closed(_bounded_error(exc))
            state_after = self._safe_observe(fallback=state_before)
            return self._result(
                status=SupervisorStepStatus.EVENT_REJECTED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_after,
                event_ids=event_ids,
                accepted_event_ids=tuple(accepted),
                rejected_event_ids=tuple(rejected),
                watcher_polled=workspace_poll is not None,
                workspace_poll=workspace_poll,
                workspace_route=workspace_route,
                interrupt_epoch=interrupt_epoch,
                reason=_bounded_error(exc),
            )

        if self._state is SupervisorState.STOPPED:
            state_after = self._safe_observe(fallback=state_before)
            return self._result(
                status=SupervisorStepStatus.STOPPED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_after,
                event_ids=event_ids,
                accepted_event_ids=tuple(accepted),
                rejected_event_ids=tuple(rejected),
                watcher_polled=workspace_poll is not None,
                workspace_poll=workspace_poll,
                workspace_route=workspace_route,
                interrupt_epoch=interrupt_epoch,
                reason=self._stop_reason,
            )

        if not execute:
            state_after = self._safe_observe(fallback=state_before)
            # Event routing/reconciliation can itself close the Goal even
            # when this step intentionally skips execution.  Publish the
            # terminal lifecycle immediately; otherwise the next caller
            # step would get one more RUNNING pass (and potentially poll a
            # watcher) before noticing closure.
            if bool(getattr(state_after.progress, "goal_closed", False)):
                self._state = SupervisorState.CLOSED
                self._goal_state = "closed"
                self._stop_reason = "goal_closed"
                return self._result(
                    status=SupervisorStepStatus.CLOSED,
                    epoch_id=epoch_id,
                    state_before=state_before,
                    state_after=state_after,
                    event_ids=event_ids,
                    accepted_event_ids=tuple(accepted),
                    rejected_event_ids=tuple(rejected),
                    watcher_polled=workspace_poll is not None,
                    workspace_poll=workspace_poll,
                    workspace_route=workspace_route,
                    interrupt_epoch=interrupt_epoch,
                    reason="goal_closed_after_events",
                )
            return self._result(
                status=SupervisorStepStatus.OBSERVED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_after,
                event_ids=event_ids,
                accepted_event_ids=tuple(accepted),
                rejected_event_ids=tuple(rejected),
                watcher_polled=workspace_poll is not None,
                workspace_poll=workspace_poll,
                workspace_route=workspace_route,
                interrupt_epoch=interrupt_epoch,
                reason="observation_only",
            )

        # Re-observe after workspace reconciliation before executing.  This
        # graph-version fence is what prevents a stale event from falling
        # through into an old adaptive epoch.
        state_after_events = self._safe_observe()
        self._graph_id = str(state_after_events.graph_id)
        self._graph_version = int(state_after_events.progress.graph_version)
        if state_after_events.progress.goal_closed:
            self._state = SupervisorState.CLOSED
            self._goal_state = "closed"
            self._stop_reason = "goal_closed"
            return self._result(
                status=SupervisorStepStatus.CLOSED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_after_events,
                event_ids=event_ids,
                accepted_event_ids=tuple(accepted),
                rejected_event_ids=tuple(rejected),
                watcher_polled=workspace_poll is not None,
                workspace_poll=workspace_poll,
                workspace_route=workspace_route,
                interrupt_epoch=interrupt_epoch,
                reason="goal_closed_before_execution",
            )

        try:
            execution = await self._agent_os.execute_online_epoch(
                self._goal,
                max_concurrency=self._max_concurrency,
                max_dispatches=self._max_dispatches_per_epoch,
                max_parallelism=self._max_parallelism,
                resource_aware=self._resource_aware,
                conflict_graph=self._conflict_graph,
                persist_epoch=self._persist_epoch,
            )
            self._epochs_attempted += 1
        except Exception as exc:
            self._epochs_attempted += 1
            self._fail_closed(_bounded_error(exc))
            state_after = self._safe_observe(fallback=state_after_events)
            return self._result(
                status=SupervisorStepStatus.FAILED_CLOSED,
                epoch_id=epoch_id,
                state_before=state_before,
                state_after=state_after,
                event_ids=event_ids,
                accepted_event_ids=tuple(accepted),
                rejected_event_ids=tuple(rejected),
                watcher_polled=workspace_poll is not None,
                workspace_poll=workspace_poll,
                workspace_route=workspace_route,
                interrupt_epoch=interrupt_epoch,
                reason=_bounded_error(exc),
            )

        state_after = self._safe_observe(fallback=state_after_events)
        self._graph_id = str(state_after.graph_id)
        self._graph_version = int(state_after.progress.graph_version)
        self._goal_state = "closed" if state_after.progress.goal_closed else "open"
        failures = tuple(getattr(execution, "failures", ()) or ())
        online = getattr(execution, "meta", {}).get("online_epoch", {})
        dispatched = tuple(online.get("actual_dispatched_task_ids", ()) or ())
        if failures:
            self._fail_closed("; ".join(str(item) for item in failures))
            status = SupervisorStepStatus.FAILED_CLOSED
            reason = "epoch_failed"
        elif state_after.progress.goal_closed or getattr(execution, "goal_state", "") == "closed":
            self._state = SupervisorState.CLOSED
            self._goal_state = "closed"
            self._stop_reason = "goal_closed"
            status = SupervisorStepStatus.CLOSED
            reason = "goal_closed"
        elif self._epochs_attempted >= self._max_epochs:
            self._state = SupervisorState.BUDGET_EXHAUSTED
            self._stop_reason = "max_epochs"
            status = SupervisorStepStatus.BUDGET_EXHAUSTED
            reason = "max_epochs"
        elif not dispatched:
            status = SupervisorStepStatus.NO_WORK
            reason = "no_dispatch"
        else:
            status = SupervisorStepStatus.EXECUTED
            reason = "epoch_executed"
        return self._result(
            status=status,
            epoch_id=epoch_id,
            state_before=state_before,
            state_after=state_after,
            event_ids=event_ids,
            accepted_event_ids=tuple(accepted),
            rejected_event_ids=tuple(rejected),
            watcher_polled=workspace_poll is not None,
            workspace_poll=workspace_poll,
            workspace_route=workspace_route,
            interrupt_epoch=interrupt_epoch,
            execution_result=execution,
            reason=reason,
        )

    async def astep(self, *args: Any, **kwargs: Any) -> SupervisorStepResult:
        """Alias emphasizing that the supervisor is asynchronous."""

        return await self.step(*args, **kwargs)

    async def run(
        self,
        max_steps: int | None = None,
        *,
        events: Iterable[Any] = (),
        poll_workspace: bool | None = None,
        execute: bool = True,
    ) -> SupervisorRunResult:
        """Run a caller-bounded sequence and return the full transcript."""

        if max_steps is not None:
            _validate_non_negative_int("max_steps", max_steps)
        if self._state is SupervisorState.CREATED:
            self.start()
        initial_events = tuple(events)
        if initial_events:
            self.submit_many(initial_events)
        remaining_budget = max(0, self._max_epochs - self._epochs_attempted)
        limit = remaining_budget if max_steps is None else min(max_steps, remaining_budget)
        steps: list[SupervisorStepResult] = []
        for index in range(limit):
            result = await self.step(
                poll_workspace=poll_workspace,
                execute=execute,
            )
            steps.append(result)
            if result.status in {
                SupervisorStepStatus.CLOSED,
                SupervisorStepStatus.STOPPED,
                SupervisorStepStatus.FAILED_CLOSED,
                SupervisorStepStatus.EVENT_REJECTED,
                SupervisorStepStatus.BUDGET_EXHAUSTED,
            }:
                break
            if result.status is SupervisorStepStatus.NO_WORK and not self._pending:
                # A caller can submit a future event and call run again; do
                # not convert a transient no-work observation into a daemon.
                break
            if index + 1 >= limit:
                break
        snapshot = self._make_snapshot()
        return SupervisorRunResult(
            goal_id=self._goal_id,
            steps=tuple(steps),
            final_snapshot=snapshot,
            stop_reason=snapshot.stop_reason
            or ("max_steps" if max_steps is not None else "max_epochs"),
        )

    async def arun(self, *args: Any, **kwargs: Any) -> SupervisorRunResult:
        return await self.run(*args, **kwargs)

    def __aiter__(self) -> EventDrivenSupervisor:
        self._aiter_started = True
        if self._state is SupervisorState.CREATED:
            self.start()
        return self

    async def __anext__(self) -> SupervisorStepResult:
        if not self._aiter_started:
            self.__aiter__()
        if self.terminal or self._epochs_attempted >= self._max_epochs:
            raise StopAsyncIteration
        result = await self.step()
        if result.status in {
            SupervisorStepStatus.CLOSED,
            SupervisorStepStatus.STOPPED,
            SupervisorStepStatus.FAILED_CLOSED,
            SupervisorStepStatus.EVENT_REJECTED,
            SupervisorStepStatus.NO_WORK,
            SupervisorStepStatus.BUDGET_EXHAUSTED,
        }:
            # Yield the terminal observation once; the following call stops.
            self._aiter_started = False
        return result

    def _observe(self, *, fallback: Any | None = None) -> Any:
        try:
            state = self._agent_os.runtime_state(self._goal)
            self._last_runtime_state = state
            return state
        except Exception:
            if fallback is not None:
                return fallback
            raise

    def _terminal_runtime_state(self) -> Any:
        """Return the last authority projection without re-observing.

        A terminal supervisor must be inert: a later ``runtime_state`` call
        could observe an externally reopened graph and a watcher poll could
        reconcile it, violating lifecycle monotonicity.  ``start`` and every
        successful step cache a projection, so terminal results can reuse it.
        If startup failed before a projection existed, retain the normal
        fail-closed behavior by making one final best-effort observation only
        for result construction; no watcher/reconciliation is performed.
        """

        if self._last_runtime_state is not None:
            return self._last_runtime_state
        try:
            return self._observe()
        except Exception as exc:
            raise ConfigurationError(_bounded_error(exc)) from exc

    def _safe_observe(self, fallback: Any | None = None) -> Any:
        try:
            state = self._observe(fallback=fallback)
            self._graph_id = str(state.graph_id)
            self._graph_version = int(state.progress.graph_version)
            self._goal_state = "closed" if state.progress.goal_closed else "open"
            return state
        except Exception as exc:
            self._fail_closed(_bounded_error(exc))
            if fallback is not None:
                return fallback
            # The initial state is required to construct a result.  Let the
            # caller see the authority failure rather than inventing state.
            raise ConfigurationError(_bounded_error(exc)) from exc

    def _check_event_version(self, event: SupervisorEvent, state: Any) -> None:
        if event.graph_id and event.graph_id != str(state.graph_id):
            raise ConfigurationError(
                f"event graph mismatch: expected={state.graph_id}, observed={event.graph_id}"
            )
        if event.graph_version is not None and int(event.graph_version) != int(
            state.progress.graph_version
        ):
            raise ConfigurationError(
                "event graph version mismatch: "
                f"expected={state.progress.graph_version}, observed={event.graph_version}"
            )

    def _poll_watcher(self, epoch_id: int) -> tuple[Any, Any]:
        if self._watcher is None:
            return None, None
        if self._route_workspace and callable(
            getattr(self._agent_os, "poll_workspace_and_route", None)
        ):
            route = self._agent_os.poll_workspace_and_route(
                self._watcher,
                epoch_id=epoch_id,
                persist=self._persist_epoch,
                reconcile_after_delivery=self._reconcile_after_delivery,
            )
            return getattr(route, "poll", None), route
        poll = self._watcher.poll_and_reconcile(
            epoch_id=epoch_id,
            persist=self._persist_epoch,
        )
        return poll, None

    def _deliver_interrupt_epoch(self, epoch: Any, state: Any) -> None:
        deliver = getattr(self._agent_os, "deliver_interrupt", None)
        if not callable(deliver):
            raise ConfigurationError("deliver_interrupts requires AgentOS.deliver_interrupt")
        attempts = {
            str(item.attempt_id): item
            for item in getattr(state.agent_cognition, "current_attempts", ())
            if getattr(item, "attempt_id", None)
        }
        for decision in getattr(epoch, "decisions", ()):
            if str(getattr(decision, "target_kind", "")) != "attempt":
                continue
            attempt = attempts.get(str(decision.target_id))
            if attempt is None:
                raise ConfigurationError(
                    f"interrupt target attempt {decision.target_id!r} is not live"
                )
            result = deliver(
                self._goal,
                claim_id=str(attempt.claim_id),
                action=str(decision.action.value),
                expected_graph_version=int(state.progress.graph_version),
                expected_semantic_epoch=int(attempt.semantic_epoch),
                attempt_id=str(attempt.attempt_id),
                task_id=str(attempt.task_id),
                interrupt_id=(decision.interrupt_ids[0] if decision.interrupt_ids else ""),
                decision_hash=str(getattr(epoch, "decision_hash", "")),
                reason="; ".join(getattr(decision, "reasons", ())) or "semantic interrupt",
            )
            status = str(getattr(result, "status", "")).lower()
            if status not in {"accepted", "delivered", "acknowledged"} and not bool(
                getattr(result, "accepted", False)
            ):
                raise ConfigurationError(
                    f"interrupt delivery refused for attempt {attempt.attempt_id!r}"
                )

    def _fail_closed(self, reason: str) -> None:
        self._state = SupervisorState.FAILED_CLOSED
        self._last_error = str(reason)
        self._stop_reason = "failed_closed"

    def _make_snapshot(self) -> SupervisorSnapshot:
        snapshot = SupervisorSnapshot(
            supervisor_id=self._supervisor_id,
            goal_id=self._goal_id,
            state=self._state,
            graph_id=self._graph_id,
            graph_version=self._graph_version,
            goal_state=self._goal_state,
            epochs_attempted=self._epochs_attempted,
            pending_event_count=len(self._pending),
            stop_reason=self._stop_reason,
            last_error=self._last_error,
        )
        self._last_snapshot = snapshot
        return snapshot

    def _result(
        self,
        *,
        status: SupervisorStepStatus,
        epoch_id: int,
        state_before: Any,
        state_after: Any,
        event_ids: tuple[str, ...] = (),
        accepted_event_ids: tuple[str, ...] = (),
        rejected_event_ids: tuple[str, ...] = (),
        watcher_polled: bool = False,
        workspace_route: Any = None,
        workspace_poll: Any = None,
        interrupt_epoch: Any = None,
        execution_result: Any = None,
        reason: str = "",
    ) -> SupervisorStepResult:
        graph_id = str(getattr(state_after, "graph_id", self._graph_id))
        graph_version = int(
            getattr(getattr(state_after, "progress", None), "graph_version", self._graph_version)
        )
        payload = {
            "supervisor_id": self._supervisor_id,
            "step_id": self._step_id,
            "epoch_id": epoch_id,
            "status": status,
            "supervisor_state": self._state,
            "goal_id": self._goal_id,
            "graph_id": graph_id,
            "graph_version": graph_version,
            "event_ids": event_ids,
            "accepted_event_ids": accepted_event_ids,
            "rejected_event_ids": rejected_event_ids,
            "watcher_polled": watcher_polled,
            "workspace_route": workspace_route,
            "workspace_poll": workspace_poll,
            "interrupt_epoch": interrupt_epoch,
            "execution_result": execution_result,
            "state_before_hash": _hash(state_before),
            "state_after_hash": _hash(state_after),
            "reason": reason,
        }
        return SupervisorStepResult(
            supervisor_id=self._supervisor_id,
            step_id=self._step_id,
            epoch_id=epoch_id,
            status=status,
            supervisor_state=self._state,
            goal_id=self._goal_id,
            graph_id=graph_id,
            graph_version=graph_version,
            event_ids=event_ids,
            accepted_event_ids=accepted_event_ids,
            rejected_event_ids=rejected_event_ids,
            watcher_polled=watcher_polled,
            workspace_route=workspace_route,
            workspace_poll=workspace_poll,
            interrupt_epoch=interrupt_epoch,
            execution_result=execution_result,
            state_before_hash=_hash(state_before),
            state_after_hash=_hash(state_after),
            reason=reason,
            result_hash=_hash(payload),
        )


def _dedupe_events(events: Iterable[SupervisorEvent]) -> tuple[SupervisorEvent, ...]:
    by_id: dict[str, SupervisorEvent] = {}
    for event in events:
        previous = by_id.get(event.event_id)
        if previous is not None and previous.fingerprint() != event.fingerprint():
            raise ConfigurationError(f"conflicting duplicate event_id={event.event_id!r}")
        by_id[event.event_id] = event
    return tuple(by_id[key] for key in sorted(by_id))


def _validate_non_negative_int(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer")
    if value < 0:
        raise ConfigurationError(f"{name} must be >= 0")


def _validate_positive_int(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} must be an integer")
    if value < 1:
        raise ConfigurationError(f"{name} must be >= 1")


# Naming aliases make the bounded facade discoverable from both the research
# terminology ("supervisor") and the execution terminology ("runner").
OnlineExecutionSupervisor = EventDrivenSupervisor
EventDrivenEpochRunner = EventDrivenSupervisor
BoundedEventSupervisor = EventDrivenSupervisor


__all__ = [
    "EVENT_SUPERVISOR_POLICY_ID",
    "EVENT_SUPERVISOR_SCHEMA_VERSION",
    "BoundedEventSupervisor",
    "EventDrivenEpochRunner",
    "EventDrivenSupervisor",
    "OnlineExecutionSupervisor",
    "SupervisorEvent",
    "SupervisorEventKind",
    "SupervisorRunResult",
    "SupervisorSnapshot",
    "SupervisorState",
    "SupervisorStepResult",
    "SupervisorStepStatus",
]
