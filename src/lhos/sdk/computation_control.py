"""Bounded online computation control for LongHorizonOS.

``EpochController`` already provides a deterministic, read-only
``observe -> reconcile -> plan`` pass.  This module adds the missing (but
deliberately small) control-plane seam around it:

``observe -> reconcile -> plan -> dispatch -> observe``

The controller emits immutable, auditable actions and may hand those actions
to an injected dispatcher.  The controller itself never invokes an Agent
executor, claims work, acquires a Lease, or writes semantic Evidence.  A
configured dispatcher can, however, enter a Harness adapter; in particular a
``START`` request may invoke a user-supplied Harness hook or legacy executor.
Scheduler/Kernel and the Harness remain the authorities for ownership and
execution.

This is intentionally a single-host, explicitly bounded loop.  There is no
background thread and no implicit retry loop.  Unknown state, graph/version
fences, malformed dispatcher responses, and dispatcher failures fail closed.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable, Iterable, Mapping
from enum import StrEnum
from typing import Any, Final, Literal, Protocol, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt

from .epoch_controller import (
    EpochController,
    EpochDecision,
    EpochDecisionStatus,
)
from .harness import (
    HarnessControlRequest,
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionAdapter,
)
from .runtime_state import CognitionAttemptState, GlobalRuntimeState, RuntimeStateView
from .semantic_interrupt import InterruptAction, SemanticInterrupt

COMPUTATION_CONTROL_SCHEMA_VERSION: Final[Literal["computation-control.v1"]] = (
    "computation-control.v1"
)
COMPUTATION_CONTROL_POLICY_ID: Final[str] = "bounded-online-computation-control.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ControlActionKind(StrEnum):
    """Actions a LongHorizonOS epoch may recommend."""

    START = "start"
    CONTINUE = "continue"
    DEFER = "defer"
    REBASE = "rebase"
    PREEMPT = "preempt"
    REVERIFY = "reverify"


class DispatchStatus(StrEnum):
    """Outcome of handing one action to an execution-unit dispatcher."""

    NOT_DISPATCHED = "not_dispatched"
    APPLIED = "applied"
    REJECTED = "rejected"
    UNSUPPORTED = "unsupported"
    FAILED = "failed"


class ControlLoopStatus(StrEnum):
    """Status of one bounded control-loop pass."""

    PLANNED = "planned"
    DISPATCHED = "dispatched"
    NOOP = "noop"
    GRAPH_CHANGED = "graph_changed"
    RECONCILE_FAILED = "reconcile_failed"
    DISPATCH_FAILED = "dispatch_failed"
    OBSERVATION_FAILED = "observation_failed"
    DISABLED = "disabled"


class ComputationAction(_FrozenModel):
    """One deterministic action proposal.

    This DTO never contains executable code.  Without a dispatcher ``START``
    remains only a proposal.  With a dispatcher it may become a Harness
    request, and the Harness adapter contract determines whether that request
    invokes user-supplied hook/executor code.

    ``agent_id``/``claim_id``/``attempt_id``/``semantic_epoch`` are optional
    at the planning boundary because a READY task need not have an execution
    owner yet.  The Harness dispatcher requires all four and rejects an
    unfenced action before entering Harness code.
    """

    schema_version: Literal["computation-control.v1"] = COMPUTATION_CONTROL_SCHEMA_VERSION
    action_id: str = Field(min_length=64, max_length=64)
    request_id: str = Field(min_length=64, max_length=64)
    epoch_id: StrictInt = Field(ge=0)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    target_kind: Literal["task", "attempt"]
    target_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    claim_id: str | None = Field(default=None, min_length=1)
    attempt_id: str | None = Field(default=None, min_length=1)
    action: ControlActionKind
    harness_operation: HarnessOperation | None = None
    semantic_epoch: StrictInt | None = Field(default=None, ge=0)
    reason: str = Field(min_length=1)
    source_decision_hash: str = Field(min_length=64, max_length=64)
    dispatchable: StrictBool = True

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ActionDispatchResult(_FrozenModel):
    """Normalized acknowledgement returned by an action dispatcher."""

    schema_version: Literal["computation-control.v1"] = COMPUTATION_CONTROL_SCHEMA_VERSION
    action_id: str = Field(min_length=64, max_length=64)
    request_id: str = Field(min_length=64, max_length=64)
    action: ControlActionKind
    status: DispatchStatus
    message: str = ""
    output: Any = None

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ComputationControlAudit(_FrozenModel):
    """Complete immutable transcript of one control-loop epoch."""

    schema_version: Literal["computation-control.v1"] = COMPUTATION_CONTROL_SCHEMA_VERSION
    controller_id: str = COMPUTATION_CONTROL_POLICY_ID
    epoch_id: StrictInt = Field(ge=0)
    status: ControlLoopStatus
    phases: tuple[str, ...] = ("observe", "reconcile", "plan", "dispatch", "observe")
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    observed_state_hash: str = Field(min_length=64, max_length=64)
    post_observed_state_hash: str | None = Field(default=None, min_length=64, max_length=64)
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    actions: tuple[ComputationAction, ...] = ()
    dispatches: tuple[ActionDispatchResult, ...] = ()
    event_ids: tuple[str, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)
    idempotency_key: str = Field(min_length=64, max_length=64)
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ActionDispatcher(Protocol):
    """A side-effect boundary supplied by the embedding runtime."""

    def __call__(
        self, action: ComputationAction
    ) -> (
        ActionDispatchResult
        | Mapping[str, Any]
        | Awaitable[ActionDispatchResult | Mapping[str, Any] | None]
        | None
    ): ...


StateProvider: TypeAlias = Callable[[], RuntimeStateView]
EventProvider: TypeAlias = Callable[
    [RuntimeStateView], Iterable[SemanticInterrupt | Mapping[str, Any]]
]
ReconcileCallback: TypeAlias = Callable[
    [RuntimeStateView, tuple[SemanticInterrupt, ...]], RuntimeStateView | None
]


class OnlineComputationController:
    """Explicit, bounded online computation control loop.

    The dispatcher is optional: without one the controller still produces a
    useful, auditable plan, but no action is executed.  ``step`` is
    synchronous; ``astep`` additionally accepts async dispatchers.
    """

    def __init__(
        self,
        state: RuntimeStateView | StateProvider | None = None,
        *,
        state_provider: StateProvider | None = None,
        observe: StateProvider | None = None,
        reconcile: ReconcileCallback | None = None,
        event_provider: EventProvider | None = None,
        dispatcher: ActionDispatcher | None = None,
        max_parallelism: int = 1,
        conflict_graph: Any | None = None,
        enabled: bool = True,
        initial_epoch: int = 0,
        dispatch_continuations: bool = False,
        dispatch_deferred: bool = False,
    ) -> None:
        provider = state_provider or observe
        initial: RuntimeStateView | None
        if callable(state):
            if provider is not None:
                raise ValueError("state cannot be callable with state_provider/observe")
            provider = state
            initial = None
        else:
            initial = state
        self._state_provider = provider
        self._state = _validate_state(initial) if initial is not None else None
        self._event_provider = event_provider
        self._dispatcher = dispatcher
        self._dispatch_continuations = bool(dispatch_continuations)
        self._dispatch_deferred = bool(dispatch_deferred)
        self._enabled = bool(enabled)
        self._controller = EpochController(
            state=self._state,
            state_provider=provider,
            reconcile=reconcile,
            frontier_policy=None,
            conflict_graph=conflict_graph,
            max_parallelism=max_parallelism,
            enabled=True,
            initial_epoch=initial_epoch,
            event_provider=None,
        )
        self._last_input_key: str | None = None
        self._last_audit: ComputationControlAudit | None = None
        self._dispatch_cache: dict[str, ActionDispatchResult] = {}
        # ``step`` performs the trailing OBSERVE phase.  Preserve that exact
        # projection for the next epoch instead of immediately calling a
        # state provider again and skipping one state transition.
        self._pending_observation: RuntimeStateView | None = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    @property
    def last_audit(self) -> ComputationControlAudit | None:
        return self._last_audit

    def step(
        self,
        state: RuntimeStateView | None = None,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        *,
        events: Iterable[SemanticInterrupt | Mapping[str, Any]] | None = None,
        dispatch: bool = True,
    ) -> ComputationControlAudit:
        """Run exactly one bounded synchronous epoch."""

        if not self._enabled:
            observed = self._safe_observe(state)
            return self._failure_audit(
                observed or self._state or _placeholder_state(),
                ControlLoopStatus.DISABLED,
                "controller is disabled",
            )
        proposals = self._resolve_events(state, event_proposals, events)
        observed = self._safe_observe(state)
        if observed is None:
            return self._failure_audit(
                self._state or _placeholder_state(),
                ControlLoopStatus.OBSERVATION_FAILED,
                "state observation failed",
            )
        key = _hash_payload({"state": observed, "events": proposals, "dispatch": dispatch})
        if key == self._last_input_key and self._last_audit is not None:
            return self._last_audit
        try:
            decision = self._controller.step(observed, proposals)
        except Exception as exc:
            audit = self._failure_audit(
                observed,
                ControlLoopStatus.RECONCILE_FAILED,
                _bounded_error(exc),
                event_ids=tuple(item.interrupt_id for item in proposals),
            )
            return self._remember(key, audit)
        actions = _derive_actions(
            decision, observed, dispatch_continuations=self._dispatch_continuations
        )
        dispatches = self._dispatch_sync(actions, dispatch=dispatch)
        post_state = self._observe_post_state()
        audit = self._make_audit(
            observed,
            decision,
            actions,
            dispatches,
            proposals,
            post_state=post_state,
        )
        if dispatches and any(item.status is DispatchStatus.FAILED for item in dispatches):
            audit = audit.model_copy(update={"status": ControlLoopStatus.DISPATCH_FAILED})
        elif dispatches and any(item.status is DispatchStatus.APPLIED for item in dispatches):
            audit = audit.model_copy(update={"status": ControlLoopStatus.DISPATCHED})
        elif audit.status is ControlLoopStatus.PLANNED and not actions:
            audit = audit.model_copy(update={"status": ControlLoopStatus.NOOP})
        return self._remember(key, audit)

    async def astep(
        self,
        state: RuntimeStateView | None = None,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        *,
        events: Iterable[SemanticInterrupt | Mapping[str, Any]] | None = None,
        dispatch: bool = True,
    ) -> ComputationControlAudit:
        """Async equivalent of :meth:`step` for async Harness dispatchers."""

        if not self._enabled:
            observed = self._safe_observe(state)
            return self._failure_audit(
                observed or self._state or _placeholder_state(),
                ControlLoopStatus.DISABLED,
                "controller is disabled",
            )
        proposals = self._resolve_events(state, event_proposals, events)
        observed = self._safe_observe(state)
        if observed is None:
            return self._failure_audit(
                self._state or _placeholder_state(),
                ControlLoopStatus.OBSERVATION_FAILED,
                "state observation failed",
            )
        key = _hash_payload({"state": observed, "events": proposals, "dispatch": dispatch})
        if key == self._last_input_key and self._last_audit is not None:
            return self._last_audit
        try:
            decision = self._controller.step(observed, proposals)
        except Exception as exc:
            return self._remember(
                key,
                self._failure_audit(
                    observed,
                    ControlLoopStatus.RECONCILE_FAILED,
                    _bounded_error(exc),
                    event_ids=tuple(item.interrupt_id for item in proposals),
                ),
            )
        actions = _derive_actions(
            decision, observed, dispatch_continuations=self._dispatch_continuations
        )
        dispatches = await self._dispatch_async(actions, dispatch=dispatch)
        post_state = self._observe_post_state()
        audit = self._make_audit(
            observed,
            decision,
            actions,
            dispatches,
            proposals,
            post_state=post_state,
        )
        if dispatches and any(item.status is DispatchStatus.FAILED for item in dispatches):
            audit = audit.model_copy(update={"status": ControlLoopStatus.DISPATCH_FAILED})
        elif dispatches and any(item.status is DispatchStatus.APPLIED for item in dispatches):
            audit = audit.model_copy(update={"status": ControlLoopStatus.DISPATCHED})
        elif audit.status is ControlLoopStatus.PLANNED and not actions:
            audit = audit.model_copy(update={"status": ControlLoopStatus.NOOP})
        return self._remember(key, audit)

    def run(
        self,
        max_epochs: int = 1,
        *,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        dispatch: bool = True,
    ) -> tuple[ComputationControlAudit, ...]:
        """Run at most ``max_epochs`` explicit synchronous epochs."""

        _validate_bound(max_epochs)
        if not self._enabled or max_epochs == 0:
            return ()
        pending = tuple(event_proposals)
        audits: list[ComputationControlAudit] = []
        for index in range(max_epochs):
            state = self._safe_observe(None)
            if state is None:
                break
            proposals = pending if index == 0 else self._events_for(state)
            audit = self.step(state, proposals, dispatch=dispatch)
            audits.append(audit)
            if audit.status in {
                ControlLoopStatus.OBSERVATION_FAILED,
                ControlLoopStatus.RECONCILE_FAILED,
                ControlLoopStatus.GRAPH_CHANGED,
                ControlLoopStatus.DISPATCH_FAILED,
                ControlLoopStatus.NOOP,
            }:
                break
            if index + 1 < max_epochs and self._state_provider is None:
                break
            pending = ()
        return tuple(audits)

    async def arun(
        self,
        max_epochs: int = 1,
        *,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        dispatch: bool = True,
    ) -> tuple[ComputationControlAudit, ...]:
        """Run at most ``max_epochs`` explicit asynchronous epochs."""

        _validate_bound(max_epochs)
        if not self._enabled or max_epochs == 0:
            return ()
        pending = tuple(event_proposals)
        audits: list[ComputationControlAudit] = []
        for index in range(max_epochs):
            state = self._safe_observe(None)
            if state is None:
                break
            proposals = pending if index == 0 else self._events_for(state)
            audit = await self.astep(state, proposals, dispatch=dispatch)
            audits.append(audit)
            if audit.status in {
                ControlLoopStatus.OBSERVATION_FAILED,
                ControlLoopStatus.RECONCILE_FAILED,
                ControlLoopStatus.GRAPH_CHANGED,
                ControlLoopStatus.DISPATCH_FAILED,
                ControlLoopStatus.NOOP,
            }:
                break
            if index + 1 < max_epochs and self._state_provider is None:
                break
            pending = ()
        return tuple(audits)

    def _resolve_events(
        self,
        state: RuntimeStateView | None,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]],
        events: Iterable[SemanticInterrupt | Mapping[str, Any]] | None,
    ) -> tuple[SemanticInterrupt, ...]:
        if events is not None:
            if tuple(event_proposals):
                raise ValueError("pass either event_proposals or events, not both")
            event_proposals = events
        values = tuple(event_proposals)
        if values or self._event_provider is None:
            return _normalize_events(values)
        observed = state or self._state
        if observed is None:
            return ()
        return _normalize_events(self._event_provider(observed))

    def _events_for(self, state: RuntimeStateView) -> tuple[SemanticInterrupt, ...]:
        if self._event_provider is None:
            return ()
        try:
            return _normalize_events(self._event_provider(state))
        except Exception:
            return ()

    def _safe_observe(self, explicit: RuntimeStateView | None) -> RuntimeStateView | None:
        try:
            if explicit is not None:
                value = _validate_state(explicit)
                self._pending_observation = None
            elif self._pending_observation is not None:
                value = self._pending_observation
                self._pending_observation = None
            elif self._state_provider is not None:
                value = _validate_state(self._state_provider())
            elif self._state is not None:
                value = self._state
            else:
                return None
            self._state = value
            return value
        except Exception:
            return None

    def _observe_post_state(self) -> RuntimeStateView | None:
        """Observe state after dispatch and retain it for the next epoch.

        A provider commonly represents a sequence of point-in-time snapshots.
        Without this cache, ``run`` would consume one snapshot in ``step``'s
        trailing OBSERVE phase and then consume another before planning the
        next epoch, silently skipping the state it had just audited.
        """

        if self._state_provider is None:
            return self._state
        try:
            value = _validate_state(self._state_provider())
        except Exception:
            return None
        self._pending_observation = value
        return value

    def _dispatch_sync(
        self,
        actions: tuple[ComputationAction, ...],
        *,
        dispatch: bool,
    ) -> tuple[ActionDispatchResult, ...]:
        if not dispatch or self._dispatcher is None:
            return tuple(_not_dispatched(action, "no dispatcher configured") for action in actions)
        results: list[ActionDispatchResult] = []
        for action in actions:
            if not action.dispatchable:
                results.append(_not_dispatched(action, "action is advisory-only"))
                continue
            cached = self._dispatch_cache.get(action.action_id)
            if cached is not None:
                results.append(cached)
                continue
            try:
                raw = self._dispatcher(action)
                if inspect.isawaitable(raw):
                    close = getattr(raw, "close", None)
                    if callable(close):
                        close()
                    result = _failed_dispatch(action, "async dispatcher requires astep()")
                else:
                    result = _normalize_dispatch_result(action, raw)
            except Exception as exc:
                result = _failed_dispatch(action, _bounded_error(exc))
            self._dispatch_cache[action.action_id] = result
            results.append(result)
        return tuple(results)

    async def _dispatch_async(
        self,
        actions: tuple[ComputationAction, ...],
        *,
        dispatch: bool,
    ) -> tuple[ActionDispatchResult, ...]:
        if not dispatch or self._dispatcher is None:
            return tuple(_not_dispatched(action, "no dispatcher configured") for action in actions)
        results: list[ActionDispatchResult] = []
        for action in actions:
            if not action.dispatchable:
                results.append(_not_dispatched(action, "action is advisory-only"))
                continue
            cached = self._dispatch_cache.get(action.action_id)
            if cached is not None:
                results.append(cached)
                continue
            try:
                raw = self._dispatcher(action)
                if inspect.isawaitable(raw):
                    raw = await raw
                result = _normalize_dispatch_result(action, raw)
            except Exception as exc:
                result = _failed_dispatch(action, _bounded_error(exc))
            self._dispatch_cache[action.action_id] = result
            results.append(result)
        return tuple(results)

    def _make_audit(
        self,
        state: RuntimeStateView,
        decision: EpochDecision,
        actions: tuple[ComputationAction, ...],
        dispatches: tuple[ActionDispatchResult, ...],
        proposals: tuple[SemanticInterrupt, ...],
        *,
        post_state: RuntimeStateView | None,
    ) -> ComputationControlAudit:
        if decision.status is EpochDecisionStatus.GRAPH_CHANGED:
            status = ControlLoopStatus.GRAPH_CHANGED
        elif decision.status is EpochDecisionStatus.RECONCILE_FAILED:
            status = ControlLoopStatus.RECONCILE_FAILED
        elif not actions:
            status = ControlLoopStatus.NOOP
        else:
            status = ControlLoopStatus.PLANNED
        payload = {
            "epoch_id": decision.epoch_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "actions": actions,
            "dispatches": dispatches,
            "events": proposals,
        }
        decision_hash = _hash_payload(payload)
        return ComputationControlAudit(
            epoch_id=decision.epoch_id,
            status=status,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            observed_state_hash=_hash_payload(state),
            post_observed_state_hash=(None if post_state is None else _hash_payload(post_state)),
            candidate_task_ids=decision.candidate_task_ids,
            selected_task_ids=decision.selected_task_ids,
            deferred_task_ids=decision.deferred_task_ids,
            parallelism_hint=decision.parallelism_hint,
            actions=actions,
            dispatches=dispatches,
            event_ids=tuple(item.interrupt_id for item in proposals),
            decision_hash=decision_hash,
            idempotency_key=_hash_payload(
                {"state": state, "events": proposals, "decision": decision.decision_hash}
            ),
            reason=decision.reason,
        )

    def _failure_audit(
        self,
        state: RuntimeStateView,
        status: ControlLoopStatus,
        reason: str,
        *,
        event_ids: tuple[str, ...] = (),
    ) -> ComputationControlAudit:
        state_hash = _hash_payload(state)
        key = _hash_payload({"state": state, "status": status.value, "reason": reason})
        return ComputationControlAudit(
            epoch_id=0,
            status=status,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            projection_hash=state.progress.projection_hash,
            observed_state_hash=state_hash,
            candidate_task_ids=(),
            selected_task_ids=(),
            deferred_task_ids=(),
            parallelism_hint=0,
            event_ids=event_ids,
            decision_hash=key,
            idempotency_key=key,
            reason=reason,
        )

    def _remember(self, key: str, audit: ComputationControlAudit) -> ComputationControlAudit:
        self._last_input_key = key
        self._last_audit = audit
        return audit


def _derive_actions(
    decision: EpochDecision,
    state: GlobalRuntimeState,
    *,
    dispatch_continuations: bool,
) -> tuple[ComputationAction, ...]:
    """Translate policy output into deterministic control actions."""

    attempts_by_id = {
        str(attempt.attempt_id): attempt
        for attempt in state.agent_cognition.current_attempts
        if attempt.attempt_id
    }
    attempts_by_task: dict[str, CognitionAttemptState] = {}
    for attempt in state.agent_cognition.current_attempts:
        previous = attempts_by_task.get(attempt.task_id)
        if previous is None or str(attempt.attempt_id or "") > str(previous.attempt_id or ""):
            attempts_by_task[attempt.task_id] = attempt

    rows: dict[tuple[str, str], ComputationAction] = {}

    def add(
        *,
        kind: ControlActionKind,
        target_kind: Literal["task", "attempt"],
        target_id: str,
        task_id: str,
        agent_id: str | None,
        claim_id: str | None,
        attempt_id: str | None,
        reason: str,
        semantic_epoch: int | None = None,
        dispatchable: bool = True,
    ) -> None:
        harness_op = {
            ControlActionKind.START: HarnessOperation.START,
            ControlActionKind.CONTINUE: HarnessOperation.CONTINUE,
            ControlActionKind.REBASE: HarnessOperation.REBASE,
            ControlActionKind.PREEMPT: HarnessOperation.PREEMPT,
        }.get(kind)
        if kind is ControlActionKind.DEFER or kind is ControlActionKind.REVERIFY:
            harness_op = None
        raw = {
            "epoch_id": decision.epoch_id,
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "target_kind": target_kind,
            "target_id": target_id,
            "task_id": task_id,
            "agent_id": agent_id,
            "claim_id": claim_id,
            "attempt_id": attempt_id,
            "semantic_epoch": semantic_epoch,
            "action": kind.value,
            "reason": reason,
            "source_decision_hash": decision.decision_hash,
        }
        action_id = _hash_payload(raw)
        action = ComputationAction(
            action_id=action_id,
            request_id=_hash_payload({"action_id": action_id, "request": "v1"}),
            epoch_id=decision.epoch_id,
            graph_id=state.graph_id,
            graph_version=state.progress.graph_version,
            target_kind=target_kind,
            target_id=target_id,
            task_id=task_id,
            agent_id=agent_id,
            claim_id=claim_id,
            attempt_id=attempt_id,
            action=kind,
            harness_operation=harness_op,
            semantic_epoch=semantic_epoch,
            reason=reason,
            source_decision_hash=decision.decision_hash,
            dispatchable=dispatchable,
        )
        key = (target_kind, target_id)
        previous = rows.get(key)
        if previous is None or _action_priority(kind) > _action_priority(previous.action):
            rows[key] = action

    # Interrupt actions have precedence over ordinary frontier actions.
    blocked_tasks: set[str] = set()
    if decision.interrupts is not None:
        for item in decision.interrupts.decisions:
            target_attempt = (
                attempts_by_id.get(item.target_id) if item.target_kind == "attempt" else None
            )
            task_id = target_attempt.task_id if target_attempt is not None else item.target_id
            if item.action in {InterruptAction.REBASE, InterruptAction.PREEMPT}:
                blocked_tasks.add(task_id)
            kind = ControlActionKind(item.action.value)
            add(
                kind=kind,
                target_kind=item.target_kind,
                target_id=item.target_id,
                task_id=task_id,
                agent_id=(target_attempt.agent_id if target_attempt is not None else None),
                claim_id=(target_attempt.claim_id if target_attempt is not None else None),
                attempt_id=(item.target_id if item.target_kind == "attempt" else None),
                reason="; ".join(item.reasons) or "semantic interrupt",
                semantic_epoch=(
                    target_attempt.semantic_epoch if target_attempt is not None else None
                ),
                dispatchable=kind not in {ControlActionKind.DEFER, ControlActionKind.REVERIFY},
            )

    for task_id in decision.selected_task_ids:
        if task_id in blocked_tasks:
            continue
        active_attempt = attempts_by_task.get(task_id)
        if active_attempt is None:
            add(
                kind=ControlActionKind.START,
                target_kind="task",
                target_id=task_id,
                task_id=task_id,
                agent_id=None,
                claim_id=None,
                attempt_id=None,
                reason="selected by online frontier policy",
            )
        else:
            add(
                kind=ControlActionKind.CONTINUE,
                target_kind="attempt",
                target_id=str(active_attempt.attempt_id or active_attempt.claim_id),
                task_id=task_id,
                agent_id=active_attempt.agent_id,
                claim_id=active_attempt.claim_id,
                attempt_id=active_attempt.attempt_id,
                reason="selected active computation",
                semantic_epoch=active_attempt.semantic_epoch,
                dispatchable=dispatch_continuations,
            )
    for task_id in decision.deferred_task_ids:
        if task_id not in blocked_tasks:
            add(
                kind=ControlActionKind.DEFER,
                target_kind="task",
                target_id=task_id,
                task_id=task_id,
                agent_id=None,
                claim_id=None,
                attempt_id=None,
                reason="frontier policy deferred task",
                dispatchable=False,
            )
    return tuple(
        sorted(
            rows.values(), key=lambda item: (item.target_kind, item.target_id, item.action.value)
        )
    )


def _action_priority(action: ControlActionKind) -> int:
    return {
        ControlActionKind.DEFER: 1,
        ControlActionKind.REVERIFY: 2,
        ControlActionKind.CONTINUE: 3,
        ControlActionKind.START: 3,
        ControlActionKind.REBASE: 4,
        ControlActionKind.PREEMPT: 5,
    }[action]


def _normalize_dispatch_result(action: ComputationAction, raw: Any) -> ActionDispatchResult:
    if isinstance(raw, ActionDispatchResult):
        if raw.action_id != action.action_id or raw.request_id != action.request_id:
            raise ValueError("dispatcher result identity does not match action")
        return raw
    if raw is None:
        return ActionDispatchResult(
            action_id=action.action_id,
            request_id=action.request_id,
            action=action.action,
            status=DispatchStatus.APPLIED,
            message="dispatcher acknowledged action",
        )
    if isinstance(raw, Mapping):
        data = dict(raw)
        data.setdefault("action_id", action.action_id)
        data.setdefault("request_id", action.request_id)
        data.setdefault("action", action.action)
        return ActionDispatchResult.model_validate(data)
    if isinstance(raw, bool):
        return ActionDispatchResult(
            action_id=action.action_id,
            request_id=action.request_id,
            action=action.action,
            status=DispatchStatus.APPLIED if raw else DispatchStatus.REJECTED,
            message="dispatcher boolean acknowledgement",
        )
    raise TypeError(f"unsupported dispatcher result type: {type(raw).__name__}")


def _not_dispatched(action: ComputationAction, reason: str) -> ActionDispatchResult:
    return ActionDispatchResult(
        action_id=action.action_id,
        request_id=action.request_id,
        action=action.action,
        status=DispatchStatus.NOT_DISPATCHED,
        message=reason,
    )


def _failed_dispatch(action: ComputationAction, reason: str) -> ActionDispatchResult:
    return ActionDispatchResult(
        action_id=action.action_id,
        request_id=action.request_id,
        action=action.action,
        status=DispatchStatus.FAILED,
        message=reason,
    )


def make_harness_dispatcher(
    sessions: Mapping[str, HarnessSessionAdapter],
) -> ActionDispatcher:
    """Adapt Harness session primitives to an :class:`ActionDispatcher`.

    ``sessions`` is keyed by attempt id first, then task id.  The adapter's
    own exact identity/revision checks remain authoritative.  This adapter
    additionally fences the *action* before constructing a
    :class:`HarnessControlRequest`: graph, task, agent, claim, attempt and
    semantic-epoch identities must all match the registered session.  A
    ``REBASE`` action may advance graph version, but must carry the current
    semantic epoch and a strictly forward target (the target epoch is
    ``semantic_epoch + 1``).  Missing or mismatched fences reject closed
    rather than fabricating ownership.

    The controller itself never invokes user code.  Once this dispatcher is
    configured, however, a ``START`` request is intentionally allowed to enter
    the Harness adapter; a stateful adapter's START hook (or a legacy
    one-shot executor) may execute user code according to that adapter's
    contract.
    """

    async def dispatch(action: ComputationAction) -> ActionDispatchResult:
        operation = action.harness_operation
        if operation is None:
            return _not_dispatched(action, "action has no Harness operation")
        preflight_error = _action_preflight_fence_error(action, operation)
        if preflight_error is not None:
            return ActionDispatchResult(
                action_id=action.action_id,
                request_id=action.request_id,
                action=action.action,
                status=DispatchStatus.REJECTED,
                message=preflight_error,
            )
        key = action.attempt_id or action.task_id
        adapter = sessions.get(key)
        # If an action carries a stale attempt id, still locate a uniquely
        # registered session by task so the identity fence can report the
        # precise mismatch instead of a generic "missing session".
        if adapter is None:
            matches = _unique_task_sessions(sessions, action.task_id)
            if len(matches) == 1:
                adapter = matches[0]
        if adapter is None:
            suffix = (
                " (multiple Harness sessions match the task; attempt identity is ambiguous)"
                if len(_unique_task_sessions(sessions, action.task_id)) > 1
                else ""
            )
            return ActionDispatchResult(
                action_id=action.action_id,
                request_id=action.request_id,
                action=action.action,
                status=DispatchStatus.REJECTED,
                message=f"no unique Harness session registered for {key!r}{suffix}",
            )
        snapshot = adapter.snapshot
        identity = snapshot.identity
        fence_error = _action_fence_error(action, identity, operation)
        if fence_error is not None:
            return ActionDispatchResult(
                action_id=action.action_id,
                request_id=action.request_id,
                action=action.action,
                status=DispatchStatus.REJECTED,
                message=fence_error,
            )
        request = HarnessControlRequest(
            request_id=action.request_id,
            operation=operation,
            session=identity,
            expected_revision=snapshot.revision,
            expected_checkpoint_id=(
                snapshot.checkpoint_id
                if operation in {HarnessOperation.CONTINUE, HarnessOperation.REBASE}
                and snapshot.state.value == "checkpointed"
                else None
            ),
            target_graph_version=(
                action.graph_version if operation is HarnessOperation.REBASE else None
            ),
            target_semantic_epoch=(
                (action.semantic_epoch or 0) + 1 if operation is HarnessOperation.REBASE else None
            ),
            reason=action.reason,
        )
        result = await adapter.control(request)
        status_map = {
            HarnessResultStatus.APPLIED: DispatchStatus.APPLIED,
            HarnessResultStatus.REJECTED: DispatchStatus.REJECTED,
            HarnessResultStatus.UNSUPPORTED: DispatchStatus.UNSUPPORTED,
            HarnessResultStatus.FAILED: DispatchStatus.FAILED,
        }
        return ActionDispatchResult(
            action_id=action.action_id,
            request_id=action.request_id,
            action=action.action,
            status=status_map[result.status],
            message=result.message,
            output=result.output,
        )

    return dispatch


def _unique_task_sessions(
    sessions: Mapping[str, HarnessSessionAdapter],
    task_id: str,
) -> tuple[HarnessSessionAdapter, ...]:
    """Return distinct sessions for ``task_id`` despite alias mapping keys."""

    by_adapter: dict[int, HarnessSessionAdapter] = {}
    for candidate in sessions.values():
        identity = getattr(getattr(candidate, "snapshot", None), "identity", None)
        if identity is None or identity.task_id != task_id:
            continue
        # The exact same adapter may be registered under both task and attempt
        # aliases.  Distinct adapter objects remain distinct even if a buggy
        # caller reused a session_id; never collapse potentially competing
        # owners based only on caller-provided identity text.
        by_adapter.setdefault(id(candidate), candidate)
    return tuple(by_adapter[key] for key in sorted(by_adapter))


def _action_preflight_fence_error(
    action: ComputationAction,
    operation: HarnessOperation,
) -> str | None:
    """Validate self-contained action fences before looking up a session."""

    expected_operation = {
        ControlActionKind.START: HarnessOperation.START,
        ControlActionKind.CONTINUE: HarnessOperation.CONTINUE,
        ControlActionKind.REBASE: HarnessOperation.REBASE,
        ControlActionKind.PREEMPT: HarnessOperation.PREEMPT,
    }.get(action.action)
    if expected_operation is None or operation is not expected_operation:
        return "action Harness operation does not match action kind"
    for name, value in (
        ("agent_id", action.agent_id),
        ("claim_id", action.claim_id),
        ("attempt_id", action.attempt_id),
        ("semantic_epoch", action.semantic_epoch),
    ):
        if value is None or (isinstance(value, str) and not value.strip()):
            return f"action is missing {name} fence"
    return None


def _action_fence_error(
    action: ComputationAction,
    identity: Any,
    operation: HarnessOperation,
) -> str | None:
    """Return a fail-closed reason when an action is not owner-fenced.

    Keeping this check at the dispatcher boundary is important: a malformed
    action must not reach an adapter hook, even if the adapter itself would
    reject the eventual request.  ``identity`` is intentionally duck-typed to
    keep this helper usable with protocol-compatible snapshots.
    """

    checks = (
        ("graph_id", action.graph_id, identity.graph_id),
        ("task_id", action.task_id, identity.task_id),
        ("agent_id", action.agent_id, identity.agent_id),
        ("claim_id", action.claim_id, identity.claim_id),
        ("attempt_id", action.attempt_id, identity.attempt_id),
    )
    for name, proposed, current in checks:
        if str(proposed) != str(current):
            return f"Harness {name} does not match action fence"

    # A non-rebase action must be based on exactly the current graph and
    # semantic epoch.  REBASE carries the *target* graph version but the
    # source semantic epoch, which the adapter advances by one below.
    if operation is not HarnessOperation.REBASE:
        if action.graph_version != identity.graph_version:
            return (
                "Harness graph_version does not match action fence "
                f"(current={identity.graph_version}, action={action.graph_version})"
            )
        if action.semantic_epoch is None:
            return "action is missing semantic_epoch fence"
        if action.semantic_epoch != identity.semantic_epoch:
            return (
                "Harness semantic_epoch does not match action fence "
                f"(current={identity.semantic_epoch}, action={action.semantic_epoch})"
            )
    else:
        if action.semantic_epoch is None:
            return "REBASE action is missing semantic_epoch fence"
        if action.semantic_epoch != identity.semantic_epoch:
            return (
                "REBASE semantic_epoch does not match current Harness cognition "
                f"(current={identity.semantic_epoch}, action={action.semantic_epoch})"
            )
        if action.graph_version < identity.graph_version:
            return (
                "REBASE graph_version cannot move backwards "
                f"(current={identity.graph_version}, action={action.graph_version})"
            )
        if (
            action.graph_version == identity.graph_version
            and action.semantic_epoch + 1 <= identity.semantic_epoch
        ):
            return "REBASE action does not advance graph version or semantic epoch"
    return None


def _normalize_events(
    values: Iterable[SemanticInterrupt | Mapping[str, Any]],
) -> tuple[SemanticInterrupt, ...]:
    by_id: dict[str, SemanticInterrupt] = {}
    for value in values:
        event = (
            value
            if isinstance(value, SemanticInterrupt)
            else SemanticInterrupt.model_validate(value)
        )
        previous = by_id.get(event.interrupt_id)
        if previous is not None and previous != event:
            raise ValueError(f"conflicting event proposals for {event.interrupt_id!r}")
        by_id[event.interrupt_id] = event
    return tuple(by_id[key] for key in sorted(by_id))


def _validate_state(value: Any) -> GlobalRuntimeState:
    if isinstance(value, GlobalRuntimeState):
        return value
    return GlobalRuntimeState.model_validate(value)


def _placeholder_state() -> GlobalRuntimeState:
    from .runtime_state import (
        AgentCognitionState,
        ContextRuntimeState,
        ProgressSemanticState,
        ResourceRuntimeState,
    )

    return GlobalRuntimeState(
        goal_id="unavailable",
        graph_id="unavailable",
        progress=ProgressSemanticState(
            graph_id="unavailable",
            graph_version=0,
            projection_hash="0" * 64,
            graph_closed=False,
            goal_closed=False,
            ready_frontier=(),
            repair_ready_frontier=(),
            verified_task_ids=(),
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=(),
        ),
        agent_cognition=AgentCognitionState(available=False, reason="unavailable"),
        context=ContextRuntimeState(available=False, reason="unavailable"),
        resources=ResourceRuntimeState(available=False, reason="unavailable"),
    )


def _hash_payload(value: Any) -> str:
    canonical = json.dumps(
        _canonical(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonical(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, tuple):
        return [_canonical(item) for item in value]
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical(item) for key, item in value.items()}
    return value


def _bounded_error(exc: BaseException, limit: int = 240) -> str:
    text = f"{type(exc).__name__}: {exc}".replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else type(exc).__name__


def _validate_bound(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("max_epochs must be a non-negative integer")


__all__ = [
    "COMPUTATION_CONTROL_POLICY_ID",
    "COMPUTATION_CONTROL_SCHEMA_VERSION",
    "ActionDispatchResult",
    "ActionDispatcher",
    "ComputationAction",
    "ComputationControlAudit",
    "ControlActionKind",
    "ControlLoopStatus",
    "DispatchStatus",
    "OnlineComputationController",
    "make_harness_dispatcher",
]
