"""A small, deterministic, event-driven scheduling-epoch controller.

The controller is deliberately a *control-plane* primitive.  It observes an
immutable :class:`~lhos.sdk.runtime_state.RuntimeStateView`, optionally lets
an injected reconciler return a newer projection, and then plans one
read-only epoch.  It never claims work, acquires/releases a lease, starts a
Harness, or executes user code.

The public loop is intentionally explicit::

    observe -> reconcile -> plan -> return EpochDecision

``EpochController`` is disabled for automatic/background use by default.
Callers can still invoke :meth:`step` explicitly; :meth:`run` is a bounded
convenience loop and is a no-op while the controller is disabled.  This keeps
the existing AgentOS execution path unchanged while providing a concrete
online-policy seam for later integration.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable, Iterable, Mapping
from enum import StrEnum
from typing import Any, Final, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, field_validator

from .conflict_graph import (
    ConflictGraph,
    DynamicParallelismPolicy,
    ParallelBatchSuggestion,
)
from .errors import ConfigurationError
from .frontier_policy import FrontierPolicy, SchedulingEpoch
from .runtime_state import GlobalRuntimeState, RuntimeStateView, UnavailableField
from .semantic_interrupt import (
    InterruptAction,
    InterruptEpoch,
    SemanticInterrupt,
    SemanticInterruptPolicy,
)

EPOCH_CONTROLLER_SCHEMA_VERSION: Final[Literal["epoch-controller.v1"]] = "epoch-controller.v1"
EPOCH_CONTROLLER_POLICY_ID: Final[str] = "deterministic-event-epoch-controller.v1"
DEFAULT_MAX_EPOCHS: Final[int] = 1_000


class EpochPhase(StrEnum):
    """Observable phases of one scheduling epoch."""

    OBSERVE = "observe"
    RECONCILE = "reconcile"
    PLAN = "plan"


class EpochDecisionStatus(StrEnum):
    """Terminal status of an :class:`EpochDecision`."""

    PLANNED = "planned"
    EMPTY_FRONTIER = "empty_frontier"
    GRAPH_CHANGED = "graph_changed"
    RECONCILE_FAILED = "reconcile_failed"
    DISABLED = "disabled"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EpochDecision(_FrozenModel):
    """Immutable output of one observe/reconcile/plan pass.

    The nested ``frontier`` and ``interrupts`` values are the exact immutable
    outputs of the existing policies.  ``selected_task_ids`` is repeated at
    the top level to make a controller transcript easy to consume without
    knowing which batching policy was selected.
    """

    schema_version: Literal["epoch-controller.v1"] = EPOCH_CONTROLLER_SCHEMA_VERSION
    controller_id: str = EPOCH_CONTROLLER_POLICY_ID
    epoch_id: StrictInt = Field(ge=0)
    status: EpochDecisionStatus
    phases: tuple[EpochPhase, ...] = (
        EpochPhase.OBSERVE,
        EpochPhase.RECONCILE,
        EpochPhase.PLAN,
    )
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    projection_hash: str = Field(min_length=1)
    event_ids: tuple[str, ...] = ()
    candidate_task_ids: tuple[str, ...] = ()
    selected_task_ids: tuple[str, ...] = ()
    deferred_task_ids: tuple[str, ...] = ()
    parallelism_hint: StrictInt = Field(ge=0)
    frontier: SchedulingEpoch | ParallelBatchSuggestion | None = None
    interrupts: InterruptEpoch | None = None
    interrupt_blocked_task_ids: tuple[str, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()
    reason: str | None = None
    reconciled: StrictBool = False
    decision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("epoch_id", "graph_version", "parallelism_hint")
    @classmethod
    def _real_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("epoch identity/version/parallelism must be an integer")
        return value

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


# A proposal is intentionally an alias for the existing immutable semantic
# interrupt DTO.  ``step`` also accepts mappings and model-like values and
# normalizes them at the boundary.
EventProposal: TypeAlias = SemanticInterrupt
EpochEventProposal: TypeAlias = SemanticInterrupt


StateProvider: TypeAlias = Callable[[], RuntimeStateView]
ReconcileCallback: TypeAlias = Callable[
    [RuntimeStateView, tuple[SemanticInterrupt, ...]], RuntimeStateView | None
]
EventProvider: TypeAlias = Callable[
    [RuntimeStateView], Iterable[SemanticInterrupt | Mapping[str, Any]]
]


class EpochController:
    """Explicit, deterministic online scheduling controller.

    Parameters
    ----------
    state:
        An initial immutable runtime projection.  A callable is treated as a
        state provider for subsequent :meth:`run` epochs.
    state_provider:
        Optional explicit state provider.  It takes precedence over a
        callable passed as ``state``.
    reconcile:
        Optional callback invoked *only* during the RECONCILE phase.  It may
        return the same/new ``RuntimeStateView`` or ``None`` to keep the
        observed projection.  The callback is never allowed to mutate the
        controller or scheduler.
    conflict_graph:
        If supplied, use the existing explicit-access
        ``DynamicParallelismPolicy``; otherwise use ``FrontierPolicy``.
    enabled:
        Automatic ``run`` is off by default.  Explicit ``step`` remains
        available so embedding applications can opt in one epoch at a time.
    """

    def __init__(
        self,
        state: RuntimeStateView | StateProvider | None = None,
        *,
        state_provider: StateProvider | None = None,
        observe: StateProvider | None = None,
        reconcile: ReconcileCallback | None = None,
        event_provider: EventProvider | None = None,
        frontier_policy: FrontierPolicy | None = None,
        interrupt_policy: SemanticInterruptPolicy | None = None,
        conflict_graph: ConflictGraph | None = None,
        max_parallelism: int = 1,
        enabled: bool = False,
        initial_epoch: int = 0,
    ) -> None:
        if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
            raise ConfigurationError("max_parallelism must be an integer")
        if max_parallelism < 1:
            raise ConfigurationError("max_parallelism must be at least 1")
        if isinstance(initial_epoch, bool) or not isinstance(initial_epoch, int):
            raise ConfigurationError("initial_epoch must be an integer")
        if initial_epoch < 0:
            raise ConfigurationError("initial_epoch must be non-negative")

        provider = state_provider or observe
        initial_state: RuntimeStateView | None
        if callable(state):
            if provider is not None:
                raise ConfigurationError(
                    "state cannot be callable when state_provider/observe is supplied"
                )
            provider = state
            initial_state = None
        else:
            initial_state = state

        self._state_provider = provider
        self._state = _validate_state(initial_state) if initial_state is not None else None
        self._reconcile = reconcile
        self._event_provider = event_provider
        self._frontier_policy = frontier_policy or FrontierPolicy(max_parallelism=max_parallelism)
        self._interrupt_policy = interrupt_policy or SemanticInterruptPolicy()
        self._conflict_graph = conflict_graph
        self._enabled = bool(enabled)
        self._next_epoch = initial_epoch
        self._last_key: str | None = None
        self._last_decision: EpochDecision | None = None

    @property
    def enabled(self) -> bool:
        """Whether :meth:`run` is enabled (automatic/background use)."""

        return self._enabled

    def enable(self) -> None:
        self._enabled = True

    def disable(self) -> None:
        self._enabled = False

    @property
    def last_decision(self) -> EpochDecision | None:
        return self._last_decision

    def step(
        self,
        state: RuntimeStateView | None = None,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        *,
        events: Iterable[SemanticInterrupt | Mapping[str, Any]] | None = None,
    ) -> EpochDecision:
        """Run exactly one explicit ``OBSERVE -> RECONCILE -> PLAN`` pass.

        ``events`` is a compatibility keyword alias for ``event_proposals``.
        Repeating the same state/proposal fingerprint is idempotent and
        returns the same immutable decision (including ``decision_hash``).
        """

        if events is not None:
            if tuple(event_proposals):
                raise ConfigurationError("pass either event_proposals or events, not both")
            event_proposals = events

        observed = self._observe(state)
        proposals = _normalize_proposals(event_proposals)
        key = _input_key(observed, proposals)
        if key == self._last_key and self._last_decision is not None:
            return self._last_decision

        epoch_id = self._next_epoch
        self._next_epoch += 1
        event_ids = tuple(item.interrupt_id for item in proposals)

        # Validate event identity before invoking user reconciliation.  An
        # old/future proposal cannot silently be interpreted against a new
        # graph snapshot.
        mismatch = _proposal_version_mismatch(observed, proposals)
        if mismatch is not None:
            decision = _fail_closed_decision(
                epoch_id=epoch_id,
                state=observed,
                event_ids=event_ids,
                status=EpochDecisionStatus.GRAPH_CHANGED,
                reason=mismatch,
                reconciled=False,
            )
            return self._remember(key, decision)

        reconciled = observed
        reconciled_flag = False
        if self._reconcile is not None:
            try:
                reconciled_flag = True
                candidate = _call_reconciler(self._reconcile, observed, proposals)
                if candidate is not None:
                    reconciled = _validate_state(candidate)
            except Exception as exc:
                decision = _fail_closed_decision(
                    epoch_id=epoch_id,
                    state=observed,
                    event_ids=event_ids,
                    status=EpochDecisionStatus.RECONCILE_FAILED,
                    reason=_bounded_error(exc),
                    reconciled=False,
                )
                return self._remember(key, decision)

        # A reconciliation callback is allowed to derive a new projection,
        # but a single epoch must never plan across a graph-version change.
        if (
            reconciled.graph_id != observed.graph_id
            or reconciled.progress.graph_version != observed.progress.graph_version
        ):
            decision = _fail_closed_decision(
                epoch_id=epoch_id,
                state=observed,
                event_ids=event_ids,
                status=EpochDecisionStatus.GRAPH_CHANGED,
                reason=(
                    "graph version changed during reconcile: "
                    f"observed={observed.progress.graph_version}, "
                    f"reconciled={reconciled.progress.graph_version}"
                ),
                reconciled=reconciled_flag,
            )
            return self._remember(key, decision)

        # The callback may return a semantically equivalent projection with a
        # changed projection hash.  It is still safe to plan because the
        # immutable graph version is unchanged; the new hash is recorded.
        interrupt_epoch = (
            self._interrupt_policy.plan(reconciled, proposals, epoch_id=epoch_id)
            if proposals
            else None
        )
        frontier: SchedulingEpoch | ParallelBatchSuggestion | None = self._plan_frontier(
            reconciled, epoch_id
        )
        blocked = _blocked_task_ids(reconciled, interrupt_epoch)
        frontier = _suppress_blocked(frontier, blocked)
        unavailable = _dedupe_unavailable(
            (tuple(getattr(frontier, "unavailable", ())) if frontier is not None else ())
            + (tuple(interrupt_epoch.unavailable) if interrupt_epoch is not None else ())
        )

        selected = tuple(getattr(frontier, "selected_task_ids", ())) if frontier else ()
        candidates = tuple(getattr(frontier, "candidate_task_ids", ())) if frontier else ()
        deferred = tuple(getattr(frontier, "deferred_task_ids", ())) if frontier else ()
        hint = int(getattr(frontier, "parallelism_hint", 0)) if frontier else 0
        status = (
            EpochDecisionStatus.EMPTY_FRONTIER if not candidates else EpochDecisionStatus.PLANNED
        )
        decision = _make_decision(
            epoch_id=epoch_id,
            status=status,
            state=reconciled,
            event_ids=event_ids,
            frontier=frontier,
            interrupts=interrupt_epoch,
            blocked=blocked,
            candidates=candidates,
            selected=selected,
            deferred=deferred,
            parallelism_hint=hint,
            unavailable=unavailable,
            reconciled=reconciled_flag,
        )
        return self._remember(key, decision)

    def run(
        self,
        max_epochs: int = 1,
        *,
        event_proposals: Iterable[SemanticInterrupt | Mapping[str, Any]] = (),
        events: Iterable[SemanticInterrupt | Mapping[str, Any]] | None = None,
    ) -> tuple[EpochDecision, ...]:
        """Run at most ``max_epochs`` explicit planning epochs.

        Automatic/background execution is opt-in: when ``enabled`` is false,
        this method returns an empty tuple without observing or reconciling.
        No claims, leases, workers, or user executors are touched.
        """

        if isinstance(max_epochs, bool) or not isinstance(max_epochs, int):
            raise ConfigurationError("max_epochs must be an integer")
        if max_epochs < 0:
            raise ConfigurationError("max_epochs must be non-negative")
        if not self._enabled or max_epochs == 0:
            return ()
        if events is not None:
            existing = tuple(event_proposals)
            if existing:
                raise ConfigurationError("pass either event_proposals or events, not both")
            event_proposals = events

        pending = tuple(event_proposals)
        decisions: list[EpochDecision] = []
        for _ in range(max_epochs):
            state = self._observe(None)
            current_events = (
                tuple(self._event_provider(state)) if self._event_provider is not None else pending
            )
            decision = self.step(state, current_events)
            if decisions and decision.decision_hash == decisions[-1].decision_hash:
                break
            decisions.append(decision)
            # A fail-closed result must stop the loop.  An empty frontier is a
            # stable terminal observation as well; returning it is more useful
            # than repeatedly emitting identical no-op epochs.
            if decision.status in {
                EpochDecisionStatus.GRAPH_CHANGED,
                EpochDecisionStatus.RECONCILE_FAILED,
                EpochDecisionStatus.EMPTY_FRONTIER,
            }:
                break
            pending = ()
        return tuple(decisions)

    def _observe(self, explicit: RuntimeStateView | None) -> RuntimeStateView:
        if explicit is not None:
            value = _validate_state(explicit)
        elif self._state_provider is not None:
            value = _validate_state(self._state_provider())
        elif self._state is not None:
            value = self._state
        else:
            raise ConfigurationError(
                "EpochController requires a RuntimeStateView or state provider"
            )
        self._state = value
        return value

    def _plan_frontier(
        self, state: RuntimeStateView, epoch_id: int
    ) -> SchedulingEpoch | ParallelBatchSuggestion:
        if self._conflict_graph is not None:
            return DynamicParallelismPolicy(
                max_parallelism=self._frontier_policy.max_parallelism
            ).suggest(state, self._conflict_graph, epoch_id=epoch_id)
        return self._frontier_policy.plan(state, epoch_id=epoch_id)

    def _remember(self, key: str, decision: EpochDecision) -> EpochDecision:
        self._last_key = key
        self._last_decision = decision
        return decision


# Names used in design notes and by early adopters.  Keep aliases so the
# controller can evolve without forcing a naming migration.
SchedulingEpochController = EpochController
EventDrivenEpochController = EpochController


def _validate_state(value: Any) -> RuntimeStateView:
    if isinstance(value, GlobalRuntimeState):
        return value
    try:
        return GlobalRuntimeState.model_validate(value)
    except Exception as exc:
        raise ConfigurationError(
            f"epoch controller requires RuntimeStateView: {_bounded_error(exc)}"
        ) from exc


def _normalize_proposals(
    values: Iterable[SemanticInterrupt | Mapping[str, Any]] | None,
) -> tuple[SemanticInterrupt, ...]:
    if values is None:
        return ()
    normalized: list[SemanticInterrupt] = []
    for value in values:
        if isinstance(value, SemanticInterrupt):
            normalized.append(value)
        elif isinstance(value, Mapping):
            normalized.append(SemanticInterrupt.model_validate(value))
        elif isinstance(value, BaseModel):
            normalized.append(SemanticInterrupt.model_validate(value.model_dump()))
        else:
            raise ConfigurationError(f"unsupported event proposal type: {type(value).__name__}")
    by_id: dict[str, SemanticInterrupt] = {}
    for item in normalized:
        previous = by_id.get(item.interrupt_id)
        if previous is not None and _canonical(previous) != _canonical(item):
            raise ConfigurationError(
                f"conflicting event proposals for interrupt_id={item.interrupt_id!r}"
            )
        by_id[item.interrupt_id] = item
    return tuple(by_id[key] for key in sorted(by_id))


def _proposal_version_mismatch(
    state: RuntimeStateView, proposals: tuple[SemanticInterrupt, ...]
) -> str | None:
    for proposal in proposals:
        if proposal.graph_id != state.graph_id:
            return (
                "event proposal graph mismatch: "
                f"expected={state.graph_id}, observed={proposal.graph_id}"
            )
        if proposal.graph_version != state.progress.graph_version:
            return (
                "event proposal graph version mismatch: "
                f"expected={state.progress.graph_version}, "
                f"observed={proposal.graph_version}"
            )
    return None


def _call_reconciler(
    callback: ReconcileCallback,
    state: RuntimeStateView,
    proposals: tuple[SemanticInterrupt, ...],
) -> RuntimeStateView | None:
    # The documented callback takes (state, proposals).  Supporting a
    # one-argument callback makes small integrations ergonomic without
    # introducing a second state mutation path.
    try:
        signature = inspect.signature(callback)
        positional = [
            item
            for item in signature.parameters.values()
            if item.kind
            in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        has_varargs = any(
            item.kind is inspect.Parameter.VAR_POSITIONAL for item in signature.parameters.values()
        )
        if len(positional) < 2 and not has_varargs:
            return callback(state)  # type: ignore[call-arg]
    except (TypeError, ValueError):
        # Some C-extension callables have no inspectable signature; use the
        # documented two-argument contract.
        pass
    return callback(state, proposals)


def _blocked_task_ids(
    state: RuntimeStateView, interrupts: InterruptEpoch | None
) -> tuple[str, ...]:
    if interrupts is None:
        return ()
    attempt_to_task = {
        str(attempt.attempt_id): str(attempt.task_id)
        for attempt in state.agent_cognition.current_attempts
        if attempt.attempt_id
    }
    blocked: set[str] = set()
    for decision in interrupts.decisions:
        if decision.action not in {
            InterruptAction.PREEMPT,
            InterruptAction.REBASE,
            InterruptAction.DEFER,
            InterruptAction.REVERIFY,
        }:
            continue
        task_id = (
            decision.target_id
            if decision.target_kind == "task"
            else attempt_to_task.get(decision.target_id)
        )
        if task_id:
            blocked.add(task_id)
    return tuple(sorted(blocked))


def _suppress_blocked(
    frontier: SchedulingEpoch | ParallelBatchSuggestion | None,
    blocked: tuple[str, ...],
) -> SchedulingEpoch | ParallelBatchSuggestion | None:
    if frontier is None or not blocked:
        return frontier
    blocked_set = set(blocked)
    selected = tuple(item for item in frontier.selected_task_ids if item not in blocked_set)
    deferred = tuple(
        sorted(set(frontier.deferred_task_ids) | (set(frontier.selected_task_ids) & blocked_set))
    )
    updated_decisions = []
    for item in frontier.decisions:
        if item.task_id in blocked_set and item.action.value == "run":
            updated_decisions.append(
                item.model_copy(
                    update={
                        "action": item.action.__class__("defer"),
                        "reason": "semantic_interrupt",
                    }
                )
            )
        else:
            updated_decisions.append(item)
    updates: dict[str, Any] = {
        "selected_task_ids": selected,
        "deferred_task_ids": deferred,
        "decisions": tuple(updated_decisions),
        "parallelism_hint": len(selected),
    }
    candidate_payload = frontier.model_copy(update=updates).model_dump(mode="json")
    candidate_payload.pop("decision_hash", None)
    digest = _hash_payload(candidate_payload)
    return frontier.model_copy(update={**updates, "decision_hash": digest})


def _make_decision(
    *,
    epoch_id: int,
    status: EpochDecisionStatus,
    state: RuntimeStateView,
    event_ids: tuple[str, ...],
    frontier: SchedulingEpoch | ParallelBatchSuggestion | None,
    interrupts: InterruptEpoch | None,
    blocked: tuple[str, ...],
    candidates: tuple[str, ...],
    selected: tuple[str, ...],
    deferred: tuple[str, ...],
    parallelism_hint: int,
    unavailable: tuple[UnavailableField, ...],
    reconciled: bool,
) -> EpochDecision:
    payload = {
        "schema_version": EPOCH_CONTROLLER_SCHEMA_VERSION,
        "controller_id": EPOCH_CONTROLLER_POLICY_ID,
        "epoch_id": epoch_id,
        "status": status.value,
        "phases": tuple(item.value for item in EpochDecision.model_fields["phases"].default),
        "graph_id": state.graph_id,
        "graph_version": state.progress.graph_version,
        "projection_hash": state.progress.projection_hash,
        "event_ids": event_ids,
        "candidate_task_ids": candidates,
        "selected_task_ids": selected,
        "deferred_task_ids": deferred,
        "parallelism_hint": parallelism_hint,
        "frontier": frontier,
        "interrupts": interrupts,
        "interrupt_blocked_task_ids": blocked,
        "unavailable": unavailable,
        "reason": None,
        "reconciled": reconciled,
    }
    return EpochDecision(
        epoch_id=epoch_id,
        status=status,
        graph_id=state.graph_id,
        graph_version=state.progress.graph_version,
        projection_hash=state.progress.projection_hash,
        event_ids=event_ids,
        candidate_task_ids=candidates,
        selected_task_ids=selected,
        deferred_task_ids=deferred,
        parallelism_hint=parallelism_hint,
        frontier=frontier,
        interrupts=interrupts,
        interrupt_blocked_task_ids=blocked,
        unavailable=unavailable,
        reconciled=reconciled,
        decision_hash=_hash_payload(payload),
    )


def _fail_closed_decision(
    *,
    epoch_id: int,
    state: RuntimeStateView,
    event_ids: tuple[str, ...],
    status: EpochDecisionStatus,
    reason: str,
    reconciled: bool,
) -> EpochDecision:
    payload = {
        "schema_version": EPOCH_CONTROLLER_SCHEMA_VERSION,
        "controller_id": EPOCH_CONTROLLER_POLICY_ID,
        "epoch_id": epoch_id,
        "status": status.value,
        "phases": tuple(item.value for item in EpochDecision.model_fields["phases"].default),
        "graph_id": state.graph_id,
        "graph_version": state.progress.graph_version,
        "projection_hash": state.progress.projection_hash,
        "event_ids": event_ids,
        "candidate_task_ids": (),
        "selected_task_ids": (),
        "deferred_task_ids": (),
        "parallelism_hint": 0,
        "frontier": None,
        "interrupts": None,
        "interrupt_blocked_task_ids": (),
        "unavailable": (),
        "reason": reason,
        "reconciled": reconciled,
    }
    return EpochDecision(
        epoch_id=epoch_id,
        status=status,
        graph_id=state.graph_id,
        graph_version=state.progress.graph_version,
        projection_hash=state.progress.projection_hash,
        event_ids=event_ids,
        parallelism_hint=0,
        reason=reason,
        reconciled=reconciled,
        decision_hash=_hash_payload(payload),
    )


def _input_key(state: RuntimeStateView, proposals: tuple[SemanticInterrupt, ...]) -> str:
    return _hash_payload(
        {
            "graph_id": state.graph_id,
            "graph_version": state.progress.graph_version,
            "projection_hash": state.progress.projection_hash,
            "state": state,
            "proposals": proposals,
        }
    )


def _bounded_error(exc: BaseException, limit: int = 240) -> str:
    text = f"{type(exc).__name__}: {exc}".replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else type(exc).__name__


def _dedupe_unavailable(
    values: Iterable[UnavailableField],
) -> tuple[UnavailableField, ...]:
    by_name: dict[str, UnavailableField] = {}
    for item in values:
        if not isinstance(item, UnavailableField):
            try:
                item = UnavailableField.model_validate(item)
            except Exception:
                continue
        by_name.setdefault(item.name, item)
    return tuple(by_name[name] for name in sorted(by_name))


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


def _hash_payload(value: Any) -> str:
    data = json.dumps(
        _canonical(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


__all__ = [
    "DEFAULT_MAX_EPOCHS",
    "EPOCH_CONTROLLER_POLICY_ID",
    "EPOCH_CONTROLLER_SCHEMA_VERSION",
    "EpochController",
    "EpochDecision",
    "EpochDecisionStatus",
    "EpochEventProposal",
    "EpochPhase",
    "EventDrivenEpochController",
    "EventProposal",
    "SchedulingEpochController",
]
