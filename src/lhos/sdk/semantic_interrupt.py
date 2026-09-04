"""Bounded semantic-interrupt routing for online Agent scheduling.

This module is intentionally an *observation/planning* primitive.  It turns
durable semantic-change notifications into deterministic proposals for the
currently observed execution epoch, but it does not stop Python callbacks,
release Claims, or acquire new Leases.  Those operations remain authoritative
in the Scheduler/Kernel and require a cooperative runtime integration.

The narrow boundary is useful even before a full preemption implementation:
callers can audit why a running attempt should continue, rebase, or be
preempted, and can persist the decision hash alongside the next scheduling
epoch.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

from .runtime_state import GlobalRuntimeState, UnavailableField

SEMANTIC_INTERRUPT_SCHEMA_VERSION: Final[Literal["semantic-interrupt.v1"]] = "semantic-interrupt.v1"
SEMANTIC_INTERRUPT_POLICY_ID: Final[str] = "deterministic-interrupt-router.v1"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _uuid() -> str:
    return uuid4().hex


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class SemanticInterruptKind(StrEnum):
    """Authoritative semantic/runtime events that may alter computation."""

    ARTIFACT_CHANGED = "artifact_changed"
    REQUIREMENT_CHANGED = "requirement_changed"
    TASK_VERIFIED = "task_verified"
    TASK_FAILED = "task_failed"
    EVIDENCE_INVALIDATED = "evidence_invalidated"
    RESOURCE_PRESSURE = "resource_pressure"
    WRITE_CONFLICT = "write_conflict"
    USER_PRIORITY_CHANGED = "user_priority_changed"
    AGENT_STALLED = "agent_stalled"


class InterruptAction(StrEnum):
    """Proposal emitted for one task/attempt in the next scheduling epoch."""

    CONTINUE = "continue"
    DEFER = "defer"
    PREEMPT = "preempt"
    REBASE = "rebase"
    REVERIFY = "reverify"


class SemanticInterrupt(_FrozenModel):
    """One immutable semantic interrupt observation.

    ``affected_*`` are explicit identities, not guesses inferred from prose.
    A caller that cannot identify the affected entity should emit an interrupt
    with empty targets and keep the work in a conservative/unhandled path.
    """

    schema_version: Literal["semantic-interrupt.v1"] = SEMANTIC_INTERRUPT_SCHEMA_VERSION
    interrupt_id: str = Field(default_factory=_uuid, min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    kind: SemanticInterruptKind
    reason: str = Field(min_length=1)
    source_task_id: str | None = None
    affected_task_ids: tuple[str, ...] = ()
    affected_attempt_ids: tuple[str, ...] = ()
    resource_keys: tuple[str, ...] = ()
    metadata: dict[str, Any] = Field(default_factory=dict)
    observed_at: datetime = Field(default_factory=_utcnow)

    @field_validator("interrupt_id", "graph_id", "reason", mode="before")
    @classmethod
    def _non_empty_strings(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("interrupt identity/reason must be non-empty")
        return normalized

    @field_validator(
        "source_task_id",
        mode="before",
    )
    @classmethod
    def _optional_task_id(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("affected_task_ids", "affected_attempt_ids", "resource_keys", mode="before")
    @classmethod
    def _normalize_ids(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))

    @model_validator(mode="before")
    @classmethod
    def _source_is_a_target(cls, value: Any) -> Any:
        # A source task is a useful shorthand, but it must never silently
        # broaden the target set to the whole graph.  Do this before field
        # validation so construction through ``__init__`` and
        # ``model_validate`` have identical behavior (Pydantic does not
        # support returning a replacement model from an ``after`` validator
        # during ``__init__``).
        if not isinstance(value, dict):
            return value
        data = dict(value)
        source = data.get("source_task_id")
        affected = data.get("affected_task_ids")
        if source is not None and not affected:
            data["affected_task_ids"] = (source,)
        return data


class InterruptDecision(_FrozenModel):
    """Collapsed proposal for one observed task or attempt."""

    target_kind: Literal["task", "attempt"]
    target_id: str = Field(min_length=1)
    action: InterruptAction
    interrupt_ids: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()

    @field_validator("target_id", mode="before")
    @classmethod
    def _target_non_empty(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("decision target_id must be non-empty")
        return normalized

    @field_validator("interrupt_ids", "reasons", mode="before")
    @classmethod
    def _normalize_texts(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


class InterruptEpoch(_FrozenModel):
    """Immutable, hashable output of one interrupt-routing pass."""

    schema_version: Literal["semantic-interrupt.v1"] = SEMANTIC_INTERRUPT_SCHEMA_VERSION
    epoch_id: StrictInt = Field(ge=0)
    policy_id: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    interrupt_ids: tuple[str, ...] = ()
    decisions: tuple[InterruptDecision, ...] = ()
    unhandled_interrupt_ids: tuple[str, ...] = ()
    unavailable: tuple[UnavailableField, ...] = ()
    decision_hash: str = Field(min_length=64, max_length=64)

    @field_validator("epoch_id", "graph_version")
    @classmethod
    def _real_int(cls, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("epoch/version must be an integer")
        return value

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


_ACTIVE_ACTIONS: dict[SemanticInterruptKind, InterruptAction] = {
    SemanticInterruptKind.ARTIFACT_CHANGED: InterruptAction.REBASE,
    SemanticInterruptKind.REQUIREMENT_CHANGED: InterruptAction.REBASE,
    SemanticInterruptKind.EVIDENCE_INVALIDATED: InterruptAction.REBASE,
    SemanticInterruptKind.TASK_FAILED: InterruptAction.REBASE,
    SemanticInterruptKind.USER_PRIORITY_CHANGED: InterruptAction.REBASE,
    SemanticInterruptKind.WRITE_CONFLICT: InterruptAction.PREEMPT,
    SemanticInterruptKind.AGENT_STALLED: InterruptAction.PREEMPT,
    SemanticInterruptKind.TASK_VERIFIED: InterruptAction.PREEMPT,
}

_READY_ACTIONS: dict[SemanticInterruptKind, InterruptAction] = {
    SemanticInterruptKind.EVIDENCE_INVALIDATED: InterruptAction.REVERIFY,
    SemanticInterruptKind.TASK_VERIFIED: InterruptAction.DEFER,
}

_ACTION_PRIORITY: dict[InterruptAction, int] = {
    InterruptAction.CONTINUE: 0,
    InterruptAction.DEFER: 1,
    InterruptAction.REVERIFY: 2,
    InterruptAction.REBASE: 3,
    InterruptAction.PREEMPT: 4,
}


class SemanticInterruptPolicy(_FrozenModel):
    """Deterministic, fail-closed router over :class:`GlobalRuntimeState`.

    The policy never mutates the state or performs a scheduler operation.
    ``PREEMPT`` and ``REBASE`` are cooperative proposals; a runtime must
    explicitly implement and acknowledge them before claiming those
    capabilities.
    """

    policy_id: str = SEMANTIC_INTERRUPT_POLICY_ID

    @field_validator("policy_id")
    @classmethod
    def _policy_non_empty(cls, value: str) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("policy_id must be non-empty")
        return normalized

    def plan(
        self,
        state: GlobalRuntimeState,
        interrupts: tuple[SemanticInterrupt, ...] | list[SemanticInterrupt],
        *,
        epoch_id: int = 0,
    ) -> InterruptEpoch:
        if not isinstance(state, GlobalRuntimeState):
            raise TypeError("state must be a GlobalRuntimeState/RuntimeStateView")
        if isinstance(epoch_id, bool) or not isinstance(epoch_id, int) or epoch_id < 0:
            raise ValueError("epoch_id must be a non-negative integer")

        normalized = tuple(
            sorted(
                (
                    item
                    if isinstance(item, SemanticInterrupt)
                    else SemanticInterrupt.model_validate(item)
                    for item in interrupts
                ),
                key=lambda item: item.interrupt_id,
            )
        )
        unavailable: list[UnavailableField] = []
        if not state.agent_cognition.available:
            unavailable.append(
                UnavailableField(
                    name="agent_cognition",
                    reason=(
                        state.agent_cognition.reason
                        or "current Agent/Attempt cognition is unavailable"
                    ),
                )
            )

        current = tuple(state.agent_cognition.current_attempts)
        attempts_by_id = {attempt.attempt_id: attempt for attempt in current}
        attempts_by_task: dict[str, list[Any]] = {}
        for attempt in current:
            attempts_by_task.setdefault(attempt.task_id, []).append(attempt)
        ready = set(state.progress.ready_frontier) | set(state.progress.repair_ready_frontier)
        verified = set(state.progress.verified_task_ids)

        # Accumulate candidate actions by target.  Multiple interrupts may
        # target one attempt; the strongest action wins deterministically.
        proposals: dict[tuple[str, str], dict[str, Any]] = {}
        unhandled: list[str] = []
        current_version = state.progress.graph_version

        for interrupt in normalized:
            if interrupt.graph_id != state.graph_id or interrupt.graph_version > current_version:
                unhandled.append(interrupt.interrupt_id)
                continue
            if not state.agent_cognition.available:
                unhandled.append(interrupt.interrupt_id)
                continue

            target_attempts: list[Any] = []
            if interrupt.affected_attempt_ids:
                target_attempts.extend(
                    attempts_by_id[item]
                    for item in interrupt.affected_attempt_ids
                    if item in attempts_by_id
                )
            if interrupt.affected_task_ids:
                for task_id in interrupt.affected_task_ids:
                    target_attempts.extend(attempts_by_task.get(task_id, ()))
            # Preserve insertion order while removing duplicate attempt IDs.
            target_attempts = list(
                {attempt.attempt_id: attempt for attempt in target_attempts}.values()
            )

            if target_attempts:
                for attempt in target_attempts:
                    action = self._active_action(interrupt, attempt.task_id in verified)
                    key = ("attempt", attempt.attempt_id)
                    self._merge_proposal(
                        proposals,
                        key,
                        action=action,
                        interrupt=interrupt,
                    )
                continue

            target_tasks = [task_id for task_id in interrupt.affected_task_ids if task_id in ready]
            if not target_tasks:
                # An interrupt about a task already verified is still useful:
                # it can tell a runtime to avoid dispatching a duplicate.
                target_tasks = [
                    task_id for task_id in interrupt.affected_task_ids if task_id in verified
                ]
            if target_tasks:
                action = _READY_ACTIONS.get(interrupt.kind, InterruptAction.DEFER)
                for task_id in sorted(set(target_tasks)):
                    self._merge_proposal(
                        proposals,
                        ("task", task_id),
                        action=action,
                        interrupt=interrupt,
                    )
            else:
                unhandled.append(interrupt.interrupt_id)

        decisions = tuple(
            InterruptDecision(
                target_kind=target_kind,  # type: ignore[arg-type]
                target_id=target_id,
                action=payload["action"],
                interrupt_ids=tuple(sorted(payload["interrupt_ids"])),
                reasons=tuple(sorted(payload["reasons"])),
            )
            for (target_kind, target_id), payload in sorted(proposals.items())
        )
        payload = {
            "schema_version": SEMANTIC_INTERRUPT_SCHEMA_VERSION,
            "epoch_id": epoch_id,
            "policy_id": self.policy_id,
            "graph_id": state.graph_id,
            "graph_version": current_version,
            "interrupt_ids": tuple(item.interrupt_id for item in normalized),
            "decisions": decisions,
            "unhandled_interrupt_ids": tuple(sorted(set(unhandled))),
            "unavailable": tuple(unavailable),
        }
        return InterruptEpoch(
            epoch_id=epoch_id,
            policy_id=self.policy_id,
            graph_id=state.graph_id,
            graph_version=current_version,
            interrupt_ids=tuple(item.interrupt_id for item in normalized),
            decisions=decisions,
            unhandled_interrupt_ids=tuple(sorted(set(unhandled))),
            unavailable=tuple(unavailable),
            decision_hash=_decision_hash(payload),
        )

    @staticmethod
    def _active_action(
        interrupt: SemanticInterrupt,
        task_is_verified: bool,
    ) -> InterruptAction:
        if interrupt.kind is SemanticInterruptKind.RESOURCE_PRESSURE:
            # Never claim that an arbitrary running callback can be stopped.
            # Only an explicit cooperative preemption declaration permits it.
            return (
                InterruptAction.PREEMPT
                if bool(interrupt.metadata.get("preemptible", False))
                else InterruptAction.CONTINUE
            )
        if task_is_verified or interrupt.kind is SemanticInterruptKind.TASK_VERIFIED:
            return InterruptAction.PREEMPT
        return _ACTIVE_ACTIONS.get(interrupt.kind, InterruptAction.CONTINUE)

    @staticmethod
    def _merge_proposal(
        proposals: dict[tuple[str, str], dict[str, Any]],
        key: tuple[str, str],
        *,
        action: InterruptAction,
        interrupt: SemanticInterrupt,
    ) -> None:
        existing = proposals.setdefault(
            key,
            {
                "action": InterruptAction.CONTINUE,
                "interrupt_ids": set(),
                "reasons": set(),
            },
        )
        if _ACTION_PRIORITY[action] > _ACTION_PRIORITY[existing["action"]]:
            existing["action"] = action
        existing["interrupt_ids"].add(interrupt.interrupt_id)
        existing["reasons"].add(interrupt.reason)


def plan_interrupts(
    state: GlobalRuntimeState,
    interrupts: tuple[SemanticInterrupt, ...] | list[SemanticInterrupt],
    *,
    epoch_id: int = 0,
) -> InterruptEpoch:
    """Convenience wrapper for one deterministic interrupt-routing pass."""

    return SemanticInterruptPolicy().plan(state, interrupts, epoch_id=epoch_id)


def _decision_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        _json_compatible(payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _json_compatible(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, tuple):
        return [_json_compatible(item) for item in value]
    if isinstance(value, list):
        return [_json_compatible(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, StrEnum):
        return value.value
    return value


__all__ = [
    "SEMANTIC_INTERRUPT_POLICY_ID",
    "SEMANTIC_INTERRUPT_SCHEMA_VERSION",
    "InterruptAction",
    "InterruptDecision",
    "InterruptEpoch",
    "SemanticInterrupt",
    "SemanticInterruptKind",
    "SemanticInterruptPolicy",
    "plan_interrupts",
]
