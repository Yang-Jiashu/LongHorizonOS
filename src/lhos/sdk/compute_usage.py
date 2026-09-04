"""Deterministic, in-memory accounting for one Agent attempt's compute usage.

This module is deliberately separate from :mod:`lhos.sdk.compute_budget`.
``compute_budget`` answers whether an estimate can be admitted; this module
records what an attempt declared, reserved, and actually measured.  A
reservation is never treated as measured consumption.

The ledger is an immutable value object.  Every mutation-like operation
returns a new ledger, making replay and deterministic testing straightforward.
It is not a durable store, a provider billing authority, or a process monitor.
Callers must supply measured usage from a trusted execution/provider boundary.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import Enum, StrEnum
from typing import Any, Final, Literal, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

COMPUTE_USAGE_SCHEMA_VERSION: Final[Literal["compute-usage.v1"]] = "compute-usage.v1"
USAGE_DIMENSIONS: Final[tuple[str, ...]] = (
    "tokens",
    "wall_time_ms",
    "cost_microusd",
    "context_tokens",
    "verification_tokens",
)


class UsageAccountingError(ValueError):
    """Base class for fail-closed accounting errors."""


class UsageIdentityError(UsageAccountingError):
    """Raised when an attempt identity is missing or inconsistent."""


class UsageConflictError(UsageAccountingError):
    """Raised when an idempotency key is reused with different content."""


class UnknownAttemptError(UsageAccountingError):
    """Raised when a transition targets an attempt absent from the ledger."""


class InvalidUsageTransition(UsageAccountingError):
    """Raised when an attempt skips or reverses a lifecycle state."""


class _Unset:
    """Typing sentinel used to distinguish omitted values from explicit None."""


_UNSET = _Unset()


class AttemptUsageState(StrEnum):
    """Lifecycle state of one immutable attempt accounting snapshot."""

    ESTIMATED = "estimated"
    RESERVED = "reserved"
    MEASURED = "measured"
    COMMITTED = "committed"
    FAILED = "failed"
    STALE = "stale"
    CANCELLED = "cancelled"


TERMINAL_ATTEMPT_STATES: Final[frozenset[AttemptUsageState]] = frozenset(
    {
        AttemptUsageState.COMMITTED,
        AttemptUsageState.FAILED,
        AttemptUsageState.STALE,
        AttemptUsageState.CANCELLED,
    }
)
MEASURED_ATTEMPT_STATES: Final[frozenset[AttemptUsageState]] = frozenset(
    {AttemptUsageState.MEASURED, *TERMINAL_ATTEMPT_STATES}
)

# A measured snapshot may be emitted without a prior reservation (for
# providers that do not expose admission reservations), but terminal states
# cannot be reopened or changed into a different terminal outcome.
_ALLOWED_TRANSITIONS: Final[dict[AttemptUsageState, frozenset[AttemptUsageState]]] = {
    AttemptUsageState.ESTIMATED: frozenset(
        {
            AttemptUsageState.RESERVED,
            AttemptUsageState.MEASURED,
            AttemptUsageState.FAILED,
            AttemptUsageState.STALE,
            AttemptUsageState.CANCELLED,
        }
    ),
    AttemptUsageState.RESERVED: frozenset(
        {
            AttemptUsageState.MEASURED,
            AttemptUsageState.FAILED,
            AttemptUsageState.STALE,
            AttemptUsageState.CANCELLED,
        }
    ),
    AttemptUsageState.MEASURED: TERMINAL_ATTEMPT_STATES,
    AttemptUsageState.COMMITTED: frozenset(),
    AttemptUsageState.FAILED: frozenset(),
    AttemptUsageState.STALE: frozenset(),
    AttemptUsageState.CANCELLED: frozenset(),
}


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


def _strict_id(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise UsageIdentityError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise UsageIdentityError(f"{field_name} must be non-empty")
    return normalized


class UsageVector(_FrozenModel):
    """Non-negative integer usage in the five scheduler accounting units."""

    tokens: StrictInt = Field(default=0, ge=0)
    wall_time_ms: StrictInt = Field(default=0, ge=0)
    cost_microusd: StrictInt = Field(default=0, ge=0)
    context_tokens: StrictInt = Field(default=0, ge=0)
    verification_tokens: StrictInt = Field(default=0, ge=0)

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_dimensions(cls, value: Any) -> Any:
        if isinstance(value, Mapping):
            unknown = set(value) - set(USAGE_DIMENSIONS)
            if unknown:
                names = ", ".join(sorted(str(item) for item in unknown))
                raise ValueError(f"unknown usage dimensions: {names}")
        return value

    @classmethod
    def zero(cls) -> UsageVector:
        return cls()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> UsageVector:
        return cls.model_validate(dict(value))

    def plus(self, other: UsageVector) -> UsageVector:
        if not isinstance(other, UsageVector):
            raise TypeError("other must be a UsageVector")
        return UsageVector(
            tokens=self.tokens + other.tokens,
            wall_time_ms=self.wall_time_ms + other.wall_time_ms,
            cost_microusd=self.cost_microusd + other.cost_microusd,
            context_tokens=self.context_tokens + other.context_tokens,
            verification_tokens=self.verification_tokens + other.verification_tokens,
        )

    def minus_clamped(self, other: UsageVector) -> UsageVector:
        """Subtract without allowing negative outstanding reservations."""

        if not isinstance(other, UsageVector):
            raise TypeError("other must be a UsageVector")
        return UsageVector(
            tokens=max(0, self.tokens - other.tokens),
            wall_time_ms=max(0, self.wall_time_ms - other.wall_time_ms),
            cost_microusd=max(0, self.cost_microusd - other.cost_microusd),
            context_tokens=max(0, self.context_tokens - other.context_tokens),
            verification_tokens=max(0, self.verification_tokens - other.verification_tokens),
        )

    def as_tuple(self) -> tuple[int, int, int, int, int]:
        return (
            self.tokens,
            self.wall_time_ms,
            self.cost_microusd,
            self.context_tokens,
            self.verification_tokens,
        )

    def is_zero(self) -> bool:
        return self == UsageVector.zero()


class AttemptUsageIdentity(_FrozenModel):
    """Strict identity for one attempt; all three components are required."""

    goal_id: StrictStr
    task_id: StrictStr
    attempt_id: StrictStr

    @field_validator("goal_id", "task_id", "attempt_id", mode="before")
    @classmethod
    def _normalize_ids(cls, value: Any, info: Any) -> str:
        return _strict_id(value, info.field_name)

    def as_tuple(self) -> tuple[str, str, str]:
        return (self.goal_id, self.task_id, self.attempt_id)


class AttemptUsageRecord(_FrozenModel):
    """One immutable point-in-time accounting snapshot for an attempt.

    ``estimated`` and ``reserved`` are declarations/admission artifacts.
    Only ``measured`` contributes to actual consumption.  The terminal state
    determines which outcome bucket receives that measured vector.
    """

    schema_version: Literal["compute-usage.v1"] = COMPUTE_USAGE_SCHEMA_VERSION
    goal_id: StrictStr
    task_id: StrictStr
    attempt_id: StrictStr
    state: AttemptUsageState
    estimated: UsageVector | None = None
    reserved: UsageVector | None = None
    measured: UsageVector | None = None

    @field_validator("goal_id", "task_id", "attempt_id", mode="before")
    @classmethod
    def _normalize_ids(cls, value: Any, info: Any) -> str:
        return _strict_id(value, info.field_name)

    @model_validator(mode="after")
    def _validate_shape(self) -> AttemptUsageRecord:
        if self.state is AttemptUsageState.ESTIMATED:
            if self.estimated is None:
                raise ValueError("estimated state requires estimated usage")
            if self.reserved is not None or self.measured is not None:
                raise ValueError("estimated state cannot contain reserved/measured usage")
        elif self.state is AttemptUsageState.RESERVED:
            if self.reserved is None:
                raise ValueError("reserved state requires reserved usage")
            if self.measured is not None:
                raise ValueError("reserved state cannot contain measured usage")
        elif self.state in MEASURED_ATTEMPT_STATES and self.measured is None:
            raise ValueError(f"{self.state.value} state requires measured usage")
        return self

    @property
    def identity(self) -> AttemptUsageIdentity:
        return AttemptUsageIdentity(
            goal_id=self.goal_id,
            task_id=self.task_id,
            attempt_id=self.attempt_id,
        )

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_ATTEMPT_STATES

    @property
    def estimate(self) -> UsageVector | None:
        """Compatibility singular alias for ``estimated``."""

        return self.estimated

    @property
    def reservation(self) -> UsageVector | None:
        """Compatibility singular alias for ``reserved``."""

        return self.reserved

    def canonical_payload(self) -> dict[str, Any]:
        payload = _canonical_payload(self.model_dump(mode="json"))
        if not isinstance(payload, dict):
            raise TypeError("attempt usage payload must be a mapping")
        return payload

    @property
    def canonical_hash(self) -> str:
        return _sha256_json(self.canonical_payload())

    def transition(
        self,
        new_state: AttemptUsageState | str,
        *,
        estimated: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
        reserved: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
        measured: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
    ) -> AttemptUsageRecord:
        """Return a validated next snapshot without mutating this record."""

        target = _coerce_state(new_state)
        if target is self.state:
            candidate = self._candidate(estimated, reserved, measured)
            if candidate == self:
                return self
            raise UsageConflictError(
                f"attempt {self.attempt_id!r} already has state {self.state.value} "
                "with different accounting"
            )
        if target not in _ALLOWED_TRANSITIONS[self.state]:
            raise InvalidUsageTransition(
                f"cannot transition attempt {self.attempt_id!r} "
                f"from {self.state.value} to {target.value}"
            )
        return self._candidate(estimated, reserved, measured, state=target)

    def _candidate(
        self,
        estimated: UsageVector | Mapping[str, Any] | _Unset | None,
        reserved: UsageVector | Mapping[str, Any] | _Unset | None,
        measured: UsageVector | Mapping[str, Any] | _Unset | None,
        *,
        state: AttemptUsageState | None = None,
    ) -> AttemptUsageRecord:
        def choose(
            value: UsageVector | Mapping[str, Any] | _Unset | None,
            previous: UsageVector | None,
            field_name: str,
        ) -> UsageVector | None:
            if value is _UNSET:
                return previous
            candidate = _coerce_optional_usage(value)
            if previous is not None and candidate != previous:
                raise UsageConflictError(
                    f"attempt {self.attempt_id!r} cannot rewrite {field_name} usage"
                )
            return candidate

        target = state or self.state
        return AttemptUsageRecord(
            goal_id=self.goal_id,
            task_id=self.task_id,
            attempt_id=self.attempt_id,
            state=target,
            estimated=choose(estimated, self.estimated, "estimated"),
            reserved=choose(reserved, self.reserved, "reserved"),
            measured=choose(measured, self.measured, "measured"),
        )


class UsageAggregate(_FrozenModel):
    """Immutable aggregate over a ledger scope.

    ``measured`` includes active measured attempts and all terminal outcomes.
    Outcome vectors are disjoint subsets of ``measured``.  ``reserved`` is the
    declared reservation total, while ``active_reserved`` contains only
    reservations whose attempts are still in ``RESERVED`` state.
    """

    schema_version: Literal["compute-usage.v1"] = COMPUTE_USAGE_SCHEMA_VERSION
    goal_id: StrictStr | None = None
    task_id: StrictStr | None = None
    attempt_id: StrictStr | None = None
    attempt_count: StrictInt = Field(default=0, ge=0)
    estimated: UsageVector = Field(default_factory=UsageVector.zero)
    reserved: UsageVector = Field(default_factory=UsageVector.zero)
    active_reserved: UsageVector = Field(default_factory=UsageVector.zero)
    measured: UsageVector = Field(default_factory=UsageVector.zero)
    committed: UsageVector = Field(default_factory=UsageVector.zero)
    failed: UsageVector = Field(default_factory=UsageVector.zero)
    stale: UsageVector = Field(default_factory=UsageVector.zero)
    cancelled: UsageVector = Field(default_factory=UsageVector.zero)

    @field_validator("goal_id", "task_id", "attempt_id", mode="before")
    @classmethod
    def _normalize_optional_ids(cls, value: Any, info: Any) -> str | None:
        if value is None:
            return None
        return _strict_id(value, info.field_name)

    @property
    def terminal_measured(self) -> UsageVector:
        return self.committed.plus(self.failed).plus(self.stale).plus(self.cancelled)

    @property
    def outstanding_reserved(self) -> UsageVector:
        return self.active_reserved

    def canonical_payload(self) -> dict[str, Any]:
        payload = _canonical_payload(self.model_dump(mode="json"))
        if not isinstance(payload, dict):
            raise TypeError("usage aggregate payload must be a mapping")
        return payload

    @property
    def canonical_hash(self) -> str:
        return _sha256_json(self.canonical_payload())


class UsageLedger(_FrozenModel):
    """Pure in-memory immutable ledger keyed by ``(goal, task, attempt)``."""

    schema_version: Literal["compute-usage.v1"] = COMPUTE_USAGE_SCHEMA_VERSION
    records: tuple[AttemptUsageRecord, ...] = ()

    @model_validator(mode="before")
    @classmethod
    def _normalize_records(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        entries = raw.get("records", ())
        if entries is None:
            entries = ()
        normalized: dict[tuple[str, str, str], AttemptUsageRecord] = {}
        for entry in entries:
            record = _coerce_record(entry)
            key = record.identity.as_tuple()
            existing = normalized.get(key)
            if existing is not None and existing.canonical_hash != record.canonical_hash:
                raise UsageConflictError(f"conflicting records for attempt identity {key!r}")
            normalized[key] = record
        raw["records"] = tuple(normalized[key] for key in sorted(normalized))
        return raw

    @classmethod
    def empty(cls) -> UsageLedger:
        return cls()

    @property
    def canonical_payload(self) -> dict[str, Any]:
        payload = _canonical_payload(self.model_dump(mode="json"))
        if not isinstance(payload, dict):
            raise TypeError("usage ledger payload must be a mapping")
        return payload

    @property
    def canonical_hash(self) -> str:
        return _sha256_json(self.canonical_payload)

    @property
    def ledger_hash(self) -> str:
        """Explicit alias used by audit/reporting callers."""

        return self.canonical_hash

    @property
    def attempt_ids(self) -> tuple[str, ...]:
        return tuple(record.attempt_id for record in self.records)

    def add(self, record: AttemptUsageRecord | Mapping[str, Any]) -> UsageLedger:
        """Insert one snapshot; exact duplicates are idempotent."""

        candidate = _coerce_record(record)
        existing = self._find(candidate.identity)
        if existing is not None:
            if existing.canonical_hash == candidate.canonical_hash:
                return self
            raise UsageConflictError(
                f"conflicting accounting snapshot for attempt {candidate.identity.as_tuple()!r}"
            )
        return UsageLedger(records=(*self.records, candidate))

    append = add
    record = add
    upsert = add

    def transition(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        new_state: AttemptUsageState | str,
        *,
        estimated: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
        reserved: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
        measured: UsageVector | Mapping[str, Any] | _Unset | None = _UNSET,
    ) -> UsageLedger:
        """Transition an existing attempt using its complete strict identity."""

        identity = AttemptUsageIdentity(
            goal_id=goal_id,
            task_id=task_id,
            attempt_id=attempt_id,
        )
        current = self._find(identity)
        if current is None:
            raise UnknownAttemptError(
                f"attempt identity {identity.as_tuple()!r} is not in the ledger"
            )
        next_record = current.transition(
            new_state,
            estimated=estimated,
            reserved=reserved,
            measured=measured,
        )
        if next_record == current:
            return self
        return self._replace(next_record)

    def record_estimate(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        usage: UsageVector | Mapping[str, Any],
    ) -> UsageLedger:
        return self.add(
            AttemptUsageRecord(
                goal_id=goal_id,
                task_id=task_id,
                attempt_id=attempt_id,
                state=AttemptUsageState.ESTIMATED,
                estimated=_coerce_usage(usage),
            )
        )

    estimate = record_estimate

    def reserve(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        usage: UsageVector | Mapping[str, Any],
    ) -> UsageLedger:
        return self.transition(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.RESERVED,
            reserved=usage,
        )

    record_reservation = reserve

    def record_measured(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        usage: UsageVector | Mapping[str, Any],
    ) -> UsageLedger:
        return self.transition(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.MEASURED,
            measured=usage,
        )

    measure = record_measured

    def finish(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        outcome: AttemptUsageState | str,
        *,
        measured: UsageVector | Mapping[str, Any] | None = None,
    ) -> UsageLedger:
        target = _coerce_state(outcome)
        if target not in TERMINAL_ATTEMPT_STATES:
            raise InvalidUsageTransition(
                "finish outcome must be committed, failed, stale, or cancelled"
            )
        current = self._find(
            AttemptUsageIdentity(goal_id=goal_id, task_id=task_id, attempt_id=attempt_id)
        )
        if current is None:
            raise UnknownAttemptError(
                f"attempt identity {(goal_id, task_id, attempt_id)!r} is not in the ledger"
            )
        if measured is not None:
            measured_value: UsageVector | Mapping[str, Any] = measured
        elif current.measured is not None:
            measured_value = current.measured
        else:
            raise InvalidUsageTransition(
                f"terminal transition for attempt {attempt_id!r} requires explicit "
                "measured usage; pass UsageVector.zero() only when zero is authoritative"
            )
        return self.transition(
            goal_id,
            task_id,
            attempt_id,
            target,
            measured=measured_value,
        )

    def commit(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        *,
        measured: UsageVector | Mapping[str, Any] | None = None,
    ) -> UsageLedger:
        return self.finish(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.COMMITTED,
            measured=measured,
        )

    def fail(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        *,
        measured: UsageVector | Mapping[str, Any] | None = None,
    ) -> UsageLedger:
        return self.finish(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.FAILED,
            measured=measured,
        )

    def mark_stale(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        *,
        measured: UsageVector | Mapping[str, Any] | None = None,
    ) -> UsageLedger:
        return self.finish(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.STALE,
            measured=measured,
        )

    def cancel(
        self,
        goal_id: str,
        task_id: str,
        attempt_id: str,
        *,
        measured: UsageVector | Mapping[str, Any] | None = None,
    ) -> UsageLedger:
        return self.finish(
            goal_id,
            task_id,
            attempt_id,
            AttemptUsageState.CANCELLED,
            measured=measured,
        )

    def aggregate(
        self,
        *,
        goal_id: str | None = None,
        task_id: str | None = None,
        attempt_id: str | None = None,
    ) -> UsageAggregate:
        """Aggregate declarations and measured outcomes for one scope."""

        normalized_goal = _strict_optional_id(goal_id, "goal_id")
        normalized_task = _strict_optional_id(task_id, "task_id")
        normalized_attempt = _strict_optional_id(attempt_id, "attempt_id")
        selected = tuple(
            record
            for record in self.records
            if (normalized_goal is None or record.goal_id == normalized_goal)
            and (normalized_task is None or record.task_id == normalized_task)
            and (normalized_attempt is None or record.attempt_id == normalized_attempt)
        )
        estimated = UsageVector.zero()
        reserved = UsageVector.zero()
        active_reserved = UsageVector.zero()
        measured = UsageVector.zero()
        committed = UsageVector.zero()
        failed = UsageVector.zero()
        stale = UsageVector.zero()
        cancelled = UsageVector.zero()
        for record in selected:
            estimated = estimated.plus(record.estimated or UsageVector.zero())
            reserved_value = record.reserved or UsageVector.zero()
            reserved = reserved.plus(reserved_value)
            if record.state is AttemptUsageState.RESERVED:
                active_reserved = active_reserved.plus(reserved_value)
            measured_value = record.measured or UsageVector.zero()
            measured = measured.plus(measured_value)
            if record.state is AttemptUsageState.COMMITTED:
                committed = committed.plus(measured_value)
            elif record.state is AttemptUsageState.FAILED:
                failed = failed.plus(measured_value)
            elif record.state is AttemptUsageState.STALE:
                stale = stale.plus(measured_value)
            elif record.state is AttemptUsageState.CANCELLED:
                cancelled = cancelled.plus(measured_value)
        return UsageAggregate(
            goal_id=normalized_goal,
            task_id=normalized_task,
            attempt_id=normalized_attempt,
            attempt_count=len(selected),
            estimated=estimated,
            reserved=reserved,
            active_reserved=active_reserved,
            measured=measured,
            committed=committed,
            failed=failed,
            stale=stale,
            cancelled=cancelled,
        )

    def by_goal(self, goal_id: str) -> UsageAggregate:
        return self.aggregate(goal_id=goal_id)

    def by_task(self, goal_id: str, task_id: str) -> UsageAggregate:
        return self.aggregate(goal_id=goal_id, task_id=task_id)

    def by_attempt(self, goal_id: str, task_id: str, attempt_id: str) -> UsageAggregate:
        return self.aggregate(goal_id=goal_id, task_id=task_id, attempt_id=attempt_id)

    def _find(self, identity: AttemptUsageIdentity) -> AttemptUsageRecord | None:
        for record in self.records:
            if record.identity == identity:
                return record
        return None

    def _replace(self, record: AttemptUsageRecord) -> UsageLedger:
        replaced = tuple(
            record if current.identity == record.identity else current for current in self.records
        )
        return UsageLedger(records=replaced)


UsageLike: TypeAlias = UsageVector | Mapping[str, Any]


def _coerce_state(value: AttemptUsageState | str) -> AttemptUsageState:
    if isinstance(value, AttemptUsageState):
        return value
    try:
        return AttemptUsageState(value)
    except (TypeError, ValueError) as exc:
        raise InvalidUsageTransition(f"unknown attempt usage state: {value!r}") from exc


def _coerce_usage(value: UsageLike) -> UsageVector:
    if isinstance(value, UsageVector):
        return value
    if isinstance(value, Mapping):
        return UsageVector.from_mapping(value)
    raise TypeError("usage must be a UsageVector or mapping")


def _coerce_optional_usage(
    value: UsageVector | Mapping[str, Any] | _Unset | None,
) -> UsageVector | None:
    if value is None:
        return None
    if isinstance(value, _Unset):
        raise TypeError("internal unset sentinel cannot be coerced as usage")
    return _coerce_usage(value)


def _coerce_record(value: AttemptUsageRecord | Mapping[str, Any]) -> AttemptUsageRecord:
    if isinstance(value, AttemptUsageRecord):
        return value
    if isinstance(value, Mapping):
        return AttemptUsageRecord.model_validate(dict(value))
    raise TypeError("record must be an AttemptUsageRecord or mapping")


def _strict_optional_id(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    return _strict_id(value, field_name)


def _canonical_payload(value: Any) -> Any:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {
            str(key): _canonical_payload(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (tuple, list)):
        return [_canonical_payload(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    return value


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(
        _canonical_payload(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "COMPUTE_USAGE_SCHEMA_VERSION",
    "MEASURED_ATTEMPT_STATES",
    "TERMINAL_ATTEMPT_STATES",
    "USAGE_DIMENSIONS",
    "AttemptUsageIdentity",
    "AttemptUsageRecord",
    "AttemptUsageState",
    "InvalidUsageTransition",
    "UnknownAttemptError",
    "UsageAccountingError",
    "UsageAggregate",
    "UsageConflictError",
    "UsageIdentityError",
    "UsageLedger",
    "UsageLike",
    "UsageVector",
]
