"""Structured utility metrics for long-running Agent computation.

The metrics in this module intentionally sit *above* the execution runtime.
They consume an auditable sequence of attempt records and do not infer hidden
provider/model behaviour.  This makes static-vs-adaptive comparisons
reproducible while still exposing the quantities that matter for the
LongHorizonOS thesis:

``verified progress / token`` and ``verified progress / minute``.

The module is dependency-light and provider agnostic.  A benchmark may either
construct :class:`ExecutionRecord` objects or pass dictionaries with the same
field names.  Unknown fields are ignored at the boundary so records emitted by
older benchmark runners remain usable.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MetricStatus = Literal["running", "verified", "stale", "failed", "preempted", "reused"]


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExecutionRecord(_FrozenModel):
    """One measured computation attempt.

    ``context_tokens`` is the context working-set size for the attempt;
    ``context_reread_tokens`` should contain only tokens reloaded from an
    earlier attempt/session.  Keeping these separate avoids double counting
    normal prompt tokens as rereads.
    """

    task_id: str = Field(min_length=1)
    attempt: int = Field(default=1, ge=1)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cached_tokens: int = Field(default=0, ge=0)
    context_tokens: int = Field(default=0, ge=0)
    context_reread_tokens: int = Field(default=0, ge=0)
    wall_time_seconds: float = Field(default=0.0, ge=0.0)
    cost_usd: float = Field(default=0.0, ge=0.0)
    verification_tokens: int = Field(default=0, ge=0)
    verification_cost_usd: float = Field(default=0.0, ge=0.0)
    verified_progress: float = Field(default=0.0, ge=0.0, le=1.0)
    status: MetricStatus = "verified"
    stale: bool = False
    reused: bool = False
    parallelism: int = Field(default=1, ge=1)

    @field_validator("task_id", mode="before")
    @classmethod
    def _normalize_task_id(cls, value: Any) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("task_id must be non-empty")
        return text

    @field_validator(
        "attempt",
        "input_tokens",
        "output_tokens",
        "cached_tokens",
        "context_tokens",
        "context_reread_tokens",
        "verification_tokens",
        "parallelism",
        mode="before",
    )
    @classmethod
    def _normalize_int(cls, value: Any) -> int:
        if isinstance(value, bool):
            raise TypeError("metric integer fields cannot be booleans")
        number = int(value)
        if number < 0:
            raise ValueError("metric integer fields must be non-negative")
        return number

    @field_validator(
        "wall_time_seconds", "cost_usd", "verification_cost_usd", "verified_progress", mode="before"
    )
    @classmethod
    def _normalize_float(cls, value: Any) -> float:
        if isinstance(value, bool):
            raise TypeError("metric float fields cannot be booleans")
        number = float(value)
        if number < 0:
            raise ValueError("metric float fields must be non-negative")
        return number

    @property
    def total_tokens(self) -> int:
        """Input plus output tokens, including cached input tokens."""

        return self.input_tokens + self.output_tokens

    @property
    def billable_tokens(self) -> int:
        """A conservative token count after subtracting reported cache hits."""

        return max(0, self.total_tokens - min(self.cached_tokens, self.input_tokens))

    @property
    def is_stale(self) -> bool:
        return self.stale or self.status == "stale"

    @property
    def is_repeated(self) -> bool:
        return self.reused or self.attempt > 1 or self.status in {"reused", "preempted"}

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class ComputationMetrics(_FrozenModel):
    """Aggregated, JSON-serializable utility metrics for one run."""

    schema_version: str = "computation-utility.v1"
    success: bool = False
    verified_progress: float = Field(default=0.0, ge=0.0, le=1.0)

    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    cached_tokens: int = Field(default=0, ge=0)
    total_tokens: int = Field(default=0, ge=0)
    billable_tokens: int = Field(default=0, ge=0)

    wall_time_seconds: float = Field(default=0.0, ge=0.0)
    model_cost_usd: float = Field(default=0.0, ge=0.0)
    verification_tokens: int = Field(default=0, ge=0)
    verification_cost_usd: float = Field(default=0.0, ge=0.0)
    total_cost_usd: float = Field(default=0.0, ge=0.0)

    context_tokens: int = Field(default=0, ge=0)
    context_reread_tokens: int = Field(default=0, ge=0)
    repeated_work_tokens: int = Field(default=0, ge=0)
    stale_work_tokens: int = Field(default=0, ge=0)
    repeated_attempts: int = Field(default=0, ge=0)
    stale_attempts: int = Field(default=0, ge=0)
    preemptions: int = Field(default=0, ge=0)
    rebases: int = Field(default=0, ge=0)
    verification_calls: int = Field(default=0, ge=0)
    context_reuse_count: int = Field(default=0, ge=0)

    average_parallelism: float = Field(default=0.0, ge=0.0)
    peak_parallelism: int = Field(default=0, ge=0)
    verified_progress_per_token: float = Field(default=0.0, ge=0.0)
    verified_progress_per_minute: float = Field(default=0.0, ge=0.0)

    def as_dict(self) -> dict[str, Any]:
        """Return JSON-compatible primitives (including derived fields)."""

        return self.model_dump(mode="json")

    def to_json(self) -> str:
        """Return canonical JSON suitable for benchmark artifacts."""

        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))

    # Familiar aliases for callers that use either spelling.
    model_dump_json_sorted = to_json


def _record(value: ExecutionRecord | Mapping[str, Any]) -> ExecutionRecord:
    if isinstance(value, ExecutionRecord):
        return value
    if not isinstance(value, Mapping):
        raise TypeError(f"execution record must be a mapping, got {type(value).__name__}")
    raw = dict(value)
    # Older runners call this field ``tokens`` and use ``seconds``.
    if "total_tokens" in raw and "input_tokens" not in raw:
        raw["input_tokens"] = int(raw["total_tokens"])
    if "seconds" in raw and "wall_time_seconds" not in raw:
        raw["wall_time_seconds"] = raw["seconds"]
    if "stale_work" in raw and "stale" not in raw:
        raw["stale"] = bool(raw["stale_work"])
    return ExecutionRecord.model_validate(
        {key: raw[key] for key in ExecutionRecord.model_fields if key in raw}
    )


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return 0.0
    return round(numerator / denominator, 12)


def aggregate_metrics(
    records: Iterable[ExecutionRecord | Mapping[str, Any]],
    *,
    success: bool = False,
    verified_progress: float | None = None,
    wall_time_seconds: float | None = None,
    model_cost_usd: float | None = None,
    verification_tokens: int | None = None,
    verification_cost_usd: float | None = None,
    context_reread_tokens: int | None = None,
    preemptions: int = 0,
    rebases: int = 0,
    parallelism_samples: Sequence[int | float] | None = None,
) -> ComputationMetrics:
    """Aggregate deterministic utility metrics from attempt records.

    The function is intentionally pure.  Explicit aggregate overrides are
    useful when a runtime records verification or scheduler timing outside
    the model-attempt event itself.
    """

    parsed = tuple(_record(item) for item in records)
    input_tokens = sum(item.input_tokens for item in parsed)
    output_tokens = sum(item.output_tokens for item in parsed)
    cached_tokens = sum(item.cached_tokens for item in parsed)
    total_tokens = input_tokens + output_tokens
    billable_tokens = max(0, total_tokens - min(cached_tokens, input_tokens))
    context_tokens = sum(item.context_tokens for item in parsed)
    reread = (
        sum(item.context_reread_tokens for item in parsed)
        if context_reread_tokens is None
        else _nonnegative_int(context_reread_tokens, "context_reread_tokens")
    )

    repeated_records = tuple(item for item in parsed if item.is_repeated)
    stale_records = tuple(item for item in parsed if item.is_stale)
    repeated_work_tokens = sum(item.total_tokens for item in repeated_records)
    stale_work_tokens = sum(item.total_tokens for item in stale_records)
    repeated_attempts = len(repeated_records)
    stale_attempts = len(stale_records)

    model_cost = (
        sum(item.cost_usd for item in parsed)
        if model_cost_usd is None
        else _nonnegative_float(model_cost_usd, "model_cost_usd")
    )
    verify_tokens = (
        sum(item.verification_tokens for item in parsed)
        if verification_tokens is None
        else _nonnegative_int(verification_tokens, "verification_tokens")
    )
    verify_cost = (
        sum(item.verification_cost_usd for item in parsed)
        if verification_cost_usd is None
        else _nonnegative_float(verification_cost_usd, "verification_cost_usd")
    )
    elapsed = (
        sum(item.wall_time_seconds for item in parsed)
        if wall_time_seconds is None
        else _nonnegative_float(wall_time_seconds, "wall_time_seconds")
    )

    if parallelism_samples is None:
        samples = tuple(float(item.parallelism) for item in parsed)
    else:
        samples = tuple(float(value) for value in parallelism_samples)
        if any(value < 0 for value in samples):
            raise ValueError("parallelism_samples must be non-negative")
    average_parallelism = round(sum(samples) / len(samples), 12) if samples else 0.0
    peak_parallelism = int(max(samples, default=0.0))
    progress = (
        sum(item.verified_progress for item in parsed)
        if verified_progress is None
        else _nonnegative_float(verified_progress, "verified_progress")
    )
    # Progress is a normalized terminal quantity.  A caller may provide per
    # record deltas, so clamp only after aggregation and reject absurd input.
    if progress > 1.0 + 1e-9:
        raise ValueError("verified_progress must be at most 1.0")
    progress = min(1.0, progress)

    total_cost = model_cost + verify_cost
    return ComputationMetrics(
        success=bool(success),
        verified_progress=round(progress, 12),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_tokens=cached_tokens,
        total_tokens=total_tokens,
        billable_tokens=billable_tokens,
        wall_time_seconds=round(elapsed, 12),
        model_cost_usd=round(model_cost, 12),
        verification_tokens=verify_tokens,
        verification_cost_usd=round(verify_cost, 12),
        total_cost_usd=round(total_cost, 12),
        context_tokens=context_tokens,
        context_reread_tokens=reread,
        repeated_work_tokens=repeated_work_tokens,
        stale_work_tokens=stale_work_tokens,
        repeated_attempts=repeated_attempts,
        stale_attempts=stale_attempts,
        preemptions=_nonnegative_int(preemptions, "preemptions"),
        rebases=_nonnegative_int(rebases, "rebases"),
        verification_calls=sum(
            1
            for item in parsed
            if item.verification_tokens > 0
            or item.verification_cost_usd > 0
            or item.status in {"verified", "failed"}
        ),
        context_reuse_count=sum(1 for item in parsed if item.reused),
        average_parallelism=average_parallelism,
        peak_parallelism=peak_parallelism,
        verified_progress_per_token=_safe_ratio(progress, float(total_tokens)),
        verified_progress_per_minute=_safe_ratio(progress, elapsed / 60.0),
    )


def compute_metrics(*args: Any, **kwargs: Any) -> ComputationMetrics:
    """Compatibility alias for :func:`aggregate_metrics`."""

    return aggregate_metrics(*args, **kwargs)


def metrics_from_records(*args: Any, **kwargs: Any) -> ComputationMetrics:
    """Compatibility alias used by benchmark adapters."""

    return aggregate_metrics(*args, **kwargs)


def verified_progress_per_token(verified_progress: float, total_tokens: int) -> float:
    """Return normalized Verified Progress gained per model token."""

    return _safe_ratio(float(verified_progress), float(total_tokens))


def verified_progress_per_minute(verified_progress: float, wall_time_seconds: float) -> float:
    """Return normalized Verified Progress gained per elapsed minute."""

    return _safe_ratio(float(verified_progress), float(wall_time_seconds) / 60.0)


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    number = int(value)
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


def _nonnegative_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{name} must be numeric")
    number = float(value)
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


__all__ = [
    "ComputationMetrics",
    "ExecutionRecord",
    "aggregate_metrics",
    "compute_metrics",
    "metrics_from_records",
    "verified_progress_per_minute",
    "verified_progress_per_token",
]
