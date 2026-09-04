"""Explicit host-telemetry to logical-capacity bridge.

The bridge is deliberately narrow:

* it converts one caller-supplied :class:`HostResourceTelemetry` sample into a
  conservative logical :class:`ResourceVector`;
* it reserves a configured fraction of each observed resource;
* it fails closed when a required metric is unavailable or malformed; and
* it performs no placement, isolation, quota enforcement, device partitioning,
  preemption, or continuous host monitoring.

Nothing in this module is enabled automatically.  A derived vector is only a
logical Scheduler admission ceiling for the selected pool.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from decimal import ROUND_FLOOR, Decimal
from typing import Any, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
)

from lhos.runtimes.multi_agent import ResourceVector

from .resource_telemetry import HostResourceTelemetry, ResourceTelemetryMetric
from .runtime_state import UnavailableField

HOST_CAPACITY_SCHEMA_VERSION: Final[Literal["host-capacity.v1"]] = "host-capacity.v1"
HOST_CAPACITY_POLICY_ID: Final[str] = "host-telemetry-reserved-capacity.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class HostCapacityPolicy(_FrozenModel):
    """Conservative mapping policy for one point-in-time host sample.

    Fractions are in ``[0, 1]`` and mean "leave this proportion outside the
    LongHorizonOS logical pool".  Rounding always goes down.  GPU/VRAM are
    required by default; CPU-only hosts must explicitly set
    ``require_gpu=False`` rather than having missing GPU telemetry fabricated
    as zero.

    ``model_slots`` are explicit logical labels.  Telemetry cannot discover
    model-serving capacity, so the bridge never infers them.
    """

    policy_id: str = HOST_CAPACITY_POLICY_ID
    cpu_reserve_fraction: float = Field(default=0.10, ge=0.0, le=1.0)
    ram_reserve_fraction: float = Field(default=0.10, ge=0.0, le=1.0)
    gpu_reserve_fraction: float = Field(default=0.0, ge=0.0, le=1.0)
    vram_reserve_fraction: float = Field(default=0.10, ge=0.0, le=1.0)
    require_gpu: StrictBool = True
    model_slots: dict[str, int] = Field(default_factory=dict)

    @field_validator(
        "cpu_reserve_fraction",
        "ram_reserve_fraction",
        "gpu_reserve_fraction",
        "vram_reserve_fraction",
    )
    @classmethod
    def _finite_fraction(cls, value: float) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("reserve fractions must be numbers")
        value = float(value)
        if not math.isfinite(value):
            raise ValueError("reserve fractions must be finite")
        return value

    @field_validator("policy_id")
    @classmethod
    def _non_empty_policy_id(cls, value: str) -> str:
        value = str(value).strip()
        if not value:
            raise ValueError("policy_id must be non-empty")
        return value

    @field_validator("model_slots")
    @classmethod
    def _valid_model_slots(cls, value: dict[str, int]) -> dict[str, int]:
        normalized: dict[str, int] = {}
        for raw_name, raw_quantity in value.items():
            name = str(raw_name).strip()
            if not name:
                raise ValueError("model slot names must be non-empty")
            if (
                isinstance(raw_quantity, bool)
                or not isinstance(raw_quantity, int)
                or raw_quantity < 0
            ):
                raise ValueError("model slot quantities must be non-negative integers")
            if raw_quantity:
                normalized[name] = raw_quantity
        return dict(sorted(normalized.items()))


class HostCapacityDecision(_FrozenModel):
    """Immutable, auditable result of one telemetry mapping."""

    schema_version: Literal["host-capacity.v1"] = HOST_CAPACITY_SCHEMA_VERSION
    policy_id: str = Field(min_length=1)
    telemetry_schema_version: str = Field(min_length=1)
    observed_at: datetime
    available: StrictBool
    capacity: ResourceVector | None = None
    unavailable: tuple[UnavailableField, ...] = ()
    telemetry_hash: str = Field(min_length=64, max_length=64)
    decision_hash: str = Field(min_length=64, max_length=64)

    @property
    def fail_closed(self) -> bool:
        return not self.available

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class HostCapacityApplyResult(_FrozenModel):
    """Result of explicitly applying one derived vector to one Agent pool."""

    pool_id: str = Field(min_length=1)
    decision: HostCapacityDecision
    applied: StrictBool = False
    previous_capacity: ResourceVector | None = None
    applied_capacity: ResourceVector | None = None
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def derive_host_capacity(
    telemetry: HostResourceTelemetry,
    policy: HostCapacityPolicy | None = None,
) -> HostCapacityDecision:
    """Derive a conservative logical vector from one explicit host sample.

    All required metrics must have an authoritative ``available`` quantity and
    the expected unit.  Any missing or contradictory value rejects the entire
    decision; partial CPU/RAM capacity is never silently mixed with unknown
    GPU state.  Callers that intentionally want a CPU-only pool must opt out
    with ``HostCapacityPolicy(require_gpu=False)``.
    """

    if not isinstance(telemetry, HostResourceTelemetry):
        raise TypeError("telemetry must be a HostResourceTelemetry")
    if policy is None:
        policy = HostCapacityPolicy()
    elif not isinstance(policy, HostCapacityPolicy):
        raise TypeError("policy must be a HostCapacityPolicy")

    required: tuple[tuple[str, ResourceTelemetryMetric, str], ...] = (
        ("cpu", telemetry.cpu, "cores"),
        ("ram", telemetry.ram, "bytes"),
        *(
            (("gpu", telemetry.gpu, "devices"), ("vram", telemetry.vram, "bytes"))
            if policy.require_gpu
            else ()
        ),
    )
    unavailable: list[UnavailableField] = []
    quantities: dict[str, int] = {}
    for name, metric, unit in required:
        reason = _metric_unavailable_reason(metric, expected_unit=unit)
        if reason is not None:
            unavailable.append(UnavailableField(name=name, reason=reason))
            continue
        quantities[name] = int(metric.available)  # type: ignore[arg-type]

    # A CPU-only policy is an explicit opt-out from GPU admission.  Preserve
    # unavailable GPU diagnostics rather than pretending the probe succeeded,
    # while keeping them non-fatal for the caller-selected CPU-only pool.
    if not policy.require_gpu:
        for name, metric, unit in (
            ("gpu", telemetry.gpu, "devices"),
            ("vram", telemetry.vram, "bytes"),
        ):
            reason = _metric_unavailable_reason(metric, expected_unit=unit)
            if reason is not None:
                unavailable.append(
                    UnavailableField(
                        name=name,
                        reason=f"optional metric ignored by CPU-only policy: {reason}",
                    )
                )

    telemetry_hash = _hash_payload(telemetry)
    capacity: ResourceVector | None = None
    fatal_unavailable = [
        item
        for item in unavailable
        if not item.reason.startswith("optional metric ignored by CPU-only policy:")
    ]
    if not fatal_unavailable:
        capacity = ResourceVector(
            cpu_millis=_reserve(
                quantities["cpu"] * 1_000,
                policy.cpu_reserve_fraction,
            ),
            ram_bytes=_reserve(
                quantities["ram"],
                policy.ram_reserve_fraction,
            ),
            gpu_count=(
                _reserve(quantities["gpu"], policy.gpu_reserve_fraction)
                if policy.require_gpu
                else 0
            ),
            vram_bytes=(
                _reserve(quantities["vram"], policy.vram_reserve_fraction)
                if policy.require_gpu
                else 0
            ),
            model_slots=policy.model_slots,
        )

    payload = {
        "schema_version": HOST_CAPACITY_SCHEMA_VERSION,
        "policy": policy,
        "telemetry_hash": telemetry_hash,
        "observed_at": telemetry.observed_at,
        "available": capacity is not None,
        "capacity": capacity,
        "unavailable": tuple(unavailable),
    }
    return HostCapacityDecision(
        policy_id=policy.policy_id,
        telemetry_schema_version=telemetry.schema_version,
        observed_at=telemetry.observed_at,
        available=capacity is not None,
        capacity=capacity,
        unavailable=tuple(unavailable),
        telemetry_hash=telemetry_hash,
        decision_hash=_hash_payload(payload),
    )


def _metric_unavailable_reason(
    metric: ResourceTelemetryMetric,
    *,
    expected_unit: str,
) -> str | None:
    if not isinstance(metric, ResourceTelemetryMetric):
        return "metric is not a ResourceTelemetryMetric"
    if metric.unit != expected_unit:
        return f"expected unit {expected_unit!r}, observed {metric.unit!r}"
    if not metric.is_available:
        return metric.reason or "metric is unavailable"
    if metric.available is None:
        return "metric has no authoritative available quantity"
    if metric.available < 0:
        return "metric available quantity is negative"
    if metric.total is not None and metric.available > metric.total:
        return "metric available quantity exceeds total"
    if (
        metric.total is not None
        and metric.used is not None
        and metric.used + metric.available > metric.total
    ):
        return "metric used + available exceeds total"
    return None


def _reserve(quantity: int, fraction: float) -> int:
    """Apply a reserve fraction with deterministic downward rounding."""

    usable = Decimal(quantity) * (Decimal(1) - Decimal(str(fraction)))
    result = int(usable.to_integral_value(rounding=ROUND_FLOOR))
    return min(int(quantity), max(0, result))


def _hash_payload(value: Any) -> str:
    canonical = json.dumps(
        _json_compatible(value),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
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
    return value


__all__ = [
    "HOST_CAPACITY_POLICY_ID",
    "HOST_CAPACITY_SCHEMA_VERSION",
    "HostCapacityApplyResult",
    "HostCapacityDecision",
    "HostCapacityPolicy",
    "derive_host_capacity",
]
