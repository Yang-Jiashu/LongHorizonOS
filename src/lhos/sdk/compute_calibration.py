"""Deterministic, measurement-driven calibration of declared compute estimates.

``compute_budget`` ranks and admits tasks purely from caller-declared
:class:`TaskComputeEstimate` values.  ``compute_usage`` records what an attempt
actually *measured*.  This module is the bridge that closes the loop: given the
MEASURED history in a :class:`UsageLedger`, it corrects the budget dimensions of
a future :class:`TaskComputeEstimate` toward what was really observed for the
same task.

Design invariants (all load-bearing):

* **Deterministic.** No wall-clock and no RNG are read here.  The correction is
  a pure function of the ledger and the estimate; replaying the same ledger
  yields the same correction, so decision hashes stay reproducible.  Ledger
  records are consumed in the ledger's own canonical sorted order.
* **Fail-safe.** With no measured history -- or no *positive* measured signal
  for a dimension -- the estimate is returned unchanged and no audit is
  emitted.  Zero history therefore reproduces today's behavior exactly, so the
  existing offline test corpus is unaffected.
* **Auditable.** Every applied correction is reported with the declared value,
  the fixed-point multiplier, the corrected value, and the sample count, so a
  reader can see exactly which numbers were adjusted and by how much.
* **Integer fixed-point only.** Multipliers are expressed in micros
  (``1_000_000`` == ``1.0x``) to match the no-floating-point-weights convention
  of the budget policy.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr

from .compute_budget import TaskComputeEstimate
from .compute_usage import UsageLedger, UsageVector

# ``1_000_000`` micros == a 1.0x multiplier.  Kept integer so calibration never
# introduces float non-determinism into a decision hash.
RATIO_SCALE: Final[int] = 1_000_000

# A single noisy observation must not be able to swing an estimate arbitrarily.
# The multiplier is clamped to a bounded band around 1.0x.
MIN_MULTIPLIER_MICROS: Final[int] = 250_000  # 0.25x
MAX_MULTIPLIER_MICROS: Final[int] = 4_000_000  # 4.0x

# Integer EWMA weight for the newest sample (numerator / denominator).  1/2
# weights the most recent observation equally against the running average.
EWMA_ALPHA_NUM: Final[int] = 1
EWMA_ALPHA_DEN: Final[int] = 2

# The five hard-budget dimensions, paired as (estimate field, usage field).
_DIMENSIONS: Final[tuple[tuple[str, str], ...]] = (
    ("estimated_tokens", "tokens"),
    ("estimated_wall_time_ms", "wall_time_ms"),
    ("estimated_cost_microusd", "cost_microusd"),
    ("estimated_context_tokens", "context_tokens"),
    ("estimated_verification_tokens", "verification_tokens"),
)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class DimensionCalibration(_FrozenModel):
    """One dimension's declared->corrected adjustment, fully audited."""

    dimension: StrictStr
    declared: StrictInt = Field(ge=0)
    measured_multiplier_micros: StrictInt = Field(ge=0)
    corrected: StrictInt = Field(ge=0)
    sample_count: StrictInt = Field(ge=0)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class EstimateCalibration(_FrozenModel):
    """Auditable record of every dimension corrected for one task estimate."""

    task_id: StrictStr
    dimensions: tuple[DimensionCalibration, ...] = ()

    @property
    def changed(self) -> bool:
        return any(dim.corrected != dim.declared for dim in self.dimensions)

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "changed": self.changed,
            "dimensions": [dim.as_dict() for dim in self.dimensions],
        }


def _ewma_multiplier_micros(samples: list[tuple[int, int]]) -> int:
    """Integer EWMA of the measured/declared ratio, clamped to the band.

    ``samples`` is a list of ``(measured, declared)`` pairs in deterministic
    order; only pairs with ``measured > 0`` and ``declared > 0`` are passed in.
    """

    ewma: int | None = None
    for measured, declared in samples:
        ratio = measured * RATIO_SCALE // declared
        if ewma is None:
            ewma = ratio
        else:
            ewma = (
                EWMA_ALPHA_NUM * ratio + (EWMA_ALPHA_DEN - EWMA_ALPHA_NUM) * ewma
            ) // EWMA_ALPHA_DEN
    if ewma is None:
        return RATIO_SCALE
    return min(MAX_MULTIPLIER_MICROS, max(MIN_MULTIPLIER_MICROS, ewma))


def _samples_by_dimension(
    estimate: TaskComputeEstimate,
    ledger: UsageLedger,
) -> dict[str, list[tuple[int, int]]]:
    """Collect per-dimension (measured, declared) pairs for this task_id.

    Only records that carry *both* an ``estimated`` (declared) and a
    ``measured`` vector contribute, and only dimensions with a strictly
    positive value on both sides.  A zero on either side is treated as "no
    signal" rather than "measured zero": with the current five-dimension
    UsageVector there is no way to distinguish a genuinely free dimension from
    one the executor simply did not report, so the fail-safe choice is to leave
    it uncalibrated.
    """

    by_dimension: dict[str, list[tuple[int, int]]] = {}
    for record in ledger.records:
        if record.task_id != estimate.task_id:
            continue
        declared_vector = record.estimated
        measured_vector = record.measured
        if declared_vector is None or measured_vector is None:
            continue
        for _estimate_field, usage_field in _DIMENSIONS:
            declared = int(getattr(declared_vector, usage_field, 0))
            measured = int(getattr(measured_vector, usage_field, 0))
            if declared > 0 and measured > 0:
                by_dimension.setdefault(usage_field, []).append((measured, declared))
    return by_dimension


def calibrate_estimate(
    estimate: TaskComputeEstimate,
    ledger: UsageLedger,
) -> tuple[TaskComputeEstimate, EstimateCalibration | None]:
    """Correct one estimate's budget dimensions toward measured history.

    Returns ``(estimate, None)`` unchanged when there is no positive measured
    signal for the task, so a caller with zero history observes today's exact
    behavior.
    """

    if not isinstance(estimate, TaskComputeEstimate):
        raise TypeError("estimate must be a TaskComputeEstimate")
    if not isinstance(ledger, UsageLedger):
        raise TypeError("ledger must be a UsageLedger")
    if not estimate.known:
        # An unknown estimate is a deliberate fail-closed marker; never
        # resurrect it with a calibrated value.
        return estimate, None

    by_dimension = _samples_by_dimension(estimate, ledger)
    if not by_dimension:
        return estimate, None

    updates: dict[str, int] = {}
    dimension_audits: list[DimensionCalibration] = []
    for estimate_field, usage_field in _DIMENSIONS:
        samples = by_dimension.get(usage_field)
        if not samples:
            continue
        multiplier = _ewma_multiplier_micros(samples)
        declared = int(getattr(estimate, estimate_field))
        corrected = declared * multiplier // RATIO_SCALE
        dimension_audits.append(
            DimensionCalibration(
                dimension=usage_field,
                declared=declared,
                measured_multiplier_micros=multiplier,
                corrected=corrected,
                sample_count=len(samples),
            )
        )
        if corrected != declared:
            updates[estimate_field] = corrected

    if not dimension_audits:
        return estimate, None
    audit = EstimateCalibration(task_id=estimate.task_id, dimensions=tuple(dimension_audits))
    if not updates:
        return estimate, audit
    return estimate.model_copy(update=updates), audit


def calibrate_estimates(
    estimates: Mapping[str, Any] | Iterable[Any],
    ledger: UsageLedger,
) -> tuple[Any, tuple[EstimateCalibration, ...]]:
    """Calibrate a mapping/iterable of estimates, preserving the container shape.

    Only concrete :class:`TaskComputeEstimate` instances are corrected; any
    other entry (a raw mapping, a malformed value, an unknown estimate) is
    passed through untouched so the pure policy's fail-closed handling is never
    altered by calibration.
    """

    if not isinstance(ledger, UsageLedger):
        raise TypeError("ledger must be a UsageLedger")

    audits: list[EstimateCalibration] = []
    if isinstance(estimates, Mapping):
        calibrated_map: dict[Any, Any] = {}
        for key, value in estimates.items():
            if isinstance(value, TaskComputeEstimate):
                corrected, audit = calibrate_estimate(value, ledger)
                calibrated_map[key] = corrected
                if audit is not None and audit.changed:
                    audits.append(audit)
            else:
                calibrated_map[key] = value
        return calibrated_map, tuple(audits)

    calibrated_list: list[Any] = []
    for value in estimates:
        if isinstance(value, TaskComputeEstimate):
            corrected, audit = calibrate_estimate(value, ledger)
            calibrated_list.append(corrected)
            if audit is not None and audit.changed:
                audits.append(audit)
        else:
            calibrated_list.append(value)
    return tuple(calibrated_list), tuple(audits)


# ── outcome-derived ranking inputs ──────────────────────────────────────────
# The cost dimensions above are corrected by a measured/declared *ratio*.
# Success and input stability are different mathematics: they are directly
# observable *rates*, so the observation replaces the declared opinion rather
# than scaling it.  Both are blended against the declared value with a small
# pseudo-sample prior, because one attempt observing 0% or 100% must not swing
# a ranking on its own.  With no observations the blend is the identity, so
# decision hashes are unchanged until real history exists.
OUTCOME_PRIOR_SAMPLES: Final[int] = 2
BASIS_POINTS_SCALE: Final[int] = 10_000

# Attempt states that carry a verdict about the *work* itself.
_WORK_SUCCESS_STATES: Final[frozenset[str]] = frozenset({"verified_semantically"})
_WORK_FAILURE_STATES: Final[frozenset[str]] = frozenset({"failed", "crashed"})
# A quarantined attempt is a verdict about the *inputs*, not the work: the agent
# may have been perfectly correct about a premise that stopped being true.
# Counting it as a work failure would blame the executor for upstream churn and
# push the scheduler away from tasks whose real problem is instability.
_INPUT_CHURN_STATES: Final[frozenset[str]] = frozenset({"stale_cognition"})


class TaskOutcomeCounts(_FrozenModel):
    """Observed terminal attempt outcomes for one task."""

    task_id: StrictStr
    verified: StrictInt = Field(default=0, ge=0)
    work_failed: StrictInt = Field(default=0, ge=0)
    input_churned: StrictInt = Field(default=0, ge=0)

    @property
    def success_samples(self) -> int:
        return self.verified + self.work_failed

    @property
    def stability_samples(self) -> int:
        return self.verified + self.work_failed + self.input_churned

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class OutcomeCalibration(_FrozenModel):
    """Auditable record of one declared->observed ranking-input correction."""

    task_id: StrictStr
    field_name: StrictStr
    declared: StrictInt = Field(ge=0)
    observed_basis_points: StrictInt | None = Field(default=None, ge=0)
    corrected: StrictInt = Field(ge=0)
    sample_count: StrictInt = Field(ge=0)

    @property
    def changed(self) -> bool:
        return self.corrected != self.declared

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def observe_task_outcomes(attempts: Iterable[Any]) -> dict[str, TaskOutcomeCounts]:
    """Count terminal attempt outcomes per task from durable Attempt records.

    Non-terminal states (dispatched/running) and ``preempted`` are ignored:
    an in-flight attempt has no verdict yet, and a preemption is a scheduler
    decision rather than evidence about the task.
    """

    verified: dict[str, int] = {}
    failed: dict[str, int] = {}
    churned: dict[str, int] = {}
    for attempt in attempts or ():
        task_id = str(getattr(attempt, "task_id", "") or "").strip()
        if not task_id:
            continue
        state = getattr(attempt, "state", None)
        state_value = str(getattr(state, "value", state) or "").strip().lower()
        if state_value in _WORK_SUCCESS_STATES:
            verified[task_id] = verified.get(task_id, 0) + 1
        elif state_value in _WORK_FAILURE_STATES:
            failed[task_id] = failed.get(task_id, 0) + 1
        elif state_value in _INPUT_CHURN_STATES:
            churned[task_id] = churned.get(task_id, 0) + 1
    task_ids = sorted(set(verified) | set(failed) | set(churned))
    return {
        task_id: TaskOutcomeCounts(
            task_id=task_id,
            verified=verified.get(task_id, 0),
            work_failed=failed.get(task_id, 0),
            input_churned=churned.get(task_id, 0),
        )
        for task_id in task_ids
    }


def _blend_rate(*, declared: int, successes: int, samples: int) -> tuple[int, int | None]:
    """Blend an observed rate against the declared one; return (corrected, observed)."""

    if samples <= 0:
        return declared, None
    observed = successes * BASIS_POINTS_SCALE // samples
    numerator = declared * OUTCOME_PRIOR_SAMPLES + successes * BASIS_POINTS_SCALE
    corrected = numerator // (OUTCOME_PRIOR_SAMPLES + samples)
    return max(0, min(BASIS_POINTS_SCALE, corrected)), observed


def calibrate_outcome_estimate(
    estimate: TaskComputeEstimate,
    outcomes: Mapping[str, TaskOutcomeCounts],
) -> tuple[TaskComputeEstimate, tuple[OutcomeCalibration, ...]]:
    """Correct the two observable ranking inputs of one estimate.

    ``verified_progress_units`` stays declared by definition -- it is the
    caller's statement of what progress is worth, not something the runtime can
    observe.  ``expected_rework_cost_units`` also stays declared: its unit scale
    is caller-defined, so measured tokens cannot be converted into it without
    inventing a scale.
    """

    counts = outcomes.get(str(estimate.task_id))
    if counts is None:
        return estimate, ()

    audits: list[OutcomeCalibration] = []
    updates: dict[str, int] = {}

    corrected_success, observed_success = _blend_rate(
        declared=estimate.success_basis_points,
        successes=counts.verified,
        samples=counts.success_samples,
    )
    if counts.success_samples > 0:
        audits.append(
            OutcomeCalibration(
                task_id=str(estimate.task_id),
                field_name="success_basis_points",
                declared=estimate.success_basis_points,
                observed_basis_points=observed_success,
                corrected=corrected_success,
                sample_count=counts.success_samples,
            )
        )
        updates["success_basis_points"] = corrected_success

    stable = counts.stability_samples - counts.input_churned
    corrected_stability, observed_stability = _blend_rate(
        declared=estimate.input_stability_basis_points,
        successes=stable,
        samples=counts.stability_samples,
    )
    if counts.stability_samples > 0:
        audits.append(
            OutcomeCalibration(
                task_id=str(estimate.task_id),
                field_name="input_stability_basis_points",
                declared=estimate.input_stability_basis_points,
                observed_basis_points=observed_stability,
                corrected=corrected_stability,
                sample_count=counts.stability_samples,
            )
        )
        updates["input_stability_basis_points"] = corrected_stability

    if not updates:
        return estimate, ()
    return estimate.model_copy(update=updates), tuple(audits)


def calibrate_outcome_estimates(
    estimates: Mapping[str, Any] | Iterable[Any],
    outcomes: Mapping[str, TaskOutcomeCounts],
) -> tuple[Any, tuple[OutcomeCalibration, ...]]:
    """Apply outcome calibration across a container, preserving its shape."""

    audits: list[OutcomeCalibration] = []
    if isinstance(estimates, Mapping):
        calibrated_map: dict[Any, Any] = {}
        for key, value in estimates.items():
            if isinstance(value, TaskComputeEstimate):
                corrected, records = calibrate_outcome_estimate(value, outcomes)
                calibrated_map[key] = corrected
                audits.extend(record for record in records if record.changed)
            else:
                calibrated_map[key] = value
        return calibrated_map, tuple(audits)

    calibrated_list: list[Any] = []
    for value in estimates:
        if isinstance(value, TaskComputeEstimate):
            corrected, records = calibrate_outcome_estimate(value, outcomes)
            calibrated_list.append(corrected)
            audits.extend(record for record in records if record.changed)
        else:
            calibrated_list.append(value)
    return tuple(calibrated_list), tuple(audits)


def computation_cost_to_usage_vector(cost: Any, *, elapsed_ms: int | None = None) -> UsageVector:
    """Map a multi-agent ``ComputationCost`` onto the five budget dimensions.

    ``elapsed_ms``, when supplied, overrides the cost's own ``elapsed_ms`` with
    an authoritative monotonic wall-clock measurement taken on the execution
    path.  Token/cost counters are taken from the cost object when the executor
    or provider populated them.
    """

    input_tokens = max(0, int(getattr(cost, "input_tokens", 0) or 0))
    output_tokens = max(0, int(getattr(cost, "output_tokens", 0) or 0))
    cached_input_tokens = max(0, int(getattr(cost, "cached_input_tokens", 0) or 0))
    monetary_micros = max(0, int(getattr(cost, "monetary_micros", 0) or 0))
    cost_elapsed = max(0, int(getattr(cost, "elapsed_ms", 0) or 0))
    wall_time_ms = cost_elapsed if elapsed_ms is None else max(0, int(elapsed_ms))
    return UsageVector(
        tokens=input_tokens + output_tokens,
        wall_time_ms=wall_time_ms,
        cost_microusd=monetary_micros,
        context_tokens=cached_input_tokens,
        verification_tokens=0,
    )


__all__ = [
    "EWMA_ALPHA_DEN",
    "EWMA_ALPHA_NUM",
    "MAX_MULTIPLIER_MICROS",
    "MIN_MULTIPLIER_MICROS",
    "RATIO_SCALE",
    "DimensionCalibration",
    "EstimateCalibration",
    "calibrate_estimate",
    "calibrate_estimates",
    "computation_cost_to_usage_vector",
]
