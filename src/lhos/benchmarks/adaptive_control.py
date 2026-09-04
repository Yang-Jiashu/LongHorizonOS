"""Deterministic static-vs-adaptive computation-control benchmark.

This is a small *controlled* simulator for the central LongHorizonOS claim:
the graph changes while work is in flight, and an online policy can defer
unstable/conflicting computation instead of paying for stale work.  It does
not call an LLM, provider, GPU, or wall-clock scheduler.  Durations and token
costs are simulated numbers, so results are reproducible and must not be
interpreted as production throughput.

The simulator deliberately keeps the control loop visible::

    OBSERVE -> SELECT BATCH -> EXECUTE -> VERIFY -> APPLY CHANGE -> repeat

``static`` selects the first READY tasks up to a fixed parallelism cap.
``adaptive`` greedily scores stability, criticality and progress utility,
serializes explicit write conflicts, and defers low-stability tasks until the
scheduled graph change has been observed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .computation_utility import (
    ComputationMetrics,
    ExecutionRecord,
    MetricStatus,
    aggregate_metrics,
)


class ControlMode(StrEnum):
    STATIC = "static"
    ADAPTIVE = "adaptive"


class SimulatedProvider(BaseModel):
    """Deterministic provider/cost profile used by the offline benchmark.

    The profile is deliberately tiny: it models the quantities that a
    long-horizon scheduler should account for (latency, token consumption and
    verification cost) without pretending to model a real provider.  The same
    profile is applied to static and adaptive runs so that the comparison
    isolates scheduling policy rather than provider quality.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider_id: str = Field(default="simulated-default", min_length=1)
    latency_multiplier: float = Field(default=1.0, gt=0.0)
    input_token_multiplier: float = Field(default=1.0, gt=0.0)
    output_token_multiplier: float = Field(default=1.0, gt=0.0)
    input_cost_per_token_usd: float = Field(default=0.0, ge=0.0)
    output_cost_per_token_usd: float = Field(default=0.00001, ge=0.0)
    verification_tokens: int = Field(default=20, ge=0)
    verification_cost_usd: float = Field(default=0.0002, ge=0.0)

    @field_validator("provider_id", mode="before")
    @classmethod
    def _provider_text(cls, value: Any) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("provider_id must be non-empty")
        return text

    @field_validator(
        "latency_multiplier",
        "input_token_multiplier",
        "output_token_multiplier",
        "input_cost_per_token_usd",
        "output_cost_per_token_usd",
        "verification_cost_usd",
        mode="before",
    )
    @classmethod
    def _finite_float(cls, value: Any) -> float:
        if isinstance(value, bool):
            raise TypeError("provider numeric fields cannot be booleans")
        number = float(value)
        if number != number or number in (float("inf"), float("-inf")):
            raise ValueError("provider numeric fields must be finite")
        return number

    @field_validator("verification_tokens", mode="before")
    @classmethod
    def _verification_token_int(cls, value: Any) -> int:
        if isinstance(value, bool):
            raise TypeError("verification_tokens must be an integer")
        return int(value)

    def input_tokens_for(self, task: ControlledTask) -> int:
        """Return the scaled context/input token count for ``task``."""

        return max(0, round(task.context_tokens * self.input_token_multiplier))

    def output_tokens_for(self, task: ControlledTask) -> int:
        """Return the scaled output token count for ``task``."""

        # Tasks have a positive nominal output budget.  Preserve at least one
        # token for any positive profile so a very cheap profile cannot erase
        # the work signal entirely.
        return max(1, round(task.token_cost * self.output_token_multiplier))

    def duration_for(self, task: ControlledTask) -> float:
        """Return the scaled simulated latency for ``task``."""

        return round(task.duration_seconds * self.latency_multiplier, 12)

    def cost_for(self, input_tokens: int, output_tokens: int) -> float:
        """Return deterministic model cost for one attempt."""

        return round(
            input_tokens * self.input_cost_per_token_usd
            + output_tokens * self.output_cost_per_token_usd,
            12,
        )


class ControlledTask(BaseModel):
    """One deterministic task in the benchmark graph."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    task_id: str = Field(min_length=1)
    token_cost: int = Field(default=100, ge=1)
    duration_seconds: float = Field(default=1.0, gt=0.0)
    context_tokens: int = Field(default=100, ge=0)
    progress_weight: float = Field(default=1.0, gt=0.0)
    criticality: int = Field(default=0, ge=0)
    input_stability: float = Field(default=1.0, ge=0.0, le=1.0)
    write_set: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    invalidated_by_change: bool = False

    @field_validator("task_id", mode="before")
    @classmethod
    def _task_text(cls, value: Any) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("task_id must be non-empty")
        return text

    @field_validator("write_set", "dependencies", mode="before")
    @classmethod
    def _tuple_text(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))


class ControlledScenario(BaseModel):
    """Immutable workload and the one external semantic-change event."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    scenario_id: str = "online-compute-v1"
    seed: int = Field(default=0, ge=0)
    tasks: tuple[ControlledTask, ...]
    max_parallelism: int = Field(default=2, ge=1)
    change_after_epoch: int = Field(default=1, ge=1)
    changed_task_ids: tuple[str, ...] = ()
    change_label: str = "api-schema"
    change_version: int = Field(default=2, ge=1)
    provider: SimulatedProvider = Field(default_factory=SimulatedProvider)

    @field_validator("tasks", mode="before")
    @classmethod
    def _task_tuple(cls, value: Any) -> tuple[ControlledTask, ...]:
        if value is None:
            return ()
        return tuple(
            item if isinstance(item, ControlledTask) else ControlledTask.model_validate(item)
            for item in value
        )

    @field_validator("changed_task_ids", mode="before")
    @classmethod
    def _changed_ids(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))

    @field_validator("provider", mode="before")
    @classmethod
    def _provider_profile(cls, value: Any) -> SimulatedProvider:
        if isinstance(value, SimulatedProvider):
            return value
        if value is None:
            return SimulatedProvider()
        return SimulatedProvider.model_validate(value)

    @field_validator("change_label", mode="before")
    @classmethod
    def _change_label_text(cls, value: Any) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("change_label must be non-empty")
        return text

    @model_validator(mode="after")
    def _validate_graph(self) -> ControlledScenario:
        ids = {task.task_id for task in self.tasks}
        if len(ids) != len(self.tasks):
            raise ValueError("scenario tasks must have unique task_id values")
        unknown_deps = {dep for task in self.tasks for dep in task.dependencies if dep not in ids}
        if unknown_deps:
            raise ValueError(f"unknown task dependencies: {sorted(unknown_deps)}")
        unknown_changes = set(self.changed_task_ids) - ids
        if unknown_changes:
            raise ValueError(f"unknown changed_task_ids: {sorted(unknown_changes)}")
        return self


class ControlledRun(BaseModel):
    """A complete deterministic run and its auditable utility metrics."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mode: ControlMode
    success: bool
    verified_task_ids: tuple[str, ...]
    records: tuple[ExecutionRecord, ...]
    selected_batches: tuple[tuple[str, ...], ...]
    events: tuple[str, ...]
    metrics: ComputationMetrics
    provider_id: str = "simulated-default"
    stale_task_ids: tuple[str, ...] = ()
    reexecuted_task_ids: tuple[str, ...] = ()
    verified_progress_trace: tuple[float, ...] = ()
    parallelism_trace: tuple[int, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        import json

        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


class ControlledComparison(BaseModel):
    """Comparison report with an explicit offline scope disclaimer."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    benchmark: str = "longhorizonos-online-compute-control"
    benchmark_version: int = 1
    scenario: ControlledScenario
    static: ControlledRun
    adaptive: ControlledRun
    comparison: dict[str, float | int | bool]
    valid: bool
    scope: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    def to_json(self) -> str:
        import json

        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


def default_scenario() -> ControlledScenario:
    """Return the canonical scenario used in docs and tests."""

    return ControlledScenario(
        tasks=(
            ControlledTask(
                task_id="api-risky",
                token_cost=300,
                duration_seconds=3.0,
                context_tokens=180,
                progress_weight=0.25,
                criticality=5,
                input_stability=0.15,
                write_set=("api-schema",),
                invalidated_by_change=True,
            ),
            ControlledTask(
                task_id="backend",
                token_cost=260,
                duration_seconds=2.0,
                context_tokens=160,
                progress_weight=0.30,
                criticality=4,
                input_stability=0.95,
                write_set=("api-schema",),
            ),
            ControlledTask(
                task_id="frontend",
                token_cost=220,
                duration_seconds=2.0,
                context_tokens=140,
                progress_weight=0.25,
                criticality=3,
                input_stability=0.90,
                write_set=("frontend",),
            ),
            ControlledTask(
                task_id="docs",
                token_cost=100,
                duration_seconds=1.0,
                context_tokens=80,
                progress_weight=0.20,
                criticality=1,
                input_stability=1.0,
                write_set=("docs",),
            ),
        ),
        max_parallelism=2,
        change_after_epoch=1,
        changed_task_ids=("api-risky",),
    )


def run_controlled_benchmark(
    scenario: ControlledScenario | Mapping[str, Any] | None = None,
    *,
    provider: SimulatedProvider | Mapping[str, Any] | None = None,
) -> ControlledComparison:
    """Run static and adaptive policies against the same deterministic graph."""

    if scenario is None:
        scenario = default_scenario()
    elif not isinstance(scenario, ControlledScenario):
        scenario = ControlledScenario.model_validate(scenario)
    if provider is not None:
        provider_model = (
            provider
            if isinstance(provider, SimulatedProvider)
            else SimulatedProvider.model_validate(provider)
        )
        scenario = scenario.model_copy(update={"provider": provider_model})
    static = _run_case(scenario, ControlMode.STATIC)
    adaptive = _run_case(scenario, ControlMode.ADAPTIVE)

    static_tokens = static.metrics.total_tokens
    adaptive_tokens = adaptive.metrics.total_tokens
    static_time = static.metrics.wall_time_seconds
    adaptive_time = adaptive.metrics.wall_time_seconds
    comparison: dict[str, float | int | bool] = {
        "same_success": static.success == adaptive.success,
        "same_verified_tasks": static.verified_task_ids == adaptive.verified_task_ids,
        "token_reduction": static_tokens - adaptive_tokens,
        "token_reduction_ratio": _ratio(static_tokens - adaptive_tokens, static_tokens),
        "wall_time_reduction_seconds": round(static_time - adaptive_time, 12),
        "wall_time_reduction_ratio": _ratio(static_time - adaptive_time, static_time),
        "stale_work_reduction": (
            static.metrics.stale_work_tokens - adaptive.metrics.stale_work_tokens
        ),
        "repeated_work_reduction": (
            static.metrics.repeated_work_tokens - adaptive.metrics.repeated_work_tokens
        ),
        "cost_reduction_usd": round(
            static.metrics.total_cost_usd - adaptive.metrics.total_cost_usd, 12
        ),
        "stale_attempt_reduction": (
            static.metrics.stale_attempts - adaptive.metrics.stale_attempts
        ),
        "reexecuted_attempt_reduction": (
            static.metrics.repeated_attempts - adaptive.metrics.repeated_attempts
        ),
        "reexecuted_task_reduction": (
            len(static.reexecuted_task_ids) - len(adaptive.reexecuted_task_ids)
        ),
        "adaptive_parallelism_peak": adaptive.metrics.peak_parallelism,
        "static_parallelism_peak": static.metrics.peak_parallelism,
        "adaptive_verified_progress_per_token": adaptive.metrics.verified_progress_per_token,
        "static_verified_progress_per_token": static.metrics.verified_progress_per_token,
        "adaptive_verified_progress_per_minute": adaptive.metrics.verified_progress_per_minute,
        "static_verified_progress_per_minute": static.metrics.verified_progress_per_minute,
    }
    valid = bool(
        static.success
        and adaptive.success
        and static.verified_task_ids == adaptive.verified_task_ids
        and adaptive.metrics.stale_work_tokens <= static.metrics.stale_work_tokens
        and adaptive.metrics.repeated_work_tokens <= static.metrics.repeated_work_tokens
    )
    scope = {
        "offline": True,
        "deterministic": True,
        # Backwards-compatible key retained for older consumers of the
        # benchmark artifact.  The more precise provider-aware key below is
        # preferred by new callers.
        "simulated_costs_and_durations": True,
        "simulated_costs_durations_and_provider": True,
        "provider_id": scenario.provider.provider_id,
        "does_not_measure": (
            "LLM quality, provider pricing, physical CPU/GPU/RAM/VRAM telemetry, "
            "distributed scheduling, hidden provenance discovery, or production throughput"
        ),
        "interpretation": (
            "This validates metric plumbing and a controlled state-change policy "
            "contrast; it is not evidence of real-model acceleration."
        ),
    }
    return ControlledComparison(
        scenario=scenario,
        static=static,
        adaptive=adaptive,
        comparison=comparison,
        valid=valid,
        scope=scope,
    )


def run_benchmark(
    scenario: ControlledScenario | Mapping[str, Any] | None = None,
    *,
    provider: SimulatedProvider | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Convenience wrapper returning a JSON-compatible mapping."""

    return run_controlled_benchmark(scenario, provider=provider).as_dict()


def run_multi_seed_benchmark(
    scenario: ControlledScenario | Mapping[str, Any] | None = None,
    *,
    seeds: Iterable[int] | None = None,
    provider: SimulatedProvider | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a bounded static-vs-adaptive sweep over explicit scenario seeds.

    The single-seed :func:`run_benchmark` contract is intentionally unchanged.
    This helper is a thin orchestration layer that runs that same contract once
    per seed and returns both the auditable per-seed reports and numeric
    mean/min/max summaries.  It is deliberately *not* a statistical claim:
    the canonical scenario is deterministic and currently carries the seed as
    metadata only.  Callers that want seed-dependent variation should provide
    a scenario whose task/change parameters are generated from each seed.

    ``seeds`` defaults to ``(0, 1, 2)`` and must contain distinct,
    non-negative integers.  Order is preserved in the ``runs`` list, making
    the output stable and easy to align with an external scenario generator.
    """

    base = _coerce_scenario(scenario)
    normalized_seeds = _normalize_seeds(seeds)

    runs: list[dict[str, Any]] = []
    reports: list[ControlledComparison] = []
    for seed in normalized_seeds:
        # ``seed`` is persisted in the report even when the canonical
        # controlled workload itself is seed-invariant.  This makes the
        # boundary explicit and lets callers provide seed-specialized
        # scenarios without changing the output schema.
        seeded = base.model_copy(update={"seed": seed})
        report = run_controlled_benchmark(seeded, provider=provider)
        reports.append(report)
        runs.append({"seed": seed, "report": report.as_dict()})

    summary = _summarize_multi_seed_reports(reports)
    scope = {
        "offline": True,
        "deterministic": True,
        "simulated_provider": True,
        "seed_count": len(normalized_seeds),
        "seed_semantics": (
            "The canonical scenario is seed-invariant; seeds are carried as "
            "auditable metadata. Seed-dependent variation requires caller-supplied "
            "scenario generation."
        ),
        "does_not_measure": (
            "LLM quality, provider pricing, physical CPU/GPU/RAM/VRAM telemetry, "
            "distributed scheduling, hidden provenance discovery, or production "
            "throughput"
        ),
        "interpretation": (
            "This is a bounded multi-seed aggregation of a deterministic simulator, "
            "not a statistically powered real-model evaluation."
        ),
    }
    return {
        "benchmark": "longhorizonos-online-compute-control-multi-seed",
        "benchmark_version": 1,
        "seeds": list(normalized_seeds),
        "runs": runs,
        "summary": summary,
        "valid": bool(summary["valid_all"]),
        "scope": scope,
    }


def _coerce_scenario(
    scenario: ControlledScenario | Mapping[str, Any] | None,
) -> ControlledScenario:
    if scenario is None:
        return default_scenario()
    if isinstance(scenario, ControlledScenario):
        return scenario
    return ControlledScenario.model_validate(scenario)


def _normalize_seeds(seeds: Iterable[int] | None) -> tuple[int, ...]:
    if seeds is None:
        return (0, 1, 2)
    if isinstance(seeds, (str, bytes, bytearray)):
        raise TypeError("seeds must be an iterable of non-negative integers")
    try:
        values = tuple(seeds)
    except TypeError as exc:
        raise TypeError("seeds must be an iterable of non-negative integers") from exc
    if not values:
        raise ValueError("seeds must contain at least one seed")
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError("seeds must contain only non-negative integers")
        if value < 0:
            raise ValueError("seeds must contain only non-negative integers")
        if value in normalized:
            raise ValueError(f"duplicate seed: {value}")
        normalized.append(value)
    return tuple(normalized)


def _numeric_summary(values: Iterable[int | float]) -> dict[str, int | float]:
    numbers = tuple(float(value) for value in values)
    if not numbers:
        return {"mean": 0.0, "min": 0.0, "max": 0.0, "count": 0}
    return {
        "mean": round(sum(numbers) / len(numbers), 12),
        "min": _normalize_number(min(numbers)),
        "max": _normalize_number(max(numbers)),
        "count": len(numbers),
    }


def _normalize_number(value: float) -> int | float:
    # Preserve integer-looking min/max values in JSON while keeping means
    # floating point.  This is cosmetic but makes CLI/report diffs readable.
    return int(value) if value.is_integer() else round(value, 12)


def _summarize_multi_seed_reports(
    reports: Iterable[ControlledComparison],
) -> dict[str, Any]:
    values = tuple(reports)
    if not values:
        # Defensive only: _normalize_seeds rejects an empty sweep.
        return {
            "valid_all": False,
            "seed_count": 0,
            "comparison": {},
            "static_metrics": {},
            "adaptive_metrics": {},
        }

    def collect_comparison(key: str) -> tuple[int | float, ...]:
        return tuple(
            value
            for report in values
            if isinstance((value := report.comparison.get(key)), (int, float))
            and not isinstance(value, bool)
        )

    def collect_metrics(mode: str, key: str) -> tuple[int | float, ...]:
        return tuple(
            value
            for report in values
            if isinstance(
                (value := getattr(report, mode).metrics.as_dict().get(key)),
                (int, float),
            )
            and not isinstance(value, bool)
        )

    comparison_keys = sorted(
        {
            key
            for report in values
            for key, value in report.comparison.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    metric_keys = sorted(
        {
            key
            for report in values
            for mode in ("static", "adaptive")
            for key, value in getattr(report, mode).metrics.as_dict().items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
    )
    comparison_summary = {key: _numeric_summary(collect_comparison(key)) for key in comparison_keys}
    static_summary = {key: _numeric_summary(collect_metrics("static", key)) for key in metric_keys}
    adaptive_summary = {
        key: _numeric_summary(collect_metrics("adaptive", key)) for key in metric_keys
    }
    return {
        "valid_all": all(report.valid for report in values),
        "seed_count": len(values),
        "same_verified_task_set_all": all(
            report.static.verified_task_ids == report.adaptive.verified_task_ids
            for report in values
        ),
        "comparison": comparison_summary,
        "static_metrics": static_summary,
        "adaptive_metrics": adaptive_summary,
    }


def _run_case(scenario: ControlledScenario, mode: ControlMode) -> ControlledRun:
    tasks = {task.task_id: task for task in scenario.tasks}
    pending = set(tasks)
    verified: set[str] = set()
    records: list[ExecutionRecord] = []
    batches: list[tuple[str, ...]] = []
    events: list[str] = []
    elapsed = 0.0
    event_fired = False
    epoch = 0
    verified_progress_trace: list[float] = []
    parallelism_trace: list[int] = []
    task_weights_total = sum(item.progress_weight for item in tasks.values()) or 1.0
    stale_task_ids: set[str] = set()

    while pending:
        ready = [
            tasks[task_id] for task_id in pending if set(tasks[task_id].dependencies) <= verified
        ]
        if not ready:
            # A malformed cyclic scenario should fail deterministically rather
            # than spin forever.
            events.append("deadlock:no-ready-tasks")
            break
        batch = _select_batch(ready, mode, scenario.max_parallelism, epoch, event_fired)
        if not batch:
            # Adaptive low-stability deferral: after the semantic event, all
            # tasks become eligible; before it, run at least one stable task.
            batch = (min(ready, key=lambda item: item.task_id),)
        batches.append(tuple(item.task_id for item in batch))
        parallelism_trace.append(len(batch))
        elapsed += max(scenario.provider.duration_for(item) for item in batch)

        conflict_winner_by_key: dict[str, str] = {}
        for item in sorted(batch, key=lambda task: task.task_id):
            for key in item.write_set:
                conflict_winner_by_key.setdefault(key, item.task_id)

        for item in batch:
            attempt = 1 + sum(1 for record in records if record.task_id == item.task_id)
            conflict = any(
                conflict_winner_by_key.get(key) != item.task_id for key in item.write_set
            )
            changed = (
                not event_fired
                and epoch < scenario.change_after_epoch
                and item.task_id in set(scenario.changed_task_ids)
            )
            stale = conflict or changed
            if stale:
                stale_task_ids.add(item.task_id)
            input_tokens = scenario.provider.input_tokens_for(item)
            output_tokens = scenario.provider.output_tokens_for(item)
            status: MetricStatus = "stale" if stale else "verified"
            record = ExecutionRecord(
                task_id=item.task_id,
                attempt=attempt,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                context_tokens=item.context_tokens,
                context_reread_tokens=(input_tokens if attempt > 1 else 0),
                wall_time_seconds=scenario.provider.duration_for(item),
                cost_usd=scenario.provider.cost_for(input_tokens, output_tokens),
                verification_tokens=scenario.provider.verification_tokens,
                verification_cost_usd=scenario.provider.verification_cost_usd,
                verified_progress=0.0,
                status=status,
                stale=stale,
                reused=attempt > 1,
                parallelism=len(batch),
            )
            records.append(record)
            if not stale:
                verified.add(item.task_id)
                pending.discard(item.task_id)
        verified_progress_trace.append(
            round(
                sum(tasks[item_id].progress_weight for item_id in verified) / task_weights_total,
                12,
            )
        )
        if not event_fired and epoch + 1 >= scenario.change_after_epoch:
            event_fired = True
            events.append(f"semantic-change:{scenario.change_label}@{scenario.change_version}")
        epoch += 1
        if epoch > len(tasks) * 4 + 4:
            events.append("guard:epoch-limit")
            break

    success = verified == set(tasks)
    metrics = aggregate_metrics(
        records,
        success=success,
        verified_progress=1.0
        if success
        else (
            sum(tasks[item].progress_weight for item in verified)
            / max(1e-12, sum(item.progress_weight for item in tasks.values()))
        ),
        wall_time_seconds=elapsed,
        preemptions=0,
        rebases=0 if mode is ControlMode.STATIC else 1 if events else 0,
        parallelism_samples=[len(batch) for batch in batches],
    )
    return ControlledRun(
        mode=mode,
        success=success,
        verified_task_ids=tuple(sorted(verified)),
        records=tuple(records),
        selected_batches=tuple(batches),
        events=tuple(events),
        metrics=metrics,
        provider_id=scenario.provider.provider_id,
        stale_task_ids=tuple(sorted(stale_task_ids)),
        reexecuted_task_ids=tuple(
            sorted(
                task_id
                for task_id in tasks
                if sum(1 for record in records if record.task_id == task_id) > 1
            )
        ),
        verified_progress_trace=tuple(verified_progress_trace),
        parallelism_trace=tuple(parallelism_trace),
    )


def _select_batch(
    ready: Iterable[ControlledTask],
    mode: ControlMode,
    max_parallelism: int,
    epoch: int,
    event_fired: bool,
) -> tuple[ControlledTask, ...]:
    values = tuple(ready)
    if mode is ControlMode.STATIC:
        return tuple(sorted(values, key=lambda item: item.task_id)[:max_parallelism])

    # Utility is deterministic and intentionally simple: expected progress
    # weighted by criticality and input stability per unit simulated cost.
    ranked = sorted(
        values,
        key=lambda item: (
            -(
                item.progress_weight
                * (1.0 + item.criticality / 10.0)
                * (0.25 + item.input_stability)
                / (item.token_cost + item.duration_seconds * 10.0)
            ),
            item.task_id,
        ),
    )
    selected: list[ControlledTask] = []
    used_writes: set[str] = set()
    for item in ranked:
        if (
            not event_fired
            and epoch < 1
            and item.input_stability < 0.5
            and item.invalidated_by_change
        ):
            continue
        if set(item.write_set) & used_writes:
            continue
        selected.append(item)
        used_writes.update(item.write_set)
        if len(selected) >= max_parallelism:
            break
    return tuple(selected)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    if float(denominator) <= 0.0:
        return 0.0
    return round(float(numerator) / float(denominator), 12)


__all__ = [
    "ControlMode",
    "ControlledComparison",
    "ControlledRun",
    "ControlledScenario",
    "ControlledTask",
    "SimulatedProvider",
    "default_scenario",
    "run_benchmark",
    "run_controlled_benchmark",
    "run_multi_seed_benchmark",
]
