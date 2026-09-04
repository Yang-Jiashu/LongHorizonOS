"""Tests for online-computation utility metrics and controlled comparison."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from lhos.benchmarks.adaptive_control import (
    ControlledScenario,
    SimulatedProvider,
    default_scenario,
    run_benchmark,
    run_controlled_benchmark,
)
from lhos.benchmarks.computation_utility import (
    ComputationMetrics,
    ExecutionRecord,
    aggregate_metrics,
    verified_progress_per_minute,
    verified_progress_per_token,
)


def test_online_compute_provider_profile_scales_auditable_metrics() -> None:
    profile = SimulatedProvider(
        provider_id="cheap-sim",
        latency_multiplier=1.5,
        input_token_multiplier=0.5,
        output_token_multiplier=0.5,
    )
    report = run_controlled_benchmark(provider=profile)

    assert report.valid is True
    assert report.scenario.provider.provider_id == "cheap-sim"
    assert report.static.provider_id == "cheap-sim"
    assert report.adaptive.provider_id == "cheap-sim"
    assert report.static.metrics.total_tokens == 1380
    assert report.adaptive.metrics.total_tokens == 720
    assert report.static.metrics.wall_time_seconds == 15.0
    assert report.adaptive.metrics.wall_time_seconds == 7.5
    assert report.comparison["stale_attempt_reduction"] == 3
    assert report.comparison["reexecuted_task_reduction"] == 2
    assert report.adaptive.verified_progress_trace[-1] == 1.0


def test_provider_profile_rejects_non_positive_multiplier() -> None:
    with pytest.raises(ValidationError, match="latency_multiplier"):
        SimulatedProvider(latency_multiplier=0)


def test_aggregate_metrics_covers_long_horizon_compute_costs() -> None:
    metrics = aggregate_metrics(
        [
            ExecutionRecord(
                task_id="a",
                input_tokens=100,
                output_tokens=20,
                cached_tokens=40,
                context_tokens=90,
                wall_time_seconds=30,
                cost_usd=0.01,
                verification_tokens=10,
                verification_cost_usd=0.002,
                status="stale",
                stale=True,
                parallelism=2,
            ),
            ExecutionRecord(
                task_id="a",
                attempt=2,
                input_tokens=60,
                output_tokens=20,
                cached_tokens=20,
                context_tokens=50,
                context_reread_tokens=30,
                wall_time_seconds=15,
                cost_usd=0.006,
                verification_tokens=10,
                verification_cost_usd=0.002,
                verified_progress=1.0,
                status="verified",
                reused=True,
                parallelism=1,
            ),
        ],
        success=True,
        wall_time_seconds=45,
        preemptions=1,
        rebases=1,
    )

    assert metrics.success
    assert metrics.verified_progress == 1.0
    assert metrics.total_tokens == 200
    assert metrics.billable_tokens == 140
    assert metrics.context_tokens == 140
    assert metrics.context_reread_tokens == 30
    assert metrics.stale_work_tokens == 120
    assert metrics.repeated_work_tokens == 80
    assert metrics.stale_attempts == 1
    assert metrics.repeated_attempts == 1
    assert metrics.preemptions == 1
    assert metrics.rebases == 1
    assert metrics.verification_tokens == 20
    assert metrics.verification_calls == 2
    assert metrics.total_cost_usd == pytest.approx(0.02)
    assert metrics.average_parallelism == 1.5
    assert metrics.peak_parallelism == 2
    assert metrics.verified_progress_per_token == 0.005
    assert metrics.verified_progress_per_minute == pytest.approx(4 / 3)


def test_utility_denominators_are_fail_safe() -> None:
    assert verified_progress_per_token(1.0, 0) == 0.0
    assert verified_progress_per_minute(1.0, 0.0) == 0.0
    assert aggregate_metrics([], verified_progress=0.0) == ComputationMetrics()


def test_metric_models_reject_invalid_costs() -> None:
    with pytest.raises(ValidationError):
        ExecutionRecord(task_id="x", input_tokens=-1)
    with pytest.raises(TypeError, match="preemptions"):
        aggregate_metrics([], preemptions=True)
    with pytest.raises(ValueError, match="verified_progress"):
        aggregate_metrics([], verified_progress=1.1)


def test_controlled_benchmark_same_goal_with_less_adaptive_rework() -> None:
    report = run_controlled_benchmark()

    assert report.valid
    assert report.static.success and report.adaptive.success
    assert report.static.verified_task_ids == report.adaptive.verified_task_ids
    assert report.static.verified_task_ids == (
        "api-risky",
        "backend",
        "docs",
        "frontend",
    )
    assert report.static.metrics.stale_work_tokens > 0
    assert report.static.metrics.repeated_work_tokens > 0
    assert report.adaptive.metrics.stale_work_tokens == 0
    assert report.adaptive.metrics.repeated_work_tokens == 0
    assert report.adaptive.metrics.total_tokens < report.static.metrics.total_tokens
    assert (
        report.adaptive.metrics.verified_progress_per_token
        > report.static.metrics.verified_progress_per_token
    )
    assert (
        report.adaptive.metrics.verified_progress_per_minute
        > report.static.metrics.verified_progress_per_minute
    )


def test_graph_change_alters_adaptive_epoch_selection() -> None:
    report = run_controlled_benchmark()
    risky = "api-risky"

    assert risky in report.static.selected_batches[0]
    assert risky not in report.adaptive.selected_batches[0]
    assert risky in report.adaptive.selected_batches[1]
    assert report.static.events == report.adaptive.events == ("semantic-change:api-schema@2",)
    # Explicit write/write conflict is never selected in one adaptive batch.
    for batch in report.adaptive.selected_batches:
        assert not {"api-risky", "backend"} <= set(batch)


def test_controlled_benchmark_is_deterministic_except_no_wall_clock_exists() -> None:
    first = run_controlled_benchmark()
    second = run_controlled_benchmark()

    assert first == second
    assert first.static.metrics.wall_time_seconds == 10.0
    assert first.adaptive.metrics.wall_time_seconds == 5.0
    assert first.scope["simulated_costs_and_durations"] is True
    assert "not evidence of real-model acceleration" in first.scope["interpretation"]


def test_report_and_metrics_are_json_serializable() -> None:
    report = run_controlled_benchmark()

    report_json = report.to_json()
    metrics_json = report.adaptive.metrics.to_json()
    assert json.loads(report_json)["valid"] is True
    assert json.loads(metrics_json)["schema_version"] == "computation-utility.v1"
    assert report_json == report.to_json()
    mapping = run_benchmark()
    assert json.loads(json.dumps(mapping))["adaptive"]["success"] is True


def test_scenario_boundary_rejects_unknown_dependencies_and_change_targets() -> None:
    base = default_scenario().model_dump(mode="json")
    base["tasks"][0]["dependencies"] = ["missing"]
    with pytest.raises(ValidationError, match="unknown task dependencies"):
        ControlledScenario.model_validate(base)

    base = default_scenario().model_dump(mode="json")
    base["changed_task_ids"] = ["missing"]
    with pytest.raises(ValidationError, match="unknown changed_task_ids"):
        ControlledScenario.model_validate(base)
