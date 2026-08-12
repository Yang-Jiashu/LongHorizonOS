"""Contract tests for the public AgentOS.run_async benchmark."""

from __future__ import annotations

import json

import pytest

from lhos.benchmarks.async_worker_runtime import run_benchmark


def test_benchmark_runs_public_sdk_to_verified_closure_with_bounded_overlap() -> None:
    report = run_benchmark(
        task_count=8,
        delay_seconds=0.015,
        max_concurrency=4,
        agent_concurrency=2,
        agent_count=2,
        min_speedup=1.05,
    )

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["benchmark"] == "agentos_run_async_end_to_end"
    assert report["baseline"]["completed_tasks"] == 8
    assert report["async_runtime"]["completed_tasks"] == 8
    assert report["async_runtime"]["peak_concurrency"] <= 4
    assert report["async_runtime"]["capacity_violations"] == 0
    assert report["async_runtime"]["ownership_admission_violations"] == 0
    assert report["async_runtime"]["resource_admission_violations"] == 0
    assert report["async_runtime"]["active_resource_reservations_after_run"] == 0
    assert all(report["async_runtime"]["correctness"].values())
    assert report["comparison"]["speedup"] >= 1.05
    assert report["scope"]["public_agentos_run_async"] is True
    assert report["scope"]["scheduler_resource_admission"] is True


def test_benchmark_report_is_json_serializable_and_rejects_bad_inputs() -> None:
    report = run_benchmark(
        task_count=4,
        delay_seconds=0.005,
        max_concurrency=2,
        agent_concurrency=1,
        agent_count=2,
        min_speedup=0.01,
    )
    rendered = json.dumps(report)
    assert '"agentos_run_async_end_to_end"' in rendered

    with pytest.raises(ValueError, match="task_count"):
        run_benchmark(task_count=0)
