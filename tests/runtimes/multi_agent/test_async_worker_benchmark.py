"""Contract tests for the public AgentOS.run_async benchmark."""

from __future__ import annotations

import json

import pytest

import lhos.benchmarks.async_worker_runtime as benchmark_module
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
    assert report["comparison"]["repetitions"] == 3
    assert report["comparison"]["estimator"].startswith("median of paired")
    assert len(report["comparison"]["speedup_samples"]) == 3
    assert len(report["samples"]) == 3
    assert report["baseline"]["repetitions"] == 3
    assert report["async_runtime"]["repetitions"] == 3
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
    with pytest.raises(ValueError, match="repetitions"):
        run_benchmark(repetitions=0)


@pytest.mark.asyncio
async def test_benchmark_uses_median_of_paired_speedups(monkeypatch) -> None:
    serial_times = iter([1.0, 10.0, 11.0])
    parallel_times = iter([1.0, 2.0, 10.0])

    async def fake_run_agentos(*, mode: str, run_concurrency: int, task_count: int, **_) -> dict:
        elapsed = next(serial_times if run_concurrency == 1 else parallel_times)
        return {
            "mode": mode,
            "elapsed_seconds": elapsed,
            "completed_tasks": task_count,
            "peak_concurrency": run_concurrency,
            "peak_by_agent": {"agent-0": 1},
            "configured_max_concurrency": run_concurrency,
            "configured_agent_concurrency": {"agent-0": 1},
            "global_capacity_violations": 0,
            "agent_capacity_violations": 0,
            "capacity_violations": 0,
            "ownership_admission_violations": 0,
            "resource_admission_violations": 0,
            "peak_active_resource_reservations": run_concurrency,
            "active_resource_reservations_after_run": 0,
            "claim_count": task_count,
            "attempt_count": task_count,
            "correctness": {"all_checks": True},
        }

    monkeypatch.setattr(benchmark_module, "_run_agentos", fake_run_agentos)
    report = await benchmark_module.run_benchmark_async(
        task_count=1,
        repetitions=3,
        min_speedup=1.05,
    )

    assert report["comparison"]["speedup_samples"] == pytest.approx([1.0, 5.0, 1.1])
    assert report["comparison"]["speedup"] == pytest.approx(1.1)
    assert report["comparison"]["parallel_over_serial_ratio"] == pytest.approx(1 / 1.1)
    assert report["valid"] is True
