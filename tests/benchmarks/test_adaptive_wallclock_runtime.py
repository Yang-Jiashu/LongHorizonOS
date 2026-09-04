"""Tests for the bounded real-wall-clock adaptive AgentOS benchmark."""

from __future__ import annotations

import json

import pytest

from lhos.benchmarks.adaptive_wallclock_runtime import main, run_benchmark


def test_adaptive_wallclock_runtime_structural_gate() -> None:
    report = run_benchmark(delay_seconds=0.002)

    assert report["valid"]
    assert report["violations"] == []
    assert report["comparison"]["same_verified_goal"]
    assert report["comparison"]["epoch_reduction"] == 1
    assert report["comparison"]["scheduler_resource_rejection_reduction"] == 1
    assert report["comparison"]["wall_clock_is_informational_not_a_gate"] is True
    assert report["workload"]["declared_conflicts"] == [
        {
            "left_task_id": "a-heavy",
            "right_task_id": "d-light",
            "resource": "workspace://shared-control",
            "reason": "write_write",
        }
    ]

    assert report["static"]["selected_batches"] == [
        ["a-heavy", "b-heavy"],
        ["b-heavy", "c-light"],
        ["d-light"],
    ]
    assert report["static"]["admitted_batches"] == [
        ["a-heavy"],
        ["b-heavy", "c-light"],
        ["d-light"],
    ]
    assert report["static"]["epochs"] == 3
    assert report["static"]["scheduler_resource_rejections"] == 1

    assert report["adaptive"]["selected_batches"] == [
        ["a-heavy", "c-light"],
        ["b-heavy", "d-light"],
    ]
    assert report["adaptive"]["admitted_batches"] == [
        ["a-heavy", "c-light"],
        ["b-heavy", "d-light"],
    ]
    assert report["adaptive"]["epochs"] == 2
    assert report["adaptive"]["scheduler_resource_rejections"] == 0

    for mode in ("static", "adaptive"):
        case = report[mode]
        audit = case["runtime_audit"]
        assert case["closure"]
        assert case["elapsed_seconds"] > 0
        assert case["executor_peak_parallelism"] == 2
        assert case["executor_peak_resources"]["cpu_millis"] == 1_000
        assert case["executor_capacity_violation_events"] == 0
        assert case["correctness"]["selected_batches_conflict_safe"]
        assert case["correctness"]["admitted_batches_conflict_safe"]
        assert case["dispatched_attempts"] == 4
        assert all(case["correctness"].values())
        assert audit["scheduler_attempts"] == 4
        assert audit["claims_created"] == 4
        assert audit["claims_with_kernel_lease"] == 4
        assert audit["claims_with_positive_fence"] == 4
        assert audit["attempt_state_counts"]["verified_semantically"] == 4
        assert audit["pass_evidence_nodes"] == 4
        assert audit["active_claims_after_run"] == 0
        assert audit["active_reservations_after_run"] == 0
        assert audit["live_kernel_leases_after_run"] == 0

    assert report["scope"]["real_asyncio_sleep"] is True
    assert report["scope"]["simulated_clock"] is False
    assert report["scope"]["wall_clock_gate"] is False
    assert "TaskClaim" in report["scope"]["authoritative_path"]
    assert "Kernel Lease" in report["scope"]["authoritative_path"]


def test_adaptive_wallclock_runtime_does_not_gate_observed_speed() -> None:
    report = run_benchmark(delay_seconds=0.001)

    assert report["valid"]
    # The value is observable but intentionally not asserted to be > 1:
    # local SQLite/CI/OS noise is allowed to dominate a tiny bounded run.
    assert isinstance(report["comparison"]["observed_speedup"], float)
    assert report["comparison"]["observed_speedup"] > 0


@pytest.mark.parametrize("delay", [0, -0.1, True, "0.1"])
def test_adaptive_wallclock_runtime_rejects_invalid_delay(delay: object) -> None:
    with pytest.raises(ValueError, match="delay_seconds"):
        run_benchmark(delay_seconds=delay)


def test_adaptive_wallclock_runtime_module_cli_emits_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["benchmark"] == "adaptive_wallclock_runtime"
    assert report["valid"] is True
    assert report["static"]["epochs"] == 3
    assert report["adaptive"]["epochs"] == 2
