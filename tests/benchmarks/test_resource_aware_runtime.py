"""Tests for the deterministic logical-resource adaptive benchmark."""

from __future__ import annotations

import json

from lhos.benchmarks.resource_aware_runtime import main, run_benchmark


def test_resource_aware_runtime_controlled_gate() -> None:
    report = run_benchmark()

    assert report["valid"]
    assert report["violations"] == []
    assert report["static"]["closure"]
    assert report["resource_aware"]["closure"]
    assert report["static"]["verified_task_ids"] == report["resource_aware"]["verified_task_ids"]

    # The resource-blind fixed batch first proposes 700 + 700 against a
    # 1,000-millicore pool.  Scheduler admits only one and replans.
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
    assert report["static"]["proposal_capacity_violations"] == 1
    assert report["static"]["scheduler_resource_rejections"] == 1

    # Resource-aware packing selects 700 + 300 in both epochs.
    assert report["resource_aware"]["selected_batches"] == [
        ["a-heavy", "c-light"],
        ["b-heavy", "d-light"],
    ]
    assert report["resource_aware"]["admitted_batches"] == [
        ["a-heavy", "c-light"],
        ["b-heavy", "d-light"],
    ]
    assert report["resource_aware"]["epochs"] == 2
    assert report["resource_aware"]["proposal_capacity_violations"] == 0
    assert report["resource_aware"]["scheduler_resource_rejections"] == 0

    # Scheduler and executor safety hold in both cases; only the advisory
    # resource-blind proposal is over capacity.
    for mode in ("static", "resource_aware"):
        assert report[mode]["admitted_capacity_violations"] == 0
        assert report[mode]["executor_capacity_violation_events"] == 0
        assert report[mode]["executor_peak_resources"]["cpu_millis"] == 1_000
        assert report[mode]["dispatched_attempts"] == 4

    assert report["comparison"] == {
        "same_verified_goal": True,
        "epoch_reduction": 1,
        "proposal_capacity_violation_reduction": 1,
        "scheduler_resource_rejection_reduction": 1,
    }


def test_resource_aware_runtime_report_is_repeatable() -> None:
    assert run_benchmark() == run_benchmark()


def test_resource_aware_runtime_module_cli_emits_json(capsys) -> None:
    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["benchmark"] == "resource_aware_adaptive_runtime"
    assert payload["valid"] is True
    assert payload["resource_aware"]["epochs"] == 2
