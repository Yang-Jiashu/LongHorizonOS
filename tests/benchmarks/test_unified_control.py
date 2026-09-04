"""Tests for the controlled unified compute-policy benchmark."""

from __future__ import annotations

import json

from lhos.benchmarks.unified_control import main, run_benchmark


def test_unified_control_closes_same_goal_and_avoids_rework() -> None:
    report = run_benchmark()

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["comparison"]["same_verified_goal"] is True
    assert report["unified"]["goal_closed"] is True
    assert report["static_fifo"]["goal_closed"] is True
    assert report["unified"]["declared_verified_progress_units"] == 100
    assert report["static_fifo"]["declared_verified_progress_units"] == 100

    assert report["unified"]["stale_attempts"] == 0
    assert report["static_fifo"]["stale_attempts"] == 1
    assert report["comparison"]["rework_risk_avoidance_units"] == 10
    assert report["comparison"]["stale_attempt_reduction"] == 1


def test_unified_control_reduces_admission_rejections_and_epochs() -> None:
    report = run_benchmark()

    assert report["static_fifo"]["scheduler_rejections"] == 2
    assert report["budget_only"]["scheduler_rejections"] == 2
    assert report["resource_conflict"]["scheduler_rejections"] == 0
    assert report["unified"]["scheduler_rejections"] == 0
    assert report["static_fifo"]["epochs"] == 4
    assert report["unified"]["epochs"] == 3
    assert report["comparison"]["scheduler_rejection_reduction"] == 2
    assert report["comparison"]["epoch_reduction"] == 1


def test_unified_control_budget_accounting_is_declared_and_fail_closed() -> None:
    report = run_benchmark()

    # Static FIFO pays for the stale backend attempt and overruns every
    # declared dimension; the composed policy remains within the same limits.
    assert set(report["static_fifo"]["budget_overrun_dimensions"]) == {
        "tokens",
        "wall_time_ms",
        "cost_microusd",
        "context_tokens",
        "verification_tokens",
    }
    assert report["unified"]["budget_overrun_dimensions"] == []
    assert report["comparison"]["static_budget_overrun"] is True
    assert report["comparison"]["unified_budget_within_declared_limits"] is True
    assert report["unified"]["budget_usage"] == {
        "tokens": 640,
        "wall_time_ms": 640,
        "cost_microusd": 64,
        "context_tokens": 320,
        "verification_tokens": 110,
    }
    assert report["comparison"]["token_reduction"] == 240


def test_unified_control_reports_component_policy_traces_and_scope() -> None:
    first = run_benchmark()
    second = run_benchmark()

    assert first == second
    assert first["unified"]["policy_ids"] == {
        "budget": "verified-progress-budget.v1",
        "resource_conflict": "resource-aware-conflict-greedy.v1",
    }
    assert first["scope"]["offline"] is True
    assert first["scope"]["budget_semantics"].startswith("declared estimate")
    assert "not evidence of real-world speed" in first["scope"]["interpretation"]
    assert json.loads(json.dumps(first))["benchmark"] == "unified_compute_control"


def test_unified_control_module_cli_emits_json(capsys) -> None:
    assert main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["benchmark"] == "unified_compute_control"
    assert payload["valid"] is True
