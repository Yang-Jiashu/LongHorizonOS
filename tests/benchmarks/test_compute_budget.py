"""Tests for the controlled verified-progress compute-budget benchmark."""

from __future__ import annotations

import json

from lhos.benchmarks.compute_budget import main, run_benchmark


def test_compute_budget_controlled_comparison_has_more_expected_progress() -> None:
    report = run_benchmark()

    assert report["valid"] is True
    assert report["violations"] == []
    assert report["comparison"]["same_declared_budget"] is True
    assert report["comparison"]["same_usage_before"] is True
    assert report["comparison"]["same_parallelism_limit"] is True
    assert report["static_lexical"]["selected_task_ids"] == [
        "repair",
        "a-low-yield",
    ]
    assert report["verified_progress_budget"]["selected_task_ids"] == [
        "repair",
        "z-high-yield",
    ]
    assert (
        report["comparison"]["budget_policy_expected_verified_progress_numerator"]
        > report["comparison"]["static_expected_verified_progress_numerator"]
    )
    assert report["comparison"]["expected_verified_progress_gain_units"] == 75


def test_compute_budget_benchmark_preserves_repair_priority() -> None:
    repair = run_benchmark()["repair_priority"]

    assert repair["repair_utility_is_lower_than_high_yield"] is True
    assert repair["static_selected_repair_first"] is True
    assert repair["budget_policy_ranked_repair_first"] is True
    assert repair["budget_policy_selected_repair_first"] is True


def test_compute_budget_benchmark_unknown_estimates_fail_closed() -> None:
    case = run_benchmark()["unknown_estimate_case"]

    assert case["passed"] is True
    assert case["selected_task_ids"] == ["known"]
    assert set(case["deferred_task_ids"]) == {"missing", "unknown"}
    assert case["reasons"]["missing"] == "estimate_unknown"
    assert case["reasons"]["unknown"] == "estimate_unknown"
    assert case["safe_under_declared_budget"] is False


def test_compute_budget_benchmark_checks_every_hard_dimension() -> None:
    cases = run_benchmark()["hard_budget_dimension_cases"]

    assert set(cases) == {
        "tokens",
        "wall_time_ms",
        "cost_microusd",
        "context_tokens",
        "verification_tokens",
    }
    for dimension, case in cases.items():
        assert case["passed"] is True
        assert case["selected_task_ids"] == []
        assert case["reason"] == "budget_exceeded"
        assert case["budget_blockers"] == [dimension]


def test_compute_budget_benchmark_is_repeatable_and_scope_is_honest() -> None:
    first = run_benchmark()
    second = run_benchmark()

    assert first == second
    assert first["scope"]["controlled_estimates"] is True
    assert "not evidence of real-model performance" in first["scope"]["interpretation"]
    assert json.loads(json.dumps(first))["benchmark"] == "compute_budget_controlled"


def test_compute_budget_benchmark_module_cli_emits_json(capsys) -> None:
    assert main() == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["valid"] is True
    assert payload["benchmark"] == "compute_budget_controlled"
