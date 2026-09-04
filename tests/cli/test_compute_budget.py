"""CLI gates for the controlled compute-budget benchmark."""

from __future__ import annotations

import json

from lhos.cli import core


def test_compute_budget_cli_json_is_machine_readable(capsys) -> None:
    assert core.main(["benchmark", "compute-budget", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["benchmark"] == "compute_budget_controlled"
    assert report["valid"] is True
    assert report["comparison"]["budget_policy_has_more_expected_verified_progress"] is True
    assert all(case["passed"] for case in report["hard_budget_dimension_cases"].values())


def test_compute_budget_cli_human_report_is_explicit_about_scope(capsys) -> None:
    assert core.main(["benchmark", "compute-budget"]) == 0
    output = capsys.readouterr().out

    assert "CONTROLLED COMPUTE-BUDGET BENCHMARK" in output
    assert "deterministic declared estimates" in output
    assert "not real LLM performance" in output
    assert "all-budget-dimensions=PASS" in output
