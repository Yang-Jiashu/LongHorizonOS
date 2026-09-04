"""CLI gates for the deterministic logical-resource runtime benchmark."""

from __future__ import annotations

import json

from lhos.cli import core


def test_resource_aware_runtime_cli_json_is_machine_readable(capsys) -> None:
    assert core.main(["benchmark", "resource-aware-runtime", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)

    assert report["benchmark"] == "resource_aware_adaptive_runtime"
    assert report["valid"] is True
    assert report["static"]["epochs"] == 3
    assert report["resource_aware"]["epochs"] == 2
    assert report["static"]["proposal_capacity_violations"] == 1
    assert report["resource_aware"]["proposal_capacity_violations"] == 0


def test_resource_aware_runtime_cli_human_report_is_explicit_about_scope(
    capsys,
) -> None:
    assert core.main(["benchmark", "resource-aware-runtime"]) == 0
    output = capsys.readouterr().out

    assert "RESOURCE-AWARE ADAPTIVE RUNTIME BENCHMARK" in output
    assert "epochs=3" in output
    assert "epochs=2" in output
    assert "deterministic synthetic logical-resource workload" in output
    assert "no wall-clock, physical GPU/CPU, or real-LLM acceleration claim" in output
