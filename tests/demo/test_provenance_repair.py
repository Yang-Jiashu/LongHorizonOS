"""Focused tests for the v0.2 provenance-repair demo."""

from __future__ import annotations

import json
import subprocess
import sys

from lhos.demo.provenance_repair import run_provenance_repair


def test_provenance_repair_semantics_are_fail_closed_and_selective(tmp_path):
    workspace, semantics = run_provenance_repair(
        workspace_dir=str(tmp_path / "workspace"),
        state_path=str(tmp_path / "workspace" / "provenance.jsonl"),
    )

    assert workspace.exists()
    assert semantics.initial_coverage
    assert set(semantics.initial_coverage.values()) == {"COMPLETE"}
    assert semantics.hidden_probe_coverage == "UNKNOWN"
    assert semantics.strict_fail_closed is True
    assert semantics.hidden_probe_strict_allowed is False
    assert semantics.affected_tasks == ["ComputeValuation", "FetchReport", "WriteConclusion"]
    assert semantics.preserved_tasks == ["IndependentResearch"]
    assert semantics.repair_frontier == ["FetchReport"]
    assert semantics.final_coverage == semantics.initial_coverage
    assert semantics.durable_replay is True
    assert semantics.journal_event_count > 0
    assert semantics.automatic_dependency_discovery is False


def test_provenance_repair_cli_json(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "lhos.cli.core", "demo", "provenance-repair", "--json"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["demo"] == "provenance-repair"
    result_data = payload["result"]
    assert result_data["strict_fail_closed"] is True
    assert result_data["durable_replay"] is True
    assert result_data["repair_frontier"] == ["FetchReport"]


def test_provenance_repair_cli_is_registered():
    from lhos.cli.core import build_parser

    args = build_parser().parse_args(["demo", "provenance-repair", "--json"])
    assert args.command == "demo"
    assert args.which == "provenance-repair"
    assert args.json is True
