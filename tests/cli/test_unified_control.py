"""Public API and CLI gates for unified adaptive compute control."""

from __future__ import annotations

import json

import lhos.sdk as sdk
from lhos.benchmarks.unified_control import run_benchmark
from lhos.cli import core
from lhos.sdk import unified_policy


def test_unified_policy_public_api_exports_every_declared_symbol() -> None:
    for name in unified_policy.__all__:
        assert name in sdk.__all__
        assert getattr(sdk, name) is getattr(unified_policy, name)


def test_unified_control_cli_json_emits_the_benchmark_report_unchanged(capsys) -> None:
    expected = run_benchmark()

    assert core.main(["benchmark", "unified-control", "--json"]) == 0

    assert json.loads(capsys.readouterr().out) == expected


def test_unified_control_cli_human_report_exposes_required_comparison(capsys) -> None:
    assert core.main(["benchmark", "unified-control"]) == 0
    output = capsys.readouterr().out

    assert "UNIFIED COMPUTE-CONTROL BENCHMARK" in output
    assert "valid: PASS" in output
    assert "same VERIFIED Goal: YES" in output
    assert "epochs: static=4 unified=3" in output
    assert "scheduler rejections: static=2 unified=0" in output
    assert "stale attempts: static=1 unified=0" in output
    assert "declared tokens: static=880 unified=640" in output


def test_unified_control_is_listed_in_benchmark_help(capsys) -> None:
    parser = core.build_parser()

    try:
        parser.parse_args(["benchmark", "--help"])
    except SystemExit as exc:
        assert exc.code == 0

    assert "unified-control" in capsys.readouterr().out
