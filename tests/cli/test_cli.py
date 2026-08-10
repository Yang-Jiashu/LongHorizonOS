"""LongHorizonOS E3 — CLI tests (Core-native status/inspect/graph, read-only)."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from lhos.cli import core
from lhos.sdk import Agent, AgentOS, scripted_executor


def _make_run(tmp_path, mutation: bool = False) -> str:
    db = tmp_path / "state" / "state.sqlite"
    db.parent.mkdir(parents=True, exist_ok=True)
    os_ = AgentOS(str(db))
    os_.add_agent(Agent("coder", specializations=("python",)))
    g = os_.goal("G")
    t1 = g.task("Inspect", agent="coder", verify=scripted_executor(artifact_id="a.py", version=1))
    g.task("Independent", agent="coder", verify=scripted_executor(artifact_id="b.md", version=1))
    g.task(
        "Implement",
        agent="coder",
        depends_on=(t1,),
        verify=scripted_executor(artifact_id="a2.py", version=1),
    )
    os_.run(g, max_dispatches=8)
    if mutation:
        os_._facts.add_version("a.py", 2, "v2")
        os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    manifest = tmp_path / "run.json"
    os_.save_run(str(manifest))
    return str(manifest)


def _cli(args: list[str], manifest: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "lhos.cli.core", *args, "--state", manifest],
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_cli_status_human(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    r = _cli(["status", "--goal", "G"], m)
    assert r.returncode == 0, r.stderr
    assert "LONGHORIZONOS STATUS" in r.stdout
    assert "Inspect" in r.stdout


def test_cli_status_json(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    r = _cli(["status", "--goal", "G", "--json"], m)
    assert r.returncode == 0, r.stderr
    data = json.loads(r.stdout)
    assert data["schema_version"] == "0.1"
    assert data["goal"] == "G"
    assert "tasks" in data and "repair_frontier" in data


def test_cli_status_json_is_deterministic(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    a = _cli(["status", "--goal", "G", "--json"], m).stdout
    b = _cli(["status", "--goal", "G", "--json"], m).stdout
    assert a == b


def test_cli_status_json_is_sorted(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    data = json.loads(_cli(["status", "--goal", "G", "--json"], m).stdout)
    keys = list(data.keys())
    assert keys == sorted(keys)


def test_cli_graph(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    r = _cli(["graph", "--goal", "G"], m)
    assert r.returncode == 0, r.stderr
    assert "Goal: G" in r.stdout


def test_cli_inspect_task(tmp_path):
    m = _make_run(tmp_path, mutation=True)
    r = _cli(["inspect", "task", "Inspect", "--goal", "G"], m)
    assert r.returncode == 0, r.stderr
    assert "Inspect" in r.stdout


def test_cli_query_is_non_mutating_graph_version(tmp_path):
    m = _make_run(tmp_path, mutation=False)
    os_ = AgentOS.open_run(m)
    gid = os_._gid_for("G")
    before = os_.vpg.get_graph(gid).current_version
    _cli(["status", "--goal", "G", "--json"], m)
    _cli(["graph", "--goal", "G"], m)
    after = os_.vpg.get_graph(gid).current_version
    assert before == after, "CLI observability must not change GraphVersion (OBS-G4)"


def test_cli_legacy_route(tmp_path, monkeypatch):
    """`lhos legacy` routes to the legacy spec-20 CLI without crashing."""
    r = subprocess.run(
        [sys.executable, "-m", "lhos.cli.core", "legacy", "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    # legacy --help should show the legacy subcommands
    assert r.returncode == 0, r.stderr
    assert "init" in r.stdout


def test_cli_legacy_init_routes_without_reparsing_prefix(tmp_path):
    db = tmp_path / "legacy.sqlite"
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "lhos.cli.core",
            "legacy",
            "init",
            "--db",
            str(db),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 0, r.stderr
    assert db.exists()


def test_benchmark_invalid_trial_reports_failure(monkeypatch, capsys):
    monkeypatch.setattr(
        "lhos.benchmarks.semantic_repair.run.run_benchmark",
        lambda quick: {
            "benchmark": "semantic-repair",
            "quick": quick,
            "total_trials": 2,
            "valid_trials": 1,
            "invalid_trials": 1,
            "aggregate": {"overall": {}},
            "raw_sha256": "abc",
        },
    )
    assert core._benchmark(quick=True, as_json=False) == 1
    assert "correctness: FAIL" in capsys.readouterr().out


def test_benchmark_json_is_machine_readable(monkeypatch, capsys):
    monkeypatch.setattr(
        "lhos.benchmarks.semantic_repair.run.run_benchmark",
        lambda quick: {
            "benchmark": "semantic-repair",
            "quick": quick,
            "total_trials": 1,
            "valid_trials": 1,
            "invalid_trials": 0,
            "aggregate": {"overall": {}},
            "raw_sha256": "abc",
        },
    )
    assert core._benchmark(quick=False, as_json=True) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["quick"] is False
    assert data["correctness_passed"] is True


def test_benchmark_modes_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        core.build_parser().parse_args(["benchmark", "--quick", "--full"])


def test_cli_state_not_found(tmp_path):
    r = subprocess.run(
        [
            sys.executable,
            "-m",
            "lhos.cli.core",
            "status",
            "--state",
            str(tmp_path / "missing.json"),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode != 0
    assert "not found" in r.stderr


def test_secret_not_leaked_in_json(tmp_path):
    m = _make_run(tmp_path, mutation=False)
    r = _cli(["status", "--goal", "G", "--json"], m)
    assert "API_KEY=" not in r.stdout


@pytest.mark.parametrize(
    ("raw", "secret"),
    [
        ("API_KEY=sk-secret", "sk-secret"),
        ("TOKEN: token-secret", "token-secret"),
        ("Authorization: Bearer bearer-secret", "bearer-secret"),
        ("Authorization: Basic basic-secret", "basic-secret"),
    ],
)
def test_redact_common_secret_forms(raw, secret):
    redacted = core._redact(raw)
    assert secret not in redacted
    assert "***" in redacted
