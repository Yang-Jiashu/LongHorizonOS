"""Focused smoke tests for the bounded online-supervisor demo."""

from __future__ import annotations

import json
import subprocess
import sys

from lhos.demo.online_supervisor import run_online_supervisor


def test_online_supervisor_demo_reaches_verified_closure() -> None:
    agent_os, semantics = run_online_supervisor()
    try:
        assert semantics.bounded is True
        assert semantics.daemon_started is False
        assert semantics.uses_real_sdk is True
        assert semantics.uses_controlled_executor is True
        assert semantics.uses_llm is False
        assert semantics.start_state == "running"
        assert semantics.observation_status == "observed"
        assert semantics.execution_statuses == ["executed", "executed", "closed"]
        assert semantics.dispatched_task_ids == ["prepare", "validate", "publish"]
        assert semantics.verified_task_ids == ["prepare", "publish", "validate"]
        assert semantics.final_closed is True
        assert semantics.final_state == "closed"
        assert semantics.stop_reason == "goal_closed"
    finally:
        agent_os.close()


def test_online_supervisor_cli_json_is_machine_readable() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lhos.cli.core", "demo", "online-supervisor", "--json"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["demo"] == "online-supervisor"
    summary = payload["result"]
    assert summary["final_closed"] is True
    assert summary["daemon_started"] is False
    assert summary["uses_llm"] is False


def test_online_supervisor_cli_human_report_states_scope() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "lhos.cli.core", "demo", "online-supervisor"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "BOUNDED EPOCHS" in result.stdout
    assert "goal closed: YES" in result.stdout
    assert "no LLM, GPU telemetry, or always-on daemon" in result.stdout
