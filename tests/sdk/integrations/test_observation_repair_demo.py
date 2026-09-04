"""Smoke gate for the public observation-token quickstart."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
DEMO = REPO / "examples" / "quickstart" / "observation_repair.py"


def test_observation_repair_quickstart_runs_safe_public_path() -> None:
    env = dict(os.environ)
    src = str(REPO / "src")
    env["PYTHONPATH"] = src + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    completed = subprocess.run(
        [sys.executable, str(DEMO)],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["schema_version"] == "observation-repair-demo-v1"
    assert result["initial_closed"] is True
    assert result["final_closed"] is True
    assert result["token"]["artifact_id"] == "source.py"
    assert result["token"]["version"] == 2
    assert len(result["token"]["content_hash"]) == 64
    assert result["token"]["graph_id"]
    assert result["affected"] == ["Build", "Review"]
    assert result["preserved"] == ["Independent"]
    assert result["repair_frontier"] == ["Build"]

    source = DEMO.read_text(encoding="utf-8")
    assert "observe_artifact" in source
    assert "repair(goal, observation=token)" in source
    assert "new_artifact_version" not in source
