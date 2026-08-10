"""End-to-end tests for Context VM demo scripts.

Each demo script is executed as a subprocess and is expected to emit a JSON
object on stdout whose ``demo`` field matches the scenario name. These tests
cover exactly five demos: the minimum set needed to assert that primary
Context VM behaviors are wired and observable.

  1. basic_load           — a successful materialized load
  2. budget_eviction      — pinning blocks eviction under pressure
  3. version_pinning      — working set is pinned to a committed version
  4. snapshot_restore     — snapshot/restore cycle preserves materialized hash
  5. process_isolation    — cross-PID handle access is denied
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[3])


def _run(script: str) -> dict:
    """Run a demo script and return its parsed JSON stdout."""
    r = subprocess.run(
        [sys.executable, f"examples/agent_os/{script}"],
        cwd=_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 0, (
        f"demo {script} failed (rc={r.returncode}): stdout={r.stdout!r} stderr={r.stderr!r}"
    )
    return json.loads(r.stdout)


def test_demo_basic_load() -> None:
    data = _run("context_basic_load.py")
    assert data["demo"] == "basic_load"
    assert "materialized_hash" in data


def test_demo_budget_eviction() -> None:
    data = _run("context_budget_eviction.py")
    assert data["demo"] == "budget_eviction"
    assert data["eviction_blocked_pinned"] is True


def test_demo_version_pinning() -> None:
    data = _run("context_version_pinning.py")
    assert data["demo"] == "version_pinning"
    assert data["v1_still_v1_after_v2_commit"] is True


def test_demo_snapshot_restore() -> None:
    data = _run("context_snapshot_restore.py")
    assert data["demo"] == "snapshot_restore"
    assert data["hashes_match"] is True


def test_demo_process_isolation() -> None:
    data = _run("context_process_isolation.py")
    assert data["demo"] == "process_isolation"
    assert data["p1_read"] is True and data["p2_read_blocked"] is True
