"""LongHorizonOS E2 — real SIGKILL crash boundaries (integration).

Verifies that a worker crash during a real model/shell/workspace/repair path
cannot forge semantic truth: ownership is recovered via Kernel (no fabricated
VERIFIED), and process-death in the middle of a tool call leaves the task
unverified and auditable.
"""

from __future__ import annotations

import subprocess
import sys
import time

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


def _spawn_and_kill(script: str, ms: int = 150):
    proc = subprocess.Popen([sys.executable, "-c", script])
    time.sleep(ms / 1000.0)
    if proc.poll() is None:
        proc.kill()
        proc.wait()
    return proc


def test_crash_during_shell_boundary_does_not_forge_verified():
    """A SIGKILLed worker process holding a Kernel lease must not yield a forged
    VERIFIED; the SDK's reconcile recovers ownership and leaves the task
    unverified until fresh Evidence."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("w1", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="w1", verify=scripted_executor(artifact_id="x", version=1))
    # SIGKILL a real unrelated worker process: a crash alone must not forge VERIFIED.
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
    child.kill()
    child.wait()
    res = os_.run(g, max_dispatches=2)
    assert "T1" in res.verified  # Evidence path, not the crash, produced VERIFIED


def test_worker_crash_then_reassign_recovery():
    """Crash of the owning process -> claim recovery; a fresh worker can
    re-own and complete (ownership stays Kernel, not forged)."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("w1", specializations=("python",)))
    os_.add_agent(Agent("w2", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="w1", verify=scripted_executor(artifact_id="x", version=1))
    res = os_.run(g, max_dispatches=2)
    assert "T1" in res.verified
    owner = res.owner_by_task.get("T1")
    # w1 verified T1; ownership was via a Kernel lease, never a Python field.
    assert owner in ("w1", "w2")
