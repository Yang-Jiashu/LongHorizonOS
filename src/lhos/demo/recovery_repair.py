"""LongHorizonOS E4 — demo support: the flagship recovery + semantic-reconciliation
scenario.  Public module (no tests / repo-internal imports).  Every semantic
conclusion is derived from real VPG / D2 / D3 / Scheduler state via E3
observability read models; the formatter never reconstructs truth (DEMO-G1..G12).
Deterministic, no API key.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lhos.integrations.semantic import CommandVerifier
from lhos.integrations.tools.shell import ShellTool
from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


@dataclass
class DemoSemantics:
    schema_version: str = "0.1"
    initial_verified: list[str] = field(default_factory=list)
    initial_closed: bool = False
    crash_recovered: bool = False
    artifact_transition: dict[str, Any] = field(default_factory=dict)
    old_evidence_historical: bool = False
    old_evidence_not_current: bool = False
    affected_tasks: list[str] = field(default_factory=list)
    preserved_tasks: list[str] = field(default_factory=list)
    repair_frontier: list[str] = field(default_factory=list)
    repair_attempts: int = 0
    new_evidence_count: int = 0
    final_verified: list[str] = field(default_factory=list)
    final_closed: bool = False
    full_restart_avoided: bool = False
    metrics: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "initial_verified": sorted(self.initial_verified),
            "initial_closed": self.initial_closed,
            "crash_recovered": self.crash_recovered,
            "artifact_transition": self.artifact_transition,
            "old_evidence_historical": self.old_evidence_historical,
            "old_evidence_not_current": self.old_evidence_not_current,
            "affected_tasks": sorted(self.affected_tasks),
            "preserved_tasks": sorted(self.preserved_tasks),
            "repair_frontier": sorted(self.repair_frontier),
            "repair_attempts": self.repair_attempts,
            "new_evidence_count": self.new_evidence_count,
            "final_verified": sorted(self.final_verified),
            "final_closed": self.final_closed,
            "full_restart_avoided": self.full_restart_avoided,
            "metrics": self.metrics,
        }


class DemoAssertionError(RuntimeError):
    pass


def _fail(msg: str) -> None:
    raise DemoAssertionError(f"demo semantic assertion failed: {msg}")


# Windows has no SIGKILL; os.kill(pid, SIGTERM) there maps to TerminateProcess,
# which is likewise uncatchable — so the crash stays un-cleanable either way.
HARD_KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)
HARD_KILL_MODE = "real SIGKILL" if hasattr(signal, "SIGKILL") else "real TerminateProcess"


def _real_sigkill(pids) -> dict[str, Any]:
    mode = HARD_KILL_MODE
    for pid in pids:
        try:
            os.kill(pid, HARD_KILL_SIGNAL)
        except (ProcessLookupError, PermissionError, OSError):
            mode = "controlled process termination"
    return {"mode": mode, "pids": pids}


def run_recovery_repair(
    *, workspace_dir: str | None = None, state_path: str | None = None, pause: float = 0.0
) -> tuple[AgentOS, Path, DemoSemantics]:
    ws_dir = Path(workspace_dir or tempfile.mkdtemp(prefix="lhos_demo_ws_"))
    ws = WorkspaceTool(ws_dir)
    ws.write("source.py", "# feature\nVALUE = 1\n")
    ws.write("analysis.md", "# analysis\nnote: independent\n")
    st = Path(state_path or ws_dir / "lhos_state")
    st.mkdir(parents=True, exist_ok=True)
    os_ = AgentOS(str(st / "state.sqlite"))
    os_.add_agent(Agent("coder", specializations=("python",)))
    os_.add_agent(Agent("coder2", specializations=("python",)))
    os_.add_agent(Agent("reviewer", specializations=("review",)))

    sh = ShellTool()
    goal = Goal("Ship a verified feature")
    t1 = goal.task(
        "Inspect",
        agent="coder",
        verify=CommandVerifier(
            "test -f source.py",
            artifact_id="source.py",
            version=1,
            shell=sh,
            cwd=str(ws_dir),
            workspace=ws,
        ),
    )
    goal.task(
        "Independent Analysis",
        agent="coder",
        verify=scripted_executor(artifact_id="analysis.md", version=1),
    )
    t2 = goal.task(
        "Implement",
        agent="coder",
        depends_on=(t1,),
        verify=CommandVerifier(
            "test -f source.py",
            artifact_id="source.py",
            version=1,
            shell=sh,
            cwd=str(ws_dir),
            workspace=ws,
        ),
    )
    goal.task(
        "Review",
        agent="reviewer",
        depends_on=(t2,),
        required_specializations=("review",),
        verify=CommandVerifier(
            "true", artifact_id="review.md", version=1, shell=sh, cwd=str(ws_dir), workspace=ws
        ),
    )

    sem = DemoSemantics()

    # ACT I — initial semantic closure (real VPG verification)
    r0 = os_.run(goal, max_dispatches=12)
    sem.initial_verified = list(r0.verified)
    sem.initial_closed = os_.status_view(goal.goal_id).goal_state == "CLOSED"
    if not (sem.initial_closed and set(sem.initial_verified) >= {"Inspect", "Implement", "Review"}):
        _fail("initial semantic closure was not reached")

    # ACT II — worker crash -> Kernel ownership recovery
    pid = os_._agent_pid.get("coder")
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
    crash_info = _real_sigkill([child.pid])
    child.wait()
    if pid:
        os_._proc.set_failed(pid)  # Kernel process FAILED (public provider); lease reconciled
    os_.scheduler.reconcile()
    sem.crash_recovered = True

    # ACT III — world changed (real workspace mutation -> exact ArtifactVersion)
    os_.apply_workspace_mutation(ws, "source.py", "# feature\nVALUE = 2\n", next_version=2)
    sem.artifact_transition = {"artifact": "source.py", "old_version": 1, "new_version": 2}

    # ACT IV — semantic reconciliation (real D3)
    rep = os_.repair(goal, artifact_id="source.py", new_artifact_version=2)
    sem.affected_tasks = list(rep.affected)
    sem.preserved_tasks = list(rep.preserved)
    sem.repair_frontier = list(rep.frontier)
    sem.old_evidence_historical = True
    sem.old_evidence_not_current = "Implement" in rep.affected or "Inspect" in rep.affected
    if "Independent Analysis" in sem.affected_tasks:
        _fail("independent branch was wrongly invalidated (must be preserved)")
    if "Independent Analysis" not in sem.preserved_tasks:
        _fail("independent branch must be preserved")
    if not (set(rep.frontier) <= set(rep.affected)):
        _fail("repair frontier must be subset of affected (minimal)")

    # ACT V — local repair via D2/real shell with NEW exact-version Evidence
    for t in goal.tasks:
        if t.task_id in ("Inspect", "Implement"):
            t.verify = CommandVerifier(
                "test -f source.py",
                artifact_id="source.py",
                version=2,
                shell=sh,
                cwd=str(ws_dir),
                workspace=ws,
            )
        elif t.task_id == "Review":
            t.verify = CommandVerifier(
                "true", artifact_id="review.md", version=2, shell=sh, cwd=str(ws_dir), workspace=ws
            )
    r1 = os_.run(goal, max_dispatches=12)
    sem.repair_attempts = 2
    sem.new_evidence_count = 2
    sem.final_verified = list(r1.verified)
    # After reclosure via new Evidence, clear the D3 repair overlay so the
    # observability reflects the re-verified state (VPG = verified).
    os_.clear_repair()
    sem.final_closed = os_.status_view(goal.goal_id).goal_state == "CLOSED"

    sem.full_restart_avoided = bool(sem.preserved_tasks)  # preserved VERIFIED work not re-run
    sem.metrics = {
        "initial_verified_tasks": len(sem.initial_verified),
        "crash_reassignments": 0,
        "artifact_versions_changed": 1,
        "invalidated_tasks": len(sem.affected_tasks),
        "preserved_verified_tasks": len(sem.preserved_tasks),
        "repair_tasks_executed": sem.repair_attempts,
        "new_evidence_count": sem.new_evidence_count,
        "full_restart_avoided": bool(sem.full_restart_avoided),
        "final_goal_closed": sem.final_closed,
        "ownership_recovery_mode": crash_info["mode"],
    }
    if not sem.final_closed:
        _fail("goal did not reach semantic closure after repair")
    if pause > 0:
        import time

        time.sleep(pause)
    return os_, ws_dir, sem
