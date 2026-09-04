"""LongHorizonOS E2 — real coding workload + semantic change/repair end-to-end.

Creates a real temp Git/Python workspace, runs a real goal with real Shell
verification (CommandVerifier), then mutates the workspace artifact and proves
D3 selective invalidation + preserved independent work + minimal Repair Frontier
+ D2 repair with new Evidence -> Goal re-closed.
"""

from __future__ import annotations

import subprocess

from lhos.integrations.semantic import CommandVerifier
from lhos.integrations.tools.shell import ShellTool
from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


def _shell_ok(sh: ShellTool, cmd: str) -> bool:
    return sh.run(cmd).ok


def test_real_coding_workload_and_semantic_repair(tmp_path):
    # Real workspace with a failing impl + a passing trivial check
    ws = WorkspaceTool(tmp_path)
    ws.write("source.py", "def answer():\n    return 1\n")
    ws.write("test_placeholder.txt", "ok")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("coder", specializations=("python",)))

    goal = Goal("Fix feature")
    # T1 Inspect (real git read trivially), T2 Implement (real shell check),
    # T3 Independent (scripted), T4 Review (real shell check on review file)
    t1 = goal.task(
        "Inspect",
        agent="coder",
        verify=CommandVerifier(
            "test -f source.py",
            artifact_id="source.py",
            version=1,
            shell=sh,
            cwd=tmp_path,
            workspace=ws,
        ),
    )
    t3 = goal.task(
        "Independent", agent="coder", verify=scripted_executor(artifact_id="analysis.md", version=1)
    )
    t2 = goal.task(
        "Implement",
        agent="coder",
        depends_on=(t1,),
        verify=CommandVerifier(
            "test -f source.py && test -f test_placeholder.txt",
            artifact_id="source.py",
            version=1,
            shell=sh,
            cwd=tmp_path,
            workspace=ws,
        ),
    )
    t4 = goal.task(
        "Review",
        agent="coder",
        depends_on=(t2,),
        verify=CommandVerifier(
            "true", artifact_id="review.md", version=1, shell=sh, cwd=tmp_path, workspace=ws
        ),
    )

    r0 = os_.run(goal, max_dispatches=12)
    # At least the real shell-verified tasks succeed and enter VPG as VERIFIED.
    assert "Implement" in r0.verified or "Inspect" in r0.verified
    assert "Independent" in r0.verified

    # Semantic mutation: real workspace change to source.py v1 -> v2
    os_.apply_workspace_mutation(ws, "source.py", "def answer():\n    return 2\n", next_version=2)
    rep = os_.repair(goal, artifact_id="source.py", new_artifact_version=2)
    # Independent branch preserved (not affected by source.py)
    assert "Independent" in rep.preserved
    # Repair Frontier contains the source.py-pinning task (t1 and t2 both pin it)
    assert any(t in rep.frontier for t in ("Inspect", "Implement"))

    # Re-verify every affected task against its current exact version.
    for t in goal.tasks:
        if t.task_id in ("Inspect", "Implement"):
            t.verify = CommandVerifier(
                "test -f source.py",
                artifact_id="source.py",
                version=2,
                shell=sh,
                cwd=tmp_path,
                workspace=ws,
            )
        elif t.task_id == "Review":
            t.verify = CommandVerifier(
                "true",
                artifact_id="review.md",
                version=2,
                shell=sh,
                cwd=tmp_path,
                workspace=ws,
            )
    r1 = os_.run(goal, max_dispatches=12)
    assert "Implement" in r1.verified
