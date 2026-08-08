"""LongHorizonOS — quickstart: real_coding_task

A real (deterministic, no-API-key) coding workload: a temp Git/Python project is
created, an agent's Tasks are verified with a real Shell (CommandVerifier)
through the Core path, then the workspace Artifact is mutated and D3 performs
selective semantic reconciliation + minimal repair with new Evidence.

Run from anywhere with lhos installed:  python real_coding_task.py
Optional real model: pass LHOS_MODEL_API_KEY/LHOS_MODEL_BASE_URL to use an
OpenAI-compatible provider; the deterministic scripted/shell path is the default
and does not require a key.
"""

import subprocess
import tempfile

from lhos.integrations.semantic import CommandVerifier
from lhos.integrations.tools.shell import ShellTool
from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

# 1) real workspace
tmp = tempfile.mkdtemp(prefix="lhos_real_coding_")
ws = WorkspaceTool(tmp)
ws.write("source.py", "def answer():\n    return 1\n")
ws.write("test_placeholder.txt", "ok")
subprocess.run(["git", "init", "-q", tmp], check=True)

# 2) real shell verifier over the workspace
sh = ShellTool()

os_ = AgentOS(":memory:")
os_.add_agent(Agent("coder", specializations=("python",)))

# 3) goal + tasks (T1 real shell, T3 scripted independent, T2 real shell,
#    T4 review)
goal = Goal("Fix feature")
t1 = goal.task(
    "Inspect",
    agent="coder",
    verify=CommandVerifier(
        "test -f source.py", artifact_id="source.py", version=1, shell=sh, cwd=tmp, workspace=ws
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
        cwd=tmp,
        workspace=ws,
    ),
)
goal.task(
    "Review",
    agent="coder",
    depends_on=(t2,),
    verify=CommandVerifier(
        "true", artifact_id="review.md", version=1, shell=sh, cwd=tmp, workspace=ws
    ),
)

r0 = os_.run(goal, max_dispatches=12)
print("Phase 1 VERIFIED:", sorted(r0.verified))

# 4) world change: mutate the real workspace Artifact source.py v1 -> v2
os_.apply_workspace_mutation(ws, "source.py", "def answer():\n    return 2\n", next_version=2)
rep = os_.repair(goal, artifact_id="source.py", new_artifact_version=2)
print("D3 affected (->STALE):", sorted(rep.affected))
print("D3 preserved (VERIFIED):", sorted(rep.preserved))
print("Repair Frontier:", sorted(rep.frontier))

# 5) re-verify with fresh Evidence (real shell on v2) -> re-close
for t in goal.tasks:
    if t.task_id == "Implement":
        t.verify = CommandVerifier(
            "test -f source.py", artifact_id="source.py", version=2, shell=sh, cwd=tmp, workspace=ws
        )
r1 = os_.run(goal, max_dispatches=12)
print("Phase 2 VERIFIED:", sorted(r1.verified))
print("GOAL RECLOSED")
