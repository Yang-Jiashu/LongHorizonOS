"""LongHorizonOS — quickstart: repair

Shows the Core differentiator through the SDK:
  1. A Goal with tasks VERIFIED (via Evidence).
  2. An ArtifactVersion mutates (source.py v1 -> v2).
  3. D3 recomputes applicability: only affected descendants go STALE,
     unaffected VERIFIED work is preserved, and a minimal Repair Frontier is
     derived.  D2 re-schedules to restore closure with fresh Evidence.
"""

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

os_ = AgentOS(":memory:")
os_.add_agent(Agent("coder", specializations=("python",)))
os_.add_agent(Agent("reviewer", specializations=("review",)))

goal = Goal("Ship")
t1 = goal.task(
    "Research", agent="coder", verify=scripted_executor(artifact_id="research.md", version=1)
)
t3 = goal.task(
    "Independent",
    agent="reviewer",
    required_specializations=("review",),
    verify=scripted_executor(artifact_id="analysis.md", version=1),
)
t2 = goal.task(
    "Implement",
    agent="coder",
    depends_on=(t1,),
    verify=scripted_executor(artifact_id="source.py", version=1),
)
t4 = goal.task(
    "Review",
    agent="reviewer",
    depends_on=(t2,),
    required_specializations=("review",),
    verify=scripted_executor(artifact_id="review.md", version=1),
)

result = os_.run(goal, max_dispatches=10)
print("Initially VERIFIED:", result.verified)

# World changes: source.py bumps to v2
repair = os_.repair(goal, artifact_id="source.py", new_artifact_version=2)
print("D3 affected (-> STALE):", repair.affected)  # T2, T4
print("D3 preserved (VERIFIED):", repair.preserved)  # T1, T3
print("Repair Frontier:", repair.frontier)  # minimal [T2]

# Re-run with fresh Evidence to restore closure
restored = os_.run(goal, max_dispatches=10)
print("Re-verified:", restored.verified)
