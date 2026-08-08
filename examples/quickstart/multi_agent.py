"""LongHorizonOS — quickstart: multi_agent

Two scripted agents (a coder and a reviewer) with different specializations.
The D2 Scheduler deterministically matches each Task to an eligible agent, claims
it via a real Kernel Lease, and VPG derives VERIFIED from Evidence.  No kernel
lease surgery in user code.
"""

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

os_ = AgentOS(":memory:")
os_.add_agent(Agent("coder", specializations=("python",)))
os_.add_agent(Agent("reviewer", specializations=("review",)))

goal = Goal("Ship feature")
research = goal.task("Research", agent="coder",
                     verify=scripted_executor(artifact_id="research.md", version=1))
impl = goal.task("Implement", agent="coder", depends_on=(research,),
                 verify=scripted_executor(artifact_id="source.py", version=1))
review = goal.task("Review", agent="reviewer", depends_on=(impl,),
                   verify=scripted_executor(artifact_id="review.md", version=1))

result = os_.run(goal, max_dispatches=8)

print("VERIFIED:", result.verified)
print("Ownership:", result.owner_by_task)   # which agent held each task
