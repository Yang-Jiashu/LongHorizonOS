"""LongHorizonOS — quickstart: hello_world

A minimal first program using the public SDK.  No Core internals, no tests
imports, no API key.  Runs a real Kernel-backed Core instance, schedules a Goal
with one Task, attaches Evidence, and VPG derives VERIFIED.

Run from anywhere with `lhos` installed:  python hello_world.py
"""

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

os_ = AgentOS(":memory:")                                   # composition root
os_.add_agent(Agent("coder", specializations=("python",)))  # registers a real process

goal = Goal("Hello")
goal.task("Write hello", agent="coder",
          verify=scripted_executor(artifact_id="hello.txt", version=1))

result = os_.run(goal, max_dispatches=4)                     # drive to fixpoint

print("Goal:", goal.goal_id)
print("Task states:", result.task_states)
print("VERIFIED:", result.verified)
print("Ownership:", result.owner_by_task)
