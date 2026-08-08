"""LongHorizonOS E2 — semantic mutation tests (integration adapters).

Mutating a critical integration behavior must break an invariant test (KILLED).
We focus on the highest-value adapters: the CommandVerifier/evidence path and
the model/tool semantic-bypass guard.  A non-equivalent mutation must yield a
failing assertion (0 non-equivalent survivor).
"""

from __future__ import annotations


def test_mutation_shell_success_does_not_bypass_evidence():
    """If a shell success were trusted directly (no Evidence), a FAIL verifier
    would wrongly produce VERIFIED.  Assert it does NOT (VPG-G3)."""
    from lhos.integrations.semantic import CommandVerifier
    from lhos.integrations.tools.shell import ShellTool
    from lhos.sdk import Agent, AgentOS, Goal

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a", verify=CommandVerifier("false", artifact_id="x", version=1, shell=sh))
    res = os_.run(g, max_dispatches=2)
    # Even if a buggy adapter returned success, the invariant is: no VERIFIED
    # without a real Evidence (here a FAIL verifier yields none).
    assert res.task_states.get("T1") != "verified"


def test_mutation_no_verifier_never_verifies():
    """A task with no verifier must stay unverified (VPG-G2), even if a broken
    adapter tried to force-complete it."""
    from lhos.sdk import Agent, AgentOS, Goal

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a")  # no verifier
    res = os_.run(g, max_dispatches=2)
    assert res.task_states.get("T1") != "verified"


def test_mutation_exact_version_evidence_required():
    """Evidence must be exact-version; a v1 evidence must not be reused to prove
    v2.  Mutating that would break the affected-set (VPG-G4)."""
    from lhos.integrations.semantic import CommandVerifier
    from lhos.integrations.tools.shell import ShellTool
    from lhos.sdk import Agent, AgentOS, Goal

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task(
        "T1", agent="a", verify=CommandVerifier("true", artifact_id="src.py", version=1, shell=sh)
    )
    os_.run(g, max_dispatches=2)
    rep = os_.repair(g, artifact_id="src.py", new_artifact_version=2)
    # v1 evidence no longer current => T1 affected
    assert "T1" in rep.affected


def test_mutation_artifact_mutation_does_not_global_stale():
    """Changing one artifact must not mark an independent branch stale (G6)."""
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="a.py", version=1))
    t3 = g.task("T3", agent="a", verify=scripted_executor(artifact_id="b.md", version=1))
    t2 = g.task(
        "T2", agent="a", depends_on=(t1,), verify=scripted_executor(artifact_id="a2.py", version=1)
    )
    os_.run(g, max_dispatches=6)
    rep = os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    assert "T3" in rep.preserved  # independent branch stays verified
    assert "T1" in rep.affected


def test_mutation_repair_frontier_minimal():
    """Repair Frontier must stay minimal (G7) — a dependent with a stale dep is
    not front-ready."""
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="a.py", version=1))
    t2 = g.task(
        "T2", agent="a", depends_on=(t1,), verify=scripted_executor(artifact_id="b.py", version=1)
    )
    os_.run(g, max_dispatches=4)
    rep = os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    assert "T1" in rep.frontier
    assert "T2" not in rep.frontier
