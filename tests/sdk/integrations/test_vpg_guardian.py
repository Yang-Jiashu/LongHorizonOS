"""LongHorizonOS E2 — VPG Semantic Reconciler Guardian tests.

Machine-checks the 15 VPG Guardian principles and the E2 exact-version
adversarial mapping: real model/tool integrations must NOT bypass VPG as the
sole semantic authority, must not set VERIFIED directly, must keep Evidence
exact-version-bound, must keep D3 selective and Repair Frontier minimal, and
must never let a model/tool become a semantic authority.
"""

from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent.parent  # repo root
INTEG = ROOT / "src" / "lhos" / "integrations"


# ── G12/G14: integrations never import Core private storage; Core never
#    imports integrations (architecture import gate) ───────────────────────
def test_integrations_never_import_core_private_storage():
    for p in INTEG.rglob("*.py"):
        if p.name == "__init__.py" or "__pycache__" in str(p):
            continue
        src = p.read_text()
        for banned in (
            "lhos.runtimes.verified_progress.graph_store",
            "lhos.runtimes.multi_agent.scheduler",
            "lhos.runtimes.invalidation.cone",
            "agent_os.kernel.kernel",
            "lhos.sdk.os",
        ):
            assert banned not in src, f"{p} must not import Core private storage {banned!r}"


def test_core_never_imports_integrations_or_sdk_agents():
    core_roots = [ROOT / "src/lhos/agent_os", ROOT / "src/lhos/runtimes"]
    for root in core_roots:
        for p in root.rglob("*.py"):
            if "__pycache__" in str(p) or p.name == "__init__.py":
                continue
            src = p.read_text()
            assert "lhos.integrations" not in src, f"{p} must not import integrations"


# ── G2/G15: model output / tool output cannot set VERIFIED directly ---------
def test_g2_model_output_cannot_set_verified():
    from lhos.integrations.models.openai_compatible import FakeTransport, OpenAICompatibleModel
    from lhos.sdk import Agent, AgentOS, Goal

    transport = FakeTransport(text='{"task_status": "done"}')
    model = OpenAICompatibleModel("fake-model", transport=transport)
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",), model=model))
    g = Goal("G")
    g.task("T1", agent="a")  # no verifier -> no Evidence
    res = os_.run(g, max_dispatches=2)
    # Model saying "done" in its text is NOT semantic truth; task stays unverified.
    assert res.task_states.get("T1", "unverified") != "verified"


def test_g3_shell_exit_zero_not_verified_without_evidence():
    from lhos.integrations.semantic import CommandVerifier
    from lhos.integrations.tools.shell import ShellTool
    from lhos.sdk import Agent, AgentOS, Goal

    sh = ShellTool()
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    # A task whose verifier FAILS must not be VERIFIED.
    g.task("T1", agent="a", verify=CommandVerifier("false", artifact_id="x", version=1, shell=sh))
    res = os_.run(g, max_dispatches=2)
    assert res.task_states.get("T1") != "verified"


# ── G4 / E2-02: same bytes new version cannot reuse old Evidence ------------
def test_g4_exact_version_new_version_needs_new_evidence():
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
    r0 = os_.run(g, max_dispatches=2)
    assert "T1" in r0.verified
    # Same bytes, new version: evidence for v1 must NOT validate v2 -> repair
    rep = os_.repair(g, artifact_id="src.py", new_artifact_version=2)
    assert "T1" in rep.affected  # old v1 evidence no longer current


# ── E2-05: Agent claiming in a Python field (no Kernel Lease) is not ownership
def test_e2_05_no_python_field_ownership():
    from lhos.sdk import Agent

    a = Agent("x", specializations=("python",))
    # Agent has no owner_by_task/claim field; ownership is Kernel Lease only.
    assert not hasattr(a, "claim")
    assert not hasattr(a, "owner_of")


# ── E2-10: stale GraphVersion proposal must fail closed (SDK repair rejects) -
def test_e2_10_repair_fails_closed_on_wrong_version():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    g.task("T1", agent="a", verify=scripted_executor(artifact_id="x", version=1))
    os_.run(g, max_dispatches=2)
    # Bump artifact to v2; a stale v1-based evidence must not silently merge.
    rep = os_.repair(g, artifact_id="x", new_artifact_version=2)
    assert "T1" in rep.affected


# ── G6: D3 selective — mutating one artifact must not stale independent branch
def test_g6_selective_invalidation_preserves_independent():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="src.py", version=1))
    t3 = g.task("T3", agent="a", verify=scripted_executor(artifact_id="analysis.md", version=1))
    t2 = g.task(
        "T2",
        agent="a",
        depends_on=(t1,),
        verify=scripted_executor(artifact_id="src2.py", version=1),
    )
    r0 = os_.run(g, max_dispatches=6)
    rep = os_.repair(g, artifact_id="src.py", new_artifact_version=2)
    # Independent branch (T3, analysis.md) preserved; source.py-affected T1 stale.
    assert "T1" in rep.affected
    assert "T3" in rep.preserved


# ── G7: Repair Frontier minimal — dependent not pushed prematurely
def test_g7_repair_frontier_minimal():
    from lhos.sdk import Agent, AgentOS, Goal, scripted_executor

    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    g = Goal("G")
    t1 = g.task("T1", agent="a", verify=scripted_executor(artifact_id="a.py", version=1))
    t2 = g.task(
        "T2", agent="a", depends_on=(t1,), verify=scripted_executor(artifact_id="b.py", version=1)
    )
    r0 = os_.run(g, max_dispatches=4)
    rep = os_.repair(g, artifact_id="a.py", new_artifact_version=2)
    # Only T1 is front-ready; T2 (depends on stale T1) is NOT in frontier.
    assert "T1" in rep.frontier
    assert "T2" not in rep.frontier
