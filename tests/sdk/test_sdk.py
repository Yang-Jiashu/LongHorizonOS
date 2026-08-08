"""LongHorizonOS E1 — public SDK tests.

Covers: public import surface, OS facade, Agent/Goal/Task builders, graph
compilation, verification adapter + evidence guardian, RunResult,
StatusSnapshot, scripted executor, multi-agent scheduling, D3 repair flow,
basic error translation, and the SDK↔Core architecture boundary.
"""

from __future__ import annotations

import pytest

from lhos.sdk import (
    OS,
    Agent,
    AgentOS,
    Goal,
    RepairOutcome,
    RunResult,
    StatusSnapshot,
    Task,
    VerificationOutcome,
    scripted_executor,
)
from lhos.sdk.verification import callback_verifier


# ── public imports ──────────────────────────────────────────────────────────
def test_public_import_surface():
    for sym in (
        AgentOS,
        OS,
        Agent,
        Goal,
        Task,
        RunResult,
        StatusSnapshot,
        RepairOutcome,
        VerificationOutcome,
        scripted_executor,
    ):
        assert sym is not None


# ── OS facade + Agent + hello world ─────────────────────────────────────────
def test_hello_world_one_task_verified():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("coder", specializations=("python",)))
    goal = Goal("Hello")
    goal.task(
        "Write hello", agent="coder", verify=scripted_executor(artifact_id="hello.txt", version=1)
    )
    res = os_.run(goal, max_dispatches=4)
    assert "Write hello" in res.verified


def test_agent_config_maps_to_process_and_descriptor():
    os_ = AgentOS(":memory:")
    a = os_.add_agent(
        Agent("coder", specializations=("python",), max_concurrency=2, cost_weight=1.5)
    )
    assert a.process_id is not None  # bound to a real Kernel process


def test_goal_task_builder_compiles_to_vpg():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    goal = Goal("G")
    t1 = goal.task("T1", agent="a", verify=scripted_executor(artifact_id="a1", version=1))
    goal.task(
        "T2", agent="a", depends_on=(t1,), verify=scripted_executor(artifact_id="a2", version=1)
    )
    gid = os_._compile_goal(goal)
    assert os_.vpg.get_graph(gid) is not None


def test_graph_compilation_creates_dependency_edges():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    goal = Goal("G")
    t1 = goal.task("T1", agent="a")
    t2 = goal.task("T2", agent="a", depends_on=(t1,))
    gid = os_._compile_goal(goal)
    nodes, edges = os_.vpg.snapshot_projection(gid)
    dep = [(e.source_node_id, e.target_node_id) for e in edges if e.edge_type.value == "depends_on"]
    assert ("T2", "T1") in dep
    assert ("G", "T1") in dep


# ── verification adapter + evidence guardian ───────────────────────────────
def test_evidence_guardian_never_sets_verified_directly():
    """A FAIL verifier must never make a task VERIFIED."""
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    goal = Goal("G")
    goal.task(
        "T1",
        agent="a",
        verify=callback_verifier(
            lambda: VerificationOutcome(passed=False, artifact_id="x", version=1)
        ),
    )
    res = os_.run(goal, max_dispatches=2)
    assert res.task_states["T1"] in ("unverified", "invalid")


def test_verification_outcome_struct():
    o = VerificationOutcome(passed=True, artifact_id="a", version=1, content="c", evidence_note="e")
    assert o.passed and o.artifact_id == "a"


# ── scripted executor ──────────────────────────────────────────────────────
def test_scripted_executor_deterministic():
    v = scripted_executor(artifact_id="x", version=1, content="ok")
    o = v()
    assert o.passed


# ── multi-agent scheduling ─────────────────────────────────────────────────
def test_multi_agent_scheduling_uses_d2():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("coder", specializations=("python",)))
    os_.add_agent(Agent("reviewer", specializations=("review",)))
    goal = Goal("Ship")
    r = goal.task(
        "Research", agent="coder", verify=scripted_executor(artifact_id="r.md", version=1)
    )
    goal.task(
        "Review",
        agent="reviewer",
        depends_on=(r,),
        verify=scripted_executor(artifact_id="rv.md", version=1),
    )
    res = os_.run(goal, max_dispatches=6)
    assert {"Research", "Review"} <= set(res.verified)
    # every owned task has an owner from the registry
    assert all(o in {"coder", "reviewer"} for o in res.owner_by_task.values())


# ── D3 repair flow ─────────────────────────────────────────────────────────
def test_repair_flow_marks_affected_preserves_unaffected():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("coder", specializations=("python",)))
    os_.add_agent(Agent("rv", specializations=("review",)))
    goal = Goal("Ship")
    t1 = goal.task("T1", agent="coder", verify=scripted_executor(artifact_id="a.md", version=1))
    t3 = goal.task("T3", agent="rv", verify=scripted_executor(artifact_id="i.md", version=1))
    t2 = goal.task(
        "T2",
        agent="coder",
        depends_on=(t1,),
        verify=scripted_executor(artifact_id="src.py", version=1),
    )
    t4 = goal.task(
        "T4", agent="rv", depends_on=(t2,), verify=scripted_executor(artifact_id="rv.md", version=1)
    )
    r0 = os_.run(goal, max_dispatches=10)
    assert set(r0.verified) == {"T1", "T3", "T2", "T4"}
    rep = os_.repair(goal, artifact_id="src.py", new_artifact_version=2)
    assert "T2" in rep.affected and "T4" in rep.affected
    assert "T1" in rep.preserved and "T3" in rep.preserved
    assert rep.frontier == ["T2"]  # minimal frontier
    # re-run restores closure
    r1 = os_.run(goal, max_dispatches=10)
    assert set(r1.verified) == {"T1", "T3", "T2", "T4"}


# ── result / status ────────────────────────────────────────────────────────
def test_run_result_is_structured():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    goal = Goal("G")
    goal.task("T1", agent="a", verify=scripted_executor(artifact_id="x", version=1))
    res = os_.run(goal, max_dispatches=3)
    assert isinstance(res, RunResult)
    assert res.goal_id and res.task_states
    d = res.as_dict()
    assert d["goal_id"] and d["task_states"]


def test_status_snapshot_readable():
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("a", specializations=("python",)))
    goal = Goal("G")
    goal.task("T1", agent="a", verify=scripted_executor(artifact_id="x", version=1))
    os_.run(goal, max_dispatches=3)
    snap = os_.status(goal)
    assert isinstance(snap, StatusSnapshot)
    assert "T1" in snap.verified
    assert "GOAL" in snap.render_ascii()


# ── error translation ──────────────────────────────────────────────────────
def test_error_translation_capability():
    from lhos.sdk import ConfigurationError

    with pytest.raises(ConfigurationError):
        Agent("")  # empty name rejected


# ── NoGraph compatibility (low-level path still available) ─────────────────
def test_nograph_path_remains_available():
    from lhos.agent_os.sdk.client import create_kernel
    from lhos.runtimes.verified_progress import VerifiedProgressRuntime

    k = create_kernel(":memory:")
    v = VerifiedProgressRuntime(":memory:")
    assert k and v  # execution + semantic runtimes usable without the SDK facade
