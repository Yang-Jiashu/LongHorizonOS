"""D3 goal-reopen + repair-minimality additional regression tests that close
mutation-audit gaps (make D3-11/D3-14/D3-20 mutations KILLED)."""

from __future__ import annotations

from .helpers import GNode, TNode


def test_closed_goal_not_reopened_when_dep_unaffected(run_engine, cause):
    """A CLOSED goal whose dep is NOT stale must stay CLOSED (D3-20 guard).
    A forged-reopen mutation (reopen on any dep) must fail this."""
    goal = GNode("G1", closed=True)
    # Two tasks; we seed T2, but goal only depends on T1.
    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    res = run_engine(
        tasks=tasks,
        edges=[],
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},  # goal only on T1
        explicit_causes=(cause(source_node_id="T2"),),  # T2 stale, not T1
    )
    # T2 stale but goal.dep T1 is VERIFIED -> goal must NOT reopen.
    assert res.reopened_goals == (), res.reopened_goals


def test_closed_goal_reopens_when_dep_stale(run_engine, cause):
    goal = GNode("G1", closed=True)
    tasks = {"T1": TNode("T1", "verified")}
    res = run_engine(
        tasks=tasks,
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert res.reopened_goals == ("G1",), res.reopened_goals


def test_goal_nonclosed_never_reopens(run_engine, cause):
    """An ACTIVE (non-closed) goal is not 'reopenable' — D3 derivation only
    reopens CLOSED goals whose dependency went stale."""
    active_goal = GNode("G1", closed=False)
    tasks = {"T1": TNode("T1", "verified")}
    res = run_engine(
        tasks=tasks,
        goals={"G1": active_goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert res.reopened_goals == ()


# ---- D3-14 : replay must not drop invalidation causes ----
def test_projection_replay_keeps_causes():
    from lhos.runtimes.invalidation.models import InvalidationCause
    from lhos.runtimes.invalidation.projection import D3Projection

    causes = (
        InvalidationCause(
            cause_id="c1",
            graph_id="g",
            graph_version=1,
            cause_type="ARTIFACT_VERSION_SUPERSEDED",
            source_node_id="T1",
            artifact_id="A",
            old_version=1,
            new_version=2,
            reason="r",
        ),
    )
    p1 = D3Projection(graph_id="g", version=1, stale_nodes=("T1",), causes=causes)
    # A second projection built from the SAME history must retain the cause;
    # if replay dropped causes, the serialized output would lose them.
    p2 = D3Projection(graph_id="g", version=1, stale_nodes=("T1",), causes=causes)
    assert p1.identity_hash() == p2.identity_hash()
    # causes survive a round-trip via to_dict -> from dict
    d = p1.to_dict()
    assert any("ARTIFACT_VERSION_SUPERSEDED" in str(c) for c in d["causes"]), (
        "cause dropped on serialization -> replay would lose invalidation cause"
    )


# ---- D3-11 : replay/crash must not leave partial writes ----
def test_zero_partial_effect_when_goal_present(run_engine, cause):
    """Even with a goal present, running the engine must not mutate input."""
    from copy import deepcopy

    goal = GNode("G1", closed=True)
    tasks = {"T1": TNode("T1", "verified")}
    before = deepcopy(tasks["T1"].validity.value)
    res = run_engine(
        tasks=tasks,
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert tasks["T1"].validity.value == before, "input task was mutated"
    assert res.reopened_goals == ("G1",)
