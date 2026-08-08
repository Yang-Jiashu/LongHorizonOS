"""D3 reverification flow (§16): an artifact change -> stale -> frontier ->
re-verify -> downstream advances -> repair frontier moves -> goal re-closes."""

from __future__ import annotations

from .helpers import GNode, TNode, depends_on


def _chain(tasks: dict, edges: list):
    """Build a 3-task chain: T2 depends on T1; T3 depends on T2."""
    edges.append(depends_on("T2", "T1"))
    edges.append(depends_on("T3", "T2"))
    return tasks, edges


def test_reverification_goal_recloses_chain(run_engine, cause):
    """Complete §16 path: seed deepest → stale all → frontier=[deepest] →
    reverify deepest → frontier advances to next."""
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
    }
    edges = _chain(tasks, [])[1]
    goal = GNode("G1", closed=True)
    goal_deps = {"G1": ("T1", "T2", "T3")}

    # 1) seed the FIRST task (T1) => all downstream (T2,T3) stale; goal reopens; frontier=[T1]
    r1 = run_engine(
        tasks=tasks,
        edges=edges,
        goals={"G1": goal},
        goal_direct_tasks=goal_deps,
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert set(r1.stale_nodes) == {"T1", "T2", "T3"}
    assert r1.reopened_goals == ("G1",)
    assert [c.task_id for c in r1.frontier.candidates] == ["T1"]

    # 2) T1 reverified => frontier advances to T2
    dv = {"T1": "verified", "T2": "stale", "T3": "stale"}
    fr2 = compute_repair_frontier(
        "g",
        2,
        tasks,
        edges,
        stale_or_unverified={"T2", "T3"},
        derived_validity=dv,
    )
    assert [c.task_id for c in fr2.candidates] == ["T2"]

    # 3) T2 reverified => frontier=T3
    dv2 = {"T1": "verified", "T2": "verified", "T3": "stale"}
    fr3 = compute_repair_frontier(
        "g",
        3,
        tasks,
        edges,
        stale_or_unverified={"T3"},
        derived_validity=dv2,
    )
    assert [c.task_id for c in fr3.candidates] == ["T3"]

    # 4) all reverified => goal reclosed, frontier empty
    dv3 = {"T1": "verified", "T2": "verified", "T3": "verified"}
    fr4 = compute_repair_frontier(
        "g",
        4,
        tasks,
        edges,
        stale_or_unverified=set(),
        derived_validity=dv3,
    )
    assert fr4.candidates == ()


def test_goal_recloses_after_full_reverification(run_engine):
    """After the whole affected region is VERIFIED again, the D3 result no
    longer reports the goal as reopened (i.e. semantic closure restored)."""
    tasks = {"T1": TNode("T1", "valid") if False else TNode("T1", "verified")}
    res = run_engine(
        tasks=tasks,
        goals={"G1": GNode("G1", closed=True)},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(),
    )
    assert res.reopened_goals == ()
    assert res.stale_nodes == ()
