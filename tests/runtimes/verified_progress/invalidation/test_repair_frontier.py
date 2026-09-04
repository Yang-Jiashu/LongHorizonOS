"""D3 Repair Frontier tests — minimality, goal reopen, incremental advance
(§13, §14, §16, §17, §33)."""

from __future__ import annotations

from .helpers import GNode, TNode, depends_on


# ---- §13: frontier only contains immediately-repairable tasks ----
def test_frontier_only_immediately_repairable(run_engine, cause):
    """T1 -> T2 -> T3 chain.  Seed = deepest (T3).  Only T3 in frontier."""
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
    }
    edges = [depends_on("T1", "T2"), depends_on("T2", "T3")]
    c = cause(source_node_id="T3")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    # All stale, but frontier = [T3] only (its deps are verified/empty).
    assert [x.task_id for x in res.frontier.candidates] == ["T3"]


# ---- §13: active claim blocks a candidate ----
def test_frontier_excludes_active_claim(run_engine, cause):
    tasks = {
        "T3": TNode("T3", "verified"),
    }
    c = cause(source_node_id="T3")
    res = run_engine(
        tasks=tasks,
        edges=[],
        explicit_causes=(c,),
        has_active_claim=lambda tid: tid == "T3",
    )
    # Even though T3 is stale and has no stale deps, an ACTIVE D2 claim means
    # it is NOT front-ready (D2 owns it).  Frontier excludes it.
    assert res.frontier.candidates == ()


# ---- §17 / §33: CLOSED goal reopens when a required dep goes stale ----
def test_goal_reopens_when_required_dep_stale(run_engine, cause):
    goal = GNode("G1", closed=True)
    tasks = {
        "T1": TNode("T1", "verified"),
    }
    c = cause(source_node_id="T1")
    res = run_engine(
        tasks=tasks,
        edges=[],
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(c,),
    )
    assert res.reopened_goals == ("G1",), res.reopened_goals


# ---- §16: incremental advance after re-verification ----
def test_incremental_repair_advance(run_engine, cause):
    """Chain T1->T2->T3.  After T1 reverified, only T2 becomes available,
    not T3.  Model: re-invalidate with derived-validity progression."""
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
        "T3": TNode("T3", "verified"),
    }
    # T1 depends on T2; T2 depends on T3.
    edges = [depends_on("T1", "T2"), depends_on("T2", "T3")]

    c = cause(source_node_id="T3")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert [x.task_id for x in res.frontier.candidates] == ["T3"]

    # T3 reverified (validity -> verified) but T2/T1 still stale:
    derived = {"T1": "stale", "T2": "stale", "T3": "verified"}
    fr2 = compute_repair_frontier(
        "g",
        1,
        tasks,
        edges,
        stale_or_unverified={"T1", "T2"},
        derived_validity=derived,
    )
    assert [x.task_id for x in fr2.candidates] == ["T2"], fr2.candidates

    # T2 reverified, only T1 left stale:
    derived3 = {"T1": "stale", "T2": "verified", "T3": "verified"}
    fr3 = compute_repair_frontier(
        "g",
        1,
        tasks,
        edges,
        stale_or_unverified={"T1"},
        derived_validity=derived3,
    )
    assert [x.task_id for x in fr3.candidates] == ["T1"], fr3.candidates
