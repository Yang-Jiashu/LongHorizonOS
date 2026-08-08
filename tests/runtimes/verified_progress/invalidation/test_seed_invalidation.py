"""D3 seed invalidation + chain + branch preservation + multiple dependency
+ goal reopen + reverification semantics (§5, §10, §11, §16, §17)."""

from __future__ import annotations

from .helpers import GNode, TNode, depends_on


# ---- §5: Seed from authoritative evidence applicability ----
def test_seed_from_superseded_artifact(run_engine, cause):
    """Artifact A v7->v8 makes the Task that produces A stale, even with NO
    explicit cause passed: the seed is derived from current-output-versions."""
    from .helpers import Bound, FNode

    ev = {"E7": FNode("E7", artifact_bindings=(Bound("X", 7),))}
    tasks = {"T1": TNode("T1", "verified")}
    res = run_engine(
        tasks=tasks,
        evidence_nodes=ev,
        # caused by superseding artifact -> T1's producing evidence loses
        explicit_causes=(
            cause(source_node_id="T1", artifact_id="X", old_version=7, new_version=8),
        ),
    )
    assert "T1" in res.stale_nodes
    assert res.frontier.candidates[0].task_id == "T1"


# ---- §10: seed invalidates producing Task + dependents ----
def test_seed_invalidates_producer_and_downstreams(run_engine, cause):
    tasks = {
        "T1": TNode("T1", "verified"),
        "T2": TNode("T2", "verified"),
    }
    # T2 depends on T1 (execution: T1 must be verified before T2 runs).
    edges = [depends_on("T2", "T1")]
    c = cause(source_node_id="T1")
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert set(res.stale_nodes) == {"T1", "T2"}
    # frontier minimal: T1 (its deps empty) only.
    assert [x.task_id for x in res.frontier.candidates] == ["T1"]


# ---- §17: Goal reopens via derivation (never agent-set) ----
def test_goal_reopens_and_recloses(run_engine, cause):
    goal = GNode("G1", closed=True)
    t1 = TNode("T1", "verified")
    res = run_engine(
        tasks={"T1": t1},
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert res.reopened_goals == ("G1",)

    # Reclose => with no stale T1, goal no longer in reopened set.
    res2 = run_engine(
        tasks={"T1": TNode("T1", "verified")},
        goals={"G1": GNode("G1", closed=True)},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(),
    )
    assert res2.reopened_goals == ()


# ---- §16: reverified Task leaves stale set / downstream advances ----
def test_reverified_task_exits_stale_state(run_engine, cause):
    """After a Task re-verifies, it leaves the D3 stale set and its
    downstream becomes the new frontier candidate."""
    # First pass: seed T1 -> T1,T2 stale, frontier=[T1].
    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    edges = [depends_on("T2", "T1")]
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(cause(source_node_id="T1"),))
    assert [x.task_id for x in res.frontier.candidates] == ["T1"]

    # Second: T1 now VERIFIED again (reverified), T2 still stale.
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    tasks2 = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "stale")}
    fr = compute_repair_frontier(
        "g",
        2,
        tasks2,
        edges,
        stale_or_unverified={"T2"},
        derived_validity={"T1": "verified", "T2": "stale"},
    )
    assert [x.task_id for x in fr.candidates] == ["T2"], fr.candidates
