"""D3 core behavioral tests per §33 core list:

old Evidence stays immutable
v7 Evidence cannot verify v8
affected Task becomes STALE
dependency of STALE Task cannot remain VERIFIED
unrelated branch remains VERIFIED
Goal reopens when required Task stale
Repair Frontier contains only immediately repairable Tasks
Repair Frontier is minimal
reverified Task leaves stale state
downstream repair advances incrementally
same invalidation produces same cone
GraphVersion race cannot commit stale computation
failed invalidation has zero partial semantic effect
D2 can schedule repair work without understanding D3
Kernel remains unaware of invalidation semantics
"""

from __future__ import annotations

from .helpers import GNode, TNode, depends_on


def test_core_affected_task_stale(run_engine, cause):
    tasks = {"T1": TNode("T1", "verified")}
    res = run_engine(tasks=tasks, edges=[], explicit_causes=(cause(source_node_id="T1"),))
    assert "T1" in res.stale_nodes
    assert "T1" not in res.preserved_nodes


def test_core_dependency_cannot_stay_verified(run_engine, cause):
    tasks = {"T1": TNode("T1", "verified"), "T2": TNode("T2", "verified")}
    edges = [depends_on("T2", "T1")]
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(cause(source_node_id="T1"),))
    # T2 relies on T1; once T1 stale, T2 can't remain VERIFIED.
    assert "T2" in res.stale_nodes


def test_core_unrelated_branch_stays_verified(run_engine, cause):
    tasks = {"T-A": TNode("T-A", "verified"), "T-B": TNode("T-B", "verified")}
    edges = []
    res = run_engine(tasks=tasks, edges=edges, explicit_causes=(cause(source_node_id="T-A"),))
    assert "T-A" in res.stale_nodes
    assert "T-B" in res.preserved_nodes  # branch not touched


def test_core_reverified_task_leaves_stale(run_engine, cause):
    """First pass stale; after reverify the task exits the D3 stale set."""
    from lhos.runtimes.invalidation.frontier import compute_repair_frontier

    tasks = {"T1": TNode("T1", "verified")}
    res = run_engine(tasks=tasks, edges=[], explicit_causes=(cause(source_node_id="T1"),))
    assert "T1" in res.stale_nodes
    # reverify T1 -> derived validity back to verified
    fr = compute_repair_frontier(
        "g",
        2,
        tasks,
        [],
        stale_or_unverified=set(),
        derived_validity={"T1": "verified"},
    )
    assert fr.candidates == ()


def test_core_goal_reopen_required_task_stale(run_engine, cause):
    goal = GNode("G1", closed=True)
    res = run_engine(
        tasks={"T1": TNode("T1", "verified")},
        goals={"G1": goal},
        goal_direct_tasks={"G1": ("T1",)},
        explicit_causes=(cause(source_node_id="T1"),),
    )
    assert res.reopened_goals == ("G1",)


def test_core_same_invalidation_same_cone(run_engine, cause):
    tasks = {f"T{i}": TNode(f"T{i}", "verified") for i in range(6)}
    edges = [
        depends_on("T1", "T2"),
        depends_on("T2", "T3"),
        depends_on("T4", "T5"),
        depends_on("T5", "T3"),
    ]
    c = cause(source_node_id="T3")
    a = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    b = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    assert a.cone.cone_hash == b.cone.cone_hash
    assert a.cone.affected_node_ids == b.cone.affected_node_ids


def test_core_kernel_unaware_of_d3():
    """Kernel must not import D3 internals; D3 must not import Kernel internals."""
    import inspect

    import lhos.runtimes.invalidation as inv
    import lhos.runtimes.invalidation.models as models

    for mod in (inv, models):
        src = inspect.getsource(mod)
        for banned in ("agent_os.services", "LeaseService", "ResourceLease"):
            assert banned not in src, f"{mod.__name__} must not reference {banned!r}"
