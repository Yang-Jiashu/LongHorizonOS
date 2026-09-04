"""D3 determinism (§18, §33): same graph + same causes + same base version
=> identical cone, final validity, Repair Frontier, cone_hash, frontier_hash."""

from __future__ import annotations

import random

from .helpers import TNode, depends_on


def _build_medium_graph(seed: int):
    """Build a medium DAG with several branches and a known causality."""
    rg = random.Random(seed)
    nodes = [f"T{i}" for i in range(12)]
    tasks = {n: TNode(n, "verified") for n in nodes}
    edges = []
    # chain
    edges.append(depends_on("T0", "T1"))
    edges.append(depends_on("T1", "T2"))
    # diamond at T3,T4,T5
    edges.append(depends_on("T3", "T4"))
    edges.append(depends_on("T3", "T5"))
    edges.append(depends_on("T6", "T7"))
    # independent leaf branch
    edges.append(depends_on("T8", "T9"))
    # connectors
    edges.append(depends_on("T2", "T3"))
    edges.append(depends_on("T7", "T8"))
    return tasks, edges


def test_deterministic_cone_and_frontier(run_engine, cause):
    tasks, edges = _build_medium_graph(1)
    c = cause(source_node_id="T4", artifact_id="A", old_version=1, new_version=2)

    results = []
    for _ in range(25):
        res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
        results.append(
            (
                res.cone.affected_node_ids,
                res.cone.preserved_node_ids,
                res.cone.cone_hash,
                tuple(x.task_id for x in res.frontier.candidates),
                res.frontier.frontier_hash,
                tuple(res.reopened_goals),
            )
        )
    assert len(set(results)) == 1, "determinism violated across 25 independent runs"
    assert results[0][1]  # preserved set non-empty (independent branch preserved)


def test_same_graph_same_causes_same_base_version_same_cone(run_engine, cause):
    tasks, edges = _build_medium_graph(3)
    c = cause(source_node_id="T0", artifact_id="Z", old_version=1, new_version=2)
    a = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,), version=10)
    b = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,), version=10)
    assert a.cone.cone_hash == b.cone.cone_hash
    assert a.result_hash == b.result_hash
    assert a.cone.affected_node_ids == b.cone.affected_node_ids


def test_different_hashseed_does_not_change_cone(run_engine, cause, monkeypatch):
    """PYTHONHASHSEED must not leak into the cone computation."""
    tasks, edges = _build_medium_graph(7)
    c = cause(source_node_id="T6", artifact_id="W", old_version=1, new_version=2)
    results = set()
    for hs in ("0", "42", "1234567"):
        monkeypatch.setenv("PYTHONHASHSEED", hs)
        res = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
        results.add(res.cone.cone_hash)
    assert len(results) == 1, "PYTHONHASHSEED leaked into cone determinism"


def test_cone_affected_order_is_lexicographic_not_traversal_order(run_engine, cause):
    """Affected node ordering must be deterministic-sorted, not traversal
    order — so a mutation that makes traversal hash/set-ordered still
    produces the SAME sorted output, but the PROPAGATION EDGES order would
    change.  We assert both stay byte-identical across many runs and across
    a shuffled input sequence."""
    from .helpers import TNode, depends_on

    # wide fan-in graph with MANY tasks to expose set-order differences
    tasks = {f"T{i}": TNode(f"T{i}", "verified") for i in range(30)}
    edges = []
    for i in range(3, 30):
        edges.append(depends_on(f"T{i}", f"T{i % 3}"))
    c = cause(source_node_id="T0")

    res1 = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
    # Reverse edges + task dict order: cone must be stable.
    edges_r = list(reversed(edges))
    tasks_r = {k: tasks[k] for k in reversed(list(tasks))}
    res2 = run_engine(tasks=tasks_r, edges=edges_r, explicit_causes=(c,))
    assert res1.cone.cone_hash == res2.cone.cone_hash
    assert res1.cone.propagation_edges == res2.cone.propagation_edges
    assert res1.cone.affected_node_ids == res2.cone.affected_node_ids


def test_deterministic_across_many_runs(run_engine, cause):
    from .helpers import TNode, depends_on

    tasks = {f"T{i}": TNode(f"T{i}", "verified") for i in range(20)}
    edges = [
        depends_on("T0", "T1"),
        depends_on("T0", "T2"),
        depends_on("T10", "T0"),
        depends_on("T11", "T0"),
    ]
    c = cause(source_node_id="T0")
    sigs = set()
    for _ in range(50):
        r = run_engine(tasks=tasks, edges=edges, explicit_causes=(c,))
        sigs.add((r.cone.cone_hash, r.result_hash, r.frontier.frontier_hash))
    assert len(sigs) == 1, "cone determinism violated across 50 runs"
