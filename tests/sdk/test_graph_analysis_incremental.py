"""Differential tests: IncrementalGraphAnalyzer vs the full-recompute oracle.

The full :func:`derive_graph_analysis` is treated as the oracle.  For randomized
DAGs, randomized completion orders, arbitrary validity assignments, and mid-run
structural mutations, the incremental analyzer must return a result that is
*identical* to the oracle — not merely close — because these fields feed a
deterministic scheduling decision hash.
"""

from __future__ import annotations

import random

from lhos.runtimes.verified_progress.models import (
    EdgeType,
    GoalNode,
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VPGEdge,
)
from lhos.sdk.graph_analysis import (
    IncrementalGraphAnalyzer,
    derive_graph_analysis,
)

_GOAL = "goal"
_VALIDITIES = (
    NodeValidity.UNVERIFIED,
    NodeValidity.VERIFIED,
    NodeValidity.STALE,
    NodeValidity.INVALID,
)


def _task(task_id: str, validity: NodeValidity = NodeValidity.UNVERIFIED) -> TaskNode:
    return TaskNode(
        node_id=task_id,
        graph_id="g",
        created_by_pid="p",
        created_in_version=0,
        updated_in_version=0,
        lifecycle=NodeLifecycle.ADMITTED,
        validity=validity,
    )


def _goal() -> GoalNode:
    return GoalNode(
        node_id=_GOAL,
        graph_id="g",
        created_by_pid="p",
        created_in_version=0,
        updated_in_version=0,
        lifecycle=NodeLifecycle.ADMITTED,
    )


def _edge(source: str, target: str, edge_id: str) -> VPGEdge:
    return VPGEdge(
        edge_id=edge_id,
        graph_id="g",
        edge_type=EdgeType.DEPENDS_ON,
        source_node_id=source,
        target_node_id=target,
        created_in_version=0,
        created_by_pid="p",
    )


def _random_dag(
    rng: random.Random, n_tasks: int, edge_prob: float
) -> tuple[dict[str, object], list[VPGEdge], list[str]]:
    """Build a random acyclic task graph plus a goal depending on some tasks."""

    ids = [f"t{i}" for i in range(n_tasks)]
    nodes: dict[str, object] = {tid: _task(tid) for tid in ids}
    nodes[_GOAL] = _goal()
    edges: list[VPGEdge] = []
    counter = 0
    # Acyclic: t{i} may depend only on earlier t{j}, j < i.
    for i in range(n_tasks):
        for j in range(i):
            if rng.random() < edge_prob:
                edges.append(_edge(ids[i], ids[j], f"e{counter}"))
                counter += 1
    goal_targets = [tid for tid in ids if rng.random() < 0.5] or [ids[-1]]
    for tid in goal_targets:
        edges.append(_edge(_GOAL, tid, f"e{counter}"))
        counter += 1
    return nodes, edges, ids


def _depends_map(nodes: dict[str, object], edges: list[VPGEdge]) -> dict[str, set[str]]:
    tasks = {
        nid
        for nid, nd in nodes.items()
        if getattr(getattr(nd, "node_type", None), "value", "") == "task"
    }
    dep: dict[str, set[str]] = {tid: set() for tid in tasks}
    for edge in edges:
        src, tgt = edge.source_node_id, edge.target_node_id
        if src in tasks and tgt in tasks:
            dep[src].add(tgt)
    return dep


def _ready_frontier(nodes: dict[str, object], edges: list[VPGEdge]) -> tuple[str, ...]:
    """A realistic READY antichain: open tasks whose task deps are all verified."""

    dep = _depends_map(nodes, edges)
    ready = []
    for tid, deps in dep.items():
        node = nodes[tid]
        validity = getattr(getattr(node, "validity", None), "value", "")
        if validity in {"unverified", "stale"} and all(
            getattr(getattr(nodes[d], "validity", None), "value", "") == "verified" for d in deps
        ):
            ready.append(tid)
    return tuple(sorted(ready))


def _assert_identical(
    analyzer: IncrementalGraphAnalyzer,
    nodes: dict[str, object],
    edges: list[VPGEdge],
) -> None:
    ready = _ready_frontier(nodes, edges)
    repair = tuple(
        t for t in ready if getattr(getattr(nodes[t], "validity", None), "value", "") == "stale"
    )
    oracle = derive_graph_analysis(
        goal_id=_GOAL,
        nodes=nodes,
        edges=edges,
        ready_frontier=ready,
        repair_ready_frontier=repair,
    )
    incremental = analyzer.analyze(
        goal_id=_GOAL,
        nodes=nodes,
        edges=edges,
        ready_frontier=ready,
        repair_ready_frontier=repair,
    )
    # Whole-object and field-by-field equality (byte-for-byte for the hash).
    assert incremental == oracle
    assert incremental.critical_path == oracle.critical_path
    assert incremental.downstream_unlock_values == oracle.downstream_unlock_values
    assert incremental.parallel_frontier == oracle.parallel_frontier
    assert repr(incremental) == repr(oracle)


def test_first_call_equals_full_recompute() -> None:
    rng = random.Random(0)
    for _ in range(20):
        nodes, edges, _ = _random_dag(rng, rng.randint(3, 20), rng.uniform(0.1, 0.6))
        _assert_identical(IncrementalGraphAnalyzer(), nodes, edges)


def test_incremental_matches_oracle_legal_completion_order() -> None:
    """Verify tasks one-per-epoch in a dependency-respecting order."""

    for seed in range(60):
        rng = random.Random(1000 + seed)
        nodes, edges, ids = _random_dag(rng, rng.randint(4, 22), rng.uniform(0.15, 0.55))
        analyzer = IncrementalGraphAnalyzer()
        _assert_identical(analyzer, nodes, edges)

        dep = _depends_map(nodes, edges)
        remaining = list(ids)
        rng.shuffle(remaining)
        completed: set[str] = set()
        # Repeatedly verify a task whose deps are all already verified.
        progress = True
        while remaining and progress:
            progress = False
            for tid in list(remaining):
                if dep[tid] <= completed:
                    nodes[tid] = _task(tid, NodeValidity.VERIFIED)
                    completed.add(tid)
                    remaining.remove(tid)
                    _assert_identical(analyzer, nodes, edges)
                    progress = True
                    break


def test_incremental_matches_oracle_arbitrary_validity_flips() -> None:
    """Arbitrary (possibly illegal) validity assignments each epoch.

    This is the case that distinguishes the correct unlock-value dependency set
    (a task's prerequisites and their siblings) from the naive "descendants"
    intuition, and exercises INVALID/STALE transitions the legal-order test
    never reaches.
    """

    for seed in range(80):
        rng = random.Random(5000 + seed)
        nodes, edges, ids = _random_dag(rng, rng.randint(3, 18), rng.uniform(0.1, 0.6))
        analyzer = IncrementalGraphAnalyzer()
        _assert_identical(analyzer, nodes, edges)
        for _ in range(rng.randint(3, 12)):
            flip = rng.sample(ids, k=rng.randint(1, len(ids)))
            for tid in flip:
                nodes[tid] = _task(tid, rng.choice(_VALIDITIES))
            _assert_identical(analyzer, nodes, edges)


def test_incremental_matches_oracle_across_structural_changes() -> None:
    """Structural mutations must trigger the full-recompute fallback correctly."""

    for seed in range(40):
        rng = random.Random(9000 + seed)
        nodes, edges, ids = _random_dag(rng, rng.randint(4, 16), rng.uniform(0.15, 0.5))
        analyzer = IncrementalGraphAnalyzer()
        _assert_identical(analyzer, nodes, edges)

        for _ in range(rng.randint(2, 6)):
            # A few incremental validity steps.
            for tid in rng.sample(ids, k=rng.randint(1, len(ids))):
                nodes[tid] = _task(tid, rng.choice(_VALIDITIES))
            _assert_identical(analyzer, nodes, edges)

            # Then a structural mutation: add a fresh task and an edge.
            new_id = f"x{rng.randint(0, 10_000)}"
            if new_id not in nodes:
                nodes[new_id] = _task(new_id, rng.choice(_VALIDITIES))
                ids.append(new_id)
                target = rng.choice(ids)
                if target != new_id:
                    edges.append(_edge(new_id, target, f"struct-{seed}-{new_id}"))
                _assert_identical(analyzer, nodes, edges)

            # And an edge removal, if any task->task edge exists.
            removable = [
                idx
                for idx, e in enumerate(edges)
                if e.source_node_id != _GOAL and e.target_node_id != _GOAL
            ]
            if removable:
                edges.pop(rng.choice(removable))
                _assert_identical(analyzer, nodes, edges)


def test_unlock_sibling_prerequisite_change_is_incremental() -> None:
    """Verifying one prerequisite must update its sibling's unlock value.

    C depends on both A and B.  Verifying A raises B's unlock value from 0 to 1
    (C now has only B outstanding).  B is neither a descendant nor an ancestor
    of A, so a descendants-only invalidation would miss it.
    """

    nodes: dict[str, object] = {
        "A": _task("A"),
        "B": _task("B"),
        "C": _task("C"),
    }
    nodes[_GOAL] = _goal()
    edges = [
        _edge(_GOAL, "C", "g-c"),
        _edge("C", "A", "c-a"),
        _edge("C", "B", "c-b"),
    ]
    analyzer = IncrementalGraphAnalyzer()
    _assert_identical(analyzer, nodes, edges)

    before = {
        u.task_id: u.value
        for u in analyzer.analyze(goal_id=_GOAL, nodes=nodes, edges=edges).downstream_unlock_values
    }
    assert before["B"] == 0

    nodes["A"] = _task("A", NodeValidity.VERIFIED)
    _assert_identical(analyzer, nodes, edges)
    after = {
        u.task_id: u.value
        for u in analyzer.analyze(goal_id=_GOAL, nodes=nodes, edges=edges).downstream_unlock_values
    }
    assert after["B"] == 1


def test_reverting_a_change_restores_exact_prior_result() -> None:
    """Path independence: state X reached from anywhere yields identical output."""

    rng = random.Random(4242)
    nodes, edges, ids = _random_dag(rng, 14, 0.4)
    analyzer = IncrementalGraphAnalyzer()
    baseline = derive_graph_analysis(goal_id=_GOAL, nodes=nodes, edges=edges)
    _assert_identical(analyzer, nodes, edges)

    for tid in ids[:5]:
        nodes[tid] = _task(tid, NodeValidity.VERIFIED)
        _assert_identical(analyzer, nodes, edges)
    for tid in ids[:5]:
        nodes[tid] = _task(tid, NodeValidity.UNVERIFIED)
        _assert_identical(analyzer, nodes, edges)

    restored = analyzer.analyze(goal_id=_GOAL, nodes=nodes, edges=edges)
    assert restored == baseline
