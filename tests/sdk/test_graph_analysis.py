"""Pure Graph control-state projection tests."""

from __future__ import annotations

from datetime import UTC, datetime

from lhos.runtimes.verified_progress.models import (
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VPGEdge,
)
from lhos.sdk.graph_analysis import derive_graph_analysis


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


def _edge(source: str, target: str, edge_id: str) -> VPGEdge:
    return VPGEdge(
        edge_id=edge_id,
        graph_id="g",
        edge_type="depends_on",
        source_node_id=source,
        target_node_id=target,
        created_in_version=0,
        created_by_pid="p",
        created_at=datetime.now(UTC),
    )


def test_chain_critical_path_and_unlock_value() -> None:
    nodes = {task_id: _task(task_id) for task_id in ("a", "b", "c")}
    edges = [_edge("goal", "c", "g-c"), _edge("c", "b", "c-b"), _edge("b", "a", "b-a")]

    analysis = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=edges,
        ready_frontier=("a",),
    )

    assert analysis.critical_path == ("a", "b", "c")
    assert [(item.task_id, item.value) for item in analysis.downstream_unlock_values] == [
        ("a", 1),
        ("b", 1),
        ("c", 0),
    ]
    assert analysis.parallel_frontier == ("a",)


def test_parallel_paths_are_stable_and_form_parallel_frontier() -> None:
    nodes = {task_id: _task(task_id) for task_id in ("a", "b", "c", "d")}
    edges = [
        _edge("goal", "c", "g-c"),
        _edge("goal", "d", "g-d"),
        _edge("c", "a", "c-a"),
        _edge("d", "b", "d-b"),
    ]

    first = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=list(reversed(edges)),
        ready_frontier=("b", "a"),
    )
    second = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=edges,
        ready_frontier=("a", "b"),
    )

    assert first == second
    assert first.critical_path == ("a", "c")
    assert first.parallel_frontier == ("a", "b")


def test_verified_and_stale_states_change_repair_signals() -> None:
    nodes = {
        "a": _task("a", NodeValidity.STALE),
        "b": _task("b", NodeValidity.UNVERIFIED),
        "c": _task("c", NodeValidity.UNVERIFIED),
    }
    edges = [_edge("goal", "c", "g-c"), _edge("c", "b", "c-b"), _edge("b", "a", "b-a")]

    stale = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=edges,
        ready_frontier=("a",),
        repair_ready_frontier=("a",),
    )
    assert stale.critical_path == ("a", "b", "c")
    assert stale.parallel_frontier == ("a",)

    nodes["a"] = _task("a", NodeValidity.VERIFIED)
    repaired = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=edges,
        ready_frontier=("b",),
        repair_ready_frontier=(),
    )
    assert repaired.critical_path == ("b", "c")
    assert repaired.parallel_frontier == ("b",)


def test_invalid_node_is_excluded_from_critical_path() -> None:
    nodes = {
        "a": _task("a", NodeValidity.INVALID),
        "b": _task("b", NodeValidity.UNVERIFIED),
    }
    edges = [_edge("goal", "b", "g-b"), _edge("b", "a", "b-a")]

    analysis = derive_graph_analysis(
        goal_id="goal",
        nodes=nodes,
        edges=edges,
        ready_frontier=(),
    )

    assert analysis.critical_path == ("b",)
