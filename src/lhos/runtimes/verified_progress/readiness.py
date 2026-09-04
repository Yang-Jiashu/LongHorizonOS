"""Deterministic Ready Frontier derivation.

task_is_ready(T)  iff
    1. lifecycle == ADMITTED
    2. validity in {UNVERIFIED, STALE}
    3. not VERIFIED, not INVALID
    4. all Task dependencies are VERIFIED
    5. structure + artifact references valid

Ready Frontier is all READY tasks sorted deterministically:
    priority DESC, topological_depth ASC, created_in_version ASC, node_id ASC
"""

from __future__ import annotations

from .models import (
    AnyNode,
    EdgeType,
    NodeLifecycle,
    NodeValidity,
    ReadinessProof,
    TaskDispatchCandidate,
    TaskNode,
    VPGEdge,
)


def _depends_on_index(edges: list[VPGEdge]) -> dict[str, list[str]]:
    """source_task -> its DEPENDS_ON target ids.

    Built once per readiness pass.  Previously every predicate scanned the whole
    edge list for one task, making the frontier computation O(V*E); with this
    index the same work is O(V+E).  Purely a lookup structure -- it changes no
    predicate and no ordering.
    """
    index: dict[str, list[str]] = {}
    for e in edges:
        if e.edge_type == EdgeType.DEPENDS_ON:
            index.setdefault(e.source_node_id, []).append(e.target_node_id)
    return index


def _task_deps_all_verified(
    task_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    dep_index: dict[str, list[str]] | None = None,
) -> bool:
    targets = (
        dep_index.get(task_id, ())
        if dep_index is not None
        else [
            e.target_node_id
            for e in edges
            if e.edge_type == EdgeType.DEPENDS_ON and e.source_node_id == task_id
        ]
    )
    for target_id in targets:
        if target_id not in nodes:
            continue
        dep = nodes[target_id]
        if not isinstance(dep, TaskNode):
            return False
        if dep.validity != NodeValidity.VERIFIED:
            return False
        if dep.lifecycle not in {
            NodeLifecycle.ADMITTED,
            NodeLifecycle.ACTIVE,
            NodeLifecycle.CLOSED,
        }:
            return False
    return True


def _compute_topo_depth(
    task_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    memo: dict[str, int],
    visiting: set[str],
    dep_index: dict[str, list[str]] | None = None,
) -> int:
    if task_id in memo:
        return memo[task_id]
    if task_id in visiting:
        return 0
    visiting.add(task_id)
    targets = (
        dep_index.get(task_id, ())
        if dep_index is not None
        else [
            e.target_node_id
            for e in edges
            if e.edge_type == EdgeType.DEPENDS_ON and e.source_node_id == task_id
        ]
    )
    max_child = 0
    for target_id in targets:
        if target_id in nodes and isinstance(nodes[target_id], TaskNode):
            child_depth = _compute_topo_depth(target_id, nodes, edges, memo, visiting, dep_index)
            max_child = max(max_child, 1 + child_depth)
    visiting.discard(task_id)
    memo[task_id] = max_child
    return max_child


def task_is_ready(
    task: TaskNode,
    *,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    dep_index: dict[str, list[str]] | None = None,
) -> bool:
    """Check readiness predicate for a single TaskNode."""
    if task.lifecycle != NodeLifecycle.ADMITTED:
        return False
    if task.validity == NodeValidity.STALE and not task.metadata.get("__repair_ready", False):
        return False
    if task.validity not in {NodeValidity.UNVERIFIED, NodeValidity.STALE}:
        return False
    return _task_deps_all_verified(task.node_id, nodes, edges, dep_index)


def compute_ready_frontier(
    graph_id: str,
    graph_version: int,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
) -> list[TaskDispatchCandidate]:
    """Return the full READY frontier sorted deterministically.

    Ordering: priority DESC, topo_depth ASC, created_in_version ASC, node_id ASC.
    """
    task_ids = compute_ready_task_ids(nodes, edges)
    frontier: list[TaskDispatchCandidate] = []
    for task_id in task_ids:
        node = nodes.get(task_id)
        if not isinstance(node, TaskNode):
            continue
        frontier.append(
            TaskDispatchCandidate(
                graph_id=graph_id,
                graph_version=graph_version,
                task_id=task_id,
                readiness_proof=ReadinessProof(
                    graph_id=graph_id,
                    graph_version=graph_version,
                    task_id=task_id,
                    lifecycle_ok=True,
                    validity_ok=True,
                    all_deps_verified=True,
                    has_execution_attempt=False,
                ),
                execution_spec=dict(node.execution_spec),
            )
        )
    return frontier


def compute_ready_task_ids(
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
) -> list[str]:
    """Return READY task ids in the same deterministic scheduler order.

    Commit-time events need only the ordered ids. Keeping that path separate
    avoids constructing two nested Pydantic proof models per READY task while
    sharing the exact predicate and ordering used by the public frontier API.
    """
    task_ids: list[str] = []
    dep_index = _depends_on_index(edges)
    for n in nodes.values():
        if not isinstance(n, TaskNode):
            continue
        if not task_is_ready(n, nodes=nodes, edges=edges, dep_index=dep_index):
            continue
        task_ids.append(n.node_id)

    # One memo shared across the whole sort: `_depth_of` used to allocate a fresh
    # empty memo per call, so every candidate recomputed its depth from scratch.
    # Depths are a pure function of (nodes, edges), which do not change here.
    depth_memo: dict[str, int] = {}
    task_ids.sort(
        key=lambda task_id: (
            -_priority_of(task_id, nodes),
            _depth_of(task_id, nodes, edges, memo=depth_memo, dep_index=dep_index),
            _created_of(task_id, nodes),
            task_id,
        )
    )
    return task_ids


def _priority_of(task_id: str, nodes: dict[str, AnyNode]) -> int:
    n = nodes.get(task_id)
    if not isinstance(n, TaskNode):
        return 0
    p = n.metadata.get("priority", 0)
    return int(p) if isinstance(p, (int, float)) else 0


def _depth_of(
    task_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    *,
    memo: dict[str, int] | None = None,
    dep_index: dict[str, list[str]] | None = None,
) -> int:
    return _compute_topo_depth(
        task_id, nodes, edges, {} if memo is None else memo, set(), dep_index
    )


def _created_of(task_id: str, nodes: dict[str, AnyNode]) -> int:
    n = nodes.get(task_id)
    return n.created_in_version if n is not None else 0
