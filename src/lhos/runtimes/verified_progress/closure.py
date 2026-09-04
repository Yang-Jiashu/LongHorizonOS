"""Logical closure derivation.

- A Task is logically closed iff validity == VERIFIED (lifecycle becomes CLOSED).
- A Goal is closed iff it depends_on >= 1 Task AND all directly depends_on
  Tasks are VERIFIED (and none STALE/INVALID/UNVERIFIED).

Closure is always DERIVED — never set by Agent Patch.
"""

from __future__ import annotations

from collections.abc import Iterable

from .models import (
    AnyNode,
    EdgeType,
    GoalNode,
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VPGEdge,
)


def goal_is_closed(
    goal: GoalNode,
    nodes: dict[str, AnyNode],
    edges: Iterable[VPGEdge],
) -> bool:
    """Goal closed iff every directly depends_on Task is VERIFIED."""
    dependent_task_ids: list[str] = []
    for e in edges:
        if (
            e.edge_type == EdgeType.DEPENDS_ON
            and e.source_node_id == goal.node_id
            and e.target_node_id in nodes
        ):
            dep = nodes[e.target_node_id]
            if isinstance(dep, TaskNode):
                dependent_task_ids.append(dep.node_id)

    # empty goal is NOT auto-closed
    if not dependent_task_ids:
        return False

    for tid in dependent_task_ids:
        d: AnyNode | None = nodes.get(tid)
        if not isinstance(d, TaskNode):
            return False
        if d.validity != NodeValidity.VERIFIED:
            return False
        if d.lifecycle not in {
            NodeLifecycle.ADMITTED,
            NodeLifecycle.ACTIVE,
            NodeLifecycle.CLOSED,
        }:
            return False
    return True


def task_should_be_closed(task: TaskNode) -> bool:
    """Lifecycle policy: a VERIFIED task may be marked CLOSED."""
    return task.validity == NodeValidity.VERIFIED and task.lifecycle in {
        NodeLifecycle.ADMITTED,
        NodeLifecycle.ACTIVE,
    }


def task_should_reopen(task: TaskNode) -> bool:
    """Lifecycle policy: a CLOSED+VERIFIED task must reopen when no longer VERIFIED."""
    if task.lifecycle != NodeLifecycle.CLOSED:
        return False
    return task.validity in {NodeValidity.STALE, NodeValidity.UNVERIFIED}
