"""Projection rebuild from Patch/Event history.

D1 projection is fully derivable: delete all projection tables, replay
Patches in order, re-admit every new node and re-apply edges.  Result must
be byte-identical (projection_hash) regardless of how many times it is rebuilt.
"""

from __future__ import annotations

from .admission import admit
from .closure import goal_is_closed, task_should_be_closed
from .events import GraphEvent, GraphEventType
from .models import (
    AnyNode,
    ArtifactRefNode,
    EdgeType,
    GoalNode,
    NodeLifecycle,
    NodeValidity,
    TaskNode,
    VPGEdge,
)
from .verification import task_is_verified


def rebuild_projection(
    graph_id: str,
    patches: list,
    edges_committed_per_patch: dict[str, list[VPGEdge]],
    nodes_committed_per_patch: dict[str, list[AnyNode]],
    *,
    facts_artifact=None,
    facts_kernel=None,
) -> tuple[dict[str, AnyNode], list[VPGEdge], list[GraphEvent]]:
    """Rebuild full graph projection given patch history.

    - patches: ordered list of GraphPatchProposal already committed.
    - edges_committed_per_patch / nodes_committed_per_patch: mapping of
      patch_id -> list of edges/nodes that patch produced (in order).

    Returns (nodes, edges, derived_events).
    """
    nodes: dict[str, AnyNode] = {}
    edges: list[VPGEdge] = []
    derived: list[GraphEvent] = []

    for i, patch in enumerate(patches, start=1):
        # 1. admit any new nodes via same pipeline
        new_nodes = nodes_committed_per_patch.get(patch.patch_id, [])
        new_edges = edges_committed_per_patch.get(patch.patch_id, [])
        for n in new_nodes:
            if n.node_id in nodes:
                nodes[n.node_id] = n
                continue
            admitted = admit(n, graph_id)
            final = admitted.node
            nodes[n.node_id] = final
            derived.append(
                GraphEvent(
                    graph_id=graph_id,
                    event_type=GraphEventType.NODE_ADDED
                    if n.node_type != "evidence"
                    else GraphEventType.EVIDENCE_ATTACHED
                    if _is_evidence_edge_going_to_verification(n, new_edges)
                    else GraphEventType.NODE_ADDED,
                    causation_patch_id=patch.patch_id,
                    node_id=n.node_id,
                    to_lifecycle=n.lifecycle.value,
                    to_validity=n.validity.value,
                    graph_version=i,
                )
            )
            if admitted.admitted:
                derived.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.NODE_ADMITTED,
                        causation_patch_id=patch.patch_id,
                        node_id=n.node_id,
                        to_lifecycle=final.lifecycle.value,
                        graph_version=i,
                    )
                )
        for e in new_edges:
            if e.edge_id not in {x.edge_id for x in edges}:
                edges.append(e)
                derived.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.EDGE_ADDED,
                        node_id=e.edge_id,
                        graph_version=i,
                    )
                )

        # recompute validity for all Tasks + Goals, produce derived events
        _recompute_all_validity(
            graph_id,
            nodes,
            edges,
            facts_artifact,
            facts_kernel,
            i,
            patch.patch_id,
            derived,
        )

    return nodes, edges, derived


def _is_evidence_edge_going_to_verification(n, edges) -> bool:
    return n.node_type == "evidence" and any(
        e.edge_type == EdgeType.PRODUCES and e.source_node_id == n.node_id for e in edges
    )


def _recompute_all_validity(
    graph_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    facts_artifact,
    facts_kernel,
    graph_version: int,
    causation_patch: str,
    out_events: list[GraphEvent],
) -> None:
    """Recompute validity for all closed-loop Tasks+Goals and emit derived events.

    Idempotent: running twice on unchanged state emits no duplicate events
    (this is guaranteed by the fact that GraphEvents are newly created
    objects with fresh uuids; callers tracking derived derivation elsewhere
    rely on the *state* not changing, not event-list equality).
    """
    # mark tasks STALE when their pinned output artifact version has moved on
    _apply_task_local_invalidation(nodes, edges, graph_version)

    for n in list(nodes.values()):
        if isinstance(n, TaskNode):
            new_validity = _compute_task_validity(n, nodes, edges, facts_artifact, facts_kernel)
            if new_validity is not None and new_validity != n.validity:
                n.validity = new_validity
                n.updated_in_version = graph_version
                if new_validity == NodeValidity.VERIFIED:
                    out_events.append(
                        GraphEvent(
                            graph_id=graph_id,
                            event_type=GraphEventType.TASK_VERIFIED_DERIVED,
                            causation_patch_id=causation_patch,
                            node_id=n.node_id,
                            graph_version=graph_version,
                        )
                    )
                    # CLOSED lifecycle follows VERIFIED
                    if task_should_be_closed(n):
                        n.lifecycle = NodeLifecycle.CLOSED
                        out_events.append(
                            GraphEvent(
                                graph_id=graph_id,
                                event_type=GraphEventType.TASK_CLOSED_DERIVED,
                                causation_patch_id=causation_patch,
                                node_id=n.node_id,
                                graph_version=graph_version,
                            )
                        )
                elif new_validity == NodeValidity.STALE:
                    out_events.append(
                        GraphEvent(
                            graph_id=graph_id,
                            event_type=GraphEventType.TASK_STALE_DERIVED,
                            causation_patch_id=causation_patch,
                            node_id=n.node_id,
                            graph_version=graph_version,
                        )
                    )
                    if n.lifecycle == NodeLifecycle.CLOSED:
                        n.lifecycle = NodeLifecycle.ACTIVE
                        out_events.append(
                            GraphEvent(
                                graph_id=graph_id,
                                event_type=GraphEventType.TASK_REOPENED_DERIVED,
                                causation_patch_id=causation_patch,
                                node_id=n.node_id,
                                graph_version=graph_version,
                            )
                        )

    # goal closure
    for n in list(nodes.values()):
        if isinstance(n, GoalNode):
            closed_now = goal_is_closed(n, nodes, edges)
            was_closed = n.lifecycle == NodeLifecycle.CLOSED
            if closed_now and not was_closed:
                n.lifecycle = NodeLifecycle.CLOSED
                n.updated_in_version = graph_version
                out_events.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.GOAL_CLOSED_DERIVED,
                        causation_patch_id=causation_patch,
                        node_id=n.node_id,
                        graph_version=graph_version,
                    )
                )
            elif not closed_now and was_closed:
                n.lifecycle = NodeLifecycle.ACTIVE
                n.updated_in_version = graph_version
                out_events.append(
                    GraphEvent(
                        graph_id=graph_id,
                        event_type=GraphEventType.GOAL_REOPENED_DERIVED,
                        causation_patch_id=causation_patch,
                        node_id=n.node_id,
                        graph_version=graph_version,
                    )
                )


def _apply_task_local_invalidation(
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    graph_version: int,
) -> None:
    """For each Task already VERIFIED, if its currently-produced ArtifactRef
    versions differ from the versions that were verified, mark the Task STALE.
    """
    for n in list(nodes.values()):
        if not isinstance(n, TaskNode):
            continue
        if n.validity != NodeValidity.VERIFIED:
            continue
        pinned_now = _task_current_artifact_versions(n.node_id, nodes, edges)
        pinned_at_verification = set(
            (b.canonical_uri, int(b.get("version", 0)))
            for b in (
                n.metadata.get("__verified_artifact_versions", [])
                if isinstance(n.metadata, dict)
                else []
            )
        )
        if pinned_at_verification and pinned_now != pinned_at_verification:
            n.validity = NodeValidity.STALE
            n.updated_in_version = graph_version
            if n.lifecycle == NodeLifecycle.CLOSED:
                n.lifecycle = NodeLifecycle.ACTIVE


def _task_current_artifact_versions(
    task_id: str,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
) -> set[tuple[str, int]]:
    out: set[tuple[str, int]] = set()
    for e in edges:
        if e.edge_type == EdgeType.PRODUCES and e.source_node_id == task_id:
            n = nodes.get(e.target_node_id)
            if isinstance(n, ArtifactRefNode):
                out.add((n.canonical_uri, n.version))
    return out


def _compute_task_validity(
    task: TaskNode,
    nodes: dict[str, AnyNode],
    edges: list[VPGEdge],
    facts_artifact,
    facts_kernel,
) -> NodeValidity | None:
    """Return the derived validity for a Task, or None if unchanged (UNVERIFIED passthrough)."""
    # STALE passthrough (no auto elevation — kernel/projection rebuild should not lift STALE)
    if task.validity == NodeValidity.STALE:
        return NodeValidity.STALE
    if task_is_verified(
        task,
        nodes=nodes,
        edges=edges,
        facts_artifact=facts_artifact,
        facts_kernel=facts_kernel,
    ):
        return NodeValidity.VERIFIED
    return None
