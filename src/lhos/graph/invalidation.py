"""Local invalidation propagation (spec section 15).

Base rule (spec pseudocode): start from direct consumers of the changed node,
mark VERIFIED / CLAIMED_DONE consumers STALE, propagate along DEPENDS_ON, and
leave pathless branches untouched.

Must-invalidate (必然破坏) deterministic rules:
- ``must_invalidate_ids``: explicit node ids (e.g. CONSTRAINT_CHANGED with an
  ``invalidates: [...]`` payload) — VERIFIED nodes become INVALIDATED;
- ``invalidate_consumers``: direct consumers of the changed node (e.g. an
  artifact was removed) — VERIFIED direct consumers become INVALIDATED.

INVALIDATED nodes are then locally "replanned": transitioned INVALIDATED ->
PENDING (a legal machine transition) so readiness can re-queue them; this is
recorded as the replanned scope. CLAIMED_DONE nodes can never become
INVALIDATED (machine), so they degrade to STALE.

Every propagation appends one INVALIDATION_PROPAGATED event carrying the
Replanning Amplification metrics (spec 15): real affected nodes, replanned
nodes; re-executed nodes are derivable from EXECUTION_STARTED retry_reason
and computed by ``invalidation_metrics``.
"""

from __future__ import annotations

from collections import deque

from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType, EventType, RuntimeEvent


def propagate_invalidation(
    graph_store,
    run_id: str,
    changed_node_id: str,
    actor: str = ActorType.RECONCILER,
    must_invalidate_ids: set[str] | None = None,
    invalidate_consumers: bool = False,
    trigger: dict | None = None,
    local_repair: bool = True,
) -> dict:
    must_invalidate_ids = set(must_invalidate_ids or set())
    graph = graph_store.load_graph(run_id)

    # Seed queue: (node_id, must_invalidate). Explicit must-invalidate targets
    # are seeded even when they are not direct consumers of the changed node
    # (e.g. a constraint invalidating an arbitrary verified node).
    queue: deque[tuple[str, bool]] = deque()
    for consumer in graph.direct_consumers(changed_node_id):
        queue.append((consumer.id, invalidate_consumers))
    for node_id in sorted(must_invalidate_ids):
        queue.append((node_id, True))

    affected: list[str] = []
    stale: list[str] = []
    invalidated: list[str] = []
    seen: set[str] = set()
    while queue:
        node_id, must = queue.popleft()
        if node_id in seen:
            continue
        seen.add(node_id)
        if node_id not in graph.nodes:
            continue
        node = graph.nodes[node_id]
        if node.state not in {NodeState.VERIFIED, NodeState.CLAIMED_DONE}:
            continue
        if must and node.state == NodeState.VERIFIED:
            graph_store.set_state(
                node.id,
                NodeState.INVALIDATED,
                actor=actor,
                event_type=EventType.NODE_INVALIDATED,
                payload_extra={
                    "changed_node_id": changed_node_id,
                    "reason": (trigger or {}).get("reason", "must-invalidate"),
                },
            )
            invalidated.append(node.id)
        else:
            # CLAIMED_DONE -> STALE is a reconciler-level forced transition
            # (spec 15 pseudocode vs section 6 machine).
            graph_store.set_state(
                node.id,
                NodeState.STALE,
                actor=actor,
                event_type=EventType.NODE_MARKED_STALE,
                payload_extra={"changed_node_id": changed_node_id},
                force=True,
            )
            stale.append(node.id)
        affected.append(node.id)
        graph = graph_store.load_graph(run_id)
        queue.extend((n.id, False) for n in graph.dependents(node.id))

    # Local replan: INVALIDATED nodes go back to PENDING so the readiness
    # pass can re-queue them once their dependencies are VERIFIED again.
    # With ``local_repair=False`` (benchmark ablation, spec 25) the INVALIDATED
    # nodes stay INVALIDATED — the run has no repair path and will be stuck.
    replanned: list[str] = []
    if local_repair:
        for node_id in invalidated:
            node = graph_store.get_node(node_id)
            node.metadata["replanned_from"] = "invalidated"
            graph_store.update_node(node, actor=actor, bump_version=False)
            graph_store.set_state(
                node_id,
                NodeState.PENDING,
                actor=actor,
                payload_extra={"reason": "local replan after invalidation"},
            )
            replanned.append(node_id)

    report = {
        "changed_node_id": changed_node_id,
        "affected_node_ids": affected,
        "affected_count": len(affected),
        "stale_node_ids": stale,
        "invalidated_node_ids": invalidated,
        "replanned_node_ids": replanned,
        "replanned_count": len(replanned),
    }
    if affected:
        graph_store._events.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.INVALIDATION_PROPAGATED,
                actor_type=actor,
                payload={**report, "trigger": trigger or {}},
            )
        )
    return report


def invalidation_metrics(event_store, run_id: str) -> list[dict]:
    """Replanning Amplification inputs (spec 15, 24.3): per propagation event,
    the real affected count, the replanned count, and how many affected nodes
    were actually re-executed afterwards (retry_reason recorded at
    EXECUTION_STARTED)."""
    events = event_store.list_events(run_id)
    propagations = [e for e in events if e.event_type == EventType.INVALIDATION_PROPAGATED]
    starts = [e for e in events if e.event_type == EventType.EXECUTION_STARTED]
    report: list[dict] = []
    for prop in propagations:
        affected = set(prop.payload.get("affected_node_ids", []))
        re_executed = {
            e.payload.get("node_id")
            for e in starts
            if e.sequence > prop.sequence
            and e.payload.get("node_id") in affected
            and e.payload.get("retry_reason") in {"stale", "invalidated"}
        }
        report.append(
            {
                "sequence": prop.sequence,
                "changed_node_id": prop.payload.get("changed_node_id"),
                "affected_count": prop.payload.get("affected_count", 0),
                "affected_node_ids": sorted(affected),
                "replanned_count": prop.payload.get("replanned_count", 0),
                "replanned_node_ids": prop.payload.get("replanned_node_ids", []),
                "re_executed_count": len(re_executed),
                "re_executed_node_ids": sorted(re_executed),
            }
        )
    return report
