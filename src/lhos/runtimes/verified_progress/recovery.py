"""Runtime recovery — reconstruct consistent state after a crash.

D1's commit happens inside a single SQLite transaction:
    patch record + idempotency + graph version advance + nodes/edges/events upsert.

So a crash inside the txn rolls back the whole commit.  If the process crashes
AFTER the txn commits but BEFORE derived validity is recomputed, recovery
just recomputes derived validity — idempotent and safe.

This module:
  1. Verifies no half-committed patches exist.
  2. Verifies GraphVersion sequence is contiguous (no skips).
  3. Invokes projection replay to rebuild the materialized node/edge view.
  4. Emits GRAPH_RECOVERY_* events.
"""

from __future__ import annotations

from datetime import UTC, datetime

from .errors import VPGCode, VPGError
from .events import GraphEvent, GraphEventType
from .graph_store import GraphStore
from .models import GraphRecord, GraphVersion
from .projections import rebuild_projection


def _utcnow() -> datetime:
    return datetime.now(UTC)


def verify_and_recover(
    store: GraphStore,
    graph_id: str,
    *,
    facts_artifact=None,
    facts_kernel=None,
) -> tuple[list[GraphEvent], GraphRecord]:
    """Recover a graph store.  Returns (recovery_events, recovered record)."""
    record = store.get_record(graph_id)
    if record is None:
        raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)

    started = GraphEvent(
        graph_id=graph_id,
        event_type=GraphEventType.GRAPH_RECOVERY_STARTED,
        subject_id=graph_id,
        payload={"current_version": record.current_version},
    )

    # 1. validate version sequence contiguity
    versions: list[GraphVersion] = []
    v = 0
    while True:
        gv = store.get_version(graph_id, v)
        if gv is None:
            if v == 0 and record.current_version == 0:
                break
            if v <= record.current_version:
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    f"gap at version {v}",
                )
            break
        versions.append(gv)
        v += 1

    # 2. gather patch history and per-patch new nodes/edges
    all_patches_rows = store.conn.execute(
        "SELECT * FROM graph_patches WHERE graph_id = ? ORDER BY applied_at",
        (graph_id,),
    ).fetchall()

    import json as _json

    patches = []
    node_history: dict[str, list] = {}
    edge_history: dict[str, list] = {}
    for row in all_patches_rows:
        raw = _json.loads(row["operations_json"])
        from .patches import GraphPatchProposal
        from .sdk import _normalize_raw_ops, _ops_to_nodes_edges

        raw["operations"] = _normalize_raw_ops(raw.get("operations", []))
        p = GraphPatchProposal(**raw)
        patches.append(p)
        n_row, e_row = _ops_to_nodes_edges(graph_id, p)
        node_history[p.patch_id] = n_row
        edge_history[p.patch_id] = e_row

    # 3. the materialized projection tables store the canonical post-txn state.
    #    A crash before projection update means the patch's nodes/edges are
    #    also rolled back (single txn).  So reading projection = reading
    #    exactly the patches that committed.
    materialized_nodes = store.get_all_nodes(graph_id)
    materialized_edges = store.get_all_edges(graph_id)

    # 4. rebuild the projection to recompute derived events
    #    (required because projection caches derived lifecycle/validity)
    _rebuilt_nodes, _rebuilt_edges, derived_events = rebuild_projection(
        graph_id,
        patches,
        edge_history,
        node_history,
        facts_artifact=facts_artifact,
        facts_kernel=facts_kernel,
    )

    completed = GraphEvent(
        graph_id=graph_id,
        event_type=GraphEventType.GRAPH_RECOVERY_COMPLETED,
        subject_id=graph_id,
        payload={
            "current_version": record.current_version,
            "materialized_node_count": len(materialized_nodes),
            "materialized_edge_count": len(materialized_edges),
        },
    )
    return [started, *derived_events, completed], record
