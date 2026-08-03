"""Rebuild the materialized graph projection from the event log (spec 5, 26.2).

Event payloads carry the full node/edge dump on every mutation, and evidence
dumps on every evidence-producing event, so replay is a deterministic
fold over the ordered log.
"""

from __future__ import annotations

from lhos.domain.models import EvidenceRef, GraphEdge, GraphNode
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore


def rebuild_projection(db: Database, run_id: str) -> dict[str, int]:
    """Delete and rebuild nodes/edges/evidence for ``run_id`` from its events.

    Returns counts of rebuilt rows. The runs table is left untouched.
    """
    events = SqliteEventStore(db).list_events(run_id)
    store = _RowWriter(db)
    with db.transaction():
        db.conn.execute("DELETE FROM nodes WHERE run_id = ?", (run_id,))
        db.conn.execute("DELETE FROM edges WHERE run_id = ?", (run_id,))
        db.conn.execute("DELETE FROM evidence WHERE run_id = ?", (run_id,))
        for event in events:
            payload = event.payload
            node = payload.get("node")
            if node:
                store.upsert_node(GraphNode(**node))
            edge = payload.get("edge")
            if edge:
                store.upsert_edge(GraphEdge(**edge))
            evidence = payload.get("evidence")
            if isinstance(evidence, dict):
                evidence = [evidence]
            if evidence:
                for raw in evidence:
                    store.insert_evidence(EvidenceRef(**raw))
    return {
        "nodes": len(
            db.conn.execute("SELECT id FROM nodes WHERE run_id = ?", (run_id,)).fetchall()
        ),
        "edges": len(
            db.conn.execute("SELECT id FROM edges WHERE run_id = ?", (run_id,)).fetchall()
        ),
        "evidence": len(
            db.conn.execute("SELECT id FROM evidence WHERE run_id = ?", (run_id,)).fetchall()
        ),
    }


class _RowWriter:
    """Reuses the graph store's row serialization without emitting events."""

    def __init__(self, db: Database):
        from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
        from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore

        self._store = SqliteGraphStore(db, SqliteEventStore(db))

    def upsert_node(self, node: GraphNode) -> None:
        self._store._upsert_node_row(node)

    def upsert_edge(self, edge: GraphEdge) -> None:
        self._store._upsert_edge_row(edge)

    def insert_evidence(self, evidence: EvidenceRef) -> None:
        self._store._insert_evidence_row(evidence)
