"""SQLite-backed GraphStore — single source of merged graph state.

Tables:
- graphs                  (graph_id PK)
- graph_versions          (graph_id, version)     — committed GraphVersion rows
- graph_patches           (patch_id PK)            — committed patch proposals
- graph_events            (event_id PK)            — append-only event store
- graph_nodes_projection  (node_id PK)              — materialized nodes
- graph_edges_projection  (edge_id PK)              — materialized edges
- graph_idempotency       (author_pid, graph_id, idempotency_key) — composite unique
- graph_validity_projection (node_id PK)
- graph_readiness_projection (task_id PK)

Patch commit is atomic: everything (patch record, events, version advance,
projection update, idempotency lock) happens inside one transaction.  If the
projection update fails, the whole commit rolls back — no half-committed state,
no GraphVersion skip, no duplicate node/edge.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from .errors import VPGCode, VPGError
from .events import GraphEvent, GraphEventType
from .models import (
    AnyNode,
    ArtifactRefNode,
    EdgeType,
    EvidenceNode,
    GoalNode,
    GraphRecord,
    GraphVersion,
    NodeType,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from .patches import GraphPatchProposal


def _utcnow_iso() -> str:
    return datetime.now(ISO_UTC).isoformat()


# We store datetimes as ISO-8601 text to keep columns TEXT-comparable.
ISO_UTC = UTC


def _dt_iso(d: datetime) -> str:
    return d.isoformat()


def _dt_from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _model_to_json(obj: Any) -> str:
    return obj.model_dump_json()  # type: ignore[no-any-return]


def _row_to_record(row: sqlite3.Row) -> GraphRecord:
    return GraphRecord(
        graph_id=row["graph_id"],
        owner_pid=row["owner_pid"],
        current_version=row["current_version"],
        created_at=_dt_from_iso(row["created_at"]),
        updated_at=_dt_from_iso(row["updated_at"]),
        closed=int(row["closed"]) == 1,
        metadata=_json_loads(row["metadata_json"]),
    )


def _json_loads(s: str) -> Any:
    import json

    return json.loads(s)


def _json_dumps(obj: Any) -> str:
    import json

    return json.dumps(obj, sort_keys=True)


# ── Schema ───────────────────────────────────────────────────────────────────
_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS graphs (
        graph_id            TEXT PRIMARY KEY,
        owner_pid           TEXT NOT NULL,
        current_version     INTEGER NOT NULL,
        created_at          TEXT NOT NULL,
        updated_at          TEXT NOT NULL,
        closed              INTEGER NOT NULL DEFAULT 0,
        metadata_json       TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_versions (
        graph_id            TEXT NOT NULL,
        version             INTEGER NOT NULL,
        parent_version      INTEGER,
        patch_id            TEXT NOT NULL,
        projection_hash     TEXT NOT NULL,
        committed_by_pid    TEXT NOT NULL,
        committed_at        TEXT NOT NULL,
        PRIMARY KEY (graph_id, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_patches (
        patch_id            TEXT PRIMARY KEY,
        graph_id            TEXT NOT NULL,
        committed_version   INTEGER NOT NULL,
        author_pid          TEXT NOT NULL,
        idempotency_key     TEXT NOT NULL,
        reason              TEXT NOT NULL DEFAULT '',
        causation_ids_json  TEXT NOT NULL DEFAULT '[]',
        operations_json     TEXT NOT NULL,
        applied_at          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_events (
        event_id            TEXT PRIMARY KEY,
        graph_id            TEXT NOT NULL,
        event_type          TEXT NOT NULL,
        causation_patch_id  TEXT,
        subject_id          TEXT,
        subject_kind        TEXT,
        node_id             TEXT,
        to_lifecycle        TEXT,
        to_validity         TEXT,
        verification_ids_json TEXT NOT NULL DEFAULT '[]',
        evidence_ids_json   TEXT NOT NULL DEFAULT '[]',
        artifact_bindings_json TEXT NOT NULL DEFAULT '[]',
        dependency_task_ids_json TEXT NOT NULL DEFAULT '[]',
        ready_frontier_json TEXT NOT NULL DEFAULT '[]',
        graph_version       INTEGER,
        payload_json        TEXT NOT NULL DEFAULT '{}',
        recorded_at         TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_nodes_projection (
        node_id             TEXT PRIMARY KEY,
        graph_id            TEXT NOT NULL,
        node_type           TEXT NOT NULL,
        payload_json        TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_edges_projection (
        edge_id             TEXT PRIMARY KEY,
        graph_id            TEXT NOT NULL,
        edge_type           TEXT NOT NULL,
        source_node_id      TEXT NOT NULL,
        target_node_id      TEXT NOT NULL,
        created_in_version  INTEGER NOT NULL,
        created_by_pid      TEXT NOT NULL,
        created_at          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_idempotency (
        author_pid          TEXT NOT NULL,
        graph_id            TEXT NOT NULL,
        idempotency_key     TEXT NOT NULL,
        patch_id            TEXT NOT NULL,
        committed_version   INTEGER NOT NULL,
        applied_at          TEXT NOT NULL,
        PRIMARY KEY (author_pid, graph_id, idempotency_key)
    )
    """,
]


class GraphStore:
    """Persistence layer for one VPG instance.

    Accepts either a SQLAlchemy-style sqlite3 connection or a path string.
    """

    def __init__(self, conn: sqlite3.Connection | str, *, read_only: bool = False) -> None:
        self._owns_conn = isinstance(conn, str)
        self._read_only = read_only
        if isinstance(conn, str):
            self.conn = sqlite3.connect(conn)
        else:
            self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        if not self._read_only:
            self.conn.execute("PRAGMA journal_mode = WAL")
            self._init_schema()

    def close(self) -> None:
        """Release the sqlite connection — only if this store opened it."""
        if self._owns_conn:
            self.conn.close()

    def _assert_writable(self) -> None:
        if self._read_only:
            raise PermissionError("read-only GraphStore cannot mutate projections")

    # ── schema ─────────────────────────────────────────────────────────────
    def _init_schema(self) -> None:
        with self.conn:
            for stmt in _SCHEMA:
                self.conn.execute(stmt)

    # ── read helpers ───────────────────────────────────────────────────────
    def get_record(self, graph_id: str) -> GraphRecord | None:
        r = self.conn.execute("SELECT * FROM graphs WHERE graph_id = ?", (graph_id,)).fetchone()
        return _row_to_record(r) if r else None

    def get_version(self, graph_id: str, version: int) -> GraphVersion | None:
        r = self.conn.execute(
            "SELECT * FROM graph_versions WHERE graph_id = ? AND version = ?",
            (graph_id, version),
        ).fetchone()
        if r is None:
            return None
        return GraphVersion(
            graph_id=r["graph_id"],
            version=r["version"],
            parent_version=r["parent_version"],
            patch_id=r["patch_id"],
            projection_hash=r["projection_hash"],
            committed_by_pid=r["committed_by_pid"],
            committed_at=_dt_from_iso(r["committed_at"]),
        )

    def get_all_nodes(self, graph_id: str) -> list[AnyNode]:
        rows = self.conn.execute(
            "SELECT payload_json FROM graph_nodes_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchall()
        out: list[AnyNode] = []
        for r in rows:
            out.append(_node_from_json(r["payload_json"]))
        return out

    def get_all_edges(self, graph_id: str) -> list[VPGEdge]:
        rows = self.conn.execute(
            "SELECT * FROM graph_edges_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchall()
        return [
            VPGEdge(
                edge_id=r["edge_id"],
                graph_id=r["graph_id"],
                edge_type=EdgeType(r["edge_type"]),
                source_node_id=r["source_node_id"],
                target_node_id=r["target_node_id"],
                created_in_version=r["created_in_version"],
                created_by_pid=r["created_by_pid"],
                created_at=_dt_from_iso(r["created_at"]),
            )
            for r in rows
        ]

    def get_events(self, graph_id: str, since_version: int | None = None) -> list[GraphEvent]:
        if since_version is None:
            rows = self.conn.execute(
                "SELECT * FROM graph_events WHERE graph_id = ? ORDER BY recorded_at, event_id",
                (graph_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM graph_events WHERE graph_id = ? AND graph_version >= ? "
                "ORDER BY recorded_at, event_id",
                (graph_id, since_version),
            ).fetchall()
        return [_row_to_event(r) for r in rows]

    def list_all_graph_ids(self) -> list[str]:
        rows = self.conn.execute("SELECT graph_id FROM graphs").fetchall()
        return [r["graph_id"] for r in rows]

    def has_idempotency(self, key: tuple[str, str, str]) -> tuple[str, int] | None:
        r = self.conn.execute(
            "SELECT patch_id, committed_version FROM graph_idempotency "
            "WHERE author_pid = ? AND graph_id = ? AND idempotency_key = ?",
            key,
        ).fetchone()
        if r is None:
            return None
        return r["patch_id"], r["committed_version"]

    # ── write helpers ──────────────────────────────────────────────────────
    def create_graph(self, record: GraphRecord) -> GraphRecord:
        self._assert_writable()
        existing = self.get_record(record.graph_id)
        if existing is not None:
            raise VPGError(VPGCode.GRAPH_ALREADY_EXISTS, record.graph_id)
        with self.conn:
            self.conn.execute(
                "INSERT INTO graphs "
                "(graph_id, owner_pid, current_version, created_at, updated_at, closed, metadata_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    record.graph_id,
                    record.owner_pid,
                    record.current_version,
                    _dt_iso(record.created_at),
                    _dt_iso(record.updated_at),
                    int(record.closed),
                    _json_dumps(record.metadata),
                ),
            )
            # initial v0 GraphVersion
            self.conn.execute(
                "INSERT INTO graph_versions "
                "(graph_id, version, parent_version, patch_id, projection_hash, committed_by_pid, committed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    record.graph_id,
                    0,
                    None,
                    "init",
                    _empty_projection_hash(record.graph_id),
                    record.owner_pid,
                    _dt_iso(record.created_at),
                ),
            )
        return record

    def close_graph(self, graph_id: str) -> None:
        self._assert_writable()
        with self.conn:
            self.conn.execute(
                "UPDATE graphs SET closed = 1, updated_at = ? WHERE graph_id = ?",
                (_dt_now_iso(), graph_id),
            )

    def update_record_version(
        self,
        graph_id: str,
        new_version: int,
        projection_hash: str,
    ) -> None:
        self._assert_writable()
        with self.conn:
            rec = self.get_record(graph_id)
            if rec is None:
                raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
            if rec.closed:
                raise VPGError(VPGCode.GRAPH_CLOSED, graph_id)
            if new_version != rec.current_version + 1:
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    f"expected next version {rec.current_version + 1}, got {new_version}",
                )
            self.conn.execute(
                "UPDATE graphs SET current_version = ?, updated_at = ? WHERE graph_id = ?",
                (new_version, _dt_now_iso(), graph_id),
            )

    def commit_graph_version(self, gv: GraphVersion) -> None:
        self._assert_writable()
        with self.conn:
            self.conn.execute(
                "INSERT INTO graph_versions "
                "(graph_id, version, parent_version, patch_id, projection_hash, committed_by_pid, committed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    gv.graph_id,
                    gv.version,
                    gv.parent_version,
                    gv.patch_id,
                    gv.projection_hash,
                    gv.committed_by_pid,
                    _dt_iso(gv.committed_at),
                ),
            )

    def commit_patch(
        self,
        patch: GraphPatchProposal,
        *,
        patch_id: str,
        committed_version: int,
        applied_at: str,
        events: Iterable[GraphEvent],
        nodes_to_upsert: Iterable[tuple[str, AnyNode]],
        edges_to_upsert: Iterable[VPGEdge],
    ) -> None:
        """Atomically commit a patch + derived state — all inside one txn.

        Either everything succeeds, or the whole txn rolls back.
        """
        self._assert_writable()
        ev_list = list(events)
        nd_list = list(nodes_to_upsert)
        ed_list = list(edges_to_upsert)
        graph_id = patch.graph_id
        new_version = committed_version
        projection_hash = _compute_projection_hash(graph_id, new_version, nd_list, ed_list)

        gv = GraphVersion(
            graph_id=graph_id,
            version=new_version,
            parent_version=new_version - 1 if new_version > 0 else None,
            patch_id=patch_id,
            projection_hash=projection_hash,
            committed_by_pid=patch.author_pid,
            committed_at=_dt_from_iso(applied_at),
        )

        with self.conn:
            # 1. idempotency
            self.conn.execute(
                "INSERT INTO graph_idempotency "
                "(author_pid, graph_id, idempotency_key, patch_id, committed_version, applied_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    patch.author_pid,
                    graph_id,
                    patch.idempotency_key,
                    patch_id,
                    new_version,
                    applied_at,
                ),
            )
            # 2. patch record
            self.conn.execute(
                "INSERT INTO graph_patches "
                "(patch_id, graph_id, committed_version, author_pid, idempotency_key, "
                "reason, causation_ids_json, operations_json, applied_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    patch_id,
                    graph_id,
                    new_version,
                    patch.author_pid,
                    patch.idempotency_key,
                    patch.reason,
                    _json_dumps(list(patch.causation_ids)),
                    _model_to_json(patch),
                    applied_at,
                ),
            )

            # 3. graph version advance
            self.conn.execute(
                "UPDATE graphs SET current_version = ?, updated_at = ? WHERE graph_id = ?",
                (new_version, applied_at, graph_id),
            )
            # 4. graph version row
            self.conn.execute(
                "INSERT INTO graph_versions "
                "(graph_id, version, parent_version, patch_id, projection_hash, committed_by_pid, committed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    gv.graph_id,
                    gv.version,
                    gv.parent_version,
                    gv.patch_id,
                    gv.projection_hash,
                    gv.committed_by_pid,
                    _dt_iso(gv.committed_at),
                ),
            )
            # 5. events
            for ev in ev_list:
                self.conn.execute(
                    "INSERT INTO graph_events "
                    "(event_id, graph_id, event_type, causation_patch_id, subject_id, subject_kind, "
                    "node_id, to_lifecycle, to_validity, verification_ids_json, evidence_ids_json, "
                    "artifact_bindings_json, dependency_task_ids_json, ready_frontier_json, "
                    "graph_version, payload_json, recorded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        ev.event_id,
                        ev.graph_id,
                        ev.event_type.value,
                        ev.causation_patch_id,
                        ev.subject_id,
                        ev.subject_kind,
                        ev.node_id,
                        ev.to_lifecycle,
                        ev.to_validity,
                        _json_dumps(list(ev.verification_ids)),
                        _json_dumps(list(ev.evidence_ids)),
                        _json_dumps(
                            [
                                b if isinstance(b, dict) else b.model_dump()
                                for b in ev.artifact_bindings
                            ]
                        ),
                        _json_dumps(list(ev.dependency_task_ids)),
                        _json_dumps(list(ev.ready_frontier)),
                        ev.graph_version,
                        _json_dumps(ev.payload),
                        _dt_iso(ev.recorded_at),
                    ),
                )

            # 6. node projection upsert
            for node_id, node in nd_list:
                self.conn.execute(
                    "INSERT INTO graph_nodes_projection (node_id, graph_id, node_type, payload_json) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(node_id) DO UPDATE SET "
                    "graph_id=excluded.graph_id, node_type=excluded.node_type, payload_json=excluded.payload_json",
                    (
                        node_id,
                        node.graph_id,
                        node.node_type.value,
                        _model_to_json(node),
                    ),
                )

            # 7. edge projection
            for edge in ed_list:
                self.conn.execute(
                    "INSERT INTO graph_edges_projection "
                    "(edge_id, graph_id, edge_type, source_node_id, target_node_id, "
                    "created_in_version, created_by_pid, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(edge_id) DO UPDATE SET "
                    "graph_id=excluded.graph_id, edge_type=excluded.edge_type, "
                    "source_node_id=excluded.source_node_id, target_node_id=excluded.target_node_id, "
                    "created_in_version=excluded.created_in_version, created_by_pid=excluded.created_by_pid, "
                    "created_at=excluded.created_at",
                    (
                        edge.edge_id,
                        edge.graph_id,
                        edge.edge_type.value,
                        edge.source_node_id,
                        edge.target_node_id,
                        edge.created_in_version,
                        edge.created_by_pid,
                        _dt_iso(edge.created_at),
                    ),
                )

    def delete_projection(self, graph_id: str) -> None:
        """Drop the materialized projection (keeps patch/event history) so it
        can be rebuilt via projection replay."""
        self._assert_writable()
        with self.conn:
            node_ids = _graph_node_ids(self.conn, graph_id)
            if node_ids:
                self.conn.execute(
                    f"DELETE FROM graph_nodes_projection WHERE node_id IN ({','.join('?' for _ in node_ids)})",
                    node_ids,
                )
            self.conn.execute(
                "DELETE FROM graph_edges_projection WHERE graph_id = ?",
                (graph_id,),
            )


def _graph_node_ids(conn: sqlite3.Connection, graph_id: str) -> list[str]:
    return [
        r[0]
        for r in conn.execute(
            "SELECT node_id FROM graph_nodes_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchall()
    ]


def _empty_projection_hash(graph_id: str) -> str:
    import hashlib

    return hashlib.sha256(f"empty:{graph_id}".encode()).hexdigest()


def _compute_projection_hash(
    graph_id: str,
    version: int,
    nodes: list[tuple[str, AnyNode]],
    edges: list[VPGEdge],
) -> str:
    """Deterministic hash of the entire materialized projection."""
    import hashlib

    h = hashlib.sha256()
    h.update(graph_id.encode())
    h.update(f"::{version}".encode())
    for node_id, node in sorted(nodes, key=lambda x: x[0]):
        h.update(node_id.encode())
        h.update(node.model_dump_json().encode())
        h.update(b"|")
    for e in sorted(edges, key=lambda e: e.edge_id):
        h.update(e.edge_id.encode())
        h.update(e.source_node_id.encode())
        h.update(e.target_node_id.encode())
        h.update(b"|")
    return h.hexdigest()


def _row_to_event(r: sqlite3.Row) -> GraphEvent:
    import json

    return GraphEvent(
        event_id=r["event_id"],
        graph_id=r["graph_id"],
        event_type=GraphEventType(r["event_type"]),
        causation_patch_id=r["causation_patch_id"],
        subject_id=r["subject_id"],
        subject_kind=r["subject_kind"],
        node_id=r["node_id"],
        to_lifecycle=r["to_lifecycle"],
        to_validity=r["to_validity"],
        verification_ids=tuple(json.loads(r["verification_ids_json"])),
        evidence_ids=tuple(json.loads(r["evidence_ids_json"])),
        artifact_bindings=tuple(json.loads(r["artifact_bindings_json"])),
        dependency_task_ids=tuple(json.loads(r["dependency_task_ids_json"])),
        ready_frontier=tuple(json.loads(r["ready_frontier_json"])),
        graph_version=r["graph_version"],
        payload=json.loads(r["payload_json"]),
        recorded_at=_dt_from_iso(r["recorded_at"]),
    )


def _node_from_json(s: str) -> AnyNode:
    import json as _json

    data = _json.loads(s)
    ntype = data["node_type"]
    mapping = {
        NodeType.GOAL: GoalNode,
        NodeType.TASK: TaskNode,
        NodeType.ARTIFACT_REF: ArtifactRefNode,
        NodeType.VERIFICATION: VerificationNode,
        NodeType.EVIDENCE: EvidenceNode,
    }
    cls = mapping[NodeType(ntype)]
    return cls(**data)  # type: ignore[no-any-return]


def _dt_now_iso() -> str:
    return datetime.now(UTC).isoformat()
