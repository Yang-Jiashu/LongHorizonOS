"""SQLite-backed GraphStore — single source of merged graph state.

Tables:
- graphs                  (graph_id PK)
- graph_versions          (graph_id, version)     — committed GraphVersion rows
- graph_patches           (patch_id PK)            — committed patch proposals
- graph_events            (event_id PK)            — append-only event store
- graph_nodes_projection  ((graph_id, node_id) PK)  — materialized nodes
- graph_edges_projection  ((graph_id, edge_id) PK)  — materialized edges
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
import threading
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .errors import VPGCode, VPGError
from .events import GraphEvent, GraphEventType, ready_frontier_hash
from .models import (
    AnyNode,
    ArtifactRefNode,
    EdgeType,
    EvidenceNode,
    GoalNode,
    GraphRecord,
    GraphVersion,
    LeaseCommitGuard,
    NodeType,
    TaskNode,
    VerificationNode,
    VPGEdge,
)
from .patches import AttachEvidenceOp, GraphPatchProposal


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


_READY_FRONTIER_SUMMARY_ENCODING = "summary-v1"


def _serialize_ready_frontier(event: GraphEvent) -> str:
    """Persist READY frontier events as a constant-size count+digest summary.

    The full frontier remains available on the in-memory event produced by the
    derivation engine.  Persisting it verbatim at every graph version creates
    an avoidable O(V²) event-log payload for a graph whose frontier grows with
    V.  A digest is sufficient for audit/determinism checks; the authoritative
    current frontier is always recomputable from the VPG projection.

    Legacy/non-frontier events retain the historical JSON-list representation.
    """

    if event.event_type != GraphEventType.READY_FRONTIER_UPDATED:
        return _json_dumps(list(event.ready_frontier))

    if event.ready_frontier:
        count = len(event.ready_frontier)
        digest = ready_frontier_hash(event.ready_frontier)
    else:
        # Permit callers that already provide only a compact summary.  This is
        # useful for storage/replay tooling and keeps the model extensible.
        count = 0 if event.ready_frontier_count is None else int(event.ready_frontier_count)
        digest = (
            ready_frontier_hash(())
            if event.ready_frontier_hash is None
            else str(event.ready_frontier_hash)
        )

    if count < 0 or len(digest) != 64:
        raise VPGError(
            VPGCode.STORAGE_ERROR,
            "READY frontier summary has an invalid count or SHA-256 digest",
        )
    return _json_dumps(
        {
            "encoding": _READY_FRONTIER_SUMMARY_ENCODING,
            "count": count,
            "hash": digest,
        }
    )


def _deserialize_ready_frontier(
    raw_json: str,
) -> tuple[tuple[str, ...], int | None, str | None]:
    """Decode both legacy full-list and compact frontier event encodings."""

    import json

    raw = json.loads(raw_json)
    if isinstance(raw, list):
        # Historical rows contain the complete ordered frontier.
        if not all(isinstance(item, str) for item in raw):
            raise VPGError(
                VPGCode.STORAGE_ERROR,
                "legacy READY frontier payload contains a non-string task id",
            )
        return tuple(raw), None, None
    if not isinstance(raw, dict) or raw.get("encoding") != _READY_FRONTIER_SUMMARY_ENCODING:
        raise VPGError(
            VPGCode.STORAGE_ERROR,
            "unknown READY frontier payload encoding",
        )
    try:
        count = int(raw["count"])
        digest = str(raw["hash"])
    except (KeyError, TypeError, ValueError) as exc:
        raise VPGError(
            VPGCode.STORAGE_ERROR,
            "malformed READY frontier summary",
        ) from exc
    if count < 0 or len(digest) != 64:
        raise VPGError(
            VPGCode.STORAGE_ERROR,
            "READY frontier summary has an invalid count or SHA-256 digest",
        )
    return (), count, digest


@dataclass(frozen=True, slots=True)
class HistoryRetentionContract:
    """The explicit lower bound of projection versions retained for recovery."""

    graph_id: str
    earliest_recoverable_version: int
    checkpoint_projection_hash: str
    updated_at: datetime
    updated_by: str
    reason: str


@dataclass(frozen=True, slots=True)
class HistoryCompactionResult:
    """Outcome of one atomic projection-history compaction."""

    graph_id: str
    previous_earliest_version: int
    earliest_recoverable_version: int
    current_version: int
    checkpoint_projection_hash: str
    deleted_snapshot_headers: int
    deleted_node_revisions: int
    deleted_edge_revisions: int


@dataclass(frozen=True, slots=True)
class TrustedProjectionMigrationPlan:
    """Hash-bound preview for an explicit materialized-projection migration."""

    graph_id: str
    current_version: int
    projection_hash: str
    node_count: int
    edge_count: int


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
        node_id             TEXT NOT NULL,
        graph_id            TEXT NOT NULL,
        node_type           TEXT NOT NULL,
        payload_json        TEXT NOT NULL,
        PRIMARY KEY (graph_id, node_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_edges_projection (
        edge_id             TEXT NOT NULL,
        graph_id            TEXT NOT NULL,
        edge_type           TEXT NOT NULL,
        source_node_id      TEXT NOT NULL,
        target_node_id      TEXT NOT NULL,
        created_in_version  INTEGER NOT NULL,
        created_by_pid      TEXT NOT NULL,
        created_at          TEXT NOT NULL,
        PRIMARY KEY (graph_id, edge_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_projection_snapshots (
        graph_id            TEXT NOT NULL,
        version             INTEGER NOT NULL,
        projection_hash     TEXT NOT NULL,
        PRIMARY KEY (graph_id, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_node_history (
        graph_id            TEXT NOT NULL,
        version             INTEGER NOT NULL,
        node_id             TEXT NOT NULL,
        node_type           TEXT NOT NULL,
        payload_json        TEXT NOT NULL,
        PRIMARY KEY (graph_id, version, node_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_graph_node_history_entity_version
        ON graph_node_history (graph_id, node_id, version DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_edge_history (
        graph_id            TEXT NOT NULL,
        version             INTEGER NOT NULL,
        edge_id             TEXT NOT NULL,
        edge_type           TEXT NOT NULL,
        source_node_id      TEXT NOT NULL,
        target_node_id      TEXT NOT NULL,
        created_in_version  INTEGER NOT NULL,
        created_by_pid      TEXT NOT NULL,
        created_at          TEXT NOT NULL,
        PRIMARY KEY (graph_id, version, edge_id)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_graph_edge_history_entity_version
        ON graph_edge_history (graph_id, edge_id, version DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_history_retention (
        graph_id                     TEXT PRIMARY KEY,
        earliest_recoverable_version INTEGER NOT NULL CHECK (
            earliest_recoverable_version >= 0
        ),
        checkpoint_projection_hash   TEXT NOT NULL,
        updated_at                   TEXT NOT NULL,
        updated_by                   TEXT NOT NULL,
        reason                       TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS graph_history_lifecycle_events (
        event_id                     TEXT PRIMARY KEY,
        graph_id                     TEXT NOT NULL,
        operation                    TEXT NOT NULL,
        previous_earliest_version    INTEGER NOT NULL,
        earliest_recoverable_version INTEGER NOT NULL,
        checkpoint_version           INTEGER NOT NULL,
        checkpoint_projection_hash   TEXT NOT NULL,
        actor                        TEXT NOT NULL,
        reason                       TEXT NOT NULL,
        recorded_at                  TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_graph_history_lifecycle_events_graph
        ON graph_history_lifecycle_events (graph_id, recorded_at, event_id)
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
        self._write_lock = threading.RLock()
        if isinstance(conn, str):
            self.conn = sqlite3.connect(conn, check_same_thread=False)
        else:
            self.conn = conn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA busy_timeout = 5000")
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

    @contextmanager
    def _immediate_transaction(self) -> Iterator[None]:
        """Run one local write transaction after taking SQLite's writer lock."""

        with self._write_lock:
            if self.conn.in_transaction:
                raise RuntimeError("GraphStore write started inside an existing transaction")
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    def _validate_lease_commit_guard(self, guard: LeaseCommitGuard) -> None:
        """Validate an exact lease generation inside the active writer txn."""

        try:
            lease = self.conn.execute(
                "SELECT lease_id, resource_id, owner_pid, mode, fencing_token, expires_at "
                "FROM leases_projection WHERE lease_id = ?",
                (guard.lease_id,),
            ).fetchone()
            token = self.conn.execute(
                "SELECT last_token FROM resource_fencing_tokens WHERE resource_id = ?",
                (guard.resource_id,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise VPGError(
                VPGCode.LEASE_FENCE_LOST,
                "lease authority tables are unavailable in this SQLite backing",
            ) from exc

        if lease is None:
            raise VPGError(VPGCode.LEASE_FENCE_LOST, "authoritative lease no longer exists")
        if (
            str(lease["lease_id"]) != guard.lease_id
            or str(lease["resource_id"]) != guard.resource_id
            or str(lease["owner_pid"]) != guard.owner_pid
            or str(lease["mode"]) != "exclusive"
            or int(lease["fencing_token"]) != guard.fencing_token
        ):
            raise VPGError(
                VPGCode.LEASE_FENCE_LOST,
                "authoritative lease identity or ownership generation changed",
            )

        try:
            observed_expiry = _dt_from_iso(str(lease["expires_at"]))
            expected_expiry = guard.expires_at
        except (TypeError, ValueError) as exc:
            raise VPGError(VPGCode.LEASE_FENCE_LOST, "lease expiry is malformed") from exc
        if observed_expiry.tzinfo is None or expected_expiry.tzinfo is None:
            raise VPGError(VPGCode.LEASE_FENCE_LOST, "lease expiry is not timezone-aware")
        # Expiry is a liveness predicate, not a new ownership generation.
        # A legal renewal may update this column (including shortening it)
        # while preserving the same lease id/token.  Validate only the
        # authoritative current expiry below.
        if observed_expiry.astimezone(UTC) <= datetime.now(UTC):
            raise VPGError(VPGCode.LEASE_FENCE_LOST, "authoritative lease expired")
        if token is None or int(token["last_token"]) != guard.fencing_token:
            raise VPGError(
                VPGCode.LEASE_FENCE_LOST,
                "resource fencing token was superseded",
            )

    def validate_commit_guard(self, guard: LeaseCommitGuard) -> None:
        """Validate a guard without mutating graph state.

        Used for guarded idempotency replays: an already committed patch is
        read-only, but a stale owner must not be able to present it as a valid
        live ownership operation after release/reassignment.
        """

        self._assert_writable()
        with self._immediate_transaction():
            self._validate_lease_commit_guard(guard)

    # ── schema ─────────────────────────────────────────────────────────────
    def _init_schema(self) -> None:
        with self.conn:
            for stmt in _SCHEMA:
                self.conn.execute(stmt)
            self._migrate_projection_primary_keys()
            self._migrate_initial_projection_snapshots()
            self._migrate_history_retention_contracts()

    def _migrate_initial_projection_snapshots(self) -> None:
        """Backfill only the provably-empty v0 snapshot for legacy databases.

        Older releases used a special ``sha256("empty:<graph_id>")`` value for
        GraphVersion 0 and had no durable projection-history tables.  Version 0
        is defined to contain no nodes or edges, so it is safe to canonicalise
        that one hash and add an empty snapshot header.  Later versions are
        deliberately *not* copied from the mutable materialized projection:
        doing so would bless potentially corrupt cache rows as audit history.
        Such legacy non-empty graphs therefore fail closed until explicitly
        migrated from a trusted backup/export.
        """

        rows = self.conn.execute(
            "SELECT g.graph_id, v.parent_version, v.patch_id, v.projection_hash "
            "FROM graphs AS g "
            "JOIN graph_versions AS v ON v.graph_id = g.graph_id AND v.version = 0"
        ).fetchall()
        for row in rows:
            graph_id = str(row["graph_id"])
            retained = self.conn.execute(
                "SELECT earliest_recoverable_version "
                "FROM graph_history_retention WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()
            lifecycle_floor = self.conn.execute(
                "SELECT 1 FROM graph_history_lifecycle_events "
                "WHERE graph_id = ? AND earliest_recoverable_version > 0 LIMIT 1",
                (graph_id,),
            ).fetchone()
            if (
                retained is not None and int(retained["earliest_recoverable_version"]) > 0
            ) or lifecycle_floor is not None:
                # An explicit compaction/migration contract intentionally
                # removed v0. Do not silently recreate a pruned snapshot on
                # reopen.
                continue
            canonical_hash = compute_projection_hash(graph_id, 0, (), ())
            legacy_hash = _empty_projection_hash(graph_id)
            if (
                row["parent_version"] is not None
                or row["patch_id"] != "init"
                or row["projection_hash"] not in {canonical_hash, legacy_hash}
            ):
                # Leave malformed history untouched.  Snapshot loading and
                # recovery will reject it with GRAPH_RECOVERY_FAILED.
                continue
            if row["projection_hash"] == legacy_hash:
                self.conn.execute(
                    "UPDATE graph_versions SET projection_hash = ? "
                    "WHERE graph_id = ? AND version = 0 AND projection_hash = ?",
                    (canonical_hash, graph_id, legacy_hash),
                )
            self.conn.execute(
                "INSERT INTO graph_projection_snapshots "
                "(graph_id, version, projection_hash) VALUES (?, 0, ?) "
                "ON CONFLICT(graph_id, version) DO NOTHING",
                (graph_id, canonical_hash),
            )

    def _migrate_history_retention_contracts(self) -> None:
        """Declare version 0 as the default retention floor without pruning.

        This metadata-only backfill is safe for legacy databases because it
        neither copies mutable projection rows nor claims that a non-zero
        snapshot exists. Missing or corrupt history still fails closed through
        ``load_projection_snapshot``.
        """

        now = _utcnow_iso()
        self.conn.execute(
            """
            INSERT INTO graph_history_retention (
                graph_id,
                earliest_recoverable_version,
                checkpoint_projection_hash,
                updated_at,
                updated_by,
                reason
            )
            SELECT
                g.graph_id,
                0,
                v.projection_hash,
                ?,
                g.owner_pid,
                'default full-history retention'
            FROM graphs AS g
            JOIN graph_versions AS v
              ON v.graph_id = g.graph_id
             AND v.version = 0
            JOIN graph_projection_snapshots AS s
              ON s.graph_id = g.graph_id
             AND s.version = 0
             AND s.projection_hash = v.projection_hash
            WHERE NOT EXISTS (
                SELECT 1
                FROM graph_history_lifecycle_events AS e
                WHERE e.graph_id = g.graph_id
            )
            ON CONFLICT(graph_id) DO NOTHING
            """,
            (now,),
        )

    def _migrate_projection_primary_keys(self) -> None:
        """Upgrade legacy globally-keyed projections to graph-local ids.

        VPG node and edge ids are scoped by ``graph_id`` throughout the public
        API.  Older databases nevertheless used ``node_id``/``edge_id`` alone
        as SQLite primary keys, so compiling a second graph with the same task
        id could move the first graph's projection row.  Rebuild both tables in
        the current transaction while preserving all existing rows.
        """

        self._migrate_projection_table(
            table="graph_nodes_projection",
            id_column="node_id",
            columns=("node_id", "graph_id", "node_type", "payload_json"),
            create_sql="""
                CREATE TABLE graph_nodes_projection (
                    node_id      TEXT NOT NULL,
                    graph_id     TEXT NOT NULL,
                    node_type    TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY (graph_id, node_id)
                )
            """,
        )
        self._migrate_projection_table(
            table="graph_edges_projection",
            id_column="edge_id",
            columns=(
                "edge_id",
                "graph_id",
                "edge_type",
                "source_node_id",
                "target_node_id",
                "created_in_version",
                "created_by_pid",
                "created_at",
            ),
            create_sql="""
                CREATE TABLE graph_edges_projection (
                    edge_id            TEXT NOT NULL,
                    graph_id           TEXT NOT NULL,
                    edge_type          TEXT NOT NULL,
                    source_node_id     TEXT NOT NULL,
                    target_node_id     TEXT NOT NULL,
                    created_in_version INTEGER NOT NULL,
                    created_by_pid     TEXT NOT NULL,
                    created_at         TEXT NOT NULL,
                    PRIMARY KEY (graph_id, edge_id)
                )
            """,
        )

    def _migrate_projection_table(
        self,
        *,
        table: str,
        id_column: str,
        columns: tuple[str, ...],
        create_sql: str,
    ) -> None:
        legacy = f"{table}_legacy_global_id"
        self._recover_projection_shadow_table(
            table=table,
            legacy=legacy,
            id_column=id_column,
            columns=columns,
            create_sql=create_sql,
        )

        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        primary_key = [
            str(row["name"])
            for row in sorted((row for row in rows if row["pk"]), key=lambda row: row["pk"])
        ]
        expected = ["graph_id", id_column]
        if primary_key == expected:
            return
        if primary_key != [id_column]:
            raise RuntimeError(
                f"unsupported {table} primary key {primary_key!r}; expected "
                f"{[id_column]!r} or {expected!r}"
            )

        self.conn.execute(f"ALTER TABLE {table} RENAME TO {legacy}")
        self.conn.execute(create_sql)
        column_list = ", ".join(columns)
        self.conn.execute(f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM {legacy}")
        self.conn.execute(f"DROP TABLE {legacy}")

    def _recover_projection_shadow_table(
        self,
        *,
        table: str,
        legacy: str,
        id_column: str,
        columns: tuple[str, ...],
        create_sql: str,
    ) -> None:
        """Finish an interrupted legacy-PK migration without dropping rows.

        A previous process may have committed ``ALTER TABLE ... RENAME`` and
        crashed before copying rows or dropping the shadow table.  Schema
        bootstrap then creates an empty destination table.  Silently ignoring
        (or dropping) the legacy table would make committed projection rows
        disappear, so copy only rows that are absent and fail closed on any
        key whose payload differs.
        """

        shadow = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (legacy,),
        ).fetchone()
        if shadow is None:
            return

        destination = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        if destination is None:
            self.conn.execute(create_sql)

        dest_pk = self._primary_key_columns(table)
        expected_pk = ["graph_id", id_column]
        if dest_pk != expected_pk:
            raise RuntimeError(
                f"cannot recover {legacy}: {table} primary key is {dest_pk!r}, "
                f"expected {expected_pk!r}"
            )
        if self._primary_key_columns(legacy) != [id_column]:
            raise RuntimeError(
                f"cannot recover {legacy}: unsupported shadow primary key "
                f"{self._primary_key_columns(legacy)!r}"
            )

        column_list = ", ".join(columns)
        key_join = f"dest.graph_id = legacy.graph_id AND dest.{id_column} = legacy.{id_column}"
        differing_columns = " OR ".join(
            f"dest.{column} IS NOT legacy.{column}" for column in columns
        )
        conflict = self.conn.execute(
            f"SELECT 1 FROM {legacy} AS legacy "
            f"JOIN {table} AS dest ON {key_join} "
            f"WHERE {differing_columns} LIMIT 1"
        ).fetchone()
        if conflict is not None:
            raise RuntimeError(
                f"cannot recover {legacy}: destination contains conflicting projection rows"
            )

        self.conn.execute(
            f"INSERT INTO {table} ({column_list}) "
            f"SELECT {column_list} FROM {legacy} AS legacy "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table} AS dest WHERE {key_join})"
        )
        self.conn.execute(f"DROP TABLE {legacy}")

    def _primary_key_columns(self, table: str) -> list[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return [
            str(row["name"])
            for row in sorted((row for row in rows if row["pk"]), key=lambda row: row["pk"])
        ]

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

    def get_history_retention_contract(
        self,
        graph_id: str,
    ) -> HistoryRetentionContract:
        """Return and structurally validate the projection-history floor."""

        record = self.get_record(graph_id)
        if record is None:
            raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
        try:
            row = self.conn.execute(
                "SELECT * FROM graph_history_retention WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"history retention metadata is unavailable for graph {graph_id!r}",
            ) from exc
        if row is None:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"history retention metadata is missing for graph {graph_id!r}",
            )

        try:
            contract = HistoryRetentionContract(
                graph_id=graph_id,
                earliest_recoverable_version=int(row["earliest_recoverable_version"]),
                checkpoint_projection_hash=str(row["checkpoint_projection_hash"]),
                updated_at=_dt_from_iso(str(row["updated_at"])),
                updated_by=str(row["updated_by"]),
                reason=str(row["reason"]),
            )
        except (TypeError, ValueError) as exc:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"history retention metadata is malformed for graph {graph_id!r}",
            ) from exc

        floor = contract.earliest_recoverable_version
        if floor < 0 or floor > record.current_version:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"history retention floor {floor} is outside graph {graph_id!r} "
                f"version range 0..{record.current_version}",
            )
        checkpoint = self.conn.execute(
            "SELECT v.projection_hash AS version_hash, "
            "s.projection_hash AS snapshot_hash "
            "FROM graph_versions AS v "
            "LEFT JOIN graph_projection_snapshots AS s "
            "ON s.graph_id = v.graph_id AND s.version = v.version "
            "WHERE v.graph_id = ? AND v.version = ?",
            (graph_id, floor),
        ).fetchone()
        if (
            checkpoint is None
            or checkpoint["snapshot_hash"] is None
            or str(checkpoint["version_hash"]) != contract.checkpoint_projection_hash
            or str(checkpoint["snapshot_hash"]) != contract.checkpoint_projection_hash
        ):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"history retention checkpoint is missing or inconsistent for "
                f"graph {graph_id!r} version {floor}",
            )
        return contract

    def has_projection_snapshot(self, graph_id: str, version: int) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM graph_projection_snapshots WHERE graph_id = ? AND version = ?",
            (graph_id, version),
        ).fetchone()
        return row is not None

    def load_projection_snapshot(
        self,
        graph_id: str,
        version: int,
    ) -> tuple[dict[str, AnyNode], list[VPGEdge]]:
        """Load and cryptographically validate one immutable projection version.

        A snapshot header distinguishes a legitimate empty graph from missing
        history.  No materialized-projection row is consulted, so recovery can
        repair corrupt cache JSON without treating it as authoritative.
        """

        contract = self.get_history_retention_contract(graph_id)
        if version < contract.earliest_recoverable_version:
            raise VPGError(
                VPGCode.GRAPH_HISTORY_PRUNED,
                f"graph {graph_id!r} version {version} was pruned; earliest "
                f"recoverable version is {contract.earliest_recoverable_version}",
            )
        header = self.conn.execute(
            "SELECT projection_hash FROM graph_projection_snapshots "
            "WHERE graph_id = ? AND version = ?",
            (graph_id, version),
        ).fetchone()
        version_row = self.conn.execute(
            "SELECT projection_hash FROM graph_versions WHERE graph_id = ? AND version = ?",
            (graph_id, version),
        ).fetchone()
        if version_row is None:
            if self.get_record(graph_id) is None:
                raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"graph version {version} is missing for graph {graph_id!r}",
            )
        if header is None:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"durable projection snapshot is missing for graph {graph_id!r} version {version}",
            )

        # History rows are append-only entity revisions.  Older databases may
        # contain a full projection at every version; the latest revision
        # query is compatible with both layouts.
        node_rows = self.conn.execute(
            """
            SELECT h.node_id, h.node_type, h.payload_json
            FROM graph_node_history AS h
            JOIN (
                SELECT node_id, MAX(version) AS version
                FROM graph_node_history
                WHERE graph_id = ? AND version <= ?
                GROUP BY node_id
            ) AS latest
              ON latest.node_id = h.node_id
             AND latest.version = h.version
            WHERE h.graph_id = ?
            ORDER BY h.node_id
            """,
            (graph_id, version, graph_id),
        ).fetchall()
        nodes: dict[str, AnyNode] = {}
        try:
            for row in node_rows:
                node = _node_from_json(row["payload_json"])
                if (
                    node.graph_id != graph_id
                    or node.node_id != row["node_id"]
                    or node.node_type.value != row["node_type"]
                ):
                    raise VPGError(
                        VPGCode.GRAPH_RECOVERY_FAILED,
                        "snapshot node key/type/payload mismatch for "
                        f"({graph_id!r}, {version}, {row['node_id']!r})",
                    )
                if node.node_id in nodes:
                    raise VPGError(
                        VPGCode.GRAPH_RECOVERY_FAILED,
                        f"snapshot contains duplicate node id {node.node_id!r}",
                    )
                nodes[node.node_id] = node
        except VPGError:
            raise
        except Exception as exc:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"cannot decode node history for graph {graph_id!r} version {version}",
            ) from exc

        edge_rows = self.conn.execute(
            """
            SELECT h.*
            FROM graph_edge_history AS h
            JOIN (
                SELECT edge_id, MAX(version) AS version
                FROM graph_edge_history
                WHERE graph_id = ? AND version <= ?
                GROUP BY edge_id
            ) AS latest
              ON latest.edge_id = h.edge_id
             AND latest.version = h.version
            WHERE h.graph_id = ?
            ORDER BY h.edge_id
            """,
            (graph_id, version, graph_id),
        ).fetchall()
        try:
            edges = [_edge_from_row(row) for row in edge_rows]
        except Exception as exc:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"cannot decode edge history for graph {graph_id!r} version {version}",
            ) from exc
        if any(edge.graph_id != graph_id for edge in edges):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "snapshot edge contains a foreign graph id",
            )
        if len({edge.edge_id for edge in edges}) != len(edges):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "snapshot contains duplicate edge ids",
            )
        node_ids = set(nodes)
        if any(
            edge.source_node_id not in node_ids or edge.target_node_id not in node_ids
            for edge in edges
        ):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "snapshot edge references a missing node",
            )

        actual_hash = compute_projection_hash(graph_id, version, nodes.values(), edges)
        snapshot_hash = str(header["projection_hash"])
        graph_version_hash = str(version_row["projection_hash"])
        if snapshot_hash != graph_version_hash or actual_hash != snapshot_hash:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"projection snapshot hash mismatch for graph {graph_id!r} version {version}",
            )
        return nodes, edges

    def preview_trusted_projection_migration(
        self,
        graph_id: str,
    ) -> TrustedProjectionMigrationPlan:
        """Preview an opt-in trust boundary for a snapshot-less legacy graph.

        The returned hash binds the exact mutable materialized projection that
        an operator is considering trusting. Calling this method never writes
        history and does not make that projection authoritative.
        """

        record = self.get_record(graph_id)
        if record is None:
            raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
        self._validate_trusted_projection_migration_source(record)
        nodes = self.get_all_nodes(graph_id)
        edges = self.get_all_edges(graph_id)
        self._validate_projection_shape(graph_id, nodes, edges, context="trusted migration")
        return TrustedProjectionMigrationPlan(
            graph_id=graph_id,
            current_version=record.current_version,
            projection_hash=compute_projection_hash(
                graph_id,
                record.current_version,
                nodes,
                edges,
            ),
            node_count=len(nodes),
            edge_count=len(edges),
        )

    def _validate_trusted_projection_migration_source(self, record: GraphRecord) -> None:
        graph_id = record.graph_id
        current_version = record.current_version
        if current_version <= 0:
            raise VPGError(
                VPGCode.TRUSTED_MIGRATION_REJECTED,
                "trusted projection migration is only for a non-zero legacy graph version",
            )

        contract = self.get_history_retention_contract(graph_id)
        if contract.earliest_recoverable_version != 0:
            raise VPGError(
                VPGCode.TRUSTED_MIGRATION_REJECTED,
                f"graph {graph_id!r} already has an explicit compacted or migrated "
                "history retention contract",
            )
        nonzero_snapshot = self.conn.execute(
            "SELECT version FROM graph_projection_snapshots "
            "WHERE graph_id = ? AND version > 0 LIMIT 1",
            (graph_id,),
        ).fetchone()
        history_row = self.conn.execute(
            """
            SELECT 'node' AS entity_kind, version
            FROM graph_node_history
            WHERE graph_id = ?
            UNION ALL
            SELECT 'edge' AS entity_kind, version
            FROM graph_edge_history
            WHERE graph_id = ?
            LIMIT 1
            """,
            (graph_id, graph_id),
        ).fetchone()
        if nonzero_snapshot is not None or history_row is not None:
            raise VPGError(
                VPGCode.TRUSTED_MIGRATION_REJECTED,
                f"graph {graph_id!r} contains projection history; trusted migration "
                "accepts only a snapshot-less legacy source",
            )

        rows = self.conn.execute(
            "SELECT version, parent_version, patch_id FROM graph_versions "
            "WHERE graph_id = ? ORDER BY version",
            (graph_id,),
        ).fetchall()
        if len(rows) != current_version + 1:
            raise VPGError(
                VPGCode.TRUSTED_MIGRATION_REJECTED,
                f"graph {graph_id!r} does not have a contiguous 0..{current_version} "
                "GraphVersion chain",
            )
        for expected_version, row in enumerate(rows):
            version = int(row["version"])
            parent = row["parent_version"]
            if version != expected_version:
                raise VPGError(
                    VPGCode.TRUSTED_MIGRATION_REJECTED,
                    f"graph {graph_id!r} has a non-contiguous GraphVersion chain",
                )
            expected_parent = None if version == 0 else version - 1
            if parent != expected_parent:
                raise VPGError(
                    VPGCode.TRUSTED_MIGRATION_REJECTED,
                    f"graph {graph_id!r} version {version} has invalid parent {parent!r}",
                )
            if version == 0:
                if str(row["patch_id"]) != "init":
                    raise VPGError(
                        VPGCode.TRUSTED_MIGRATION_REJECTED,
                        f"graph {graph_id!r} has an invalid version-0 origin",
                    )
                continue
            patch = self.conn.execute(
                "SELECT graph_id, committed_version FROM graph_patches WHERE patch_id = ?",
                (row["patch_id"],),
            ).fetchone()
            if (
                patch is None
                or str(patch["graph_id"]) != graph_id
                or int(patch["committed_version"]) != version
            ):
                raise VPGError(
                    VPGCode.TRUSTED_MIGRATION_REJECTED,
                    f"graph {graph_id!r} version {version} is not bound to a "
                    "matching committed patch",
                )
        self._reject_future_history(graph_id, current_version)

    @staticmethod
    def _validate_projection_shape(
        graph_id: str,
        nodes: Iterable[AnyNode],
        edges: Iterable[VPGEdge],
        *,
        context: str,
    ) -> None:
        node_list = list(nodes)
        edge_list = list(edges)
        node_ids = [node.node_id for node in node_list]
        edge_ids = [edge.edge_id for edge in edge_list]
        if len(node_ids) != len(set(node_ids)) or len(edge_ids) != len(set(edge_ids)):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"{context} projection contains duplicate entity ids",
            )
        if any(node.graph_id != graph_id for node in node_list) or any(
            edge.graph_id != graph_id for edge in edge_list
        ):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"{context} projection contains a foreign graph id",
            )
        node_id_set = set(node_ids)
        if any(
            edge.source_node_id not in node_id_set or edge.target_node_id not in node_id_set
            for edge in edge_list
        ):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"{context} projection edge references a missing node",
            )

    def _reject_future_history(self, graph_id: str, current_version: int) -> None:
        future = self.conn.execute(
            """
            SELECT 'snapshot' AS entity_kind, version
            FROM graph_projection_snapshots
            WHERE graph_id = ? AND version > ?
            UNION ALL
            SELECT 'node' AS entity_kind, version
            FROM graph_node_history
            WHERE graph_id = ? AND version > ?
            UNION ALL
            SELECT 'edge' AS entity_kind, version
            FROM graph_edge_history
            WHERE graph_id = ? AND version > ?
            LIMIT 1
            """,
            (
                graph_id,
                current_version,
                graph_id,
                current_version,
                graph_id,
                current_version,
            ),
        ).fetchone()
        if future is not None:
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                f"graph {graph_id!r} contains future {future['entity_kind']} "
                f"history at version {future['version']}",
            )

    def get_node(self, graph_id: str, node_id: str) -> AnyNode | None:
        """Point lookup for one node.

        ``(graph_id, node_id)`` is the projection primary key, so this is an
        index seek.  Semantically identical to calling ``get_all_nodes`` and
        indexing by node_id -- it just avoids reading and revalidating the whole
        projection to answer a single-row question.
        """
        row = self.conn.execute(
            "SELECT payload_json FROM graph_nodes_projection WHERE graph_id = ? AND node_id = ?",
            (graph_id, node_id),
        ).fetchone()
        if row is None:
            return None
        node = _node_from_json(row["payload_json"])
        if node.graph_id != graph_id or node.node_id != node_id:
            raise VPGError(
                VPGCode.STORAGE_ERROR,
                f"node projection key/payload mismatch for ({graph_id!r}, {node_id!r})",
            )
        return node

    def get_edge(self, graph_id: str, edge_id: str) -> VPGEdge | None:
        """Point lookup for one edge using its graph-local primary key."""
        row = self.conn.execute(
            "SELECT * FROM graph_edges_projection WHERE graph_id = ? AND edge_id = ?",
            (graph_id, edge_id),
        ).fetchone()
        if row is None:
            return None
        edge = _edge_from_row(row)
        if edge.graph_id != graph_id or edge.edge_id != edge_id:
            raise VPGError(
                VPGCode.STORAGE_ERROR,
                f"edge projection key/payload mismatch for ({graph_id!r}, {edge_id!r})",
            )
        return edge

    def get_all_nodes(self, graph_id: str) -> list[AnyNode]:
        rows = self.conn.execute(
            "SELECT node_id, payload_json FROM graph_nodes_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchall()
        out: list[AnyNode] = []
        for r in rows:
            node = _node_from_json(r["payload_json"])
            if node.graph_id != graph_id or node.node_id != r["node_id"]:
                raise VPGError(
                    VPGCode.STORAGE_ERROR,
                    f"node projection key/payload mismatch for ({graph_id!r}, {r['node_id']!r})",
                )
            out.append(node)
        return out

    def get_all_edges(self, graph_id: str) -> list[VPGEdge]:
        rows = self.conn.execute(
            "SELECT * FROM graph_edges_projection WHERE graph_id = ?",
            (graph_id,),
        ).fetchall()
        edges = [_edge_from_row(r) for r in rows]
        if any(edge.graph_id != graph_id for edge in edges):
            raise VPGError(
                VPGCode.STORAGE_ERROR,
                f"edge projection contains a foreign graph row for {graph_id!r}",
            )
        return edges

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
            # v0 is a real, durable empty projection snapshot.  Keep the
            # header even though there are no node/edge history rows so that
            # recovery can distinguish an empty graph from missing history.
            empty_hash = compute_projection_hash(record.graph_id, 0, (), ())
            self.conn.execute(
                "UPDATE graph_versions SET projection_hash = ? WHERE graph_id = ? AND version = 0",
                (empty_hash, record.graph_id),
            )
            self.conn.execute(
                "INSERT INTO graph_projection_snapshots "
                "(graph_id, version, projection_hash) VALUES (?, ?, ?)",
                (record.graph_id, 0, empty_hash),
            )
            self.conn.execute(
                "INSERT INTO graph_history_retention "
                "(graph_id, earliest_recoverable_version, checkpoint_projection_hash, "
                "updated_at, updated_by, reason) VALUES (?, 0, ?, ?, ?, ?)",
                (
                    record.graph_id,
                    empty_hash,
                    _dt_iso(record.created_at),
                    record.owner_pid,
                    "default full-history retention",
                ),
            )
        return record

    def compact_projection_history(
        self,
        graph_id: str,
        *,
        retain_from_version: int,
        compacted_by: str,
        reason: str,
    ) -> HistoryCompactionResult:
        """Atomically retain projection recovery only from one version onward.

        GraphVersion, patch, event, and idempotency audit rows are deliberately
        preserved. Only projection snapshot headers and entity revisions below
        the new recovery floor are pruned. A complete, hash-validated
        checkpoint is installed at the floor before the transaction commits.
        """

        self._assert_writable()
        if not compacted_by.strip():
            raise ValueError("compacted_by must identify the operator")
        if not reason.strip():
            raise ValueError("reason must explain the retention decision")

        with self._immediate_transaction():
            record = self.get_record(graph_id)
            if record is None:
                raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
            contract = self.get_history_retention_contract(graph_id)
            previous_floor = contract.earliest_recoverable_version
            current_version = record.current_version
            if retain_from_version < previous_floor:
                raise ValueError(
                    f"cannot lower history floor from {previous_floor} to "
                    f"{retain_from_version}; pruned history cannot be recovered"
                )
            if retain_from_version > current_version:
                raise ValueError(
                    f"retain_from_version {retain_from_version} exceeds current "
                    f"graph version {current_version}"
                )

            checkpoint_nodes, checkpoint_edges = self.load_projection_snapshot(
                graph_id,
                retain_from_version,
            )
            checkpoint_version = self.get_version(graph_id, retain_from_version)
            if checkpoint_version is None:
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    f"checkpoint GraphVersion {retain_from_version} is missing",
                )
            checkpoint_hash = checkpoint_version.projection_hash

            # Validate every version that the new contract promises before
            # making destructive changes. The transaction also performs the
            # same verification after rewriting the checkpoint.
            for version in range(retain_from_version, current_version + 1):
                self.load_projection_snapshot(graph_id, version)
            self._reject_future_history(graph_id, current_version)

            if retain_from_version == previous_floor:
                return HistoryCompactionResult(
                    graph_id=graph_id,
                    previous_earliest_version=previous_floor,
                    earliest_recoverable_version=previous_floor,
                    current_version=current_version,
                    checkpoint_projection_hash=checkpoint_hash,
                    deleted_snapshot_headers=0,
                    deleted_node_revisions=0,
                    deleted_edge_revisions=0,
                )

            deleted_snapshot_headers = self.conn.execute(
                "DELETE FROM graph_projection_snapshots WHERE graph_id = ? AND version < ?",
                (graph_id, retain_from_version),
            ).rowcount
            deleted_node_revisions = self.conn.execute(
                "DELETE FROM graph_node_history WHERE graph_id = ? AND version < ?",
                (graph_id, retain_from_version),
            ).rowcount
            deleted_edge_revisions = self.conn.execute(
                "DELETE FROM graph_edge_history WHERE graph_id = ? AND version < ?",
                (graph_id, retain_from_version),
            ).rowcount

            # Replace any incremental rows at the floor with a complete
            # checkpoint. This makes all later retained revisions independent
            # from the history being pruned.
            self.conn.execute(
                "DELETE FROM graph_node_history WHERE graph_id = ? AND version = ?",
                (graph_id, retain_from_version),
            )
            self.conn.execute(
                "DELETE FROM graph_edge_history WHERE graph_id = ? AND version = ?",
                (graph_id, retain_from_version),
            )
            self.conn.executemany(
                "INSERT INTO graph_node_history "
                "(graph_id, version, node_id, node_type, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        retain_from_version,
                        node.node_id,
                        node.node_type.value,
                        _model_to_json(node),
                    )
                    for node in sorted(
                        checkpoint_nodes.values(),
                        key=lambda item: item.node_id,
                    )
                ],
            )
            self.conn.executemany(
                "INSERT INTO graph_edge_history "
                "(graph_id, version, edge_id, edge_type, source_node_id, "
                "target_node_id, created_in_version, created_by_pid, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        retain_from_version,
                        edge.edge_id,
                        edge.edge_type.value,
                        edge.source_node_id,
                        edge.target_node_id,
                        edge.created_in_version,
                        edge.created_by_pid,
                        _dt_iso(edge.created_at),
                    )
                    for edge in sorted(checkpoint_edges, key=lambda item: item.edge_id)
                ],
            )
            recorded_at = _utcnow_iso()
            self.conn.execute(
                "UPDATE graph_history_retention SET "
                "earliest_recoverable_version = ?, "
                "checkpoint_projection_hash = ?, updated_at = ?, "
                "updated_by = ?, reason = ? WHERE graph_id = ?",
                (
                    retain_from_version,
                    checkpoint_hash,
                    recorded_at,
                    compacted_by,
                    reason,
                    graph_id,
                ),
            )
            self.conn.execute(
                "INSERT INTO graph_history_lifecycle_events "
                "(event_id, graph_id, operation, previous_earliest_version, "
                "earliest_recoverable_version, checkpoint_version, "
                "checkpoint_projection_hash, actor, reason, recorded_at) "
                "VALUES (?, ?, 'compact', ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid4()),
                    graph_id,
                    previous_floor,
                    retain_from_version,
                    retain_from_version,
                    checkpoint_hash,
                    compacted_by,
                    reason,
                    recorded_at,
                ),
            )

            for version in range(retain_from_version, current_version + 1):
                self.load_projection_snapshot(graph_id, version)

            return HistoryCompactionResult(
                graph_id=graph_id,
                previous_earliest_version=previous_floor,
                earliest_recoverable_version=retain_from_version,
                current_version=current_version,
                checkpoint_projection_hash=checkpoint_hash,
                deleted_snapshot_headers=deleted_snapshot_headers,
                deleted_node_revisions=deleted_node_revisions,
                deleted_edge_revisions=deleted_edge_revisions,
            )

    def migrate_snapshotless_legacy_projection(
        self,
        graph_id: str,
        *,
        expected_current_version: int,
        expected_projection_hash: str,
        trusted: bool,
        trusted_by: str,
        reason: str,
    ) -> TrustedProjectionMigrationPlan:
        """Establish a trusted baseline from a legacy materialized projection.

        This is intentionally opt-in and lossy: projection recovery begins at
        the current version because earlier projections cannot be proven from
        a snapshot-less database. The caller must preview and confirm the exact
        projection hash; the operation never guesses trust automatically.
        """

        self._assert_writable()
        if trusted is not True:
            raise VPGError(
                VPGCode.TRUSTED_MIGRATION_REJECTED,
                "trusted=True is required to bless mutable materialized rows",
            )
        if not trusted_by.strip():
            raise ValueError("trusted_by must identify the operator")
        if not reason.strip():
            raise ValueError("reason must document why the projection is trusted")

        with self._immediate_transaction():
            preview = self.preview_trusted_projection_migration(graph_id)
            if preview.current_version != expected_current_version:
                raise VPGError(
                    VPGCode.TRUSTED_MIGRATION_REJECTED,
                    f"preview version {expected_current_version} no longer matches "
                    f"current version {preview.current_version}",
                )
            if preview.projection_hash != expected_projection_hash:
                raise VPGError(
                    VPGCode.TRUSTED_MIGRATION_REJECTED,
                    "preview projection hash no longer matches the materialized projection",
                )

            nodes = self.get_all_nodes(graph_id)
            edges = self.get_all_edges(graph_id)
            self._validate_projection_shape(
                graph_id,
                nodes,
                edges,
                context="trusted migration",
            )
            current_version = preview.current_version
            previous_floor = self.get_history_retention_contract(
                graph_id
            ).earliest_recoverable_version
            recorded_at = _utcnow_iso()

            self.conn.execute(
                "DELETE FROM graph_projection_snapshots WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.execute(
                "DELETE FROM graph_node_history WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.execute(
                "DELETE FROM graph_edge_history WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.execute(
                "UPDATE graph_versions SET projection_hash = ? WHERE graph_id = ? AND version = ?",
                (preview.projection_hash, graph_id, current_version),
            )
            self.conn.execute(
                "INSERT INTO graph_projection_snapshots "
                "(graph_id, version, projection_hash) VALUES (?, ?, ?)",
                (graph_id, current_version, preview.projection_hash),
            )
            self.conn.executemany(
                "INSERT INTO graph_node_history "
                "(graph_id, version, node_id, node_type, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        current_version,
                        node.node_id,
                        node.node_type.value,
                        _model_to_json(node),
                    )
                    for node in sorted(nodes, key=lambda item: item.node_id)
                ],
            )
            self.conn.executemany(
                "INSERT INTO graph_edge_history "
                "(graph_id, version, edge_id, edge_type, source_node_id, "
                "target_node_id, created_in_version, created_by_pid, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        current_version,
                        edge.edge_id,
                        edge.edge_type.value,
                        edge.source_node_id,
                        edge.target_node_id,
                        edge.created_in_version,
                        edge.created_by_pid,
                        _dt_iso(edge.created_at),
                    )
                    for edge in sorted(edges, key=lambda item: item.edge_id)
                ],
            )
            self.conn.execute(
                "UPDATE graph_history_retention SET "
                "earliest_recoverable_version = ?, "
                "checkpoint_projection_hash = ?, updated_at = ?, "
                "updated_by = ?, reason = ? WHERE graph_id = ?",
                (
                    current_version,
                    preview.projection_hash,
                    recorded_at,
                    trusted_by,
                    reason,
                    graph_id,
                ),
            )
            self.conn.execute(
                "INSERT INTO graph_history_lifecycle_events "
                "(event_id, graph_id, operation, previous_earliest_version, "
                "earliest_recoverable_version, checkpoint_version, "
                "checkpoint_projection_hash, actor, reason, recorded_at) "
                "VALUES (?, ?, 'trusted_projection_migration', ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid4()),
                    graph_id,
                    previous_floor,
                    current_version,
                    current_version,
                    preview.projection_hash,
                    trusted_by,
                    reason,
                    recorded_at,
                ),
            )

            migrated_nodes, migrated_edges = self.load_projection_snapshot(
                graph_id,
                current_version,
            )
            if (
                len(migrated_nodes) != preview.node_count
                or len(migrated_edges) != preview.edge_count
            ):
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    "trusted migration checkpoint count mismatch",
                )
            return preview

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
        """Reject the legacy split-version write path.

        A graph version is valid only when its patch, events, materialized
        projection, and immutable projection snapshot commit together.  This
        historical helper advanced ``graphs.current_version`` in a standalone
        transaction and could therefore create an unrecoverable version gap.
        Keep the method as an explicit fail-closed compatibility surface
        instead of silently preserving that unsafe behavior.
        """

        del graph_id, new_version, projection_hash
        self._assert_writable()
        raise RuntimeError(
            "standalone graph version updates are disabled; use commit_patch() "
            "so the version and durable projection snapshot commit atomically"
        )

    def commit_graph_version(self, gv: GraphVersion) -> None:
        """Reject the legacy split GraphVersion insert path.

        See :meth:`update_record_version`.  A standalone row would bypass the
        durable snapshot invariant and make verified recovery impossible.
        """

        del gv
        self._assert_writable()
        raise RuntimeError(
            "standalone GraphVersion commits are disabled; use commit_patch() "
            "so all graph state commits in one transaction"
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
        projection_nodes: Iterable[AnyNode] | None = None,
        projection_edges: Iterable[VPGEdge] | None = None,
        commit_guard: LeaseCommitGuard | None = None,
    ) -> None:
        """Atomically commit a patch + derived state — all inside one txn.

        Either everything succeeds, or the whole txn rolls back.
        """
        self._assert_writable()
        ev_list = list(events)
        nd_list = list(nodes_to_upsert)
        ed_list = list(edges_to_upsert)
        full_nodes = list(projection_nodes) if projection_nodes is not None else None
        full_edges = list(projection_edges) if projection_edges is not None else None
        graph_id = patch.graph_id
        new_version = committed_version
        if patch_id != patch.patch_id:
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "commit patch id does not match the proposal payload",
            )
        if new_version != patch.expected_graph_version + 1:
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "committed version must be exactly one greater than the "
                "proposal's expected graph version",
            )
        if any(node_id != node.node_id or node.graph_id != graph_id for node_id, node in nd_list):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "node projection upsert contains a mismatched node id or graph id",
            )
        if any(edge.graph_id != graph_id for edge in ed_list):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "edge projection upsert contains a foreign graph id",
            )
        if any(event.graph_id != graph_id for event in ev_list):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "graph event batch contains a foreign graph id",
            )
        if full_nodes is None or full_edges is None:
            raise ValueError(
                "commit_patch requires the complete projection so durable "
                "recovery history cannot be silently reduced to a delta"
            )
        node_ids = [node.node_id for node in full_nodes]
        edge_ids = [edge.edge_id for edge in full_edges]
        if len(node_ids) != len(set(node_ids)):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "full node projection contains duplicate node ids",
            )
        if len(edge_ids) != len(set(edge_ids)):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "full edge projection contains duplicate edge ids",
            )
        if any(node.graph_id != graph_id for node in full_nodes):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "full node projection contains a foreign graph id",
            )
        if any(edge.graph_id != graph_id for edge in full_edges):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "full edge projection contains a foreign graph id",
            )
        full_node_id_set = set(node_ids)
        if any(
            edge.source_node_id not in full_node_id_set
            or edge.target_node_id not in full_node_id_set
            for edge in full_edges
        ):
            raise VPGError(
                VPGCode.PATCH_REJECTED,
                "full edge projection references a missing node",
            )
        with self._immediate_transaction():
            # The writer lock is held before consulting operational ownership.
            # Release/reassignment and graph commit therefore have one durable
            # serialization order instead of a check-then-commit TOCTOU.
            if commit_guard is not None:
                self._validate_lease_commit_guard(commit_guard)

            current = self.conn.execute(
                "SELECT current_version, closed FROM graphs WHERE graph_id = ?",
                (graph_id,),
            ).fetchone()
            if current is None:
                raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
            if int(current["closed"]) == 1:
                raise VPGError(VPGCode.GRAPH_CLOSED, graph_id)
            if int(current["current_version"]) != new_version - 1:
                raise VPGError(
                    VPGCode.GRAPH_VERSION_CONFLICT,
                    f"expected graph version {new_version - 1}, "
                    f"current is {current['current_version']}",
                )

            # Refuse to extend a graph whose durable parent snapshot is missing
            # or corrupt.  Validate and retain the parent under the same writer
            # transaction so history revisions can be computed against durable
            # state rather than against a mutable cache.
            parent_nodes, parent_edges = self.load_projection_snapshot(graph_id, new_version - 1)
            future_history = self.conn.execute(
                """
                SELECT 'node' AS entity_kind, h.version, h.node_id AS entity_id
                FROM graph_node_history AS h
                WHERE h.graph_id = ?
                  AND h.version >= ?
                UNION ALL
                SELECT 'edge' AS entity_kind, h.version, h.edge_id AS entity_id
                FROM graph_edge_history AS h
                WHERE h.graph_id = ?
                  AND h.version >= ?
                LIMIT 1
                """,
                (graph_id, new_version, graph_id, new_version),
            ).fetchone()
            if future_history is not None:
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    "durable history contains a future "
                    f"{future_history['entity_kind']} revision "
                    f"{future_history['entity_id']!r} at version "
                    f"{future_history['version']}; repair history before committing",
                )
            full_nodes_by_id = {node.node_id: node for node in full_nodes}
            # The materialized projection is a cache, but it is also the
            # candidate source used by the SDK.  Existing durable entities
            # must therefore still match the verified parent before a new
            # commit can extend the history.  Otherwise a valid JSON edit to
            # the cache could be blessed as a new durable revision.
            materialized_nodes = self.get_all_nodes(graph_id)
            materialized_edges = self.get_all_edges(graph_id)
            materialized_nodes_by_id = {node.node_id: node for node in materialized_nodes}
            materialized_edges_by_id = {edge.edge_id: edge for edge in materialized_edges}
            for node_id, parent_node in parent_nodes.items():
                materialized_node = materialized_nodes_by_id.get(node_id)
                if materialized_node is None or _model_to_json(materialized_node) != _model_to_json(
                    parent_node
                ):
                    raise VPGError(
                        VPGCode.GRAPH_RECOVERY_FAILED,
                        "materialized node cache does not match its durable "
                        f"parent revision for node {node_id!r}; recover before "
                        "committing another patch",
                    )
            for edge_id, parent_edge in {edge.edge_id: edge for edge in parent_edges}.items():
                materialized_edge = materialized_edges_by_id.get(edge_id)
                if materialized_edge is None or _model_to_json(materialized_edge) != _model_to_json(
                    parent_edge
                ):
                    raise VPGError(
                        VPGCode.GRAPH_RECOVERY_FAILED,
                        "materialized edge cache does not match its durable "
                        f"parent revision for edge {edge_id!r}; recover before "
                        "committing another patch",
                    )
            attached_evidence_ids = {
                op.evidence_node_id for op in patch.operations if isinstance(op, AttachEvidenceOp)
            }
            extra_materialized_node_ids = set(materialized_nodes_by_id) - set(parent_nodes)
            allowed_materialized_evidence_ids: set[str] = set()
            staged_evidence_ids: set[str] = set()
            for node_id in extra_materialized_node_ids:
                materialized_node = materialized_nodes_by_id[node_id]
                candidate_node = full_nodes_by_id.get(node_id)
                if (
                    isinstance(materialized_node, EvidenceNode)
                    and isinstance(candidate_node, EvidenceNode)
                    and _model_to_json(materialized_node) == _model_to_json(candidate_node)
                ):
                    if node_id in attached_evidence_ids:
                        allowed_materialized_evidence_ids.add(node_id)
                    else:
                        staged_evidence_ids.add(node_id)
                    continue
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    "materialized node cache contains an entity absent from "
                    f"the durable parent: {node_id!r}; only an EvidenceNode "
                    "explicitly referenced by this patch's AttachEvidenceOp "
                    "may enter through the legacy compatibility path",
                )
            extra_materialized_edge_ids = set(materialized_edges_by_id) - {
                edge.edge_id for edge in parent_edges
            }
            if extra_materialized_edge_ids:
                edge_id = sorted(extra_materialized_edge_ids)[0]
                raise VPGError(
                    VPGCode.GRAPH_RECOVERY_FAILED,
                    "materialized edge cache contains an entity absent from "
                    f"the durable parent: {edge_id!r}; recover before committing",
                )
            if staged_evidence_ids & {node_id for node_id, _node in nd_list}:
                node_id = sorted(staged_evidence_ids & {node_id for node_id, _node in nd_list})[0]
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "a staged EvidenceNode may become durable only through "
                    f"this patch's AttachEvidenceOp: {node_id!r}",
                )
            durable_nodes = [node for node in full_nodes if node.node_id not in staged_evidence_ids]
            durable_nodes_by_id = {node.node_id: node for node in durable_nodes}
            durable_node_id_set = set(durable_nodes_by_id)
            if any(
                edge.source_node_id not in durable_node_id_set
                or edge.target_node_id not in durable_node_id_set
                for edge in full_edges
            ):
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "candidate projection references a staged EvidenceNode "
                    "without attaching it through the current patch",
                )
            durable_edges = full_edges
            durable_edges_by_id = {edge.edge_id: edge for edge in durable_edges}
            for node_id, node in nd_list:
                candidate_node = durable_nodes_by_id.get(node_id)
                if candidate_node is None or _model_to_json(node) != _model_to_json(candidate_node):
                    raise VPGError(
                        VPGCode.PATCH_REJECTED,
                        "node projection upsert does not match the supplied "
                        f"candidate projection for node {node_id!r}",
                    )
            for edge in ed_list:
                candidate_edge = durable_edges_by_id.get(edge.edge_id)
                if candidate_edge is None or _model_to_json(edge) != _model_to_json(candidate_edge):
                    raise VPGError(
                        VPGCode.PATCH_REJECTED,
                        "edge projection upsert does not match the supplied "
                        f"candidate projection for edge {edge.edge_id!r}",
                    )
            missing_parent_nodes = set(parent_nodes) - set(full_nodes_by_id)
            missing_parent_edges = {edge.edge_id for edge in parent_edges} - set(
                durable_edges_by_id
            )
            if missing_parent_nodes or missing_parent_edges:
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "candidate projection silently deletes durable entities; "
                    "deletion requires an explicit tombstone protocol",
                )
            upsert_node_ids = {node_id for node_id, _node in nd_list}
            unaccounted_candidate_node_ids = (
                set(durable_nodes_by_id)
                - set(parent_nodes)
                - upsert_node_ids
                - allowed_materialized_evidence_ids
            )
            if unaccounted_candidate_node_ids:
                node_id = sorted(unaccounted_candidate_node_ids)[0]
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "candidate projection contains a new node not supplied by "
                    f"this patch's projection delta: {node_id!r}",
                )
            upsert_edge_ids = {edge.edge_id for edge in ed_list}
            unaccounted_candidate_edge_ids = (
                set(durable_edges_by_id) - {edge.edge_id for edge in parent_edges} - upsert_edge_ids
            )
            if unaccounted_candidate_edge_ids:
                edge_id = sorted(unaccounted_candidate_edge_ids)[0]
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "candidate projection contains a new edge not supplied by "
                    f"this patch's projection delta: {edge_id!r}",
                )
            projection_hash = compute_projection_hash(
                graph_id,
                new_version,
                durable_nodes,
                durable_edges,
            )
            gv = GraphVersion(
                graph_id=graph_id,
                version=new_version,
                parent_version=new_version - 1 if new_version > 0 else None,
                patch_id=patch_id,
                projection_hash=projection_hash,
                committed_by_pid=patch.author_pid,
                committed_at=_dt_from_iso(applied_at),
            )
            parent_edges_by_id = {edge.edge_id: edge for edge in parent_edges}
            node_revisions = []
            for node in durable_nodes:
                parent_node_revision: AnyNode | None = parent_nodes.get(node.node_id)
                # Compare the exact serialized representation used by
                # compute_projection_hash() and history persistence.  A
                # semantic dict reordering must not produce a new hash without
                # also producing a durable revision row.
                if parent_node_revision is None or _model_to_json(
                    parent_node_revision
                ) != _model_to_json(node):
                    node_revisions.append(node)
            edge_revisions = []
            for edge in durable_edges:
                parent_edge_revision: VPGEdge | None = parent_edges_by_id.get(edge.edge_id)
                if parent_edge_revision is None or _model_to_json(
                    parent_edge_revision
                ) != _model_to_json(edge):
                    edge_revisions.append(edge)

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
            advanced = self.conn.execute(
                "UPDATE graphs SET current_version = ?, updated_at = ? "
                "WHERE graph_id = ? AND current_version = ? AND closed = 0",
                (new_version, applied_at, graph_id, new_version - 1),
            )
            if advanced.rowcount != 1:
                current = self.conn.execute(
                    "SELECT current_version, closed FROM graphs WHERE graph_id = ?",
                    (graph_id,),
                ).fetchone()
                if current is None:
                    raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
                if int(current["closed"]) == 1:
                    raise VPGError(VPGCode.GRAPH_CLOSED, graph_id)
                raise VPGError(
                    VPGCode.GRAPH_VERSION_CONFLICT,
                    f"expected graph version {new_version - 1}, "
                    f"current is {current['current_version']}",
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
                        _serialize_ready_frontier(ev),
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
                    "ON CONFLICT(graph_id, node_id) DO UPDATE SET "
                    "node_type=excluded.node_type, payload_json=excluded.payload_json",
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
                    "ON CONFLICT(graph_id, edge_id) DO UPDATE SET "
                    "edge_type=excluded.edge_type, "
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

            materialized_nodes = self.get_all_nodes(graph_id)
            materialized_edges = self.get_all_edges(graph_id)
            materialized_nodes = [
                node for node in materialized_nodes if node.node_id not in staged_evidence_ids
            ]
            materialized_hash = compute_projection_hash(
                graph_id,
                new_version,
                materialized_nodes,
                materialized_edges,
            )
            if materialized_hash != projection_hash:
                raise VPGError(
                    VPGCode.PATCH_REJECTED,
                    "projection delta does not materialize the supplied full snapshot",
                )

            # 8. Immutable entity-revision history.  Only nodes/edges changed
            # by this patch are written; the loader reconstructs a version by
            # taking each entity's latest revision at or before that version.
            # The snapshot header still records the full target projection
            # hash, and all rows live in the same transaction as the patch,
            # GraphVersion and materialized-cache writes.
            self.conn.execute(
                "INSERT INTO graph_projection_snapshots "
                "(graph_id, version, projection_hash) VALUES (?, ?, ?)",
                (graph_id, new_version, projection_hash),
            )
            self.conn.executemany(
                "INSERT INTO graph_node_history "
                "(graph_id, version, node_id, node_type, payload_json) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        new_version,
                        node.node_id,
                        node.node_type.value,
                        _model_to_json(node),
                    )
                    for node in sorted(node_revisions, key=lambda item: item.node_id)
                ],
            )
            self.conn.executemany(
                "INSERT INTO graph_edge_history "
                "(graph_id, version, edge_id, edge_type, source_node_id, "
                "target_node_id, created_in_version, created_by_pid, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        graph_id,
                        new_version,
                        edge.edge_id,
                        edge.edge_type.value,
                        edge.source_node_id,
                        edge.target_node_id,
                        edge.created_in_version,
                        edge.created_by_pid,
                        _dt_iso(edge.created_at),
                    )
                    for edge in sorted(edge_revisions, key=lambda item: item.edge_id)
                ],
            )

    def delete_projection(self, graph_id: str) -> None:
        """Drop the materialized projection (keeps patch/event history) so it
        can be rebuilt via projection replay."""
        self._assert_writable()
        with self.conn:
            self.conn.execute(
                "DELETE FROM graph_nodes_projection WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.execute(
                "DELETE FROM graph_edges_projection WHERE graph_id = ?",
                (graph_id,),
            )

    def replace_projection(
        self,
        graph_id: str,
        *,
        expected_graph_version: int,
        nodes: Iterable[AnyNode],
        edges: Iterable[VPGEdge],
    ) -> None:
        """Atomically replace one graph's materialized projection.

        Replay is performed before this method is called.  The conditional
        no-op update acquires SQLite's writer lock and verifies that no patch
        advanced the graph while replay was running.  The following delete and
        inserts therefore either all commit for the expected version or all
        roll back, preserving the previous projection on any failure.
        """

        self._assert_writable()
        node_list = sorted(nodes, key=lambda node: node.node_id)
        edge_list = sorted(edges, key=lambda edge: edge.edge_id)
        node_ids = [node.node_id for node in node_list]
        edge_ids = [edge.edge_id for edge in edge_list]
        if len(node_ids) != len(set(node_ids)):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "rebuilt projection contains duplicate node ids",
            )
        if len(edge_ids) != len(set(edge_ids)):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "rebuilt projection contains duplicate edge ids",
            )
        if any(node.graph_id != graph_id for node in node_list):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "rebuilt node projection contains a foreign graph id",
            )
        if any(edge.graph_id != graph_id for edge in edge_list):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "rebuilt edge projection contains a foreign graph id",
            )
        node_id_set = set(node_ids)
        if any(
            edge.source_node_id not in node_id_set or edge.target_node_id not in node_id_set
            for edge in edge_list
        ):
            raise VPGError(
                VPGCode.GRAPH_RECOVERY_FAILED,
                "rebuilt edge projection references a missing node",
            )

        with self.conn:
            locked = self.conn.execute(
                "UPDATE graphs SET current_version = current_version "
                "WHERE graph_id = ? AND current_version = ?",
                (graph_id, expected_graph_version),
            )
            if locked.rowcount != 1:
                current = self.conn.execute(
                    "SELECT current_version FROM graphs WHERE graph_id = ?",
                    (graph_id,),
                ).fetchone()
                if current is None:
                    raise VPGError(VPGCode.GRAPH_NOT_FOUND, graph_id)
                raise VPGError(
                    VPGCode.GRAPH_VERSION_CONFLICT,
                    f"projection replay expected graph version "
                    f"{expected_graph_version}, current is "
                    f"{current['current_version']}",
                )

            self.conn.execute(
                "DELETE FROM graph_nodes_projection WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.execute(
                "DELETE FROM graph_edges_projection WHERE graph_id = ?",
                (graph_id,),
            )
            self.conn.executemany(
                "INSERT INTO graph_nodes_projection "
                "(node_id, graph_id, node_type, payload_json) VALUES (?, ?, ?, ?)",
                [
                    (
                        node.node_id,
                        node.graph_id,
                        node.node_type.value,
                        _model_to_json(node),
                    )
                    for node in node_list
                ],
            )
            self.conn.executemany(
                "INSERT INTO graph_edges_projection "
                "(edge_id, graph_id, edge_type, source_node_id, target_node_id, "
                "created_in_version, created_by_pid, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        edge.edge_id,
                        edge.graph_id,
                        edge.edge_type.value,
                        edge.source_node_id,
                        edge.target_node_id,
                        edge.created_in_version,
                        edge.created_by_pid,
                        _dt_iso(edge.created_at),
                    )
                    for edge in edge_list
                ],
            )


def _empty_projection_hash(graph_id: str) -> str:
    import hashlib

    return hashlib.sha256(f"empty:{graph_id}".encode()).hexdigest()


def compute_projection_hash(
    graph_id: str,
    version: int,
    nodes: Iterable[AnyNode],
    edges: Iterable[VPGEdge],
) -> str:
    """Deterministic hash of the entire materialized projection."""
    import hashlib

    h = hashlib.sha256()
    h.update(graph_id.encode())
    h.update(f"::{version}".encode())
    for node in sorted(nodes, key=lambda item: item.node_id):
        h.update(node.node_id.encode())
        h.update(node.model_dump_json().encode())
        h.update(b"|")
    for e in sorted(edges, key=lambda e: e.edge_id):
        h.update(e.edge_id.encode())
        h.update(e.model_dump_json().encode())
        h.update(b"|")
    return h.hexdigest()


def _row_to_event(r: sqlite3.Row) -> GraphEvent:
    import json

    event_type = GraphEventType(r["event_type"])
    ready_frontier: tuple[str, ...] = ()
    ready_frontier_count: int | None = None
    ready_frontier_digest: str | None = None
    if event_type == GraphEventType.READY_FRONTIER_UPDATED:
        (
            ready_frontier,
            ready_frontier_count,
            ready_frontier_digest,
        ) = _deserialize_ready_frontier(r["ready_frontier_json"])
    else:
        raw_frontier = json.loads(r["ready_frontier_json"])
        if not isinstance(raw_frontier, list) or not all(
            isinstance(item, str) for item in raw_frontier
        ):
            raise VPGError(
                VPGCode.STORAGE_ERROR,
                "event ready_frontier_json must be a JSON string list",
            )
        ready_frontier = tuple(raw_frontier)

    return GraphEvent(
        event_id=r["event_id"],
        graph_id=r["graph_id"],
        event_type=event_type,
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
        ready_frontier=ready_frontier,
        ready_frontier_count=ready_frontier_count,
        ready_frontier_hash=ready_frontier_digest,
        graph_version=r["graph_version"],
        payload=json.loads(r["payload_json"]),
        recorded_at=_dt_from_iso(r["recorded_at"]),
    )


def _edge_from_row(r: sqlite3.Row) -> VPGEdge:
    """Single place that maps a graph_edges_projection row to a VPGEdge."""
    return VPGEdge(
        edge_id=r["edge_id"],
        graph_id=r["graph_id"],
        edge_type=EdgeType(r["edge_type"]),
        source_node_id=r["source_node_id"],
        target_node_id=r["target_node_id"],
        created_in_version=r["created_in_version"],
        created_by_pid=r["created_by_pid"],
        created_at=_dt_from_iso(r["created_at"]),
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
