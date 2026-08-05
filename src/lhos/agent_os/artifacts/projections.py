"""Artifact FS projection read/write helpers.

Each method serializes/deserializes between pydantic models and SQLite rows.
All projection tables can be rebuilt from the Journal.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lhos.agent_os.artifacts.models import (
    ArtifactHandle,
    ArtifactNamespace,
    ArtifactRecord,
    ArtifactVersion,
    ArtifactWatch,
    NamespaceMount,
    NamespaceSnapshot,
    WriteTransaction,
)
from lhos.agent_os.storage.sqlite import SQLiteStorage


class ArtifactProjections:
    """Read/write helpers for artifact projection tables."""

    def __init__(self, storage: SQLiteStorage):
        self._storage = storage

    # ── Artifacts ──────────────────────────────────────────────────────────

    def upsert_artifact(self, rec: ArtifactRecord) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO artifacts_projection
                   (artifact_id, namespace_id, canonical_uri, current_version,
                    artifact_type, created_by_pid, created_at, updated_at,
                    deleted, metadata_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(artifact_id) DO UPDATE SET
                    current_version = excluded.current_version,
                    updated_at = excluded.updated_at,
                    deleted = excluded.deleted,
                    metadata_json = excluded.metadata_json""",
                (
                    rec.artifact_id,
                    rec.namespace_id,
                    rec.canonical_uri,
                    rec.current_version,
                    rec.artifact_type,
                    rec.created_by_pid,
                    rec.created_at.isoformat(),
                    rec.updated_at.isoformat(),
                    int(rec.deleted),
                    SQLiteStorage.dumps(rec.metadata),
                ),
            )

    def get_artifact(self, artifact_id: str) -> ArtifactRecord | None:
        row = self._storage.query_one(
            "SELECT * FROM artifacts_projection WHERE artifact_id = ?",
            (artifact_id,),
        )
        return self._row_to_artifact(row) if row else None

    def get_artifact_by_uri(self, canonical_uri: str) -> ArtifactRecord | None:
        row = self._storage.query_one(
            "SELECT * FROM artifacts_projection WHERE canonical_uri = ?",
            (canonical_uri,),
        )
        return self._row_to_artifact(row) if row else None

    def list_artifacts(self, namespace_id: str) -> list[ArtifactRecord]:
        rows = self._storage.query_all(
            "SELECT * FROM artifacts_projection WHERE namespace_id = ? AND deleted = 0 ORDER BY canonical_uri",
            (namespace_id,),
        )
        return [self._row_to_artifact(r) for r in rows]

    # ── Versions ───────────────────────────────────────────────────────────

    def insert_version(self, ver: ArtifactVersion) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT OR IGNORE INTO artifact_versions_projection
                   (artifact_id, version, content_ref, content_hash, size_bytes,
                    parent_version, committed_by_pid, committed_action_id, committed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ver.artifact_id,
                    ver.version,
                    ver.content_ref,
                    ver.content_hash,
                    ver.size_bytes,
                    ver.parent_version,
                    ver.committed_by_pid,
                    ver.committed_action_id,
                    ver.committed_at.isoformat(),
                ),
            )

    def get_version(self, artifact_id: str, version: int) -> ArtifactVersion | None:
        row = self._storage.query_one(
            "SELECT * FROM artifact_versions_projection WHERE artifact_id = ? AND version = ?",
            (artifact_id, version),
        )
        return self._row_to_version(row) if row else None

    def list_versions(self, artifact_id: str) -> list[ArtifactVersion]:
        rows = self._storage.query_all(
            "SELECT * FROM artifact_versions_projection WHERE artifact_id = ? ORDER BY version",
            (artifact_id,),
        )
        return [self._row_to_version(r) for r in rows]

    def count_committed_bytes(self, namespace_id: str) -> int:
        row = self._storage.query_one(
            """SELECT COALESCE(SUM(v.size_bytes), 0) AS total
               FROM artifact_versions_projection v
               JOIN artifacts_projection a ON v.artifact_id = a.artifact_id
               WHERE a.namespace_id = ?""",
            (namespace_id,),
        )
        return row["total"] if row else 0

    # ── Handles ────────────────────────────────────────────────────────────

    def upsert_handle(self, h: ArtifactHandle) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO artifact_handles_projection
                   (handle_id, pid, artifact_id, mode, opened_version, expected_version,
                    lease_id, transaction_id, opened_at, closed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(handle_id) DO UPDATE SET
                    closed_at = excluded.closed_at,
                    transaction_id = excluded.transaction_id""",
                (
                    h.handle_id,
                    h.pid,
                    h.artifact_id,
                    h.mode,
                    h.opened_version,
                    h.expected_version,
                    h.lease_id,
                    h.transaction_id,
                    h.opened_at.isoformat(),
                    h.closed_at.isoformat() if h.closed_at else None,
                ),
            )

    def get_handle(self, handle_id: str) -> ArtifactHandle | None:
        row = self._storage.query_one(
            "SELECT * FROM artifact_handles_projection WHERE handle_id = ?",
            (handle_id,),
        )
        return self._row_to_handle(row) if row else None

    def list_open_handles_for_pid(self, pid: str) -> list[ArtifactHandle]:
        rows = self._storage.query_all(
            "SELECT * FROM artifact_handles_projection WHERE pid = ? AND closed_at IS NULL",
            (pid,),
        )
        return [self._row_to_handle(r) for r in rows]

    def count_open_handles(self, pid: str) -> int:
        row = self._storage.query_one(
            "SELECT COUNT(*) AS cnt FROM artifact_handles_projection WHERE pid = ? AND closed_at IS NULL",
            (pid,),
        )
        return row["cnt"] if row else 0

    # ── Transactions ───────────────────────────────────────────────────────

    def upsert_transaction(self, tx_model: WriteTransaction) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO write_transactions_projection
                   (transaction_id, artifact_id, pid, expected_version,
                    staged_content_ref, staged_content_hash, staged_size_bytes,
                    state, idempotency_key, created_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(transaction_id) DO UPDATE SET
                    staged_content_ref = excluded.staged_content_ref,
                    staged_content_hash = excluded.staged_content_hash,
                    staged_size_bytes = excluded.staged_size_bytes,
                    state = excluded.state,
                    finished_at = excluded.finished_at""",
                (
                    tx_model.transaction_id,
                    tx_model.artifact_id,
                    tx_model.pid,
                    tx_model.expected_version,
                    tx_model.staged_content_ref,
                    tx_model.staged_content_hash,
                    tx_model.staged_size_bytes,
                    tx_model.state,
                    tx_model.idempotency_key,
                    tx_model.created_at.isoformat(),
                    tx_model.finished_at.isoformat() if tx_model.finished_at else None,
                ),
            )

    def get_transaction(self, transaction_id: str) -> WriteTransaction | None:
        row = self._storage.query_one(
            "SELECT * FROM write_transactions_projection WHERE transaction_id = ?",
            (transaction_id,),
        )
        return self._row_to_transaction(row) if row else None

    def find_transaction_by_idempotency(
        self, artifact_id: str, pid: str, idempotency_key: str
    ) -> WriteTransaction | None:
        row = self._storage.query_one(
            """SELECT * FROM write_transactions_projection
               WHERE artifact_id = ? AND pid = ? AND idempotency_key = ?""",
            (artifact_id, pid, idempotency_key),
        )
        return self._row_to_transaction(row) if row else None

    def list_active_transactions_for_pid(self, pid: str) -> list[WriteTransaction]:
        rows = self._storage.query_all(
            """SELECT * FROM write_transactions_projection
               WHERE pid = ? AND state NOT IN ('committed', 'aborted', 'conflicted', 'uncertain')""",
            (pid,),
        )
        return [self._row_to_transaction(r) for r in rows]

    # ── Namespaces ─────────────────────────────────────────────────────────

    def upsert_namespace(self, ns: ArtifactNamespace) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO namespaces_projection
                   (namespace_id, owner_pid, root_uri, quota_bytes, max_open_handles, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(namespace_id) DO UPDATE SET
                    quota_bytes = excluded.quota_bytes,
                    max_open_handles = excluded.max_open_handles""",
                (
                    ns.namespace_id,
                    ns.owner_pid,
                    ns.root_uri,
                    ns.quota_bytes,
                    ns.max_open_handles,
                    ns.created_at.isoformat(),
                ),
            )

    def get_namespace(self, namespace_id: str) -> ArtifactNamespace | None:
        row = self._storage.query_one(
            "SELECT * FROM namespaces_projection WHERE namespace_id = ?",
            (namespace_id,),
        )
        return self._row_to_namespace(row) if row else None

    # ── Mounts ─────────────────────────────────────────────────────────────

    def upsert_mount(self, m: NamespaceMount) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO mounts_projection
                   (mount_id, namespace_id, mount_point, source_namespace_id,
                    source_prefix, mode, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(mount_id) DO NOTHING""",
                (
                    m.mount_id,
                    m.namespace_id,
                    m.mount_point,
                    m.source_namespace_id,
                    m.source_prefix,
                    m.mode,
                    m.created_at.isoformat(),
                ),
            )

    def list_mounts(self, namespace_id: str) -> list[NamespaceMount]:
        rows = self._storage.query_all(
            "SELECT * FROM mounts_projection WHERE namespace_id = ? ORDER BY mount_point",
            (namespace_id,),
        )
        return [self._row_to_mount(r) for r in rows]

    # ── Watches ────────────────────────────────────────────────────────────

    def upsert_watch(self, w: ArtifactWatch) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO artifact_watches_projection
                   (watch_id, pid, namespace_id, uri_prefix, created_at, active)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(watch_id) DO UPDATE SET active = excluded.active""",
                (
                    w.watch_id,
                    w.pid,
                    w.namespace_id,
                    w.uri_prefix,
                    w.created_at.isoformat(),
                    int(w.active),
                ),
            )

    def list_active_watches(self, namespace_id: str) -> list[ArtifactWatch]:
        rows = self._storage.query_all(
            "SELECT * FROM artifact_watches_projection WHERE namespace_id = ? AND active = 1",
            (namespace_id,),
        )
        return [self._row_to_watch(r) for r in rows]

    # ── Snapshots ──────────────────────────────────────────────────────────

    def upsert_snapshot(self, snap: NamespaceSnapshot) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO namespace_snapshots_projection
                   (snapshot_id, namespace_id, artifact_versions_json,
                    content_refs_json, created_by_pid, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    snap.snapshot_id,
                    snap.namespace_id,
                    SQLiteStorage.dumps(snap.artifact_versions),
                    SQLiteStorage.dumps(snap.content_refs),
                    snap.created_by_pid,
                    snap.created_at.isoformat(),
                ),
            )

    def get_snapshot(self, snapshot_id: str) -> NamespaceSnapshot | None:
        row = self._storage.query_one(
            "SELECT * FROM namespace_snapshots_projection WHERE snapshot_id = ?",
            (snapshot_id,),
        )
        return self._row_to_snapshot(row) if row else None

    # ── Idempotency ────────────────────────────────────────────────────────

    def insert_idempotency(
        self,
        idempotency_key: str,
        artifact_id: str,
        pid: str,
        transaction_id: str,
        result_state: str,
        result_version: int | None,
    ) -> None:
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT OR IGNORE INTO artifact_idempotency
                   (idempotency_key, artifact_id, pid, transaction_id,
                    result_state, result_version, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    idempotency_key,
                    artifact_id,
                    pid,
                    transaction_id,
                    result_state,
                    result_version,
                    datetime.utcnow().isoformat(),
                ),
            )

    def get_idempotency(
        self, idempotency_key: str, artifact_id: str, pid: str
    ) -> dict[str, Any] | None:
        return self._storage.query_one(
            """SELECT * FROM artifact_idempotency
               WHERE idempotency_key = ? AND artifact_id = ? AND pid = ?""",
            (idempotency_key, artifact_id, pid),
        )

    # ── Row → Model converters ─────────────────────────────────────────────

    @staticmethod
    def _row_to_artifact(row: dict[str, Any]) -> ArtifactRecord:
        return ArtifactRecord(
            artifact_id=row["artifact_id"],
            namespace_id=row["namespace_id"],
            canonical_uri=row["canonical_uri"],
            current_version=row["current_version"],
            artifact_type=row["artifact_type"],
            created_by_pid=row["created_by_pid"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            deleted=bool(row["deleted"]),
            metadata=SQLiteStorage.loads(row["metadata_json"]) or {},
        )

    @staticmethod
    def _row_to_version(row: dict[str, Any]) -> ArtifactVersion:
        return ArtifactVersion(
            artifact_id=row["artifact_id"],
            version=row["version"],
            content_ref=row["content_ref"],
            content_hash=row["content_hash"],
            size_bytes=row["size_bytes"],
            parent_version=row["parent_version"],
            committed_by_pid=row["committed_by_pid"],
            committed_action_id=row["committed_action_id"],
            committed_at=datetime.fromisoformat(row["committed_at"]),
        )

    @staticmethod
    def _row_to_handle(row: dict[str, Any]) -> ArtifactHandle:
        return ArtifactHandle(
            handle_id=row["handle_id"],
            pid=row["pid"],
            artifact_id=row["artifact_id"],
            mode=row["mode"],
            opened_version=row["opened_version"],
            expected_version=row["expected_version"],
            lease_id=row["lease_id"],
            transaction_id=row["transaction_id"],
            opened_at=datetime.fromisoformat(row["opened_at"]),
            closed_at=datetime.fromisoformat(row["closed_at"]) if row["closed_at"] else None,
        )

    @staticmethod
    def _row_to_transaction(row: dict[str, Any]) -> WriteTransaction:
        return WriteTransaction(
            transaction_id=row["transaction_id"],
            artifact_id=row["artifact_id"],
            pid=row["pid"],
            expected_version=row["expected_version"],
            staged_content_ref=row["staged_content_ref"],
            staged_content_hash=row["staged_content_hash"],
            staged_size_bytes=row["staged_size_bytes"],
            state=row["state"],
            idempotency_key=row["idempotency_key"],
            created_at=datetime.fromisoformat(row["created_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
        )

    @staticmethod
    def _row_to_namespace(row: dict[str, Any]) -> ArtifactNamespace:
        return ArtifactNamespace(
            namespace_id=row["namespace_id"],
            owner_pid=row["owner_pid"],
            root_uri=row["root_uri"],
            quota_bytes=row["quota_bytes"],
            max_open_handles=row["max_open_handles"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_mount(row: dict[str, Any]) -> NamespaceMount:
        return NamespaceMount(
            mount_id=row["mount_id"],
            namespace_id=row["namespace_id"],
            mount_point=row["mount_point"],
            source_namespace_id=row["source_namespace_id"],
            source_prefix=row["source_prefix"],
            mode=row["mode"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_watch(row: dict[str, Any]) -> ArtifactWatch:
        return ArtifactWatch(
            watch_id=row["watch_id"],
            pid=row["pid"],
            namespace_id=row["namespace_id"],
            uri_prefix=row["uri_prefix"],
            created_at=datetime.fromisoformat(row["created_at"]),
            active=bool(row["active"]),
        )

    @staticmethod
    def _row_to_snapshot(row: dict[str, Any]) -> NamespaceSnapshot:
        return NamespaceSnapshot(
            snapshot_id=row["snapshot_id"],
            namespace_id=row["namespace_id"],
            artifact_versions=SQLiteStorage.loads(row["artifact_versions_json"]) or {},
            content_refs=SQLiteStorage.loads(row["content_refs_json"]) or {},
            created_by_pid=row["created_by_pid"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )
