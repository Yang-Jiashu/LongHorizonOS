"""Artifact FS Service — the core orchestrator for versioned artifact operations.

Responsibilities:
- Resolve URIs (workspace:/// → artifact://ns-<pid>/...)
- Enforce capability checks
- Manage handles (open/close/read/write)
- Coordinate write transactions (begin → stage → commit/abort)
- Enforce optimistic concurrency (expected_version)
- Support idempotency keys for retries
- Recover from crash (resolve UNCERTAIN transactions)
- Enforce quotas

Atomic Write Protocol:
1. begin_write(pid, uri, idempotency_key, expected_version) → transaction
2. stage(transaction_id, content) → staged metadata
3. commit(transaction_id) → new version (or conflict/uncertain)
4. If crash between stage and commit: transaction is UNCERTAIN on recovery

Recovery:
- UNCERTAIN transactions are resolved by inspecting the storage driver:
  - If content was committed to CAS → mark as COMMITTED
  - If content is still in staging → mark as ABORTED
  - If cannot determine → remains UNCERTAIN (manual intervention)
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lhos.agent_os.artifacts.errors import (
    ArtifactNotFound,
    IdempotencyConflict,
    InvalidArtifactURI,
    NamespaceNotFound,
    QuotaExceeded,
    TransactionNotFound,
    VersionConflict,
)
from lhos.agent_os.artifacts.models import (
    ArtifactHandle,
    ArtifactRecord,
    ArtifactVersion,
    ArtifactWatch,
    WriteTransaction,
)
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.uri import CanonicalURI, resolve_workspace_uri
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import KernelEvent
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService

# Default write lease TTL (5 minutes)
_DEFAULT_WRITE_LEASE_TTL_SECONDS = 300

# Default max open handles per process
_DEFAULT_MAX_OPEN_HANDLES = 64

# Default quota (100 MB)
_DEFAULT_QUOTA_BYTES = 100 * 1024 * 1024

# Try to import SignalService — optional dependency
try:
    from lhos.agent_os.services.signal_service import SignalService
except ImportError:
    SignalService = None  # type: ignore[assignment, misc]


class ArtifactFSService:
    """Core service for versioned artifact file system operations."""

    def __init__(
        self,
        projections: ArtifactProjections,
        storage_driver: LocalArtifactStorageDriver,
        journal: JournalService,
        capability_service: CapabilityService | None = None,
        lease_service: LeaseService | None = None,
        signal_service: SignalService | None = None,  # type: ignore[valid-type]
    ) -> None:
        self._projections = projections
        self._storage_driver = storage_driver
        self._journal = journal
        self._capability_service = capability_service
        self._lease_service = lease_service
        self._signal_service = signal_service

    # ── Read ──────────────────────────────────────────────────────────────

    def read(
        self,
        pid: str,
        uri: str,
        version: int | None = None,
    ) -> bytes:
        """Read artifact content.

        If version is None, reads the current version.
        """
        canonical = self._resolve_and_check(pid, uri, "read")

        artifact = self._projections.get_artifact_by_uri(canonical.canonical)
        if artifact is None or artifact.deleted:
            raise ArtifactNotFound(canonical.canonical)

        if artifact.current_version == 0:
            raise ArtifactNotFound(canonical.canonical)

        target_version = version or artifact.current_version
        if target_version < 1 or target_version > artifact.current_version:
            raise VersionConflict(
                artifact.artifact_id,
                expected=target_version,
                actual=artifact.current_version,
            )

        ver = self._projections.get_version(artifact.artifact_id, target_version)
        if ver is None:
            raise ArtifactNotFound(f"{canonical.canonical}@v{target_version}")

        return self._storage_driver.read(ver.content_ref)

    def read_metadata(
        self,
        pid: str,
        uri: str,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Read artifact metadata without content."""
        canonical = self._resolve_and_check(pid, uri, "read")

        artifact = self._projections.get_artifact_by_uri(canonical.canonical)
        if artifact is None or artifact.deleted:
            raise ArtifactNotFound(canonical.canonical)

        target_version = version or artifact.current_version
        ver = self._projections.get_version(artifact.artifact_id, target_version)
        if ver is None:
            raise ArtifactNotFound(f"{canonical.canonical}@v{target_version}")

        return {
            "artifact_id": artifact.artifact_id,
            "canonical_uri": artifact.canonical_uri,
            "namespace_id": artifact.namespace_id,
            "version": ver.version,
            "size_bytes": ver.size_bytes,
            "content_hash": ver.content_hash,
            "committed_at": ver.committed_at.isoformat(),
            "committed_by_pid": ver.committed_by_pid,
        }

    def list_artifacts(self, pid: str, uri_prefix: str | None = None) -> list[dict[str, Any]]:
        """List artifacts in the caller's namespace."""
        namespace_id = f"ns-{pid}"
        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            raise NamespaceNotFound(namespace_id)

        artifacts = self._projections.list_artifacts(namespace_id)
        result = []
        for a in artifacts:
            if uri_prefix and not a.canonical_uri.startswith(uri_prefix):
                continue
            result.append(
                {
                    "artifact_id": a.artifact_id,
                    "canonical_uri": a.canonical_uri,
                    "current_version": a.current_version,
                    "artifact_type": a.artifact_type,
                }
            )
        return result

    def list_versions(self, pid: str, uri: str) -> list[dict[str, Any]]:
        """List all versions of an artifact."""
        canonical = self._resolve_and_check(pid, uri, "read")
        artifact = self._projections.get_artifact_by_uri(canonical.canonical)
        if artifact is None:
            raise ArtifactNotFound(canonical.canonical)

        versions = self._projections.list_versions(artifact.artifact_id)
        return [
            {
                "version": v.version,
                "size_bytes": v.size_bytes,
                "content_hash": v.content_hash,
                "committed_at": v.committed_at.isoformat(),
                "parent_version": v.parent_version,
            }
            for v in versions
        ]

    # ── Open / Close Handles ───────────────────────────────────────────────

    def open(
        self,
        pid: str,
        uri: str,
        mode: str = "read",
        expected_version: int | None = None,
    ) -> ArtifactHandle:
        """Open a handle to an artifact.

        Read mode: pins current version.
        Write mode: acquires exclusive lease.
        Append mode: acquires exclusive lease, opens at current version.
        """
        if mode not in ("read", "write", "append"):
            raise InvalidArtifactURI(uri, f"invalid mode: {mode}")

        canonical = self._resolve_and_check(pid, uri, mode)

        # Check handle quota
        open_count = self._projections.count_open_handles(pid)
        if open_count >= _DEFAULT_MAX_OPEN_HANDLES:
            raise QuotaExceeded(f"max_open_handles={_DEFAULT_MAX_OPEN_HANDLES}")

        artifact = self._projections.get_artifact_by_uri(canonical.canonical)
        if artifact is None:
            if mode == "read":
                raise ArtifactNotFound(canonical.canonical)
            # Write/append can create new artifact
            artifact = self._create_artifact_record(canonical, pid)

        # Acquire lease for write/append
        lease_id: str | None = None
        if mode in ("write", "append") and self._lease_service:
            resource_id = f"artifact:{artifact.artifact_id}"
            leases = self._lease_service.atomic_acquire(
                pid,
                [{"resource_id": resource_id, "mode": "exclusive"}],
            )
            lease_id = leases[0].lease_id if leases else None

        handle = ArtifactHandle(
            pid=pid,
            artifact_id=artifact.artifact_id,
            mode=mode,  # type: ignore[arg-type]
            opened_version=artifact.current_version if mode != "write" else None,
            expected_version=expected_version,
            lease_id=lease_id,
        )
        self._projections.upsert_handle(handle)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_HANDLE_OPENED",
            payload=handle.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return handle

    def close(self, handle_id: str) -> bool:
        """Close a handle and release any held lease."""
        handle = self._projections.get_handle(handle_id)
        if handle is None or not handle.is_open:
            return False

        handle.closed_at = datetime.now(UTC)
        self._projections.upsert_handle(handle)

        # Release write lease
        if handle.lease_id and self._lease_service:
            self._lease_service.release([handle.lease_id])

        ev = KernelEvent(
            pid=handle.pid,
            event_type="ARTIFACT_HANDLE_CLOSED",
            payload={"handle_id": handle_id, "lease_id": handle.lease_id},
        )
        self._journal.append_event(ev)
        return True

    def close_all_for_pid(self, pid: str) -> int:
        """Close all open handles for a process (on exit/crash)."""
        handles = self._projections.list_open_handles_for_pid(pid)
        count = 0
        for h in handles:
            if self.close(h.handle_id):
                count += 1
        return count

    # ── Write Transactions ────────────────────────────────────────────────

    def begin_write(
        self,
        pid: str,
        uri: str,
        idempotency_key: str,
        expected_version: int | None = None,
    ) -> WriteTransaction:
        """Begin an atomic write transaction.

        Idempotency: if a transaction with the same key exists, return it.
        """
        # Check idempotency
        canonical = self._resolve_and_check(pid, uri, "write")
        artifact = self._projections.get_artifact_by_uri(canonical.canonical)

        if artifact is None:
            artifact = self._create_artifact_record(canonical, pid)

        # Check idempotency cache
        existing = self._projections.find_transaction_by_idempotency(
            artifact.artifact_id, pid, idempotency_key
        )
        if existing:
            if existing.state == "committed":
                return existing
            if existing.state in ("aborted", "conflicted"):
                raise IdempotencyConflict(
                    existing.transaction_id,
                    existing.state,
                )
            # Transaction is still open/staged — return it
            return existing

        # Check optimistic concurrency
        if expected_version is not None and artifact.current_version != expected_version:
            raise VersionConflict(
                artifact.artifact_id,
                expected=expected_version,
                actual=artifact.current_version,
            )

        txn = WriteTransaction(
            artifact_id=artifact.artifact_id,
            pid=pid,
            expected_version=expected_version or artifact.current_version,
            idempotency_key=idempotency_key,
        )
        self._projections.upsert_transaction(txn)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_TXN_BEGUN",
            payload=txn.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return txn

    def stage(
        self,
        transaction_id: str,
        content: bytes | str,
    ) -> WriteTransaction:
        """Stage content for a write transaction."""
        txn = self._projections.get_transaction(transaction_id)
        if txn is None:
            raise TransactionNotFound(transaction_id)
        if txn.state in ("committed", "aborted", "conflicted"):
            raise IdempotencyConflict(transaction_id, txn.state)

        # Stage content in storage driver
        staged = self._storage_driver.stage(transaction_id, content)

        # Check quota
        artifact = self._projections.get_artifact(txn.artifact_id)
        if artifact:
            ns = self._projections.get_namespace(artifact.namespace_id)
            if ns and ns.quota_bytes is not None:
                current_usage = self._projections.count_committed_bytes(artifact.namespace_id)
                if current_usage + staged.size_bytes > ns.quota_bytes:
                    self._storage_driver.abort(transaction_id)
                    raise QuotaExceeded(
                        f"namespace {artifact.namespace_id} quota: "
                        f"{current_usage + staged.size_bytes} > {ns.quota_bytes}"
                    )

        # Update transaction
        txn.staged_content_ref = staged.content_ref
        txn.staged_content_hash = staged.content_hash
        txn.staged_size_bytes = staged.size_bytes
        txn.state = "staged"
        self._projections.upsert_transaction(txn)

        ev = KernelEvent(
            pid=txn.pid,
            event_type="ARTIFACT_TXN_STAGED",
            payload=txn.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return txn

    def commit(self, transaction_id: str) -> WriteTransaction:
        """Commit a staged write transaction.

        Atomic steps:
        1. Verify transaction is staged
        2. Re-check optimistic concurrency (no concurrent commits)
        3. Commit content to storage CAS
        4. Create new ArtifactVersion
        5. Update ArtifactRecord.current_version
        6. Mark transaction as committed
        7. Record idempotency result

        If crash occurs between steps 3 and 6, recovery will resolve.
        """
        txn = self._projections.get_transaction(transaction_id)
        if txn is None:
            raise TransactionNotFound(transaction_id)
        if txn.state == "committed":
            return txn  # Idempotent
        if txn.state in ("aborted", "conflicted"):
            raise IdempotencyConflict(transaction_id, txn.state)
        if txn.state != "staged":
            raise VersionConflict(txn.artifact_id, expected=None, actual=0)

        artifact = self._projections.get_artifact(txn.artifact_id)
        if artifact is None:
            raise ArtifactNotFound(txn.artifact_id)

        # Re-check optimistic concurrency
        if txn.expected_version is not None and artifact.current_version != txn.expected_version:
            txn.state = "conflicted"
            txn.finished_at = datetime.now(UTC)
            self._projections.upsert_transaction(txn)
            self._storage_driver.abort(transaction_id)

            ev = KernelEvent(
                pid=txn.pid,
                event_type="ARTIFACT_TXN_CONFLICTED",
                payload=txn.model_dump(mode="json"),
            )
            self._journal.append_event(ev)
            raise VersionConflict(
                artifact.artifact_id,
                expected=txn.expected_version,
                actual=artifact.current_version,
            )

        # Step 3: Commit content to storage CAS
        commit_result = self._storage_driver.commit(transaction_id)

        if (
            not commit_result.committed
            and not commit_result.content_ref
            and not self._storage_driver.exists(txn.staged_content_ref)
        ):
            # Content was never staged or already committed
            txn.state = "uncertain"
            txn.finished_at = datetime.now(UTC)
            self._projections.upsert_transaction(txn)
            ev = KernelEvent(
                pid=txn.pid,
                event_type="ARTIFACT_TXN_UNCERTAIN",
                payload=txn.model_dump(mode="json"),
            )
            self._journal.append_event(ev)
            return txn

        # Step 4: Create new version
        new_version = artifact.current_version + 1
        ver = ArtifactVersion(
            artifact_id=artifact.artifact_id,
            version=new_version,
            content_ref=txn.staged_content_ref,
            content_hash=txn.staged_content_hash,
            size_bytes=txn.staged_size_bytes,
            parent_version=artifact.current_version if artifact.current_version > 0 else None,
            committed_by_pid=txn.pid,
            committed_action_id=txn.transaction_id,  # Use txn as action proxy
        )
        self._projections.insert_version(ver)

        # Step 5: Update artifact
        artifact.current_version = new_version
        artifact.updated_at = datetime.now(UTC)
        self._projections.upsert_artifact(artifact)

        # Step 6: Mark transaction committed
        txn.state = "committed"
        txn.finished_at = datetime.now(UTC)
        self._projections.upsert_transaction(txn)

        # Step 7: Record idempotency
        self._projections.insert_idempotency(
            txn.idempotency_key,
            artifact.artifact_id,
            txn.pid,
            transaction_id,
            "committed",
            new_version,
        )

        ev = KernelEvent(
            pid=txn.pid,
            event_type="ARTIFACT_TXN_COMMITTED",
            payload={
                **txn.model_dump(mode="json"),
                "new_version": new_version,
            },
        )
        self._journal.append_event(ev)

        # Step 8: Notify watches
        self._notify_watches(artifact.canonical_uri, new_version, txn.pid)

        return txn

    def abort(self, transaction_id: str) -> WriteTransaction:
        """Abort a write transaction."""
        txn = self._projections.get_transaction(transaction_id)
        if txn is None:
            raise TransactionNotFound(transaction_id)
        if txn.state in ("committed",):
            return txn  # Can't abort committed
        if txn.state in ("aborted",):
            return txn  # Idempotent

        # Clean up staged content
        self._storage_driver.abort(transaction_id)

        txn.state = "aborted"
        txn.finished_at = datetime.now(UTC)
        self._projections.upsert_transaction(txn)

        ev = KernelEvent(
            pid=txn.pid,
            event_type="ARTIFACT_TXN_ABORTED",
            payload=txn.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return txn

    # ── Convenience: write in one call ─────────────────────────────────────

    def write(
        self,
        pid: str,
        uri: str,
        content: bytes | str,
        idempotency_key: str,
        expected_version: int | None = None,
    ) -> WriteTransaction:
        """Convenience: begin + stage + commit in one call."""
        txn = self.begin_write(pid, uri, idempotency_key, expected_version)
        if txn.state == "committed":
            return txn  # Idempotent replay
        self.stage(txn.transaction_id, content)
        return self.commit(txn.transaction_id)

    # ── Recovery ──────────────────────────────────────────────────────────

    def recover(self) -> dict[str, Any]:
        """Recover from crash.

        For each UNCERTAIN transaction:
        1. Check if content exists in CAS (was committed before crash)
        2. If yes: mark as COMMITTED, create version if missing
        3. If no: mark as ABORTED

        For orphaned staging files:
        1. Check if any known transaction references them
        2. If not: delete

        Returns summary of recovery actions.
        """
        results: dict[str, Any] = {
            "uncertain_resolved": 0,
            "orphaned_cleaned": 0,
            "versions_created": 0,
        }

        # Find all uncertain transactions
        # (We need to scan all transactions — no dedicated query yet)
        # For now, use a direct query
        rows = self._projections._storage.query_all(
            "SELECT * FROM write_transactions_projection WHERE state = 'uncertain'"
        )
        for row in rows:
            txn = ArtifactProjections._row_to_transaction(row)
            if self._storage_driver.exists(txn.staged_content_ref):
                # Content was committed — resolve as committed
                artifact = self._projections.get_artifact(txn.artifact_id)
                if artifact:
                    new_version = artifact.current_version
                    ver = ArtifactVersion(
                        artifact_id=artifact.artifact_id,
                        version=new_version,
                        content_ref=txn.staged_content_ref,
                        content_hash=txn.staged_content_hash,
                        size_bytes=txn.staged_size_bytes,
                        parent_version=artifact.current_version
                        if artifact.current_version > 0
                        else None,
                        committed_by_pid=txn.pid,
                        committed_action_id=txn.transaction_id,
                    )
                    self._projections.insert_version(ver)
                    artifact.current_version = new_version
                    artifact.updated_at = datetime.now(UTC)
                    self._projections.upsert_artifact(artifact)
                    results["versions_created"] += 1

                txn.state = "committed"
                txn.finished_at = datetime.now(UTC)
                self._projections.upsert_transaction(txn)
            else:
                # Content not in CAS — abort
                txn.state = "aborted"
                txn.finished_at = datetime.now(UTC)
                self._projections.upsert_transaction(txn)

            results["uncertain_resolved"] += 1

        # Clean up orphaned staging files
        known_txns = {
            r["transaction_id"]
            for r in self._projections._storage.query_all(
                "SELECT transaction_id FROM write_transactions_projection"
            )
        }
        orphan_report = self._storage_driver.recover(known_txns)
        orphaned = self._storage_driver.list_orphaned_staging()
        results["orphaned_cleaned"] = len(orphaned) - len(orphan_report)

        return results

    # ── Watches ───────────────────────────────────────────────────────────

    def watch(self, pid: str, uri_prefix: str) -> ArtifactWatch:
        """Register a watch for artifact changes matching uri_prefix.

        When any artifact whose canonical URI starts with uri_prefix is committed,
        an ARTIFACT_CHANGED signal is sent to the watching process.
        """
        namespace_id = f"ns-{pid}"
        watch = ArtifactWatch(
            pid=pid,
            namespace_id=namespace_id,
            uri_prefix=uri_prefix,
        )
        self._projections.upsert_watch(watch)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_WATCH_REGISTERED",
            payload=watch.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return watch

    def unwatch(self, watch_id: str) -> bool:
        """Deactivate a watch."""
        with self._projections._storage.transaction() as tx:
            tx.execute(
                "UPDATE artifact_watches_projection SET active = 0 WHERE watch_id = ?",
                (watch_id,),
            )
        return True

    def list_watches(self, pid: str) -> list[ArtifactWatch]:
        """List all active watches for a process."""
        namespace_id = f"ns-{pid}"
        return self._projections.list_active_watches(namespace_id)

    def _notify_watches(self, canonical_uri: str, new_version: int, writer_pid: str) -> int:
        """Notify all watches matching the given URI.

        Sends ARTIFACT_CHANGED signals to watching processes.
        Does NOT notify the writer process itself.

        Returns count of signals sent.
        """
        if self._signal_service is None:
            return 0

        # Find all active watches across all namespaces
        # We need to check all watches — not just the writer's namespace
        rows = self._projections._storage.query_all(
            "SELECT * FROM artifact_watches_projection WHERE active = 1 AND pid != ?",
            (writer_pid,),
        )

        count = 0
        for row in rows:
            watch = ArtifactProjections._row_to_watch(row)
            if canonical_uri.startswith(watch.uri_prefix):
                self._signal_service.send(
                    target_pid=watch.pid,
                    signal_type="ARTIFACT_CHANGED",
                    source_pid=writer_pid,
                    payload={
                        "canonical_uri": canonical_uri,
                        "new_version": new_version,
                        "watch_id": watch.watch_id,
                        "uri_prefix": watch.uri_prefix,
                    },
                )
                count += 1

        return count

    # ── Quota ─────────────────────────────────────────────────────────────

    def get_namespace_usage(self, namespace_id: str) -> dict[str, Any]:
        """Get namespace storage usage statistics.

        Returns:
            total_bytes: Sum of all committed version sizes
            artifact_count: Number of non-deleted artifacts
            version_count: Total number of versions
            quota_bytes: Configured quota (None = unlimited)
            quota_used_pct: Percentage of quota used (None if no quota)
        """
        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            raise NamespaceNotFound(namespace_id)

        total_bytes = self._projections.count_committed_bytes(namespace_id)
        artifacts = self._projections.list_artifacts(namespace_id)
        version_count = sum(len(self._projections.list_versions(a.artifact_id)) for a in artifacts)
        quota_bytes = ns.quota_bytes
        quota_used_pct = (total_bytes / quota_bytes * 100) if quota_bytes else None

        return {
            "total_bytes": total_bytes,
            "artifact_count": len(artifacts),
            "version_count": version_count,
            "quota_bytes": quota_bytes,
            "quota_used_pct": quota_used_pct,
        }

    # ── Delete ────────────────────────────────────────────────────────────

    def delete(self, pid: str, uri: str) -> bool:
        """Soft-delete an artifact (mark as deleted)."""
        canonical = self._resolve_and_check(pid, uri, "write")
        artifact = self._projections.get_artifact_by_uri(canonical.canonical)
        if artifact is None:
            raise ArtifactNotFound(canonical.canonical)

        artifact.deleted = True
        artifact.updated_at = datetime.now(UTC)
        self._projections.upsert_artifact(artifact)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_DELETED",
            payload={"artifact_id": artifact.artifact_id, "canonical_uri": canonical.canonical},
        )
        self._journal.append_event(ev)

        return True

    # ── Projection ────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        """Handle journal events for projection rebuild."""
        if ev.event_type == "ARTIFACT_CREATED":
            rec = ArtifactRecord(**ev.payload)
            self._projections.upsert_artifact(rec)
        elif ev.event_type == "ARTIFACT_VERSION_COMMITTED":
            ver = ArtifactVersion(**ev.payload)
            self._projections.insert_version(ver)
            # Also update artifact record
            artifact = self._projections.get_artifact(ver.artifact_id)
            if artifact:
                artifact.current_version = ver.version
                artifact.updated_at = ver.committed_at
                self._projections.upsert_artifact(artifact)
        elif ev.event_type == "ARTIFACT_HANDLE_OPENED":
            handle = ArtifactHandle(**ev.payload)
            self._projections.upsert_handle(handle)
        elif ev.event_type == "ARTIFACT_HANDLE_CLOSED":
            handle_id = ev.payload["handle_id"]
            closed_handle: ArtifactHandle | None = self._projections.get_handle(handle_id)
            if closed_handle:
                closed_handle.closed_at = datetime.now(UTC)
                self._projections.upsert_handle(closed_handle)
        elif ev.event_type == "ARTIFACT_TXN_BEGUN":
            txn = WriteTransaction(**ev.payload)
            self._projections.upsert_transaction(txn)
        elif ev.event_type in (
            "ARTIFACT_TXN_STAGED",
            "ARTIFACT_TXN_COMMITTED",
            "ARTIFACT_TXN_ABORTED",
            "ARTIFACT_TXN_CONFLICTED",
            "ARTIFACT_TXN_UNCERTAIN",
        ):
            txn = WriteTransaction(**{k: v for k, v in ev.payload.items() if k != "new_version"})
            self._projections.upsert_transaction(txn)
            if ev.event_type == "ARTIFACT_TXN_COMMITTED" and "new_version" in ev.payload:
                new_version = ev.payload["new_version"]
                ver = ArtifactVersion(
                    artifact_id=txn.artifact_id,
                    version=new_version,
                    content_ref=txn.staged_content_ref,
                    content_hash=txn.staged_content_hash,
                    size_bytes=txn.staged_size_bytes,
                    parent_version=new_version - 1 if new_version > 1 else None,
                    committed_by_pid=txn.pid,
                    committed_action_id=txn.transaction_id,
                    committed_at=datetime.fromisoformat(txn.created_at.isoformat())
                    if txn.finished_at
                    else datetime.now(UTC),
                )
                self._projections.insert_version(ver)
                artifact = self._projections.get_artifact(txn.artifact_id)
                if artifact:
                    artifact.current_version = new_version
                    artifact.updated_at = datetime.now(UTC)
                    self._projections.upsert_artifact(artifact)
        elif ev.event_type == "ARTIFACT_DELETED":
            artifact_id = ev.payload["artifact_id"]
            artifact = self._projections.get_artifact(artifact_id)
            if artifact:
                artifact.deleted = True
                artifact.updated_at = datetime.now(UTC)
                self._projections.upsert_artifact(artifact)
        elif ev.event_type == "ARTIFACT_WATCH_REGISTERED":
            watch = ArtifactWatch(**ev.payload)
            self._projections.upsert_watch(watch)

    # ── Internal ──────────────────────────────────────────────────────────

    def _resolve_and_check(self, pid: str, uri: str, operation: str) -> CanonicalURI:
        """Resolve URI, check capabilities, and resolve through mounts for reads."""
        namespace_id = f"ns-{pid}"
        canonical = resolve_workspace_uri(uri, namespace_id)

        # Capability check
        if self._capability_service:
            resource = f"artifact://{canonical.namespace_id}/**"
            op_map = {"read": "read", "write": "write", "append": "write"}
            if not self._capability_service.check(pid, resource, op_map.get(operation, operation)):
                raise CapabilityDenied(pid, canonical.canonical, operation)

        # Mount resolution: for read operations, check if path falls under a mount
        if operation == "read" and canonical.namespace_id == namespace_id:
            # First: check if artifact exists locally (COW copy may have been created)
            local_artifact = self._projections.get_artifact_by_uri(canonical.canonical)
            if local_artifact is None or local_artifact.deleted:
                # Not local — check mounts
                mounts = self._projections.list_mounts(namespace_id)
                for mnt in mounts:
                    mp = mnt.mount_point
                    if canonical.path == mp or canonical.path.startswith(mp + "/"):
                        relative = canonical.path[len(mp) :].lstrip("/")
                        resolved_path = (
                            f"{mnt.source_prefix}/{relative}".strip("/")
                            if mnt.source_prefix
                            else relative
                        )
                        if mnt.mode in ("shared_readonly", "copy_on_write", "shared_readwrite"):
                            # Resolve to source namespace
                            canonical.namespace_id = mnt.source_namespace_id
                            canonical.path = resolved_path
                            canonical.canonical = (
                                f"artifact://{mnt.source_namespace_id}/{resolved_path}"
                            )
                        break

        return canonical

    def _create_artifact_record(self, canonical: CanonicalURI, pid: str) -> ArtifactRecord:
        """Create a new artifact record."""
        namespace_id = canonical.namespace_id
        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            raise NamespaceNotFound(namespace_id)

        rec = ArtifactRecord(
            namespace_id=namespace_id,
            canonical_uri=canonical.canonical,
            created_by_pid=pid,
        )
        self._projections.upsert_artifact(rec)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_CREATED",
            payload=rec.model_dump(mode="json"),
        )
        self._journal.append_event(ev)

        return rec
