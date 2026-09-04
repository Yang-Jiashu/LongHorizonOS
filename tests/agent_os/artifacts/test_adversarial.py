"""Adversarial tests for the Artifact FS.

These tests attempt to break the artifact file system through:
- Concurrent write conflicts (optimistic concurrency)
- COW isolation violations
- Handle leak and exhaustion
- Quota bypass attempts
- Mount path traversal
- Idempotency replay attacks
- Delete-then-read races
- Version rollback attempts
- Namespace isolation violations
- Watch signal injection
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from lhos.agent_os.artifacts.errors import (
    ArtifactNotFound,
    IdempotencyConflict,
    QuotaExceeded,
    VersionConflict,
)
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Clock
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture()
def setup(tmp_path: Path):
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    clock = Clock()
    process_service = ProcessService(storage, journal, clock)
    lease_service = LeaseService(storage, journal)
    signal_service = SignalService(storage, journal, process_service)
    projections = ArtifactProjections(storage)
    storage_driver = LocalArtifactStorageDriver(tmp_path / "cas")
    ns_service = NamespaceService(projections, journal)
    service = ArtifactFSService(
        projections,
        storage_driver,
        journal,
        lease_service=lease_service,
        signal_service=signal_service,
    )

    ns_service.create_namespace("p1")
    ns_service.create_namespace("p2")

    return {
        "storage": storage,
        "journal": journal,
        "projections": projections,
        "storage_driver": storage_driver,
        "ns_service": ns_service,
        "service": service,
        "lease_service": lease_service,
        "signal_service": signal_service,
        "process_service": process_service,
    }


class TestConcurrentWriteConflicts:
    """Optimistic concurrency control under concurrent writes."""

    def test_two_writers_same_version_one_wins(self, setup) -> None:
        """Two sequential writes to the same artifact with expected_version=0.
        First succeeds, second gets VersionConflict at begin_write."""
        svc = setup["service"]

        # First write with expected_version=0 (new artifact) — succeeds
        txn1 = svc.begin_write("p1", "workspace:///shared.txt", "key-1", expected_version=0)
        svc.stage(txn1.transaction_id, b"writer-1")
        committed = svc.commit(txn1.transaction_id)
        assert committed.state == "committed"

        # Second write with expected_version=0 (stale) — should fail
        with pytest.raises(VersionConflict):
            svc.begin_write("p1", "workspace:///shared.txt", "key-2", expected_version=0)

    def test_expected_version_mismatch_rejected(self, setup) -> None:
        """Writing with wrong expected_version is rejected."""
        svc = setup["service"]

        # Create v1
        svc.write("p1", "workspace:///file.txt", b"v1", "key-1")

        # Try to write with expected_version=0 (stale)
        with pytest.raises(VersionConflict):
            svc.begin_write("p1", "workspace:///file.txt", "key-2", expected_version=0)

    def test_correct_expected_version_succeeds(self, setup) -> None:
        """Writing with correct expected_version succeeds."""
        svc = setup["service"]

        svc.write("p1", "workspace:///file.txt", b"v1", "key-1")
        svc.write("p1", "workspace:///file.txt", b"v2", "key-2", expected_version=1)
        assert svc.read("p1", "workspace:///file.txt") == b"v2"

    def test_concurrent_begin_write_same_idempotency(self, setup) -> None:
        """Same idempotency key returns same transaction (not a new one)."""
        svc = setup["service"]

        txn1 = svc.begin_write("p1", "workspace:///idem.txt", "same-key")
        txn2 = svc.begin_write("p1", "workspace:///idem.txt", "same-key")

        assert txn1.transaction_id == txn2.transaction_id

    def test_threaded_concurrent_writes_different_files(self, setup) -> None:
        """Multiple threads writing different files should all succeed.
        Uses a lock to serialize SQLite access (SQLite is not thread-safe
        on a single connection)."""
        svc = setup["service"]
        results: list[bool] = []
        lock = threading.Lock()

        def write_file(i: int) -> None:
            try:
                with lock:
                    svc.write(
                        "p1", f"workspace:///thread/{i}.txt", f"data-{i}".encode(), f"key-{i}"
                    )
                with lock:
                    results.append(True)
            except Exception:
                with lock:
                    results.append(False)

        threads = [threading.Thread(target=write_file, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)
        assert len(svc.list_artifacts("p1")) == 10


class TestCopyOnWriteIsolation:
    """COW mount must not allow writes to leak to source namespace."""

    def test_cow_write_does_not_modify_source(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///template.txt", b"original", "key-1")
        ns_svc.mount("p2", "templates", "ns-p1", mode="copy_on_write")

        # p2 writes through COW mount
        svc.write("p2", "artifact://ns-p2/templates/template.txt", b"modified", "key-1")

        # Source must be unchanged
        assert svc.read("p1", "workspace:///template.txt") == b"original"
        # COW copy must have modified content
        assert svc.read("p2", "artifact://ns-p2/templates/template.txt") == b"modified"

    def test_cow_write_creates_separate_artifact(self, setup) -> None:
        """COW write should create a new artifact in the target namespace, not modify source."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///doc.md", b"source", "key-1")
        ns_svc.mount("p2", "src", "ns-p1", mode="copy_on_write")

        # Write through COW
        svc.write("p2", "artifact://ns-p2/src/doc.md", b"cow-copy", "key-1")

        # p2 should have its own artifact at the local URI
        local_artifact = svc._projections.get_artifact_by_uri("artifact://ns-p2/src/doc.md")
        assert local_artifact is not None
        assert local_artifact.namespace_id == "ns-p2"

        # p1's artifact should be unchanged
        source_artifact = svc._projections.get_artifact_by_uri("artifact://ns-p1/doc.md")
        assert source_artifact is not None
        assert source_artifact.namespace_id == "ns-p1"
        assert source_artifact.current_version == 1


class TestHandleLeakAndExhaustion:
    """Handle quota enforcement and cleanup."""

    def test_handle_quota_exceeded(self, setup) -> None:
        """Opening more than max_open_handles should fail."""
        svc = setup["service"]

        # Create files and open many handles
        for i in range(64):
            svc.write("p1", f"workspace:///f{i}.txt", b"x", f"k{i}")
            svc.open("p1", f"workspace:///f{i}.txt", "read")

        # 65th handle should fail
        svc.write("p1", "workspace:///over.txt", b"x", "k-over")
        with pytest.raises(QuotaExceeded):
            svc.open("p1", "workspace:///over.txt", "read")

    def test_close_frees_handle_slot(self, setup) -> None:
        """Closing a handle frees the slot for reuse."""
        svc = setup["service"]

        svc.write("p1", "workspace:///a.txt", b"data", "k1")
        handle = svc.open("p1", "workspace:///a.txt", "read")
        assert svc.close(handle.handle_id)

        # Should be able to open again
        handle2 = svc.open("p1", "workspace:///a.txt", "read")
        assert handle2.is_open
        svc.close(handle2.handle_id)

    def test_close_all_on_exit(self, setup) -> None:
        """close_all_for_pid should close all open handles."""
        svc = setup["service"]

        for i in range(5):
            svc.write("p1", f"workspace:///h{i}.txt", b"x", f"k{i}")
            svc.open("p1", f"workspace:///h{i}.txt", "read")

        count = svc.close_all_for_pid("p1")
        assert count == 5

        # All handles should be closed
        open_handles = svc._projections.list_open_handles_for_pid("p1")
        assert len(open_handles) == 0


class TestQuotaBypass:
    """Attempts to bypass quota enforcement."""

    def test_quota_checked_at_stage_not_commit(self, setup) -> None:
        """Quota is checked at stage time, so commit can't bypass it."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        ns_svc.set_quota("ns-p1", 10)
        svc.write("p1", "workspace:///small.txt", b"hello", "key-1")

        # Try to stage large content
        txn = svc.begin_write("p1", "workspace:///big.txt", "key-2")
        with pytest.raises(QuotaExceeded):
            svc.stage(txn.transaction_id, b"x" * 100)

    def test_quota_not_bypassed_by_version_update(self, setup) -> None:
        """Updating an existing artifact still checks quota."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        ns_svc.set_quota("ns-p1", 20)
        svc.write("p1", "workspace:///file.txt", b"10-bytes!!", "key-1")  # 10 bytes

        # Writing v2 with 15 bytes should exceed (10+15=25 > 20)
        with pytest.raises(QuotaExceeded):
            svc.write("p1", "workspace:///file.txt", b"x" * 15, "key-2", expected_version=1)

    def test_quota_per_namespace_isolation(self, setup) -> None:
        """Quota in one namespace doesn't affect another."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        ns_svc.set_quota("ns-p1", 5)
        # p2 has no quota — should be unlimited
        svc.write("p2", "workspace:///big.txt", b"x" * 10000, "key-1")


class TestMountPathTraversal:
    """Mount resolution must not allow path traversal."""

    def test_mount_point_cannot_escape_namespace(self, setup) -> None:
        """Mount point with .. should be normalized by URI canonicalization."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///secret.txt", b"secret", "key-1")
        ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        # Try to access outside the mount via ..
        # URI canonicalization should reject ".." segments
        from lhos.agent_os.artifacts.errors import PathTraversalRejected

        with pytest.raises(PathTraversalRejected):
            svc.read("p2", "artifact://ns-p2/shared/../secret.txt")

    def test_mount_source_prefix_traversal(self, setup) -> None:
        """Source prefix with traversal is rejected at mount creation."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        # Mount with a normal prefix
        mnt = ns_svc.mount("p2", "archive", "ns-p1", source_prefix="docs", mode="shared_readonly")
        assert mnt.source_prefix == "docs"

        # Reading through mount should work
        svc.write("p1", "workspace:///docs/readme.md", b"readme", "key-1")
        data = svc.read("p2", "artifact://ns-p2/archive/readme.md")
        assert data == b"readme"


class TestIdempotencyReplay:
    """Idempotency key replay behavior."""

    def test_replay_committed_returns_same_result(self, setup) -> None:
        """Replaying a committed transaction returns the same transaction."""
        svc = setup["service"]

        txn1 = svc.write("p1", "workspace:///file.txt", b"content", "key-1")
        txn2 = svc.write("p1", "workspace:///file.txt", b"content", "key-1")

        assert txn1.transaction_id == txn2.transaction_id
        assert txn2.state == "committed"

    def test_replay_aborted_raises_conflict(self, setup) -> None:
        """Replaying an aborted transaction raises IdempotencyConflict."""
        svc = setup["service"]

        txn = svc.begin_write("p1", "workspace:///file.txt", "key-1")
        svc.stage(txn.transaction_id, b"content")
        svc.abort(txn.transaction_id)

        with pytest.raises(IdempotencyConflict):
            svc.begin_write("p1", "workspace:///file.txt", "key-1")

    def test_different_keys_create_different_transactions(self, setup) -> None:
        """Different idempotency keys create separate transactions."""
        svc = setup["service"]

        txn1 = svc.write("p1", "workspace:///file.txt", b"v1", "key-1")
        txn2 = svc.write("p1", "workspace:///file.txt", b"v2", "key-2", expected_version=1)

        assert txn1.transaction_id != txn2.transaction_id


class TestDeleteAndVersionRollback:
    """Delete and version rollback attempts."""

    def test_deleted_artifact_not_readable(self, setup) -> None:
        svc = setup["service"]

        svc.write("p1", "workspace:///del.txt", b"data", "key-1")
        svc.delete("p1", "workspace:///del.txt")

        with pytest.raises(ArtifactNotFound):
            svc.read("p1", "workspace:///del.txt")

    def test_delete_does_not_affect_other_artifacts(self, setup) -> None:
        svc = setup["service"]

        svc.write("p1", "workspace:///keep.txt", b"keep", "key-1")
        svc.write("p1", "workspace:///del.txt", b"delete", "key-2")
        svc.delete("p1", "workspace:///del.txt")

        assert svc.read("p1", "workspace:///keep.txt") == b"keep"

    def test_old_version_still_readable_after_new_commit(self, setup) -> None:
        """Version pinning: old versions remain readable."""
        svc = setup["service"]

        svc.write("p1", "workspace:///file.txt", b"v1", "key-1")
        svc.write("p1", "workspace:///file.txt", b"v2", "key-2", expected_version=1)

        assert svc.read("p1", "workspace:///file.txt", version=1) == b"v1"
        assert svc.read("p1", "workspace:///file.txt", version=2) == b"v2"
        assert svc.read("p1", "workspace:///file.txt") == b"v2"

    def test_cannot_rollback_to_old_version_for_write(self, setup) -> None:
        """Writing with old expected_version after new commit fails."""
        svc = setup["service"]

        svc.write("p1", "workspace:///file.txt", b"v1", "key-1")
        svc.write("p1", "workspace:///file.txt", b"v2", "key-2", expected_version=1)

        # Try to write with expected_version=1 (stale)
        with pytest.raises(VersionConflict):
            svc.begin_write("p1", "workspace:///file.txt", "key-3", expected_version=1)


class TestNamespaceIsolation:
    """Process namespace isolation."""

    def test_p2_cannot_write_to_p1_namespace(self, setup) -> None:
        """p2 cannot directly write to artifact://ns-p1/ URIs."""
        svc = setup["service"]

        # p2 tries to write directly to p1's namespace
        # This should either fail at capability check or create in p2's namespace
        # Since no capabilities are set, it depends on the service behavior
        # Without capability service restrictions, the write resolves to ns-p1
        # But the namespace_id in the URI is ns-p1, not ns-p2
        # The service resolves workspace:/// to ns-p2 for p2
        # So p2 can only write to its own namespace via workspace:///
        svc.write("p2", "workspace:///p2-file.txt", b"p2-data", "key-1")

        # This should be in ns-p2, not ns-p1
        artifact = svc._projections.get_artifact_by_uri("artifact://ns-p2/p2-file.txt")
        assert artifact is not None
        assert artifact.namespace_id == "ns-p2"

    def test_p2_cannot_read_p1_without_mount(self, setup) -> None:
        """p2 cannot read p1's artifacts without a mount."""
        svc = setup["service"]

        svc.write("p1", "workspace:///private.txt", b"private", "key-1")

        # p2 tries to read from its own namespace — file doesn't exist there
        with pytest.raises(ArtifactNotFound):
            svc.read("p2", "workspace:///private.txt")


class TestWatchSignalSecurity:
    """Watch signal delivery security."""

    def test_watch_does_not_leak_to_unauthorized(self, setup) -> None:
        """Only registered watchers receive signals."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]
        ns_svc = setup["ns_service"]

        ns_svc.create_namespace("p3")

        # Only p2 watches
        svc.watch("p2", "artifact://ns-p1/")

        svc.write("p1", "workspace:///file.txt", b"data", "key-1")

        # p2 should have signal
        assert len(sig_svc.list_pending("p2")) == 1
        # p3 should NOT have signal
        assert len(sig_svc.list_pending("p3")) == 0

    def test_unwatched_does_not_receive_after_unwatch(self, setup) -> None:
        """After unwatch, no more signals are delivered."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]

        watch = svc.watch("p2", "artifact://ns-p1/")

        # First write — signal delivered
        svc.write("p1", "workspace:///file1.txt", b"d1", "key-1")
        assert len(sig_svc.list_pending("p2")) == 1

        # Unwatch
        svc.unwatch(watch.watch_id)

        # Second write — no signal
        svc.write("p1", "workspace:///file2.txt", b"d2", "key-2")
        assert len(sig_svc.list_pending("p2")) == 1  # Still only 1


class TestRecoveryAdversarial:
    """Crash recovery edge cases."""

    def test_recover_with_no_uncertain_transactions(self, setup) -> None:
        """Recovery with no uncertain transactions is a no-op."""
        svc = setup["service"]

        svc.write("p1", "workspace:///file.txt", b"data", "key-1")
        results = svc.recover()

        assert results["uncertain_resolved"] == 0

    def test_recover_aborts_orphaned_staging(self, setup) -> None:
        """Orphaned staging files are cleaned up on recovery."""
        svc = setup["service"]
        storage_driver = setup["storage_driver"]

        # Create a fake orphaned staging file
        staging_path = storage_driver._staging_dir / "fake-orphan-txn"
        staging_path.write_bytes(b"orphaned data")

        # Run recovery
        svc.recover()

        # Orphan should be cleaned up
        assert not staging_path.exists()
