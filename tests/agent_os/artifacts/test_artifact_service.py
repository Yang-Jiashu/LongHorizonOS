"""Tests for ArtifactFSService — atomic write transactions and recovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhos.agent_os.artifacts.errors import (
    ArtifactNotFound,
    IdempotencyConflict,
    QuotaExceeded,
    TransactionNotFound,
    VersionConflict,
)
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture()
def setup(tmp_path: Path):
    """Create a fully wired ArtifactFSService with journal and projections."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    storage_driver = LocalArtifactStorageDriver(tmp_path / "cas")
    ns_service = NamespaceService(projections, journal)
    service = ArtifactFSService(projections, storage_driver, journal)

    # Create a namespace for test process
    ns_service.create_namespace("p1")

    return {
        "storage": storage,
        "journal": journal,
        "projections": projections,
        "storage_driver": storage_driver,
        "ns_service": ns_service,
        "service": service,
        "pid": "p1",
    }


class TestBasicWriteRead:
    """Basic write → read lifecycle."""

    def test_write_creates_artifact(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn = svc.write(pid, "workspace:///src/main.py", b"print('hello')", "idem-1")

        assert txn.state == "committed"
        assert txn.staged_size_bytes == len(b"print('hello')")

    def test_read_returns_written_content(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///data.txt", b"hello world", "idem-1")

        data = svc.read(pid, "workspace:///data.txt")
        assert data == b"hello world"

    def test_write_multiple_versions(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///file.txt", b"v1", "idem-1")
        svc.write(pid, "workspace:///file.txt", b"v2", "idem-2")
        svc.write(pid, "workspace:///file.txt", b"v3", "idem-3")

        # Current version should be v3
        assert svc.read(pid, "workspace:///file.txt") == b"v3"

        # Read specific versions
        assert svc.read(pid, "workspace:///file.txt", version=1) == b"v1"
        assert svc.read(pid, "workspace:///file.txt", version=2) == b"v2"

    def test_read_nonexistent_raises(self, setup) -> None:
        svc = setup["service"]
        with pytest.raises(ArtifactNotFound):
            svc.read(setup["pid"], "workspace:///nonexistent.txt")

    def test_read_metadata(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///meta.txt", b"metadata test", "idem-1")

        meta = svc.read_metadata(pid, "workspace:///meta.txt")
        assert meta["version"] == 1
        assert meta["size_bytes"] == len(b"metadata test")
        assert meta["canonical_uri"] == "artifact://ns-p1/meta.txt"

    def test_list_versions(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///multi.txt", b"v1", "idem-1")
        svc.write(pid, "workspace:///multi.txt", b"v2", "idem-2")

        versions = svc.list_versions(pid, "workspace:///multi.txt")
        assert len(versions) == 2
        assert versions[0]["version"] == 1
        assert versions[1]["version"] == 2


class TestOptimisticConcurrency:
    """Optimistic concurrency control via expected_version."""

    def test_write_with_correct_expected_version(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///concurrent.txt", b"v1", "idem-1")
        # expected_version=1 should succeed
        svc.write(pid, "workspace:///concurrent.txt", b"v2", "idem-2", expected_version=1)
        assert svc.read(pid, "workspace:///concurrent.txt") == b"v2"

    def test_write_with_wrong_expected_version_raises(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///concurrent.txt", b"v1", "idem-1")

        with pytest.raises(VersionConflict):
            svc.write(pid, "workspace:///concurrent.txt", b"v2", "idem-2", expected_version=99)

    def test_concurrent_writers_conflict(self, setup) -> None:
        """Two writers: first commits, second with same expected_version conflicts."""
        svc = setup["service"]
        pid = setup["pid"]

        # Both start with expected_version=0 (empty file)
        txn1 = svc.begin_write(pid, "workspace:///race.txt", "idem-1", expected_version=0)
        svc.stage(txn1.transaction_id, b"writer 1")
        svc.commit(txn1.transaction_id)

        # Second writer also expected version 0, but file is now at version 1
        with pytest.raises(VersionConflict):
            svc.begin_write(pid, "workspace:///race.txt", "idem-2", expected_version=0)


class TestIdempotency:
    """Idempotency key handling for retries."""

    def test_same_idempotency_key_returns_committed_txn(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn1 = svc.write(pid, "workspace:///idem.txt", b"content", "key-1")
        txn2 = svc.write(pid, "workspace:///idem.txt", b"different", "key-1")

        assert txn1.transaction_id == txn2.transaction_id
        assert txn2.state == "committed"
        # Content should be from first write
        assert svc.read(pid, "workspace:///idem.txt") == b"content"

    def test_aborted_txn_with_same_key_raises(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn = svc.begin_write(pid, "workspace:///abort.txt", "key-1")
        svc.stage(txn.transaction_id, b"staged")
        svc.abort(txn.transaction_id)

        with pytest.raises(IdempotencyConflict):
            svc.begin_write(pid, "workspace:///abort.txt", "key-1")

    def test_different_idempotency_keys_create_different_versions(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///multi.txt", b"v1", "key-1")
        svc.write(pid, "workspace:///multi.txt", b"v2", "key-2")

        versions = svc.list_versions(pid, "workspace:///multi.txt")
        assert len(versions) == 2


class TestTransactionLifecycle:
    """Explicit begin → stage → commit/abort."""

    def test_begin_stage_commit(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn = svc.begin_write(pid, "workspace:///explicit.txt", "key-1")
        assert txn.state == "open"

        txn = svc.stage(txn.transaction_id, b"explicit content")
        assert txn.state == "staged"

        txn = svc.commit(txn.transaction_id)
        assert txn.state == "committed"

        assert svc.read(pid, "workspace:///explicit.txt") == b"explicit content"

    def test_begin_stage_abort(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn = svc.begin_write(pid, "workspace:///aborted.txt", "key-1")
        svc.stage(txn.transaction_id, b"will be aborted")
        txn = svc.abort(txn.transaction_id)
        assert txn.state == "aborted"

        # Artifact should not have content
        with pytest.raises(ArtifactNotFound):
            svc.read(pid, "workspace:///aborted.txt")

    def test_commit_nonexistent_raises(self, setup) -> None:
        svc = setup["service"]
        with pytest.raises(TransactionNotFound):
            svc.commit("nonexistent-txn")

    def test_abort_nonexistent_raises(self, setup) -> None:
        svc = setup["service"]
        with pytest.raises(TransactionNotFound):
            svc.abort("nonexistent-txn")

    def test_double_commit_is_idempotent(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        txn = svc.write(pid, "workspace:///double.txt", b"content", "key-1")
        txn2 = svc.commit(txn.transaction_id)
        assert txn2.state == "committed"


class TestHandles:
    """Open/close handle lifecycle."""

    def test_open_read_handle(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///handle.txt", b"content", "key-1")
        handle = svc.open(pid, "workspace:///handle.txt", mode="read")
        assert handle.mode == "read"
        assert handle.is_open
        assert handle.opened_version == 1

    def test_close_handle(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///handle.txt", b"content", "key-1")
        handle = svc.open(pid, "workspace:///handle.txt", mode="read")
        assert svc.close(handle.handle_id) is True

        handle = svc._projections.get_handle(handle.handle_id)
        assert not handle.is_open

    def test_close_all_for_pid(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///a.txt", b"a", "key-1")
        svc.write(pid, "workspace:///b.txt", b"b", "key-2")
        svc.open(pid, "workspace:///a.txt")
        svc.open(pid, "workspace:///b.txt")

        count = svc.close_all_for_pid(pid)
        assert count == 2


class TestQuotaEnforcement:
    """Namespace quota enforcement."""

    def test_quota_exceeded_raises(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]
        pid = setup["pid"]

        # Set a very small quota (10 bytes)
        ns_svc.set_quota("ns-p1", 10)

        with pytest.raises(QuotaExceeded):
            svc.write(pid, "workspace:///big.txt", b"x" * 100, "key-1")

    def test_quota_allows_small_writes(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]
        pid = setup["pid"]

        ns_svc.set_quota("ns-p1", 100)
        svc.write(pid, "workspace:///small.txt", b"x" * 50, "key-1")
        assert svc.read(pid, "workspace:///small.txt") == b"x" * 50


class TestDelete:
    """Soft-delete artifacts."""

    def test_delete_makes_artifact_invisible(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]

        svc.write(pid, "workspace:///delete.txt", b"content", "key-1")
        svc.delete(pid, "workspace:///delete.txt")

        with pytest.raises(ArtifactNotFound):
            svc.read(pid, "workspace:///delete.txt")


class TestRecovery:
    """Crash recovery for uncertain transactions."""

    def test_recover_resolves_uncertain_to_committed(self, setup, tmp_path: Path) -> None:
        """Simulate crash between CAS commit and version creation."""
        svc = setup["service"]
        pid = setup["pid"]
        storage_driver = setup["storage_driver"]
        projections = setup["projections"]

        # Create artifact and begin write
        svc.write(pid, "workspace:///crash.txt", b"initial", "key-0")
        txn = svc.begin_write(pid, "workspace:///crash.txt", "key-1", expected_version=1)
        svc.stage(txn.transaction_id, b"crash content")

        # Manually commit to CAS (simulating storage commit happening)
        storage_driver.commit(txn.transaction_id)

        # Re-read the transaction to get staged content_ref
        txn = projections.get_transaction(txn.transaction_id)

        # Mark transaction as UNCERTAIN (simulating crash before version creation)
        txn.state = "uncertain"
        txn.finished_at = None
        projections.upsert_transaction(txn)

        # Run recovery
        results = svc.recover()
        assert results["uncertain_resolved"] == 1
        assert results["versions_created"] == 1

        # Verify content is now readable at version 2
        data = svc.read(pid, "workspace:///crash.txt", version=2)
        assert data == b"crash content"

    def test_recover_aborts_when_content_missing(self, setup) -> None:
        """Uncertain transaction without CAS content → aborted."""
        svc = setup["service"]
        pid = setup["pid"]
        projections = setup["projections"]

        svc.write(pid, "workspace:///crash2.txt", b"initial", "key-0")
        txn = svc.begin_write(pid, "workspace:///crash2.txt", "key-1", expected_version=1)
        txn = svc.stage(txn.transaction_id, b"crash content")

        # Abort in storage (simulating no CAS content)
        setup["storage_driver"].abort(txn.transaction_id)

        # Re-read transaction to preserve staged_content_ref
        txn = projections.get_transaction(txn.transaction_id)

        # Mark as UNCERTAIN
        txn.state = "uncertain"
        txn.finished_at = None
        projections.upsert_transaction(txn)

        results = svc.recover()
        assert results["uncertain_resolved"] == 1

        # Transaction should be aborted
        recovered = projections.get_transaction(txn.transaction_id)
        assert recovered.state == "aborted"


class TestProjectionRebuild:
    """Verify that projections can be rebuilt from journal."""

    def test_rebuild_preserves_artifacts(self, setup) -> None:
        svc = setup["service"]
        pid = setup["pid"]
        journal = setup["journal"]
        storage = setup["storage"]
        ns_service = setup["ns_service"]
        storage_driver = setup["storage_driver"]

        # Write some artifacts
        svc.write(pid, "workspace:///rebuild1.txt", b"content1", "key-1")
        svc.write(pid, "workspace:///rebuild2.txt", b"content2", "key-2")

        # Rebuild projections from journal
        new_projections = ArtifactProjections(storage)
        new_ns = NamespaceService(new_projections, journal)
        new_svc = ArtifactFSService(new_projections, storage_driver, journal)

        journal.rebuild_projections([ns_service, new_ns, new_svc])

        # Verify artifacts are still readable
        data1 = new_svc.read(pid, "workspace:///rebuild1.txt")
        assert data1 == b"content1"
