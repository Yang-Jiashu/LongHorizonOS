"""Tests for the local artifact storage driver."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver


@pytest.fixture()
def storage(tmp_path: Path) -> LocalArtifactStorageDriver:
    return LocalArtifactStorageDriver(tmp_path / "artifact_store")


class TestStageAndCommit:
    """Stage → commit → read lifecycle."""

    def test_stage_returns_hash_and_size(self, storage: LocalArtifactStorageDriver) -> None:
        content = b"hello world"
        result = storage.stage("txn-1", content)

        expected_hash = hashlib.sha256(content).hexdigest()
        assert result.content_hash == expected_hash
        assert result.content_ref == expected_hash
        assert result.size_bytes == len(content)

    def test_commit_makes_content_readable(self, storage: LocalArtifactStorageDriver) -> None:
        content = b"hello world"
        storage.stage("txn-1", content)
        storage.commit("txn-1")

        data = storage.read(hashlib.sha256(content).hexdigest())
        assert data == content

    def test_commit_is_idempotent(self, storage: LocalArtifactStorageDriver) -> None:
        content = b"hello world"
        storage.stage("txn-1", content)

        # First commit
        result1 = storage.commit("txn-1")
        assert result1.committed is True

        # Second commit (staging file already gone)
        result2 = storage.commit("txn-1")
        assert result2.committed is False

    def test_content_deduplication(self, storage: LocalArtifactStorageDriver) -> None:
        """Same content committed twice should only create one CAS entry."""
        content = b"deduplicated content"

        storage.stage("txn-1", content)
        storage.commit("txn-1")

        storage.stage("txn-2", content)
        storage.commit("txn-2")

        assert storage.content_count() == 1

    def test_different_content_creates_separate_entries(
        self, storage: LocalArtifactStorageDriver
    ) -> None:
        storage.stage("txn-1", b"content A")
        storage.commit("txn-1")

        storage.stage("txn-2", b"content B")
        storage.commit("txn-2")

        assert storage.content_count() == 2

    def test_staged_content_not_readable_before_commit(
        self, storage: LocalArtifactStorageDriver
    ) -> None:
        content = b"secret"
        storage.stage("txn-1", content)
        content_hash = hashlib.sha256(content).hexdigest()

        # Should not be readable yet
        assert not storage.exists(content_hash)

        # After commit
        storage.commit("txn-1")
        assert storage.exists(content_hash)


class TestAbort:
    """Abort removes staged content."""

    def test_abort_removes_staging(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"to be aborted")
        assert storage.abort("txn-1") is True

        # Content should not be in CAS
        assert storage.content_count() == 0

    def test_abort_nonexistent_returns_false(self, storage: LocalArtifactStorageDriver) -> None:
        assert storage.abort("nonexistent") is False

    def test_abort_then_commit_fails_gracefully(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"content")
        storage.abort("txn-1")

        result = storage.commit("txn-1")
        assert result.committed is False


class TestReadOperations:
    """Read and inspection operations."""

    def test_read_nonexistent_raises(self, storage: LocalArtifactStorageDriver) -> None:
        with pytest.raises(FileNotFoundError):
            storage.read("nonexistent_hash")

    def test_exists_returns_false_for_nonexistent(
        self, storage: LocalArtifactStorageDriver
    ) -> None:
        assert not storage.exists("nonexistent_hash")

    def test_size_returns_correct_size(self, storage: LocalArtifactStorageDriver) -> None:
        content = b"x" * 1024
        storage.stage("txn-1", content)
        storage.commit("txn-1")

        content_hash = hashlib.sha256(content).hexdigest()
        assert storage.size(content_hash) == 1024

    def test_inspect_staged_transaction(self, storage: LocalArtifactStorageDriver) -> None:
        content = b"staged content"
        storage.stage("txn-1", content)

        status = storage.inspect_transaction("txn-1")
        assert status.status == "staged"
        assert status.content_hash == hashlib.sha256(content).hexdigest()

    def test_inspect_committed_transaction(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"committed")
        storage.commit("txn-1")

        status = storage.inspect_transaction("txn-1")
        assert status.status == "unknown"  # staging file gone

    def test_inspect_nonexistent_transaction(self, storage: LocalArtifactStorageDriver) -> None:
        status = storage.inspect_transaction("nonexistent")
        assert status.status == "unknown"


class TestStageFromFile:
    """Stage from existing file."""

    def test_stage_from_file(self, storage: LocalArtifactStorageDriver, tmp_path: Path) -> None:
        source = tmp_path / "source.txt"
        source.write_text("file content")

        result = storage.stage_from_file("txn-1", source)
        assert result.size_bytes == len(b"file content")

        storage.commit("txn-1")
        data = storage.read(result.content_ref)
        assert data == b"file content"


class TestRecovery:
    """Crash recovery."""

    def test_recover_keeps_known_transactions(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"known")
        storage.stage("txn-2", b"orphaned")

        # Simulate crash recovery: txn-1 is known, txn-2 is orphaned
        results = storage.recover({"txn-1"})
        assert "txn-1" in results
        assert "txn-2" not in results

        # txn-1 staging should still exist
        status = storage.inspect_transaction("txn-1")
        assert status.status == "staged"

        # txn-2 should be cleaned up
        status = storage.inspect_transaction("txn-2")
        assert status.status == "unknown"

    def test_list_orphaned_staging(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"content 1")
        storage.stage("txn-2", b"content 2")

        orphans = storage.list_orphaned_staging()
        assert set(orphans) == {"txn-1", "txn-2"}

    def test_new_driver_inherits_staging_files(self, tmp_path: Path) -> None:
        """Creating a new driver on the same root should see existing staging files."""
        root = tmp_path / "store"
        storage1 = LocalArtifactStorageDriver(root)
        storage1.stage("txn-1", b"persistent")

        # Create new driver instance
        storage2 = LocalArtifactStorageDriver(root)
        orphans = storage2.list_orphaned_staging()
        assert "txn-1" in orphans


class TestStats:
    """Storage statistics."""

    def test_total_size(self, storage: LocalArtifactStorageDriver) -> None:
        storage.stage("txn-1", b"x" * 100)
        storage.commit("txn-1")

        storage.stage("txn-2", b"y" * 200)
        storage.commit("txn-2")

        assert storage.total_size() == 300

    def test_total_size_empty(self, storage: LocalArtifactStorageDriver) -> None:
        assert storage.total_size() == 0

    def test_content_count_with_dedup(self, storage: LocalArtifactStorageDriver) -> None:
        # Same content twice
        storage.stage("txn-1", b"same")
        storage.commit("txn-1")
        storage.stage("txn-2", b"same")
        storage.commit("txn-2")

        # Different content
        storage.stage("txn-3", b"different")
        storage.commit("txn-3")

        assert storage.content_count() == 2  # dedup


class TestStringContent:
    """Stage with string content (auto-encoded as UTF-8)."""

    def test_stage_string_content(self, storage: LocalArtifactStorageDriver) -> None:
        text = "Hello, 世界"
        result = storage.stage("txn-1", text)

        storage.commit("txn-1")
        data = storage.read(result.content_ref)
        assert data == text.encode("utf-8")

    def test_stage_empty_content(self, storage: LocalArtifactStorageDriver) -> None:
        result = storage.stage("txn-1", b"")
        assert result.size_bytes == 0
        assert result.content_hash == hashlib.sha256(b"").hexdigest()

        storage.commit("txn-1")
        data = storage.read(result.content_ref)
        assert data == b""
