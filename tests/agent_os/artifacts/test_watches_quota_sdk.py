"""Tests for artifact watches (signal delivery), quota enforcement, and SDK API."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhos.agent_os.artifacts.errors import QuotaExceeded
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Clock
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture()
def setup(tmp_path: Path):
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    clock = Clock()
    process_service = ProcessService(storage, journal, clock)
    signal_service = SignalService(storage, journal, process_service)
    projections = ArtifactProjections(storage)
    storage_driver = LocalArtifactStorageDriver(tmp_path / "cas")
    ns_service = NamespaceService(projections, journal)
    service = ArtifactFSService(
        projections,
        storage_driver,
        journal,
        signal_service=signal_service,
    )
    sdk = ArtifactSDK(service, ns_service)

    # Create namespaces for two processes
    ns_service.create_namespace("p1")
    ns_service.create_namespace("p2")

    return {
        "storage": storage,
        "journal": journal,
        "projections": projections,
        "storage_driver": storage_driver,
        "ns_service": ns_service,
        "service": service,
        "signal_service": signal_service,
        "process_service": process_service,
        "sdk": sdk,
    }


class TestWatches:
    """Artifact watch registration and signal delivery."""

    def test_register_watch(self, setup) -> None:
        svc = setup["service"]
        watch = svc.watch("p1", "artifact://ns-p1/docs/")

        assert watch.pid == "p1"
        assert watch.uri_prefix == "artifact://ns-p1/docs/"
        assert watch.active is True

    def test_list_watches(self, setup) -> None:
        svc = setup["service"]
        svc.watch("p1", "artifact://ns-p1/docs/")
        svc.watch("p1", "artifact://ns-p1/reports/")

        watches = svc.list_watches("p1")
        assert len(watches) == 2

    def test_unwatch(self, setup) -> None:
        svc = setup["service"]
        watch = svc.watch("p1", "artifact://ns-p1/docs/")
        assert svc.unwatch(watch.watch_id) is True

        watches = svc.list_watches("p1")
        assert len(watches) == 0

    def test_watch_fires_signal_on_commit(self, setup) -> None:
        """When p1 writes, p2's watch should fire an ARTIFACT_CHANGED signal."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]

        # p2 registers a watch on p1's namespace
        svc.watch("p2", "artifact://ns-p1/")

        # p1 writes a file — should trigger watch
        svc.write("p1", "workspace:///report.md", b"content", "key-1")

        # Check that p2 received a signal
        pending = sig_svc.list_pending("p2")
        assert len(pending) == 1
        assert pending[0].signal_type == "ARTIFACT_CHANGED"
        assert pending[0].payload["canonical_uri"] == "artifact://ns-p1/report.md"
        assert pending[0].payload["new_version"] == 1
        assert pending[0].source_pid == "p1"

    def test_watch_prefix_matching(self, setup) -> None:
        """Watch should only fire for URIs matching the prefix."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]

        # Watch only docs/ prefix
        svc.watch("p2", "artifact://ns-p1/docs/")

        # Write to docs/ — should trigger
        svc.write("p1", "workspace:///docs/readme.md", b"readme", "key-1")

        # Write to reports/ — should NOT trigger
        svc.write("p1", "workspace:///reports/quarterly.md", b"report", "key-2")

        pending = sig_svc.list_pending("p2")
        assert len(pending) == 1
        assert pending[0].payload["canonical_uri"] == "artifact://ns-p1/docs/readme.md"

    def test_writer_not_notified(self, setup) -> None:
        """The process that writes should NOT receive its own watch signal."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]

        # p1 watches its own namespace
        svc.watch("p1", "artifact://ns-p1/")

        # p1 writes — should NOT receive a signal
        svc.write("p1", "workspace:///file.txt", b"data", "key-1")

        pending = sig_svc.list_pending("p1")
        assert len(pending) == 0

    def test_multiple_watchers(self, setup) -> None:
        """Multiple processes watching the same URI should all get signals."""
        svc = setup["service"]
        sig_svc = setup["signal_service"]
        ns_svc = setup["ns_service"]

        # Create p3 namespace
        ns_svc.create_namespace("p3")

        # Both p2 and p3 watch p1's namespace
        svc.watch("p2", "artifact://ns-p1/")
        svc.watch("p3", "artifact://ns-p1/")

        # p1 writes
        svc.write("p1", "workspace:///shared.md", b"shared", "key-1")

        # Both should have signals
        assert len(sig_svc.list_pending("p2")) == 1
        assert len(sig_svc.list_pending("p3")) == 1

    def test_watch_survives_rebuild(self, setup) -> None:
        """Watches should survive journal projection rebuild."""
        svc = setup["service"]
        journal = setup["journal"]
        storage = setup["storage"]
        storage_driver = setup["storage_driver"]
        ns_svc = setup["ns_service"]

        svc.watch("p2", "artifact://ns-p1/")

        # Rebuild
        new_projections = ArtifactProjections(storage)
        new_ns = NamespaceService(new_projections, journal)
        new_svc = ArtifactFSService(new_projections, storage_driver, journal)

        journal.rebuild_projections([ns_svc, new_ns, new_svc])

        watches = new_svc.list_watches("p2")
        assert len(watches) == 1
        assert watches[0].uri_prefix == "artifact://ns-p1/"


class TestQuotaEnforcement:
    """Namespace storage quota enforcement."""

    def test_quota_exceeded_on_stage(self, setup) -> None:
        """Writing beyond quota should raise QuotaExceeded."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        # Set a tiny quota (10 bytes)
        ns_svc.set_quota("ns-p1", 10)

        # Write 5 bytes — should succeed
        svc.write("p1", "workspace:///small.txt", b"hello", "key-1")

        # Write 10 more bytes — should fail
        with pytest.raises(QuotaExceeded):
            svc.write("p1", "workspace:///big.txt", b"0123456789", "key-2")

    def test_quota_not_exceeded_without_limit(self, setup) -> None:
        """No quota means unlimited."""
        svc = setup["service"]

        # No quota set — should succeed
        svc.write("p1", "workspace:///big.txt", b"x" * 10000, "key-1")

    def test_get_namespace_usage(self, setup) -> None:
        """Test usage reporting."""
        svc = setup["service"]

        svc.write("p1", "workspace:///a.txt", b"aaaa", "key-1")
        svc.write("p1", "workspace:///b.txt", b"bbbbbbbb", "key-2")

        usage = svc.get_namespace_usage("ns-p1")
        assert usage["total_bytes"] == 12  # 4 + 8
        assert usage["artifact_count"] == 2
        assert usage["version_count"] == 2
        assert usage["quota_bytes"] is None
        assert usage["quota_used_pct"] is None

    def test_get_namespace_usage_with_quota(self, setup) -> None:
        """Test usage reporting with quota."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        ns_svc.set_quota("ns-p1", 100)
        svc.write("p1", "workspace:///data.txt", b"x" * 30, "key-1")

        usage = svc.get_namespace_usage("ns-p1")
        assert usage["total_bytes"] == 30
        assert usage["quota_bytes"] == 100
        assert usage["quota_used_pct"] == 30.0

    def test_version_increments_usage(self, setup) -> None:
        """Each version contributes to total usage."""
        svc = setup["service"]

        svc.write("p1", "workspace:///file.txt", b"v1-data", "key-1")
        svc.write("p1", "workspace:///file.txt", b"v2-data-longer", "key-2")

        usage = svc.get_namespace_usage("ns-p1")
        # v1 = 7 bytes, v2 = 14 bytes → total 21
        assert usage["total_bytes"] == 21
        assert usage["version_count"] == 2
        assert usage["artifact_count"] == 1


class TestSDK:
    """ArtifactSDK high-level API tests."""

    def test_sdk_write_and_read(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///hello.txt", b"Hello, World!")
        assert sdk.read("p1", "workspace:///hello.txt") == b"Hello, World!"

    def test_sdk_read_text(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///text.md", "UTF-8 text content")
        assert sdk.read_text("p1", "workspace:///text.md") == "UTF-8 text content"

    def test_sdk_stat(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///stat.txt", b"content")
        meta = sdk.stat("p1", "workspace:///stat.txt")

        assert meta["canonical_uri"] == "artifact://ns-p1/stat.txt"
        assert meta["version"] == 1
        assert meta["size_bytes"] == 7

    def test_sdk_list(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///docs/a.md", b"a")
        sdk.write("p1", "workspace:///docs/b.md", b"b")
        sdk.write("p1", "workspace:///other.txt", b"c")

        all_items = sdk.list("p1")
        assert len(all_items) == 3

        docs_only = sdk.list("p1", "artifact://ns-p1/docs/")
        assert len(docs_only) == 2

    def test_sdk_list_versions(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///v.txt", b"v1", "k1")
        sdk.write("p1", "workspace:///v.txt", b"v2", "k2")
        sdk.write("p1", "workspace:///v.txt", b"v3", "k3")

        versions = sdk.list_versions("p1", "workspace:///v.txt")
        assert len(versions) == 3
        assert versions[0]["version"] == 1
        assert versions[2]["version"] == 3

    def test_sdk_delete(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///del.txt", b"data")
        assert sdk.exists("p1", "workspace:///del.txt") is True

        sdk.delete("p1", "workspace:///del.txt")
        assert sdk.exists("p1", "workspace:///del.txt") is False

    def test_sdk_open_close(self, setup) -> None:
        sdk = setup["sdk"]

        # First create the file
        sdk.write("p1", "workspace:///handle.txt", b"data")

        handle = sdk.open("p1", "workspace:///handle.txt", "read")
        assert handle.mode == "read"
        assert handle.is_open

        assert sdk.close(handle.handle_id) is True

        # Verify handle is closed in projections (re-fetch)
        handle2 = setup["service"]._projections.get_handle(handle.handle_id)
        assert handle2 is not None
        assert not handle2.is_open

    def test_sdk_close_all(self, setup) -> None:
        sdk = setup["sdk"]

        # Create files first
        sdk.write("p1", "workspace:///a.txt", b"a")
        sdk.write("p1", "workspace:///b.txt", b"b")
        sdk.write("p1", "workspace:///c.txt", b"c")

        sdk.open("p1", "workspace:///a.txt", "read")
        sdk.open("p1", "workspace:///b.txt", "read")
        sdk.open("p1", "workspace:///c.txt", "read")

        count = sdk.close_all("p1")
        assert count == 3

    def test_sdk_mount_and_read(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///shared.md", b"shared content")
        sdk.mount("p2", "mnt", "p1", mode="shared_readonly")

        data = sdk.read("p2", "artifact://ns-p2/mnt/shared.md")
        assert data == b"shared content"

    def test_sdk_snapshot(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///snap1.txt", b"v1", "k1")
        sdk.write("p1", "workspace:///snap2.txt", b"v2", "k2")

        snap = sdk.snapshot("p1")
        assert len(snap.artifact_versions) == 2

        retrieved = sdk.get_snapshot(snap.snapshot_id)
        assert retrieved is not None
        assert retrieved.snapshot_id == snap.snapshot_id

    def test_sdk_watch_and_signal(self, setup) -> None:
        sdk = setup["sdk"]
        sig_svc = setup["signal_service"]

        sdk.watch("p2", "artifact://ns-p1/")

        sdk.write("p1", "workspace:///notify.txt", b"data")

        pending = sig_svc.list_pending("p2")
        assert len(pending) == 1
        assert pending[0].signal_type == "ARTIFACT_CHANGED"

    def test_sdk_auto_idempotency_key(self, setup) -> None:
        sdk = setup["sdk"]

        # Write with auto-generated key
        txn = sdk.write("p1", "workspace:///auto.txt", b"content")
        assert txn.state == "committed"

        # Same content with another auto key — new version
        txn2 = sdk.write("p1", "workspace:///auto.txt", b"content v2")
        assert txn2.state == "committed"
        assert txn2.transaction_id != txn.transaction_id

    def test_sdk_get_usage(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.write("p1", "workspace:///usage.txt", b"12345", "k1")

        usage = sdk.get_usage("p1")
        assert usage["total_bytes"] == 5
        assert usage["artifact_count"] == 1

    def test_sdk_set_quota(self, setup) -> None:
        sdk = setup["sdk"]

        sdk.set_quota("ns-p1", 50)
        usage = sdk.get_usage("p1")
        assert usage["quota_bytes"] == 50

    def test_sdk_exists_nonexistent(self, setup) -> None:
        sdk = setup["sdk"]

        assert sdk.exists("p1", "workspace:///nope.txt") is False
