"""Tests for namespace mounts, snapshots, and copy-on-write."""

from __future__ import annotations

from pathlib import Path

import pytest

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture()
def setup(tmp_path: Path):
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    storage_driver = LocalArtifactStorageDriver(tmp_path / "cas")
    ns_service = NamespaceService(projections, journal)
    service = ArtifactFSService(projections, storage_driver, journal)

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
    }


class TestMounts:
    """Namespace mount creation and resolution."""

    def test_create_mount(self, setup) -> None:
        ns_svc = setup["ns_service"]
        mnt = ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        assert mnt.namespace_id == "ns-p2"
        assert mnt.mount_point == "shared"
        assert mnt.source_namespace_id == "ns-p1"
        assert mnt.mode == "shared_readonly"

    def test_list_mounts(self, setup) -> None:
        ns_svc = setup["ns_service"]
        ns_svc.mount("p2", "shared", "ns-p1")
        ns_svc.mount("p2", "archive", "ns-p1", source_prefix="old")

        mounts = ns_svc.list_mounts("p2")
        assert len(mounts) == 2

    def test_unmount(self, setup) -> None:
        ns_svc = setup["ns_service"]
        ns_svc.mount("p2", "shared", "ns-p1")
        assert ns_svc.unmount("p2", "shared") is True

        mounts = ns_svc.list_mounts("p2")
        assert len(mounts) == 0

    def test_unmount_nonexistent(self, setup) -> None:
        ns_svc = setup["ns_service"]
        assert ns_svc.unmount("p2", "nonexistent") is False

    def test_resolve_mount_direct_hit(self, setup) -> None:
        ns_svc = setup["ns_service"]
        ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        resolved_ns, resolved_path, mode = ns_svc.resolve_mount("ns-p2", "shared/file.txt")
        assert resolved_ns == "ns-p1"
        assert resolved_path == "file.txt"
        assert mode == "shared_readonly"

    def test_resolve_mount_with_source_prefix(self, setup) -> None:
        ns_svc = setup["ns_service"]
        ns_svc.mount("p2", "archive", "ns-p1", source_prefix="old/reports", mode="shared_readonly")

        resolved_ns, resolved_path, _mode = ns_svc.resolve_mount("ns-p2", "archive/q1.txt")
        assert resolved_ns == "ns-p1"
        assert resolved_path == "old/reports/q1.txt"

    def test_resolve_no_mount_returns_original(self, setup) -> None:
        ns_svc = setup["ns_service"]
        resolved_ns, resolved_path, mode = ns_svc.resolve_mount("ns-p2", "local/file.txt")
        assert resolved_ns == "ns-p2"
        assert resolved_path == "local/file.txt"
        assert mode == "private"

    def test_read_through_shared_readonly_mount(self, setup) -> None:
        """p1 writes a file, p2 mounts p1's namespace and reads it."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        # p1 writes a file
        svc.write("p1", "workspace:///docs/api.md", b"API reference", "key-1")

        # p2 mounts p1's namespace at /shared
        ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        # p2 reads through the mount
        data = svc.read("p2", "artifact://ns-p2/shared/docs/api.md")
        assert data == b"API reference"


class TestSnapshots:
    """Namespace snapshot creation and restoration."""

    def test_create_snapshot(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///a.txt", b"content A", "key-1")
        svc.write("p1", "workspace:///b.txt", b"content B", "key-2")

        snap = ns_svc.create_snapshot("p1")

        assert snap.namespace_id == "ns-p1"
        assert len(snap.artifact_versions) == 2
        assert "artifact://ns-p1/a.txt" in snap.artifact_versions
        assert snap.artifact_versions["artifact://ns-p1/a.txt"] == 1

    def test_snapshot_is_immutable(self, setup) -> None:
        """Snapshot captures state at creation time."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///snap.txt", b"v1", "key-1")
        snap = ns_svc.create_snapshot("p1")

        # Write new version after snapshot
        svc.write("p1", "workspace:///snap.txt", b"v2", "key-2")

        # Snapshot should still show version 1
        assert snap.artifact_versions["artifact://ns-p1/snap.txt"] == 1

    def test_get_snapshot_by_id(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///file.txt", b"content", "key-1")
        snap = ns_svc.create_snapshot("p1")

        retrieved = ns_svc.get_snapshot(snap.snapshot_id)
        assert retrieved is not None
        assert retrieved.snapshot_id == snap.snapshot_id

    def test_snapshot_captures_content_refs(self, setup) -> None:
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///ref.txt", b"important", "key-1")
        snap = ns_svc.create_snapshot("p1")

        # Should have content_refs mapping
        assert len(snap.content_refs) == 1
        for key, ref in snap.content_refs.items():
            assert ":" in key  # artifact_id:version
            assert ref  # non-empty content_ref


class TestCopyOnWrite:
    """Copy-on-write mount semantics."""

    def test_cow_read_from_source(self, setup) -> None:
        """COW mount reads from source until written."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///template.txt", b"original", "key-1")
        ns_svc.mount("p2", "templates", "ns-p1", mode="copy_on_write")

        # p2 reads through COW mount — should get source content
        data = svc.read("p2", "artifact://ns-p2/templates/template.txt")
        assert data == b"original"

    def test_cow_write_creates_local_copy(self, setup) -> None:
        """COW mount: writing creates a copy in target namespace."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]

        svc.write("p1", "workspace:///template.txt", b"original", "key-1")
        ns_svc.mount("p2", "templates", "ns-p1", mode="copy_on_write")

        # p2 writes through COW mount — creates local copy
        svc.write("p2", "artifact://ns-p2/templates/template.txt", b"modified", "key-1")

        # p1's original should be unchanged
        assert svc.read("p1", "workspace:///template.txt") == b"original"

        # p2 should see its modified version
        assert svc.read("p2", "artifact://ns-p2/templates/template.txt") == b"modified"


class TestProjectionRebuildWithMounts:
    """Verify mounts and snapshots survive journal rebuild."""

    def test_mounts_survive_rebuild(self, setup) -> None:
        ns_svc = setup["ns_service"]
        journal = setup["journal"]
        storage = setup["storage"]
        storage_driver = setup["storage_driver"]

        ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        # Rebuild
        new_projections = ArtifactProjections(storage)
        new_ns = NamespaceService(new_projections, journal)
        new_svc = ArtifactFSService(new_projections, storage_driver, journal)

        journal.rebuild_projections([ns_svc, new_ns, new_svc])

        mounts = new_ns.list_mounts("p2")
        assert len(mounts) == 1
        assert mounts[0].mount_point == "shared"
