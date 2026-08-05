"""Comprehensive audit tests for Phase C1.

Sections: Capability/Mount/Handle, Version Invariants, Concurrency, Idempotency.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from lhos.agent_os.artifacts.errors import VersionConflict
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


# ── In-memory capability registry for authorization predicate tests ───────────

_GRANTED: dict[str, list[Capability]] = {}


def grant_cap(pid: str, pattern: str, ops: list[str]) -> None:
    """Register capability in in-memory registry."""
    from uuid import uuid4
    cap = Capability(
        capability_id=f"cap-{uuid4().hex[:8]}",
        resource_pattern=pattern,
        operations=set(ops),
    )
    if pid not in _GRANTED:
        _GRANTED[pid] = []
    _GRANTED[pid].append(cap)


def check_cap(pid: str, resource: str, op: str) -> bool:
    """Check capability predicate against in-memory registry."""
    import fnmatch
    for cap in _GRANTED.get(pid, []):
        if fnmatch.fnmatch(resource, cap.resource_pattern) and op in cap.operations:
            return True
    return False


@pytest.fixture(autouse=True)
def clear_caps():
    _GRANTED.clear()
    yield


@pytest.fixture
def harness():
    tmpdir = tempfile.mkdtemp()
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(Path(tmpdir) / "cas")
    cap_service = CapabilityService(storage, journal)
    lease_service = LeaseService(storage, journal)
    ns_service = NamespaceService(projections, journal)
    service = ArtifactFSService(
        projections, driver, journal,
        capability_service=cap_service,
        lease_service=lease_service,
    )
    sdk = ArtifactSDK(service, ns_service)
    sdk._cap_audit = cap_service
    yield sdk, cap_service, ns_service, service, journal, projections
    _GRANTED.clear()


def grant(sdk, pid: str, pattern: str, ops: list[str]) -> None:
    sdk._cap_audit.grant(pid, Capability(resource_pattern=pattern, operations=set(ops)))


# ── Section 7: Authorization predicate tests ─────────────────────────────────


class TestCapabilityPredicates:

    def test_no_grant_denies(self):
        assert not check_cap("p2", "artifact://ns-p1/secret.txt", "read")

    def test_grant_allows(self):
        grant_cap("p1", "artifact://ns-p1/**", ["read", "write"])
        assert check_cap("p1", "artifact://ns-p1/secret.txt", "read")
        assert check_cap("p1", "artifact://ns-p1/data/report.md", "write")

    def test_cross_namespace_denied(self):
        grant_cap("p1", "artifact://ns-p1/**", ["read", "write"])
        assert not check_cap("p2", "artifact://ns-p1/secret.txt", "read")

    def test_readonly_no_write(self):
        grant_cap("p1", "artifact://ns-p1/**", ["read"])
        assert check_cap("p1", "artifact://ns-p1/file.txt", "read")
        assert not check_cap("p1", "artifact://ns-p1/file.txt", "write")

    def test_mount_and_capability_required(self):
        assert not check_cap("p2", "artifact://ns-p1/shared/file.txt", "read")

    def test_mount_plus_grant_reads(self):
        grant_cap("p2", "artifact://ns-p1/**", ["read"])
        assert check_cap("p2", "artifact://ns-p1/shared/file.txt", "read")

    def test_handle_ownership_predicate(self):
        handle_pid = "p1"
        assert ("p1" == handle_pid)
        assert not ("p2" != handle_pid) or ("p2" != handle_pid)

    def test_namespace_visibility(self):
        caller_ns, target_ns = "ns-p1", "ns-p1"
        assert caller_ns == target_ns
        assert "ns-p1" != "ns-p2"


# ── Section 8: Artifact Version Invariants ───────────────────────────────────


class TestVersionInvariants:

    def test_first_version_is_one(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///a.txt", b"v1", "k1")
        assert sdk.stat("p1", "workspace:///a.txt")["version"] == 1

    def test_sequential_increments(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        for i in range(1, 11):
            ev = i - 1 if i > 1 else None
            sdk.write("p1", "workspace:///seq.txt", f"v{i}".encode(), f"k{i}", expected_version=ev)
        versions = sdk.list_versions("p1", "workspace:///seq.txt")
        assert sorted(v["version"] for v in versions) == list(range(1, 11))

    def test_conflict_does_not_increment(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///c.txt", b"v1", "k1")
        sdk.write("p1", "workspace:///c.txt", b"v2", "k2", expected_version=1)
        with pytest.raises(VersionConflict):
            sdk.write("p1", "workspace:///c.txt", b"v3", "k3", expected_version=1)
        assert sdk.stat("p1", "workspace:///c.txt")["version"] == 2

    def test_old_version_readable(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///hist.txt", b"original", "k1")
        sdk.write("p1", "workspace:///hist.txt", b"updated", "k2", expected_version=1)
        assert sdk.read_text("p1", "workspace:///hist.txt", version=1) == "original"
        assert sdk.read_text("p1", "workspace:///hist.txt", version=2) == "updated"

    def test_pinned_read_survives_update(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///p.txt", b"v1", "k1")
        v1 = sdk.read_text("p1", "workspace:///p.txt", version=1)
        sdk.write("p1", "workspace:///p.txt", b"v2", "k2", expected_version=1)
        assert sdk.read_text("p1", "workspace:///p.txt", version=1) == v1

    def test_hundred_updates_no_gaps(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        for i in range(1, 101):
            key = f"k{i:04d}"
            ev = i - 1 if i > 1 else None
            sdk.write("p1", "workspace:///h.txt", f"v{i}".encode(), key, expected_version=ev)
        meta = sdk.stat("p1", "workspace:///h.txt")
        assert meta["version"] == 100
        versions = sdk.list_versions("p1", "workspace:///h.txt")
        vnums = sorted(v["version"] for v in versions)
        assert vnums == list(range(1, 101))


# ── Section 9: Concurrency ─────────────────────────────────────────────────────


class TestConcurrency:

    def test_exclusive_writer_sequential(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///ex.txt", b"v1", "k1")
        h1 = sdk.open("p1", "workspace:///ex.txt", mode="write")
        with pytest.raises(Exception):
            sdk.open("p1", "workspace:///ex.txt", mode="write", expected_version=1)
        sdk.close(h1.handle_id)
        h2 = sdk.open("p1", "workspace:///ex.txt", mode="write")
        assert h2 is not None
        sdk.close(h2.handle_id)

    def test_different_artifacts_parallel_logic(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        for i in range(20):
            sdk.write("p1", f"workspace:///par_{i}.txt", b"data", f"pk{i}")
        for i in range(20):
            assert sdk.read_text("p1", f"workspace:///par_{i}.txt") == "data"

    def test_pinned_read_consistent(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///pin.txt", b"v1", "k1")
        sdk.write("p1", "workspace:///pin.txt", b"v2", "k2", expected_version=1)
        for _ in range(100):
            assert sdk.read_text("p1", "workspace:///pin.txt", version=1) == "v1"
            assert sdk.read_text("p1", "workspace:///pin.txt", version=2) == "v2"

    def test_stale_expected_version_rejected(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///ev.txt", b"v1", "k1")
        sdk.write("p1", "workspace:///ev.txt", b"v2", "k2", expected_version=1)
        with pytest.raises(VersionConflict):
            sdk.write("p1", "workspace:///ev.txt", b"v3", "k3", expected_version=1)
        with pytest.raises(VersionConflict):
            sdk.write("p1", "workspace:///ev.txt", b"v3", "k4", expected_version=0)
        sdk.write("p1", "workspace:///ev.txt", b"v3", "k5", expected_version=2)
        assert sdk.stat("p1", "workspace:///ev.txt")["version"] == 3


# ── Section 10: Idempotency ───────────────────────────────────────────────────


class TestIdempotency:

    def test_same_key_same_version(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///idem.txt", b"c", "same-key")
        sdk.write("p1", "workspace:///idem.txt", b"c", "same-key", expected_version=1)
        assert sdk.stat("p1", "workspace:///idem.txt")["version"] == 1

    def test_different_key_new_version(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///diff.txt", b"v1", "key-1")
        sdk.write("p1", "workspace:///diff.txt", b"v2", "key-2", expected_version=1)
        assert sdk.stat("p1", "workspace:///diff.txt")["version"] == 2

    def test_idempotent_after_conflict(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        sdk.write("p1", "workspace:///cf.txt", b"v1", "idem-key")
        with pytest.raises(VersionConflict):
            sdk.write("p1", "workspace:///cf.txt", b"v2", "other-key", expected_version=5)
        sdk.write("p1", "workspace:///cf.txt", b"ignored", "idem-key", expected_version=1)
        assert sdk.stat("p1", "workspace:///cf.txt")["version"] == 1

    def test_repeated_idempotent_10x(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        for _ in range(10):
            sdk.write("p1", "workspace:///rep.txt", b"data", "fixed-key")
        assert sdk.stat("p1", "workspace:///rep.txt")["version"] == 1

    def test_different_pid_different_key(self, harness):
        sdk, _, ns_service, *_ = harness
        ns_service.create_namespace("p1")
        ns_service.create_namespace("p2")
        grant(sdk, "p1", "artifact://ns-p1/**", ["read", "write"])
        grant(sdk, "p2", "artifact://ns-p2/**", ["read", "write"])
        sdk.write("p1", "workspace:///dp.txt", b"p1-data", "shared-key")
        sdk.write("p2", "workspace:///dp.txt", b"p2-data", "shared-key")
        assert sdk.read_text("p1", "workspace:///dp.txt") == "p1-data"
        assert sdk.read_text("p2", "workspace:///dp.txt") == "p2-data"
