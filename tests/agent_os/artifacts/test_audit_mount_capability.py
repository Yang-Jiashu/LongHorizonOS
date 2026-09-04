"""SRV-01 regression: mount resolution must NOT bypass source-namespace capabilities.

A caller that holds read capability only on its own namespace must NOT be
able to read artifacts from a mounted source namespace without also being
granted capability on the source. Reads through a mount must check BOTH:
(1) caller capability on the call namespace (already enforced), and
(2) caller capability on the RESOLVED source namespace (the bypass).

This test reads `README`'s SRV-01 scenario.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.storage.sqlite import SQLiteStorage


@pytest.fixture
def env(tmp_path):
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    lease_service = LeaseService(storage, journal)
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(tmp_path / "cas")
    cap_service = CapabilityService(storage, journal)
    service = ArtifactFSService(
        projections,
        driver,
        journal,
        capability_service=cap_service,
        lease_service=lease_service,
    )
    ns_service = NamespaceService(projections, journal)
    return {
        "storage": storage,
        "journal": journal,
        "projections": projections,
        "service": service,
        "ns_service": ns_service,
        "cap_service": cap_service,
    }


def _grant(env, pid: str, pattern: str, ops):
    env["cap_service"].grant(pid, Capability(resource_pattern=pattern, operations=set(ops)))


class TestMountDoesNotBypassCapability:
    """After mount resolution, source-namespace capability must be enforced."""

    def test_cross_namespace_read_needs_source_capability(self, env):
        """P1 holds only workspace-read on its own ns. Mount ns-p1@data → ns-p2.
        Reading artifact://ns-p1/data/shared.txt must raise CapabilityDenied
        because P1 has no capability on ns-p2."""
        ns_svc = env["ns_service"]
        svc = env["service"]
        ns_svc.create_namespace("p1")
        ns_svc.create_namespace("p2")

        # P2 writes a private artifact.
        _grant(env, "p2", "artifact://ns-p2/**", ["read", "write"])
        svc.write("p2", "workspace:///shared.txt", b"secret", "k-p2")

        # P1 gets ONLY read capability on its own namespace.
        _grant(env, "p1", "artifact://ns-p1/**", ["read"])

        # P1 mounts p2's namespace as readonly.
        # (Caller pattern granted p1 its own namespace; the mount itself is an
        # administration operation — not what we're testing here.)
        ns_svc.mount("p1", "data", "ns-p2", mode="shared_readonly")

        # P1 attempts to read through the mount path. Must be DENIED at the
        # source-namespace capability check.
        with pytest.raises(CapabilityDenied):
            svc.read("p1", "artifact://ns-p1/data/shared.txt")

    def test_cross_namespace_read_allowed_with_source_capability(self, env):
        """Same scenario but P1 is ALSO granted read on ns-p2. Allowed."""
        ns_svc = env["ns_service"]
        svc = env["service"]
        ns_svc.create_namespace("p1")
        ns_svc.create_namespace("p2")

        _grant(env, "p2", "artifact://ns-p2/**", ["read", "write"])
        svc.write("p2", "workspace:///shared.txt", b"shared", "k-p2")

        # P1 gets read on its own namespace AND on ns-p2.
        _grant(env, "p1", "artifact://ns-p1/**", ["read"])
        _grant(env, "p1", "artifact://ns-p2/**", ["read"])

        ns_svc.mount("p1", "data", "ns-p2", mode="shared_readonly")

        data = svc.read("p1", "artifact://ns-p1/data/shared.txt")
        assert data == b"shared"

    def test_local_read_still_works_without_mount(self, env):
        """Regression: reads on the caller's own namespace still work."""
        ns_svc = env["ns_service"]
        svc = env["service"]
        ns_svc.create_namespace("p1")
        _grant(env, "p1", "artifact://ns-p1/**", ["read", "write"])
        svc.write("p1", "workspace:///local.txt", b"local", "k1")
        assert svc.read("p1", "workspace:///local.txt") == b"local"
