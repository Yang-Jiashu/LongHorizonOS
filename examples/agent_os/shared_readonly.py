"""Demo 2: Namespace Isolation + Shared Read-Only

Shows how one Process cannot access another's Namespace,
and how explicit Mount + Capability enable safe read-only sharing.

Run: python -m examples.agent_os.shared_readonly
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = SQLiteStorage(":memory:")
        journal = JournalService(storage)
        projections = ArtifactProjections(storage)
        driver = LocalArtifactStorageDriver(Path(tmpdir) / "cas")
        ns_service = NamespaceService(projections, journal)
        service = ArtifactFSService(projections, driver, journal)
        sdk = ArtifactSDK(service, ns_service)

        # Create two processes
        ns_service.create_namespace("p1")
        ns_service.create_namespace("p2")

        print("=== Demo 2: Namespace Isolation ===\n")

        # P1 creates a secret
        sdk.write("p1", "workspace:///secret.txt", b"top secret data", "s1")
        print("[p1] Created secret.txt in private namespace")

        # P2 tries to access P1's namespace directly — DENIED
        try:
            sdk.read_text("p2", "artifact://ns-p1/secret.txt")
            print("[p2] ERROR: Should not see p1's secret!")
        except CapabilityDenied:
            print("[p2] Correctly denied read: cannot access p1's namespace")

        # P2 tries to write to its own namespace using p1's URI — DENIED
        # (capability is checked against the canonical URI)
        try:
            sdk.write("p2", "workspace:///secret.txt", b"hacked", "hack1")
            # This may succeed in p2's own namespace; that's fine — namespaces are isolated
            print("[p2] Wrote to its OWN namespace (isolated from p1)")
        except CapabilityDenied:
            print("[p2] Capability denied for cross-namespace write")

        print("\n--- Now enable explicit read-only sharing ---\n")

        # P1 shares with P2 via readonly mount
        sdk.mount("p2", "shared", "p1", mode="shared_readonly")
        print("[p2] Mounted p1's namespace at /shared (read-only)")

        # P2 reads through mount — SUCCESS
        data = sdk.read_text("p2", "artifact://ns-p2/shared/secret.txt")
        print(f"[p2] Read through mount: {data!r}")

        # P2 tries to write through readonly mount — DENIED
        try:
            sdk.write("p2", "artifact://ns-p2/shared/secret.txt", b"modified", "modify1")
            print("[p2] ERROR: Should not write through readonly mount!")
        except CapabilityDenied:
            print("[p2] Correctly denied: readonly mount cannot be written")

        # P1's original unchanged
        original = sdk.read_text("p1", "workspace:///secret.txt")
        print(f"[p1] Original unchanged: {original!r}")

        print("\nDemo 2 complete.")


if __name__ == "__main__":
    main()
