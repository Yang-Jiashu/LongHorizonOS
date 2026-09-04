"""Demo 1: Private Workspace

Shows how an Agent Process gets an isolated Namespace,
creates versioned Artifacts, and pins reads to old versions.

Run: python -m examples.agent_os.private_workspace
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
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

        # Spawn P1 — auto-creates private namespace
        ns_service.create_namespace("p1")

        print("=== Demo 1: Private Workspace ===\n")

        # Create notes.md v1
        sdk.write("p1", "workspace:///notes.md", b"# Notes\n- item 1", "create-v1")
        print("[p1] Created notes.md v1")

        # Read v1
        data = sdk.read_text("p1", "workspace:///notes.md")
        print(f"[p1] Read notes.md: {data!r}")

        # Pin read to v1 BEFORE writing v2 (read with explicit version)
        pinned_data = sdk.read_text("p1", "workspace:///notes.md", version=1)
        print(f"[p1] Pinned read v1: {pinned_data!r}")

        # Write v2
        sdk.write(
            "p1",
            "workspace:///notes.md",
            b"# Notes\n- item 1\n- item 2",
            "create-v2",
            expected_version=1,
        )
        print("[p1] Updated notes.md to v2")

        # Pinned read still sees v1
        still_v1 = sdk.read_text("p1", "workspace:///notes.md", version=1)
        print(f"[p1] Pinned handle still sees v1: {still_v1!r}")

        # New read sees v2
        current_data = sdk.read_text("p1", "workspace:///notes.md")
        print(f"[p1] Current read sees v2: {current_data!r}")

        # List all versions
        versions = sdk.list_versions("p1", "workspace:///notes.md")
        print(f"[p1] All versions: {[v['version'] for v in versions]}")

        print("\n[p1] Demo 1 complete.")


if __name__ == "__main__":
    main()
