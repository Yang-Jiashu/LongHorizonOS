"""Demo 3: Optimistic Concurrency / Lost-Update Prevention

Shows how expected_version prevents lost updates.
Two agents both prepare updates based on v1;
the second commit must fail.

Run: python -m examples.agent_os.optimistic_conflict
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from lhos.agent_os.artifacts.errors import VersionConflict
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

        ns_service.create_namespace("p1")
        ns_service.create_namespace("p2")

        # Share p1's namespace with p2
        sdk.mount("p2", "shared", "p1", mode="shared_readonly")

        print("=== Demo 3: Optimistic Concurrency ===\n")

        # Initial state: v1
        sdk.write("p1", "workspace:///data.json", b'{"count": 0}', "init")
        print("[p1] Created data.json v1")

        # Both P1 and P2 read v1
        data_p1 = sdk.read_text("p1", "workspace:///data.json")
        data_p2 = sdk.read_text("p2", "artifact://ns-p2/shared/data.json")
        print(f"[p1] Reads v1: {data_p1!r}")
        print(f"[p2] Reads v1 through mount: {data_p2!r}")

        # P1 commits v2 first
        sdk.write(
            "p1",
            "workspace:///data.json",
            b'{"count": 1}',
            "p1-update",
            expected_version=1,
        )
        print("\n[p1] Committed v2 with expected_version=1")

        # P2 tries to commit based on stale expected_version=1 — CONFLICT
        try:
            sdk.write(
                "p1",
                "workspace:///data.json",
                b'{"count": 99}',
                "p2-update",
                expected_version=1,
            )
            print("[p2] ERROR: Should have gotten version conflict!")
        except VersionConflict as e:
            print(f"[p2] Correctly got conflict: {e}")

        # Current version is v2 with P1's data
        current = sdk.read_text("p1", "workspace:///data.json")
        versions = sdk.list_versions("p1", "workspace:///data.json")
        print(f"\nFinal state: {current!r}")
        print(f"Total versions: {len(versions)} (no duplicate)")
        print("P2's update was NOT lost — it was safely rejected")

        print("\nDemo 3 complete.")


if __name__ == "__main__":
    main()
