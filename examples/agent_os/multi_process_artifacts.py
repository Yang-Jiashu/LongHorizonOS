"""Demo 5: Multi-Process Artifact Pipeline (README flagship)

Shows the full LongHorizonOS value proposition:
- Process identity (each agent has its own PID)
- Namespace isolation (no cross-access without explicit sharing)
- Explicit sharing (readonly mount with capability enforcement)
- Artifact versioning (every write creates a new immutable version)
- Pinned reads (old reads still see old versions)

Scenario:
  Researcher produces research.json@v1
  Coordinator enables readonly sharing for Reviewer
  Reviewer reads fixed v1 and produces review.json@v1
  Researcher updates to research.json@v2
  Reviewer's old read still sees v1
  New Reviewer read sees v2

Run: python -m examples.agent_os.multi_process_artifacts
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

        # Create three agent processes
        ns_service.create_namespace("researcher")
        ns_service.create_namespace("reviewer")

        print("=" * 60)
        print("  Demo 5: Multi-Process Artifact Pipeline")
        print("=" * 60)

        # === Step 1: Researcher produces research.json@v1 ===
        print("\n[researcher] Producing research.json@v1...")
        sdk.write(
            "researcher",
            "workspace:///research.json",
            b'{"hypothesis": "X causes Y", "confidence": 0.85}',
            "r-v1",
        )
        print("  Created research.json v1")

        # === Step 2: Coordinator enables readonly sharing ===
        print("\n[coordinator] Enabling readonly sharing for reviewer...")
        sdk.mount("reviewer", "inputs", "researcher", mode="shared_readonly")
        print("  reviewer can read researcher's artifacts at /inputs")

        # === Step 3: Reviewer reads fixed v1 and produces review.json@v1 ===
        print("\n[reviewer] Reading research through mount...")
        research_v1_data = sdk.read_text(
            "reviewer", "artifact://ns-reviewer/inputs/research.json", version=1
        )
        print(f"  Read research v1: {research_v1_data[:50]}...")

        sdk.write(
            "reviewer",
            "workspace:///review.json",
            b'{"verdict": "weak evidence", "score": 3}',
            "rev-v1",
        )
        print("  Created review.json v1")

        # === Step 4: Researcher updates to research.json@v2 ===
        print("\n[researcher] Updating research.json to v2...")
        sdk.write(
            "researcher",
            "workspace:///research.json",
            b'{"hypothesis": "X causes Y", "confidence": 0.92, "n": 500}',
            "r-v2",
            expected_version=1,
        )
        print("  Created research.json v2")

        # === Step 5: Reviewer's pinned read still sees v1 ===
        print("\n[reviewer] Checking pinned read after update...")
        pinned_data = sdk.read_text(
            "reviewer", "artifact://ns-reviewer/inputs/research.json", version=1
        )
        assert "0.85" in pinned_data, "Pinned read must see old version"
        print(f"  Pinned read still sees v1: {pinned_data[:50]}...")

        # === Step 6: New Reviewer read sees v2 ===
        print("\n[reviewer] Opening new read for current version...")
        new_data = sdk.read_text("reviewer", "artifact://ns-reviewer/inputs/research.json")
        assert "0.92" in new_data, "New read must see new version"
        print(f"  New read sees v2: {new_data[:50]}...")

        # === Summary ===
        print("\n" + "=" * 60)
        print("  Summary")
        print("=" * 60)
        researcher_versions = sdk.list_versions("researcher", "workspace:///research.json")
        reviewer_versions = sdk.list_versions("reviewer", "workspace:///review.json")
        print(f"  researcher:  research.json has {len(researcher_versions)} versions")
        print(f"  reviewer:    review.json has {len(reviewer_versions)} versions")
        print("  pinned read: still sees research.json v1")
        print("=" * 60)
        print("\nDemo 5 complete.")


if __name__ == "__main__":
    main()
