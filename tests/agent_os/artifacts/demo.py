"""Demo script for the Artifact File System.

Demonstrates:
1. Basic read/write/versioning
2. Namespace mounts (shared_readonly, copy_on_write)
3. Snapshots
4. Artifact watches with signal delivery
5. Quota enforcement
6. Crash recovery

Run: python -m tests.agent_os.artifacts.demo
"""

from __future__ import annotations

import tempfile
from pathlib import Path

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


def demo_basic_operations(sdk: ArtifactSDK) -> None:
    print("\n=== Demo 1: Basic Read/Write/Versioning ===")

    # Write
    sdk.write("p1", "workspace:///report.md", b"# Q3 Report\nRevenue: $10M", "v1")
    print("  Wrote report.md v1")

    # Read
    data = sdk.read_text("p1", "workspace:///report.md")
    print(f"  Read: {data[:30]}...")

    # Write new version
    sdk.write("p1", "workspace:///report.md", b"# Q3 Report\nRevenue: $12M", "v2", expected_version=1)
    print("  Wrote report.md v2")

    # List versions
    versions = sdk.list_versions("p1", "workspace:///report.md")
    print(f"  Versions: {len(versions)}")
    for v in versions:
        print(f"    v{v['version']}: {v['size_bytes']} bytes, hash={v['content_hash'][:8]}...")

    # Read old version
    old = sdk.read_text("p1", "workspace:///report.md", version=1)
    print(f"  Read v1: {old[:30]}...")


def demo_mounts(sdk: ArtifactSDK) -> None:
    print("\n=== Demo 2: Namespace Mounts ===")

    # p1 writes shared content
    sdk.write("p1", "workspace:///templates/base.html", b"<html>base</html>", "t1")
    print("  p1 wrote templates/base.html")

    # p2 mounts p1's namespace as shared_readonly
    sdk.mount("p2", "shared", "p1", mode="shared_readonly")
    print("  p2 mounted p1's namespace at /shared (shared_readonly)")

    # p2 reads through mount
    data = sdk.read_text("p2", "artifact://ns-p2/shared/templates/base.html")
    print(f"  p2 read through mount: {data}")

    # p3 mounts as copy_on_write
    sdk.create_namespace("p3")
    sdk.mount("p3", "templates", "p1", mode="copy_on_write")
    print("  p3 mounted p1's namespace at /templates (copy_on_write)")

    # p3 reads original
    data = sdk.read_text("p3", "artifact://ns-p3/templates/templates/base.html")
    print(f"  p3 read original: {data}")

    # p3 writes through COW — creates local copy
    sdk.write("p3", "artifact://ns-p3/templates/templates/base.html", b"<html>modified</html>", "cow1")
    print("  p3 wrote through COW mount")

    # p1's original is unchanged
    data = sdk.read_text("p1", "workspace:///templates/base.html")
    print(f"  p1's original unchanged: {data}")

    # p3 sees modified version
    data = sdk.read_text("p3", "artifact://ns-p3/templates/templates/base.html")
    print(f"  p3 sees modified: {data}")


def demo_snapshots(sdk: ArtifactSDK) -> None:
    print("\n=== Demo 3: Snapshots ===")

    sdk.write("p1", "workspace:///data/a.txt", b"alpha", "s1")
    sdk.write("p1", "workspace:///data/b.txt", b"beta", "s2")
    print("  p1 wrote 2 files")

    snap = sdk.snapshot("p1")
    print(f"  Created snapshot: {snap.snapshot_id[:8]}...")
    print(f"  Captured {len(snap.artifact_versions)} artifacts")

    # Write new versions
    sdk.write("p1", "workspace:///data/a.txt", b"alpha-v2", "s3", expected_version=1)
    print("  Updated a.txt to v2")

    # Snapshot still shows v1
    assert snap.artifact_versions["artifact://ns-p1/data/a.txt"] == 1
    print("  Snapshot still shows a.txt at v1")


def demo_watches(sdk: ArtifactSDK, sig_svc: SignalService) -> None:
    print("\n=== Demo 4: Artifact Watches ===")

    # p2 watches p1's namespace
    sdk.watch("p2", "artifact://ns-p1/")
    print("  p2 registered watch on artifact://ns-p1/")

    # p1 writes — p2 gets signal
    sdk.write("p1", "workspace:///notify.txt", b"notification", "w1")
    print("  p1 wrote notify.txt")

    pending = sig_svc.list_pending("p2")
    print(f"  p2 received {len(pending)} signal(s)")
    if pending:
        sig = pending[0]
        print(f"    type={sig.signal_type}, uri={sig.payload['canonical_uri']}, v{sig.payload['new_version']}")


def demo_quotas(sdk: ArtifactSDK) -> None:
    print("\n=== Demo 5: Quota Enforcement ===")

    # Use a fresh process to avoid pre-existing data
    sdk.create_namespace("p4")
    sdk.set_quota("ns-p4", 100)
    print("  Set p4 quota: 100 bytes")

    sdk.write("p4", "workspace:///quota/small.txt", b"small", "q1")
    print("  Wrote 5 bytes — OK")

    usage = sdk.get_usage("p4")
    print(f"  Usage: {usage['total_bytes']}/{usage['quota_bytes']} bytes ({usage['quota_used_pct']:.0f}%)")

    # Try to exceed quota
    from lhos.agent_os.artifacts.errors import QuotaExceeded
    try:
        sdk.write("p4", "workspace:///quota/big.txt", b"x" * 200, "q2")
        print("  ERROR: Should have been rejected!")
    except QuotaExceeded:
        print("  Correctly rejected write exceeding quota")


def demo_recovery(sdk: ArtifactSDK) -> None:
    print("\n=== Demo 6: Crash Recovery ===")

    sdk.write("p1", "workspace:///recovery.txt", b"safe data", "r1")
    print("  Wrote recovery.txt")

    results = sdk.recover()
    print(f"  Recovery: {results['uncertain_resolved']} uncertain resolved, {results['orphaned_cleaned']} orphans cleaned")
    print(f"  Data intact: {sdk.read_text('p1', 'workspace:///recovery.txt')}")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = SQLiteStorage(":memory:")
        journal = JournalService(storage)
        clock = Clock()
        process_service = ProcessService(storage, journal, clock)
        signal_service = SignalService(storage, journal, process_service)
        projections = ArtifactProjections(storage)
        storage_driver = LocalArtifactStorageDriver(Path(tmpdir) / "cas")
        ns_service = NamespaceService(projections, journal)
        service = ArtifactFSService(
            projections, storage_driver, journal,
            signal_service=signal_service,
        )
        sdk = ArtifactSDK(service, ns_service)

        ns_service.create_namespace("p1")
        ns_service.create_namespace("p2")

        print("=" * 60)
        print("  Artifact FS Demo — Phase C1")
        print("=" * 60)

        demo_basic_operations(sdk)
        demo_mounts(sdk)
        demo_snapshots(sdk)
        demo_watches(sdk, signal_service)
        demo_quotas(sdk)
        demo_recovery(sdk)

        print("\n" + "=" * 60)
        print("  All demos completed successfully!")
        print("=" * 60)


if __name__ == "__main__":
    main()
