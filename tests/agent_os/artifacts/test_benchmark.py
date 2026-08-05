"""Microbenchmarks for the Artifact FS.

Measures throughput and latency for:
1. Sequential writes (new artifacts)
2. Sequential reads
3. Version updates (same artifact, multiple versions)
4. Mount + read-through
5. Snapshot creation
6. Watch + signal delivery

Run: python -m pytest tests/agent_os/artifacts/test_benchmark.py -v -s
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Clock
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
    ns_service.create_namespace("p1")
    ns_service.create_namespace("p2")
    return {"service": service, "ns_service": ns_service}


class TestBenchmarks:
    """Microbenchmark suite — raw timing measurements."""

    def test_timing_sequential_writes(self, setup) -> None:
        """Benchmark: 200 sequential writes to new artifacts."""
        svc = setup["service"]
        n = 200

        start = time.perf_counter()
        for i in range(n):
            svc.write("p1", f"workspace:///timing/{i}.txt", f"data-{i}".encode(), f"t-{i}")
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  Sequential writes: {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 100, f"Write throughput too low: {rate:.0f} ops/s"

    def test_timing_sequential_reads(self, setup) -> None:
        """Benchmark: 200 sequential reads."""
        svc = setup["service"]
        n = 200

        for i in range(n):
            svc.write("p1", f"workspace:///timing/r{i}.txt", f"data-{i}".encode(), f"p-{i}")

        start = time.perf_counter()
        for i in range(n):
            svc.read("p1", f"workspace:///timing/r{i}.txt")
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  Sequential reads: {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 100, f"Read throughput too low: {rate:.0f} ops/s"

    def test_timing_version_updates(self, setup) -> None:
        """Benchmark: 50 version updates to the same artifact."""
        svc = setup["service"]
        n = 50

        svc.write("p1", "workspace:///bench/versioned.txt", b"v0", "vu-0")

        start = time.perf_counter()
        for i in range(1, n + 1):
            svc.write(
                "p1",
                "workspace:///bench/versioned.txt",
                f"v{i}".encode(),
                f"vu-{i}",
                expected_version=i,
            )
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  Version updates: {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 50, f"Version update throughput too low: {rate:.0f} ops/s"

    def test_timing_mount_readthrough(self, setup) -> None:
        """Benchmark: 100 reads through shared_readonly mount."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]
        n = 100

        for i in range(n):
            svc.write("p1", f"workspace:///timing/m{i}.txt", f"mnt-{i}".encode(), f"mt-{i}")
        ns_svc.mount("p2", "shared", "ns-p1", mode="shared_readonly")

        start = time.perf_counter()
        for i in range(n):
            svc.read("p2", f"artifact://ns-p2/shared/timing/m{i}.txt")
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  Mount read-through: {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 50, f"Mount read throughput too low: {rate:.0f} ops/s"

    def test_timing_snapshot_creation(self, setup) -> None:
        """Benchmark: Create snapshot of namespace with 50 artifacts."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]
        n = 50

        for i in range(n):
            svc.write("p1", f"workspace:///bench/snap{i}.txt", f"snap-{i}".encode(), f"sn-{i}")

        start = time.perf_counter()
        snap = ns_svc.create_snapshot("p1")
        elapsed = time.perf_counter() - start

        latency_ms = elapsed * 1000
        print(
            f"\n  Snapshot creation: {len(snap.artifact_versions)} artifacts in {latency_ms:.2f}ms"
        )
        assert latency_ms < 1000, f"Snapshot too slow: {latency_ms:.2f}ms"

    def test_timing_watch_signal_delivery(self, setup) -> None:
        """Benchmark: 50 writes with watch signal delivery to 1 watcher."""
        svc = setup["service"]
        n = 50

        svc.watch("p2", "artifact://ns-p1/")

        start = time.perf_counter()
        for i in range(n):
            svc.write("p1", f"workspace:///bench/w{i}.txt", f"w-{i}".encode(), f"w-{i}")
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  Write + watch signal: {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 50, f"Watch+write throughput too low: {rate:.0f} ops/s"

    def test_timing_cow_write_isolation(self, setup) -> None:
        """Benchmark: COW mount write creates local copy."""
        svc = setup["service"]
        ns_svc = setup["ns_service"]
        n = 20

        # p1 creates templates
        for i in range(n):
            svc.write("p1", f"workspace:///cow/t{i}.txt", f"template-{i}".encode(), f"ct-{i}")

        # p2 mounts COW
        ns_svc.mount("p2", "src", "ns-p1", mode="copy_on_write")

        start = time.perf_counter()
        for i in range(n):
            svc.write(
                "p2", f"artifact://ns-p2/src/cow/t{i}.txt", f"modified-{i}".encode(), f"cw-{i}"
            )
        elapsed = time.perf_counter() - start

        rate = n / elapsed
        latency_ms = (elapsed / n) * 1000
        print(
            f"\n  COW write (local copy): {n} ops in {elapsed:.3f}s = {rate:.0f} ops/s ({latency_ms:.2f}ms/op)"
        )
        assert rate > 20, f"COW write throughput too low: {rate:.0f} ops/s"
