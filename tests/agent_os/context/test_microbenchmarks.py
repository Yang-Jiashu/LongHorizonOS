"""Performance microbenchmarks for the Context VM.

Each benchmark calls an operation in a tight loop and asserts a minimum
operations/sec threshold. Thresholds are conservative to pass on modest
machines. These are smoke-style perf guards — not rigorous benchmarks.
"""

from __future__ import annotations

import time
from typing import Any

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import (
    ContentRef,
    ContextManifest,
    _content_hash_for,
)
from lhos.agent_os.context.pager import compute_pages_for_ref
from lhos.agent_os.context.policies import RefPages, select_pages_v1
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from tests.agent_os.context.conftest import (
    _AllowsAllCaps,
    _ArtifactSupplier,
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _ref(
    env: dict[str, Any],
    uri: str,
    content: bytes,
    priority: int = 0,
    required: bool = True,
) -> ContentRef:
    env["artifact_sdk"].write(env["pid"], uri, content, f"bench-{uri}-{priority}")
    ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    return ContentRef(
        ref_id=f"r-{uri}",
        canonical_uri=art["canonical_uri"],
        artifact_id=ver["artifact_id"],
        version=ver["version"],
        content_hash=ver["content_hash"],
        media_type="text/markdown",
        priority=priority,
        required=required,
    )


def _build_new_service(env: dict[str, Any], pid: str = "p1") -> ContextService:
    return ContextService(
        content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )


class _FakeSupplier:
    """Fake content supplier that returns pre-set bytes without the Artifact FS."""

    def __init__(self, content: bytes) -> None:
        self._content = content

    def read_version(self, **_: Any) -> bytes:
        return self._content

    def read_version_size(self, **_: Any) -> int:
        return len(self._content)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Manifest validation perf
# ═══════════════════════════════════════════════════════════════════════════════


class TestManifestValidationPerf:
    def test_1000_manifest_validations_under_1s(self, env: dict[str, Any]) -> None:
        """Building + validating 1000 manifests must take < 1s."""
        ref = _ref(env, "workspace:///bench_valid.md", b"validation benchmark content " * 2)
        manifests = [
            ContextManifest(
                owner_pid=env["pid"],
                refs=(ref,),
                token_budget=10_000 + i,
                page_size_bytes=64,
            )
            for i in range(1000)
        ]
        svc = env["ctx_svc"]
        pid = env["pid"]
        start = time.perf_counter()
        for m in manifests:
            svc.validate_manifest(m, caller_pid=pid)
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"1000 validations took {elapsed:.3f}s (threshold: 1.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Page compilation perf
# ═══════════════════════════════════════════════════════════════════════════════


class TestPageCompilationPerf:
    def test_1000_page_compilations_under_1s(self) -> None:
        """1000 calls to compute_pages_for_ref must take < 1s."""
        estimator = DeterministicByteTokenEstimator()
        content = b"page compilation benchmark content " * 10
        ref = ContentRef(
            ref_id="r-bench-page",
            canonical_uri="artifact://ns-p1/bench_page.md",
            artifact_id="art-bench-1",
            version=1,
            content_hash=_content_hash_for(content),
            media_type="text/plain",
            priority=0,
            required=True,
        )
        supplier = _FakeSupplier(content)
        start = time.perf_counter()
        for _ in range(1000):
            compute_pages_for_ref(
                ref=ref, content_supplier=supplier, estimator=estimator, page_size=64
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"1000 page compilations took {elapsed:.3f}s (threshold: 1.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Deterministic selection perf
# ═══════════════════════════════════════════════════════════════════════════════


class TestDeterministicSelectionPerf:
    def test_1000_policy_calls_under_1s(self) -> None:
        """1000 calls to select_pages_v1 must take < 1s."""
        estimator = DeterministicByteTokenEstimator()
        content = bytes(range(256)) * 4
        ref = ContentRef(
            ref_id="r-bench-pol",
            canonical_uri="artifact://ns-p1/bench_pol.md",
            artifact_id="art-pol-1",
            version=1,
            content_hash=_content_hash_for(content),
            media_type="text/plain",
            priority=5,
            required=True,
        )
        supplier = _FakeSupplier(content)
        pages_tuple = tuple(
            compute_pages_for_ref(
                ref=ref,
                content_supplier=supplier,
                estimator=estimator,
                page_size=128,
            )
        )
        rp = RefPages(ref=ref, pages=pages_tuple)
        manifest = ContextManifest(
            owner_pid="p1",
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=128,
        )
        start = time.perf_counter()
        for _ in range(1000):
            select_pages_v1(manifest=manifest, ref_pages=[rp])
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"1000 policy calls took {elapsed:.3f}s (threshold: 1.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Cache hit perf (same manifest load reused)
# ═══════════════════════════════════════════════════════════════════════════════


class TestCacheHitPerf:
    def test_1000_cache_hits_under_1s(self, env: dict[str, Any]) -> None:
        """Using idempotency key, 1000 loads with same key take < 1s (cached replay)."""
        ref = _ref(env, "workspace:///bench_cache.md", b"cache hit content " * 3)
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        # Warm-up (populates cache).
        env["ctx_sdk"].load(pid=env["pid"], manifest=manifest, idempotency_key="bench-cache")
        start = time.perf_counter()
        for _ in range(1000):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest, idempotency_key="bench-cache")
        elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"1000 cache hits took {elapsed:.3f}s (threshold: 1.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Cold load perf
# ═══════════════════════════════════════════════════════════════════════════════


class TestColdLoadPerf:
    def test_100_distinct_loads_under_5s(self, env: dict[str, Any]) -> None:
        """Loading 100 distinct cold manifests must take < 5s."""
        refs_list = []
        for i in range(100):
            content = f"cold-load-unique-content-{i} ".encode() * 8
            r = _ref(env, f"workspace:///bench_cold_{i}.md", content, required=True)
            refs_list.append(r)
        start = time.perf_counter()
        for r in refs_list:
            m = ContextManifest(
                owner_pid=env["pid"],
                refs=(r,),
                token_budget=100_000,
                page_size_bytes=64,
            )
            env["ctx_sdk"].load(pid=env["pid"], manifest=m)
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, f"100 cold loads took {elapsed:.3f}s (threshold: 5.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Snapshot/restore perf
# ═══════════════════════════════════════════════════════════════════════════════


def _ref_and_load_for_restore(env: dict[str, Any]) -> tuple:
    """Write one artifact and load it; used by TestSnapshotRestorePerf."""
    ref = _ref(
        env, "workspace:///bench_restore.md", b"snapshot-restore-bench-data " * 5, required=True
    )
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=(ref,),
        token_budget=100_000,
        page_size_bytes=32,
    )
    h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
    return h, loaded


class TestSnapshotRestorePerf:
    def test_100_restores_under_5s(self, env: dict[str, Any]) -> None:
        """Snapshot once, then restore 100 times into fresh services, < 5s."""
        handle, loaded = _ref_and_load_for_restore(env)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        start = time.perf_counter()
        handles = []
        for _ in range(100):
            new_svc = _build_new_service(env)
            new_sdk = ContextSDK(new_svc)
            new_svc._snaps[snap.snapshot_id] = snap
            h, restored = new_sdk.restore_snapshot(pid=env["pid"], snapshot_id=snap.snapshot_id)
            assert restored.materialized_hash == loaded.materialized_hash
            handles.append((new_sdk, h))
        for sdk, h in handles:
            sdk.close(pid=env["pid"], handle_id=h.handle_id)
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, f"100 restores took {elapsed:.3f}s (threshold: 5.0s)"


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Eviction perf
# ═══════════════════════════════════════════════════════════════════════════════


class TestEvictionPerf:
    def test_100_evictions_under_5s(self, env: dict[str, Any]) -> None:
        """Evict a working set 100 times; must take < 5s."""

        def _a_load() -> tuple:
            content_bytes = b"eviction-bench-data " * 4
            ref = _ref(env, "workspace:///bench_evict.md", content_bytes, required=True)
            manifest = ContextManifest(
                owner_pid=env["pid"],
                refs=(ref,),
                token_budget=100_000,
                page_size_bytes=32,
            )
            return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)

        handle, _loaded = _a_load()
        start = time.perf_counter()
        for _ in range(100):
            env["ctx_svc"].evict(
                pid=env["pid"],
                working_set_id=handle.working_set_id,
                target_tokens=100_000,
            )
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, f"100 evictions took {elapsed:.3f}s (threshold: 5.0s)"
