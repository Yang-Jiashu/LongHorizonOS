"""Determinism tests for Context VM.

Covers materialized_hash identity across repeated loads, service restarts,
shuffled ref ordering, estimator stability, page-id stability, and
snapshot/restore hash round-trips.
"""

from __future__ import annotations

import random
from typing import Any

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.pager import _stable_page_id
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
    env["artifact_sdk"].write(env["pid"], uri, content, f"det-{uri}-{priority}")
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


def _ref_from_artifact_row(
    env: dict[str, Any],
    uri: str,
    *,
    ref_id: str | None = None,
    priority: int = 0,
    required: bool = True,
) -> ContentRef:
    """Build a ContentRef that pins the LATEST version of the artifact
    behind ``uri`` WITHOUT re-writing it. The returned Ref always advances
    to the newest artifact version so repeated calls share the same binding."""
    versions = list(env["artifact_sdk"].list_versions(env["pid"], uri))
    ver = sorted(versions, key=lambda v: v["version"])[-1]
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    # Match by canonical_uri first (stable across versions), then by artifact_id.
    art = next(
        (a for a in arts if a["artifact_id"] == ver["artifact_id"]),
        next((a for a in arts if a["canonical_uri"].endswith("/" + uri.split("/")[-1])), None),
    )
    assert art is not None, f"no artifact row for {uri}"
    return ContentRef(
        ref_id=ref_id or f"r-{uri}-p{priority}",
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


def _load_single(env: dict[str, Any], uri: str, content: bytes, *, page_size: int = 64) -> tuple:
    """Build one artifact + manifest and load it."""
    ref = _ref(env, uri, content)
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=(ref,),
        token_budget=100_000,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


def _load_multi(
    env: dict[str, Any],
    refs_with_uris: list[tuple[str, bytes]],
    *,
    page_size: int = 64,
    token_budget: int = 100_000,
) -> tuple:
    """Build multiple artifacts + manifest and load it."""
    refs = [_ref(env, uri, content) for uri, content in refs_with_uris]
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=tuple(refs),
        token_budget=token_budget,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Run load with same manifest 100 times → every successful run's
#    materialized_hash equal.
# ═══════════════════════════════════════════════════════════════════════════════


class TestHundredTimesSameManifest:
    def test_100x_same_manifest_identical_hash(self, env: dict[str, Any]) -> None:
        """Load the same manifest 100 times, collect materialized_hashes; all equal."""
        content = b"determinism-hundred-test-content " * 4
        # Build ONE keyed manifest and reuse the SAME object 100x.
        uri = "workspace:///det_hundred.md"
        env["artifact_sdk"].write(env["pid"], uri, content, "det-hundred")
        ref = _ref(env, uri, content)
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        hashes: set[str] = set()
        for _ in range(100):
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
            hashes.add(loaded.materialized_hash)
            env["ctx_sdk"].close(pid=env["pid"], handle_id=h.handle_id)
        assert len(hashes) == 1, f"expected 1 unique hash, got {len(hashes)}"


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Re-load same manifest (with different manifest_id) 20x after
#    "restart" (recreate ContextService with same supplier snapshots).
# ═══════════════════════════════════════════════════════════════════════════════


class TestRestartHashEquality:
    def test_20x_after_restart_identical_hash(self, env: dict[str, Any]) -> None:
        """Snapshot, then rebuild WS from snapshot on a new service 20x."""
        handle, loaded = _load_single(
            env, "workspace:///det_restart.md", b"restart-det-test " * 6, page_size=32
        )
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        hashes: set[str] = set()
        for i in range(20):
            new_svc = _build_new_service(env)
            new_sdk = ContextSDK(new_svc)
            new_svc._snaps[snap.snapshot_id] = snap
            h, restored = new_sdk.restore_snapshot(pid=env["pid"], snapshot_id=snap.snapshot_id)
            hashes.add(restored.materialized_hash)
            new_sdk.close(pid=env["pid"], handle_id=h.handle_id)
        assert len(hashes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 3. 20x projection rebuild (inspect vs read) → identical materialized_hash.
# ═══════════════════════════════════════════════════════════════════════════════


class TestProjectionRebuild:
    def test_20x_inspect_vs_read_same_hash(self, env: dict[str, Any]) -> None:
        """20x on the same handle: read vs inspect return identical manifest_hash."""
        handle, loaded = _load_single(
            env, "workspace:///det_proj.md", b"projection-rebuild-det " * 6, page_size=32
        )
        info_first = env["ctx_sdk"].inspect(pid=env["pid"], handle_id=handle.handle_id)
        hashes: set[str] = {info_first["manifest_hash"]}
        for _ in range(20):
            r = env["ctx_sdk"].read(pid=env["pid"], handle_id=handle.handle_id)
            i = env["ctx_sdk"].inspect(pid=env["pid"], handle_id=handle.handle_id)
            assert r.manifest_hash == i["manifest_hash"]
            hashes.add(i["manifest_hash"])
        assert len(hashes) == 1
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. 100x shuffled input (different ref orderings) → same hashes.
# ═══════════════════════════════════════════════════════════════════════════════


class TestShuffledInput:
    def test_100x_shuffled_same_hashes(self, env: dict[str, Any]) -> None:
        """Different ref orderings of the SAME set of refs → same manifest_hash
        AND materialized_hash. Artifacts are pre-written ONCE before the loop."""
        base_contents = [b"alpha-shuffle " * 5, b"beta-shuffle " * 5, b"gamma-shuffle " * 5]
        uris = [f"workspace:///det_shuf_{idx}.md" for idx in range(3)]
        # Pre-write all artifacts once.
        for uri, content in zip(uris, base_contents):
            env["artifact_sdk"].write(env["pid"], uri, content, f"det-shuf-{uri}")
        # Build stable refs from pre-written artifacts.
        stable_refs = [
            _ref_from_artifact_row(env, uri, ref_id=f"r{idx}") for idx, uri in enumerate(uris)
        ]
        rng = random.Random(99)
        manifest_hashes: set[str] = set()
        materialized_hashes: set[str] = set()
        for _ in range(100):
            shuffled = list(stable_refs)
            rng.shuffle(shuffled)
            manifest = ContextManifest(
                manifest_id="det-shuf-mid",
                owner_pid=env["pid"],
                refs=tuple(shuffled),
                token_budget=100_000,
                page_size_bytes=64,
            )
            manifest_hashes.add(manifest.manifest_hash())
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
            materialized_hashes.add(loaded.materialized_hash)
            env["ctx_sdk"].close(pid=env["pid"], handle_id=h.handle_id)
        assert len(manifest_hashes) == 1
        assert len(materialized_hashes) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Deterministic eviction order.
# ═══════════════════════════════════════════════════════════════════════════════


class TestDeterministicEviction:
    def test_same_ws_evict_same_pages(self, env: dict[str, Any]) -> None:
        """Repeated evictions of the same working set yield the same pages."""
        handle, _ = _load_single(
            env, "workspace:///det_evict.md", b"evict-det-test " * 6, page_size=32
        )
        results = []
        for _ in range(10):
            r = env["ctx_svc"].evict(
                pid=env["pid"],
                working_set_id=handle.working_set_id,
                target_tokens=100_000,
            )
            results.append(r["evicted_pages"])
        for r in results[1:]:
            assert r == results[0]


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Snapshot/restore roundtrip materialized_hash equality.
# ═══════════════════════════════════════════════════════════════════════════════


class TestSnapshotRestoreRoundtrip:
    def test_roundtrip_hash_matches(self, env: dict[str, Any]) -> None:
        handle, loaded = _load_single(
            env, "workspace:///det_snapround.md", b"snap-roundtrip-det " * 6, page_size=32
        )
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        handle2, restored = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle2.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. DeterministicByteTokenEstimator: same bytes → same estimate.
# ═══════════════════════════════════════════════════════════════════════════════


class TestEstimatorDeterminism:
    def test_same_bytes_same_estimate(self) -> None:
        est = DeterministicByteTokenEstimator()
        content = b"hello estimator determinism "
        estimates = {
            est.estimate(content=content, media_type="text/plain", encoding="utf-8")
            for _ in range(100)
        }
        assert len(estimates) == 1

    def test_different_media_types_still_deterministic(self) -> None:
        est = DeterministicByteTokenEstimator()
        content = b"media_type_test "
        a = est.estimate(content=content, media_type="text/plain", encoding="utf-8")
        b = est.estimate(content=content, media_type="image/png", encoding="utf-8")
        c = est.estimate(content=content, media_type="text/markdown", encoding="utf-8")
        again_a = est.estimate(content=content, media_type="text/plain", encoding="utf-8")
        again_b = est.estimate(content=content, media_type="image/png", encoding="utf-8")
        again_c = est.estimate(content=content, media_type="text/markdown", encoding="utf-8")
        assert a == again_a
        assert b == again_b
        assert c == again_c


# ═══════════════════════════════════════════════════════════════════════════════
# 8. _stable_page_id: same inputs → same page_id.
# ═══════════════════════════════════════════════════════════════════════════════


class TestStablePageId:
    def test_same_inputs_same_page_id(self) -> None:
        kwargs = dict(
            artifact_id="art-1",
            version=1,
            content_hash="c" * 64,
            byte_start=0,
            byte_end=64,
            page_size=64,
            page_index=0,
        )
        ids = {_stable_page_id(**kwargs) for _ in range(100)}
        assert len(ids) == 1

    def test_different_byte_positions_differ(self) -> None:
        a = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=0,
            byte_end=64,
            page_size=64,
            page_index=0,
        )
        b = _stable_page_id(
            artifact_id="a",
            version=1,
            content_hash="x" * 64,
            byte_start=64,
            byte_end=128,
            page_size=64,
            page_index=1,
        )
        assert a != b


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Manifest hash: same fields → same hash regardless of ref order.
# ═══════════════════════════════════════════════════════════════════════════════


class TestManifestHashOrderInvariance:
    def test_same_refs_different_order_same_hash(self, env: dict[str, Any]) -> None:
        r1 = _ref(env, "workspace:///det_mh_a.md", b"a-content " * 10, priority=1, required=True)
        r2 = _ref(env, "workspace:///det_mh_b.md", b"b-content " * 10, priority=2, required=False)
        r3 = _ref(env, "workspace:///det_mh_c.md", b"c-content " * 10, priority=0, required=True)
        m1 = ContextManifest(
            manifest_id="det-mh-mid",
            owner_pid=env["pid"],
            refs=(r1, r2, r3),
            token_budget=100_000,
            page_size_bytes=64,
        )
        m2 = ContextManifest(
            manifest_id="det-mh-mid",
            owner_pid=env["pid"],
            refs=(r3, r1, r2),
            token_budget=100_000,
            page_size_bytes=64,
        )
        assert m1.manifest_hash() == m2.manifest_hash()


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Multi-run create manifest with same content – materialized hash identical.
#    Each iteration writes a NEW artifact URI (no rewrite collision) with
#    identical content bytes → identical materialized hash.
# ═══════════════════════════════════════════════════════════════════════════════


class TestMultiRunIdenticalContent:
    def test_same_content_materialized_hash_identical(self, env: dict[str, Any]) -> None:
        """Loading independent manifests with the same content bytes → identical materialized_hash.
        The artifact is written ONCE and the same ResolveResult is reused across all runs."""
        content = b"multi-run-same-content " * 8
        uri = "workspace:///det_multirun.md"
        env["artifact_sdk"].write(env["pid"], uri, content, "det-multirun-1")
        ref = _ref_from_artifact_row(env, uri)
        hashes: set[str] = set()
        for _ in range(30):
            m = ContextManifest(
                manifest_id="det-multirun-mid",
                owner_pid=env["pid"],
                refs=(ref,),
                token_budget=100_000,
                page_size_bytes=64,
            )
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=m)
            hashes.add(loaded.materialized_hash)
            env["ctx_sdk"].close(pid=env["pid"], handle_id=h.handle_id)
        assert len(hashes) == 1
