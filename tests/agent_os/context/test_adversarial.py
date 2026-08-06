"""Adversarial + edge-case tests for the Context VM.

A 500-seed Manifest corpus plus numbered adversarial scenarios covering
malformed content, budget boundaries, hash integrity, pinning invariants,
UTF-8/binary edge cases, and failure-mode recovery.
"""

from __future__ import annotations

import random
from typing import Any

import pytest

from lhos.agent_os.context.errors import (
    ErrCapabilityDenied,
    ErrInvalidContentHash,
    ErrInvalidRange,
    ErrRequiredBudgetExceeded,
    ErrSnapshotCorrupt,
)
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from tests.agent_os.context.conftest import (
    _AllowsAllCaps,
    _ArtifactSupplier,
    _DenyAllCaps,
)

# ── helpers ──────────────────────────────────────────────────────────────────


def _ref(
    env: dict[str, Any],
    uri: str,
    content: bytes,
    *,
    priority: int = 0,
    required: bool = True,
    required_map: dict[str, bool] | None = None,
    start_byte: int | None = None,
    end_byte: int | None = None,
    media_type: str = "text/markdown",
) -> ContentRef:
    """Write one artifact and return a version-pinned ContentRef."""
    env["artifact_sdk"].write(env["pid"], uri, content, f"adv-a-{uri}-{priority}-{required}")
    ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    is_required = required_map[uri] if required_map and uri in required_map else required
    return ContentRef(
        ref_id=f"r-{uri}",
        canonical_uri=art["canonical_uri"],
        artifact_id=ver["artifact_id"],
        version=ver["version"],
        content_hash=ver["content_hash"],
        media_type=media_type,
        priority=priority,
        required=is_required,
        start_byte=start_byte,
        end_byte=end_byte,
    )


def _load(env: dict[str, Any], *contents: bytes, page_size: int = 64) -> tuple:
    refs = [_ref(env, f"workspace:///adv_ws_{i}.md", c) for i, c in enumerate(contents)]
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=tuple(refs),
        token_budget=100_000,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


def _ref_with_hash_override(
    env: dict[str, Any],
    uri: str,
    content: bytes,
    fake_hash: str,
    *,
    priority: int = 0,
    required: bool = True,
    media_type: str = "text/markdown",
) -> ContentRef:
    """Write an artifact but override the content_hash (for hash-tampering tests)."""
    env["artifact_sdk"].write(env["pid"], uri, content, f"adv-fake-{uri}")
    ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    return ContentRef(
        ref_id=f"r-{uri}",
        canonical_uri=art["canonical_uri"],
        artifact_id=ver["artifact_id"],
        version=ver["version"],
        content_hash=fake_hash,  # deliberate mismatch
        media_type=media_type,
        priority=priority,
        required=required,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Manifest Corpus (500 seeds)
# ═══════════════════════════════════════════════════════════════════════════════


class TestManifestCorpus:
    """500-seed parametrized Manifest corpus."""

    @staticmethod
    def _build_and_load(env: dict[str, Any], seed: int) -> tuple:
        rng = random.Random(seed)
        n_refs = rng.randint(1, 4)
        refs: list[ContentRef] = []
        for idx in range(n_refs):
            content_size = rng.randint(0, 128)
            content = bytes(rng.getrandbits(8) for _ in range(content_size))
            uri = f"workspace:///adv_corpus_{seed}_{idx}.md"
            env["artifact_sdk"].write(env["pid"], uri, content, f"adv-corpus-{seed}-{idx}")
            ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
            arts = env["artifact_svc"].list_artifacts(env["pid"])
            art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
            priority = rng.randint(0, 10)
            required = rng.choice([True, False])
            refs.append(
                ContentRef(
                    ref_id=f"r-corpus-{seed}-{idx}",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    priority=priority,
                    required=required,
                )
            )
        page_size = rng.choice([16, 32, 64, 128])
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=tuple(refs),
            token_budget=100_000,
            page_size_bytes=page_size,
        )
        try:
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
            return (h, loaded, None)
        except Exception as exc:
            return (None, None, exc)

    @staticmethod
    def _build_bad_hash_and_load(env: dict[str, Any], seed: int) -> tuple:
        """For seeds 200-299: build a manifest where exactly ONE ref has a
        wrong content_hash; expect ErrInvalidContentHash."""
        rng = random.Random(seed)
        n_refs = rng.randint(2, 4)
        refs: list[ContentRef] = []
        bad_ref_idx = rng.randint(0, n_refs - 1)
        for idx in range(n_refs):
            content_size = rng.randint(0, 128)
            content = bytes(rng.getrandbits(8) for _ in range(content_size))
            uri = f"workspace:///adv_badhash_{seed}_{idx}.md"
            env["artifact_sdk"].write(env["pid"], uri, content, f"adv-bh-{seed}-{idx}")
            ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
            arts = env["artifact_svc"].list_artifacts(env["pid"])
            art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
            # Tamper the hash on the designated ref.
            chash = "f" * 64 if idx == bad_ref_idx else ver["content_hash"]
            refs.append(
                ContentRef(
                    ref_id=f"r-bh-{seed}-{idx}",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=chash,
                    media_type="text/markdown",
                    priority=0,
                    required=True,
                )
            )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=tuple(refs),
            token_budget=100_000,
            page_size_bytes=64,
        )
        try:
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
            return (h, loaded, None)
        except Exception as exc:
            return (None, None, exc)

    @staticmethod
    def _build_large_and_load(env: dict[str, Any], seed: int) -> tuple:
        """For seeds 400-499: large artifacts (up to 4KB pages × 3 pages)."""
        rng = random.Random(seed)
        n_refs = rng.randint(1, 3)
        refs: list[ContentRef] = []
        page_size = 4096
        for idx in range(n_refs):
            content_size = rng.randint(0, 3 * page_size)
            content = bytes(rng.getrandbits(8) for _ in range(content_size))
            uri = f"workspace:///adv_large_{seed}_{idx}.md"
            env["artifact_sdk"].write(env["pid"], uri, content, f"adv-lg-{seed}-{idx}")
            ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
            arts = env["artifact_svc"].list_artifacts(env["pid"])
            art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
            refs.append(
                ContentRef(
                    ref_id=f"r-lg-{seed}-{idx}",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    priority=0,
                    required=True,
                )
            )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=tuple(refs),
            token_budget=1_000_000,
            page_size_bytes=page_size,
        )
        try:
            h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
            return (h, loaded, None)
        except Exception as exc:
            return (None, None, exc)

    @pytest.mark.parametrize("seed", range(500))
    def test_manifest_corpus_seed(self, env: dict[str, Any], seed: int) -> None:
        from lhos.agent_os.context.errors import ContextVMError

        if seed < 200:
            h, loaded, err = self._build_and_load(env, seed)
        elif seed < 300:
            h, loaded, err = self._build_bad_hash_and_load(env, seed)
        elif seed < 400:
            h, loaded, err = self._build_and_load(env, seed)
        else:
            h, loaded, err = self._build_large_and_load(env, seed)

        if seed >= 200 and seed < 300:
            assert err is not None, f"seed {seed}: expected ErrInvalidContentHash, got success"
            assert isinstance(err, ErrInvalidContentHash), (
                f"seed {seed}: expected ErrInvalidContentHash, got {type(err).__name__}: {err}"
            )
        else:
            if err is not None:
                assert isinstance(err, ContextVMError), (
                    f"seed {seed}: unexpected error {type(err).__name__}: {err}"
                )
            else:
                assert loaded is not None
                assert loaded.materialized_hash, f"seed {seed}: materialized_hash empty"
                assert len(loaded.materialized_hash) == 64


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Negative token estimate edge (NUL bytes)
# ═══════════════════════════════════════════════════════════════════════════════


class TestNegativeTokenEdge:
    def test_nul_byte_content_succeeds(self, env: dict[str, Any]) -> None:
        """Content containing NUL bytes should load without any negative estimate."""
        content = b"\x00" * 64 + b"a" * 32 + b"\x00" * 32
        handle, loaded = _load(env, content)
        assert loaded.materialized_hash
        for p in loaded.ordered_pages:
            assert p.estimated_tokens >= 0
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Huge estimate (token_budget=1)
# ═══════════════════════════════════════════════════════════════════════════════


class TestHugeEstimate:
    def test_token_budget_one_required_fails(self, env: dict[str, Any]) -> None:
        """With token_budget=1 and a non-empty required ref, load must fail."""
        env["artifact_sdk"].write(
            env["pid"], "workspace:///adv_big_est.md", b"some content here", "adv-big-est"
        )
        ver = next(
            iter(env["artifact_sdk"].list_versions(env["pid"], "workspace:///adv_big_est.md"))
        )
        arts = env["artifact_svc"].list_artifacts(env["pid"])
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id="r-big",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=1,
            page_size_bytes=64,
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Duplicate ref_id
# ═══════════════════════════════════════════════════════════════════════════════


class TestDuplicateRefId:
    def test_duplicate_ref_id_detected(self, env: dict[str, Any]) -> None:
        """Two ContentRef with the same ref_id must raise ErrDuplicateRefId."""
        content_a = b"alpha content"
        content_b = b"beta content"
        env["artifact_sdk"].write(env["pid"], "workspace:///adv_dup_a.md", content_a, "adv-dup-a")
        env["artifact_sdk"].write(env["pid"], "workspace:///adv_dup_b.md", content_b, "adv-dup-b")
        ver_a = next(
            iter(env["artifact_sdk"].list_versions(env["pid"], "workspace:///adv_dup_a.md"))
        )
        arts = env["artifact_svc"].list_artifacts(env["pid"])
        art_a = next(a for a in arts if a["artifact_id"] == ver_a["artifact_id"])
        ver_b = next(
            iter(env["artifact_sdk"].list_versions(env["pid"], "workspace:///adv_dup_b.md"))
        )
        art_b = next(a for a in arts if a["artifact_id"] == ver_b["artifact_id"])
        shared_id = "r-shared-duplicate"
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id=shared_id,
                    canonical_uri=art_a["canonical_uri"],
                    artifact_id=ver_a["artifact_id"],
                    version=ver_a["version"],
                    content_hash=ver_a["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
                ContentRef(
                    ref_id=shared_id,
                    canonical_uri=art_b["canonical_uri"],
                    artifact_id=ver_b["artifact_id"],
                    version=ver_b["version"],
                    content_hash=ver_b["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=100_000,
            page_size_bytes=64,
        )
        from lhos.agent_os.context.errors import ErrDuplicateRefId

        with pytest.raises(ErrDuplicateRefId):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Wrong hash
# ═══════════════════════════════════════════════════════════════════════════════


class TestWrongHash:
    def test_bad_content_hash_raises(self, env: dict[str, Any]) -> None:
        """A ContentRef whose content_hash does not match the artifact must fail."""
        ref = _ref_with_hash_override(
            env, "workspace:///adv_wrong.md", b"genuine content", "0" * 64
        )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        with pytest.raises(ErrInvalidContentHash):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Range overbound
# ═══════════════════════════════════════════════════════════════════════════════


class TestRangeOverbound:
    def test_start_byte_exceeds_content_raises(self, env: dict[str, Any]) -> None:
        """start_byte > content length must raise ErrInvalidRange."""
        content = b"hello"
        ref = _ref(
            env, "workspace:///adv_range.md", content, required=True, start_byte=999, end_byte=1000
        )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        with pytest.raises(ErrInvalidRange):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. UTF-8 truncation boundary
# ═══════════════════════════════════════════════════════════════════════════════


class TestUtf8TruncationBoundary:
    def test_3byte_utf8_split_across_page_works(self, env: dict[str, Any]) -> None:
        """A 3-byte UTF-8 char split across a page boundary: loading succeeds."""
        content = b"a" * 63 + "é".encode() + b"b" * 63
        handle, loaded = _load(env, content, page_size=64)
        assert loaded.materialized_hash
        assert len(loaded.ordered_pages) >= 2
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. Binary artifact
# ═══════════════════════════════════════════════════════════════════════════════


class TestBinaryArtifact:
    def test_image_png_media_type_succeeds(self, env: dict[str, Any]) -> None:
        """media_type=image/png with random bytes loads successfully."""
        rng = random.Random(12345)
        content = bytes(rng.getrandbits(8) for _ in range(200))
        ref = _ref(env, "workspace:///adv_binary.md", content, media_type="image/png")
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 9. Empty artifact
# ═══════════════════════════════════════════════════════════════════════════════


class TestEmptyArtifact:
    def test_empty_content_single_zero_page(self, env: dict[str, Any]) -> None:
        """Empty content should produce one empty page with size_bytes == 0."""
        handle, loaded = _load(env, b"")
        assert loaded.materialized_hash
        assert len(loaded.ordered_pages) == 1
        assert loaded.ordered_pages[0].size_bytes == 0
        assert loaded.ordered_pages[0].content == b""
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. Zero optional pages (all required)
# ═══════════════════════════════════════════════════════════════════════════════


class TestZeroOptionalPages:
    def test_all_required_refs_load(self, env: dict[str, Any]) -> None:
        """Manifest with only required refs and high budget: all load."""
        c1 = b"one " * 20
        c2 = b"two " * 20
        ref1 = _ref(env, "workspace:///adv_req1.md", c1, required=True, priority=5)
        ref2 = _ref(env, "workspace:///adv_req2.md", c2, required=True, priority=3)
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref1, ref2),
            token_budget=100_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.materialized_hash
        assert len(loaded.omitted_refs) == 0
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 11. All required, low budget
# ═══════════════════════════════════════════════════════════════════════════════


class TestAllRequiredLowBudget:
    def test_all_required_low_budget_raises(self, env: dict[str, Any]) -> None:
        """All required refs with token_budget=1: ErrRequiredBudgetExceeded."""
        env["artifact_sdk"].write(env["pid"], "workspace:///adv_lowb.md", b"x" * 256, "adv-lowb")
        ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], "workspace:///adv_lowb.md")))
        arts = env["artifact_svc"].list_artifacts(env["pid"])
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id="r-low",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=1,
            page_size_bytes=64,
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 12. Multi-handle pin
# ═══════════════════════════════════════════════════════════════════════════════


class TestMultiHandlePin:
    def test_pins_on_two_handles_no_cross_interference(self, env: dict[str, Any]) -> None:
        """Pin a page on handle A; pinning on handle B must not affect A's pins."""
        ref_a = _ref(env, "workspace:///adv_mh_a.md", b"A" * 64, required=True)
        ref_b = _ref(env, "workspace:///adv_mh_b.md", b"B" * 64, required=True)
        m_a = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref_a,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        m_b = ContextManifest(
            owner_pid=env["pid"],
            refs=(ref_b,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        h_a, l_a = env["ctx_sdk"].load(pid=env["pid"], manifest=m_a)
        h_b, l_b = env["ctx_sdk"].load(pid=env["pid"], manifest=m_b)
        pid_a = l_a.ordered_pages[0].page_id
        pid_b = l_b.ordered_pages[0].page_id
        env["ctx_sdk"].pin(pid=env["pid"], handle_id=h_a.handle_id, page_ids=[pid_a])
        env["ctx_sdk"].pin(pid=env["pid"], handle_id=h_b.handle_id, page_ids=[pid_b])
        assert env["ctx_svc"]._pin_counts.get(pid_a, 0) >= 1
        assert env["ctx_svc"]._pin_counts.get(pid_b, 0) >= 1
        env["ctx_sdk"].close(pid=env["pid"], handle_id=h_a.handle_id)
        env["ctx_sdk"].close(pid=env["pid"], handle_id=h_b.handle_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 13. Unreachable eviction target
# ═══════════════════════════════════════════════════════════════════════════════


class TestUnreachableEvictionTarget:
    def test_pin_all_pages_evict_nothing(self, env: dict[str, Any]) -> None:
        """Pin all pages of a working set; evict with huge target returns empty."""
        handle, loaded = _load(env, b"O" * 128, page_size=64)
        pages = [p.page_id for p in loaded.ordered_pages]
        env["ctx_sdk"].pin(pid=env["pid"], handle_id=handle.handle_id, page_ids=pages)
        result = env["ctx_svc"].evict(
            pid=env["pid"],
            working_set_id=handle.working_set_id,
            target_tokens=10_000_000,
        )
        assert result["evicted_pages"] == []
        assert result["tokens_freed"] == 0


# ═══════════════════════════════════════════════════════════════════════════════
# 14. Corrupt snapshot blob
# ═══════════════════════════════════════════════════════════════════════════════


class TestCorruptSnapshotBlob:
    def test_mutated_page_binding_hash_detected(self, env: dict[str, Any]) -> None:
        """Snapshot, mutate first page_binding's content_hash, restore -> ErrSnapshotCorrupt."""
        handle, loaded = _load(env, b"corrupt-snap-adv " * 5, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        cb = snap.page_bindings[0]
        tampered = cb.model_copy(update={"content_hash": "a" * 64})
        bad_snap = snap.model_copy(update={"page_bindings": (tampered,)})
        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=env["pid"]),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[bad_snap.snapshot_id] = bad_snap
        with pytest.raises(ErrSnapshotCorrupt):
            new_sdk.restore_snapshot(pid=env["pid"], snapshot_id=bad_snap.snapshot_id)


# ═══════════════════════════════════════════════════════════════════════════════
# 15. Manifest owner_pid mismatch
# ═══════════════════════════════════════════════════════════════════════════════


class TestManifestOwnerPidMismatch:
    def test_owner_pid_mismatch_vs_caller_denied(self, env: dict[str, Any]) -> None:
        """Manifest with owner_pid != caller pid fails capability check."""
        env["artifact_sdk"].write(
            env["pid"], "workspace:///adv_owner.md", b"owner-mismatch-content", "adv-owner"
        )
        ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], "workspace:///adv_owner.md")))
        arts = env["artifact_svc"].list_artifacts(env["pid"])
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
        manifest = ContextManifest(
            owner_pid="p_invalid",
            refs=(
                ContentRef(
                    ref_id="r-owner",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=100_000,
            page_size_bytes=64,
        )
        with pytest.raises(ErrCapabilityDenied):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 16. Capability revoked after load — second load into denied service fails.
# ═══════════════════════════════════════════════════════════════════════════════


class TestCapabilityRevokedAfterLoad:
    def test_second_load_into_deny_service_raises(self, env: dict[str, Any]) -> None:
        """After a successful load, a second load using a service whose
        capabilities are denied must raise ErrCapabilityDenied."""
        pid = env["pid"]
        # First load works with the default allow-all env.
        handle, loaded = _load(env, b"cap-revoke-adv-A " * 5, page_size=32)
        assert loaded.materialized_hash
        env["ctx_sdk"].close(pid=pid, handle_id=handle.handle_id)

        # Build a fresh artifact for the second load attempt.
        env["artifact_sdk"].write(
            pid, "workspace:///adv_revoke2.md", b"cap-revoke-adv-B " * 5, "adv-revoke-B"
        )
        ver = next(iter(env["artifact_sdk"].list_versions(pid, "workspace:///adv_revoke2.md")))
        arts = env["artifact_svc"].list_artifacts(pid)
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])

        # Build denied-caps service and try to load — should fail with ErrCapabilityDenied.
        deny_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=_DenyAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        deny_sdk = ContextSDK(deny_svc)
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r-revoke",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=ver["artifact_id"],
                    version=ver["version"],
                    content_hash=ver["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=100_000,
            page_size_bytes=64,
        )
        with pytest.raises(ErrCapabilityDenied):
            deny_sdk.load(pid=pid, manifest=manifest)


# ═══════════════════════════════════════════════════════════════════════════════
# 17. Process crash + re-pin after cleanup
# ═══════════════════════════════════════════════════════════════════════════════


class TestProcessCrashRePinAfterCleanup:
    def test_pin_then_cleanup_makes_handle_dead(self, env: dict[str, Any]) -> None:
        """Load, pin a page, cleanup_process -> handle closed -> handle unusable."""
        handle, loaded = _load(env, b"crash-pin-adv " * 5, page_size=64)
        pages = [p.page_id for p in loaded.ordered_pages]
        env["ctx_sdk"].pin(pid=env["pid"], handle_id=handle.handle_id, page_ids=pages[:1])
        result = env["ctx_sdk"].cleanup_process(env["pid"])
        assert result["released_handles"] >= 1
        info = env["ctx_sdk"].inspect(pid=env["pid"], handle_id=handle.handle_id)
        assert info["closed"] is True
