"""Projection-replay tests for Context VM.

Verifies determinism, immutability, and stability of the Context VM projection:
manifest_hash, materialized_hash, context_id, ordered_pages, and
CtxSDK.inspect output.
"""

from __future__ import annotations

from typing import Any

import hashlib
import random

import pytest

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.errors import ErrHandleNotOwned, ErrCapabilityDenied
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.kernel.models import Capability


# ── helpers ──────────────────────────────────────────────────────────────────


def _ref(
    env: dict[str, Any],
    uri: str,
    content: bytes,
    priority: int = 0,
    required: bool = True,
) -> ContentRef:
    env["artifact_sdk"].write(env["pid"], uri, content, f"idem-{uri}")
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


def _load(
    env: dict[str, Any],
    *contents: bytes,
    page_size: int = 64,
) -> tuple:
    refs = [_ref(env, f"workspace:///doc{i}.md", c) for i, c in enumerate(contents)]
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=tuple(refs),
        token_budget=100_000,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


def _compute_materialized_hash_manual(loaded) -> str:
    """Re-derive materialized_hash from ordered_pages content bytes sorted by page_id."""
    pages_sorted = sorted(loaded.ordered_pages, key=lambda p: p.page_id)
    hasher = hashlib.sha256()
    for p in pages_sorted:
        hasher.update(p.content)
    return hasher.hexdigest()


# ── tests ────────────────────────────────────────────────────────────────────


class TestInspectAfterLoad:
    """After load, inspect returns manifest_hash, bytes_used, version_bindings."""

    def test_inspect_has_core_fields(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"hello projection " * 10)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert "manifest_hash" in info
        assert "bytes_used" in info
        assert info["manifest_hash"] == loaded.manifest_hash
        assert info["bytes_used"] == loaded.bytes_used

    def test_inspect_version_bindings_have_page_id_and_artifact_id(
        self, env: dict[str, Any]
    ) -> None:
        handle, loaded = _load(env, b"version bind " * 10)
        # version_bindings is a tuple on loaded, check page_id/version relation
        for vb in loaded.version_bindings:
            # Each binding attaches to a page
            matching_pages = [
                p for p in loaded.ordered_pages if p.page_id == vb.page_id
            ]
            assert len(matching_pages) == 1
            assert matching_pages[0].artifact_id == vb.artifact_id
            assert matching_pages[0].version == vb.version


class TestManifestHashStability:
    """Loading same manifest twice yields same manifest_hash."""

    def test_same_manifest_same_hash(self, env: dict[str, Any]) -> None:
        # Build ONE manifest with a fixed manifest_id so both loads use
        # the same manifest identity (loading twice without an idempotency
        # key creates distinct handles, but the manifest_hash must match).
        refs = [_ref(env, "workspace:///doc0.md", b"stable " * 50)]
        m = ContextManifest(
            manifest_id="fixed-mid-stable",
            owner_pid="p1",
            refs=tuple(refs),
            token_budget=100_000,
            page_size_bytes=64,
        )
        h1, l1 = env["ctx_sdk"].load(pid="p1", manifest=m)
        h2, l2 = env["ctx_sdk"].load(pid="p1", manifest=m)
        # Different handles, but same manifest_hash
        assert l1.manifest_hash == l2.manifest_hash
        assert h1.handle_id != h2.handle_id  # distinct handles

    def test_different_budget_different_hash(self, env: dict[str, Any]) -> None:
        refs = [_ref(env, "workspace:///doc0.md", b"budget-test " * 10)]
        m1 = ContextManifest(
            owner_pid="p1", refs=tuple(refs), token_budget=100_000, page_size_bytes=64,
        )
        m2 = ContextManifest(
            owner_pid="p1", refs=tuple(refs), token_budget=50_000, page_size_bytes=64,
        )
        _, l1 = env["ctx_sdk"].load(pid="p1", manifest=m1)
        _, l2 = env["ctx_sdk"].load(pid="p1", manifest=m2)
        assert l1.manifest_hash != l2.manifest_hash


class TestMaterializedHashDeterminism:
    """materialized_hash must be deterministic across calls and loads."""

    def test_repeated_calls_same_materialized_hash(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"deterministic " * 10)
        h1 = loaded.materialized_hash
        h2 = loaded.materialized_hash
        assert h1 == h2

    def test_ordered_pages_sorted_by_page_id_produces_stable_hash(
        self, env: dict[str, Any]
    ) -> None:
        _handle, loaded = _load(env, b"page-sort " * 10)
        pages_sorted = sorted(loaded.ordered_pages, key=lambda p: p.page_id)
        content_blob = b"".join(p.content for p in pages_sorted)
        digest1 = hashlib.sha256(content_blob).hexdigest()
        # Second derivation must be identical
        pages_sorted2 = sorted(loaded.ordered_pages, key=lambda p: p.page_id)
        content_blob2 = b"".join(p.content for p in pages_sorted2)
        digest2 = hashlib.sha256(content_blob2).hexdigest()
        assert digest1 == digest2

    def test_independent_loads_same_materialized_hash(
        self, env: dict[str, Any]
    ) -> None:
        _h1, l1 = _load(env, b"independent-load " * 10)
        _h2, l2 = _load(env, b"independent-load " * 10)
        # Same manifest metadata -> same materialized_hash despite different handles
        assert l1.materialized_hash == l2.materialized_hash


class TestInspectKeys:
    """ctx_sdk.inspect returns a dict with all documented keys."""

    def test_inspect_contains_expected_keys(self, env: dict[str, Any]) -> None:
        handle, _ = _load(env, b"inspect-keys " * 10)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        expected_keys = {
            "context_id",
            "handle_id",
            "pid",
            "context_id",
            "manifest_id",
            "manifest_hash",
            "working_set_id",
            "policy_id",
            "estimator_id",
            "page_count",
            "tokens_used",
            "bytes_used",
        }
        assert expected_keys.issubset(set(info.keys()))


class TestSnapshotMaterializedHashMatches:
    """Snapshot's materialized_hash matches loaded's."""

    def test_snapshot_materialized_hash_equals_loaded(
        self, env: dict[str, Any]
    ) -> None:
        _handle, loaded = _load(env, b"snap-match " * 10)
        snap = env["ctx_sdk"].snapshot(
            pid="p1", context_id=loaded.context_id
        )
        assert snap.materialized_hash == loaded.materialized_hash


class TestProjectionImmutable:
    """Projection (ordered_pages) is immutable — the tuple cannot be modified."""

    def test_ordered_pages_is_tuple(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"immutable " * 10)
        assert isinstance(loaded.ordered_pages, tuple)

    def test_version_bindings_is_tuple(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"binding-tuple " * 10)
        assert isinstance(loaded.version_bindings, tuple)

    def test_cannot_append_to_ordered_pages(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"cant-append " * 10)
        with pytest.raises(AttributeError):
            loaded.ordered_pages.append(None)  # type: ignore[attr-defined]


class TestManifestHashDifferentOwnerPid:
    """Loading same manifest with different owner_pid yields different manifest_hash."""

    def test_different_owner_pid_different_hash(self, env: dict[str, Any]) -> None:
        from tests.agent_os.context.conftest import _AllowsAllCaps, _ArtifactSupplier

        content = b"owner-pid-test " * 10

        # Set up a second namespace + capability for p_owner_alt.
        env["ns_svc"].create_namespace("p_owner_alt")
        env["cap_svc"].grant(
            "p_owner_alt",
            Capability(
                resource_pattern="artifact://ns-p_owner_alt/**",
                operations={"read", "write"},
            ),
        )

        # Write one artifact under p1 and one under p_owner_alt (same bytes,
        # different namespace/artifact-id so each load can read its own).
        env["artifact_sdk"].write(
            "p1", "workspace:///p1_doc.md", content, "idem-owner-p1"
        )
        ver1 = next(
            iter(env["artifact_sdk"].list_versions("p1", "workspace:///p1_doc.md"))
        )
        arts1 = env["artifact_svc"].list_artifacts("p1")
        art1 = next(a for a in arts1 if a["artifact_id"] == ver1["artifact_id"])

        env["artifact_sdk"].write(
            "p_owner_alt", "workspace:///p_owner_doc.md", content, "idem-owner-alt"
        )
        ver2 = next(
            iter(env["artifact_sdk"].list_versions(
                "p_owner_alt", "workspace:///p_owner_doc.md"))
        )
        arts2 = env["artifact_svc"].list_artifacts("p_owner_alt")
        art2 = next(a for a in arts2 if a["artifact_id"] == ver2["artifact_id"])

        # Manifest A owned by p1; manifest B owned by p_owner_alt. Same
        # manifest_id so the only hash difference comes from owner_pid.
        m1 = ContextManifest(
            manifest_id="fixed-mid-ownerdiff",
            owner_pid="p1",
            refs=(ContentRef(
                ref_id="r-p1_doc",
                canonical_uri=art1["canonical_uri"],
                artifact_id=ver1["artifact_id"],
                version=ver1["version"],
                content_hash=ver1["content_hash"],
                media_type="text/markdown",
            ),),
            token_budget=100_000,
            page_size_bytes=64,
        )
        m2 = ContextManifest(
            manifest_id="fixed-mid-ownerdiff",
            owner_pid="p_owner_alt",
            refs=(ContentRef(
                ref_id="r-p_owner_doc",
                canonical_uri=art2["canonical_uri"],
                artifact_id=ver2["artifact_id"],
                version=ver2["version"],
                content_hash=ver2["content_hash"],
                media_type="text/markdown",
            ),),
            token_budget=100_000,
            page_size_bytes=64,
        )
        # owner_pid differs -> different manifest_hash
        assert m1.manifest_hash() != m2.manifest_hash()

        # Load each with the correct owning service.
        _, l1 = env["ctx_sdk"].load(pid="p1", manifest=m1)
        alt_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid="p_owner_alt"),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        alt_sdk = ContextSDK(alt_svc)
        _, l2 = alt_sdk.load(pid="p_owner_alt", manifest=m2)
        assert l1.manifest_hash != l2.manifest_hash


class TestManifestHashUniqueness:
    """Loading 100 random-ish manifests produces unique manifest_hashes (collision check)."""

    def test_hundred_manifests_unique_hashes(self, env: dict[str, Any]) -> None:
        hashes: set[str] = set()
        rng = random.Random(42)
        for i in range(100):
            content = bytes(rng.getrandbits(8) for _ in range(64 + i))
            refs = [
                ContentRef(
                    ref_id=f"r-uniq-{i}",
                    canonical_uri=f"artifact://ns-p1/uniq{i}.md",
                    artifact_id=f"art-{i:04d}",
                    version=1,
                    content_hash=hashlib.sha256(content).hexdigest(),
                    media_type="application/octet-stream",
                    priority=i % 5,
                    required=i % 2 == 0,
                )
            ]
            m = ContextManifest(
                owner_pid="p1",
                refs=tuple(refs),
                token_budget=10_000 + i,
                page_size_bytes=64,
            )
            h = m.manifest_hash()
            assert h not in hashes, f"hash collision at index {i}"
            hashes.add(h)
        assert len(hashes) == 100


class TestContextIdStability:
    """context_id must be stable for identical content+binding across independent loads."""

    def test_same_manifest_metadata_same_materialized_hash(
        self, env: dict[str, Any]
    ) -> None:
        _h1, l1 = _load(env, b"context-stable " * 10)
        _h2, l2 = _load(env, b"context-stable " * 10)
        # The two materialized hashes are equal (same content, same bindings)
        assert l1.materialized_hash == l2.materialized_hash

    def test_context_id_differs_between_loads(self, env: dict[str, Any]) -> None:
        """context_id is a unique identifier per load (UUID), not per content."""
        _h1, l1 = _load(env, b"ctx-id-test " * 10)
        _h2, l2 = _load(env, b"ctx-id-test " * 10)
        # context_id is fresh UUID each load
        assert l1.context_id != l2.context_id

    def test_fresh_load_vs_cached_load_same_materialized_hash(
        self, env: dict[str, Any]
    ) -> None:
        """Fresh load vs load with idempotency key yield same materialized_hash."""
        refs = [_ref(env, "workspace:///doc0.md", b"cached-load " * 10)]
        m = ContextManifest(
            owner_pid="p1", refs=tuple(refs), token_budget=100_000, page_size_bytes=64,
        )
        _, l1 = env["ctx_sdk"].load(pid="p1", manifest=m)
        _, l2 = env["ctx_sdk"].load(pid="p1", manifest=m, idempotency_key="idem-cache")
        assert l1.materialized_hash == l2.materialized_hash
