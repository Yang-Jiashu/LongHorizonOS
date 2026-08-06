"""Snapshot + restore lifecycle tests for Context VM.

Covers snapshot creation, immutability, integrity verification on restore,
cross-service restart round-trips, and deterministic replay.
"""

from __future__ import annotations

from typing import Any

import pytest

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.errors import ErrSnapshotCorrupt
from lhos.agent_os.context.models import (
    ContentRef,
    ContextManifest,
    PageBinding,
)
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from tests.agent_os.context.conftest import (
    _AllowsAllCaps,
    _ArtifactSupplier,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_one(env: dict[str, Any], content: bytes, page_size: int = 64):
    """Write one artifact, build a version-pinned manifest, and load it.

    Returns (handle, loaded).
    """
    pid = env["pid"]
    uri = f"workspace:///doc-{len(content)}.md"
    env["artifact_sdk"].write(pid, uri, content, f"idem-{uri}")
    ver = next(iter(env["artifact_sdk"].list_versions(pid, uri)))
    arts = env["artifact_svc"].list_artifacts(pid)
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    manifest = ContextManifest(
        owner_pid=pid,
        refs=(
            ContentRef(
                ref_id="doc",
                canonical_uri=art["canonical_uri"],
                artifact_id=ver["artifact_id"],
                version=ver["version"],
                content_hash=ver["content_hash"],
                media_type="text/markdown",
                priority=10,
                required=True,
            ),
        ),
        token_budget=100_000,
        page_size_bytes=page_size,
    )
    h, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
    return h, loaded


# ── tests ────────────────────────────────────────────────────────────────────


class TestSnapshotCreation:
    """Snapshot object structure and creation semantics."""

    def test_snapshot_has_unique_id_page_bindings_and_materialized_hash(self, env):
        """Snapshot must carry a unique id, non-empty page_bindings, and a
        materialized_hash matching the loaded context."""
        handle, loaded = _load_one(env, b"hello snapshot world\n" * 30, page_size=64)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        assert snap.snapshot_id
        assert isinstance(snap.snapshot_id, str)
        assert len(snap.snapshot_id) > 0
        assert len(snap.page_bindings) > 0
        assert snap.materialized_hash == loaded.materialized_hash

    def test_snapshot_page_bindings_include_required_fields(self, env):
        """Each PageBinding must include page_hash, byte_start, byte_end."""
        handle, loaded = _load_one(env, b"page field content here\n" * 10, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        for pb in snap.page_bindings:
            assert pb.page_hash, "page_hash must be non-empty"
            assert isinstance(pb.byte_start, int)
            assert isinstance(pb.byte_end, int)
            assert pb.byte_start < pb.byte_end

    def test_snapshot_materialized_hash_matches_loaded(self, env):
        """Snapshot.materialized_hash equals the loaded context's materialized_hash."""
        _, loaded = _load_one(env, b"deterministic materialized hash test\n")
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        assert snap.materialized_hash == loaded.materialized_hash

    def test_snapshot_preserves_version_bindings(self, env):
        """Snapshot's page_bindings must preserve the version + content_hash of
        each artifact version that was loaded."""
        pid = env["pid"]
        content = b"version preserved content\n"
        uri = "workspace:///snapver_preserve.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-snap-ver")
        ver = next(iter(env["artifact_sdk"].list_versions(pid, uri)))
        arts = env["artifact_svc"].list_artifacts(pid)
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="doc",
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
        _, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        snap = env["ctx_sdk"].snapshot(pid=pid, context_id=loaded.context_id)

        for pb in snap.page_bindings:
            assert pb.version == ver["version"]
            assert pb.content_hash == ver["content_hash"]
            assert pb.canonical_uri == art["canonical_uri"]
            assert pb.artifact_id == ver["artifact_id"]


class TestSnapshotRestore:
    """Restore semantics: content fidelity, hash equality, integrity."""

    def test_restore_returns_same_materialized_hash(self, env):
        """Restoring a snapshot must produce a LoadedContext whose
        materialized_hash matches the snapshot's."""
        _, loaded = _load_one(env, b"restore hash check content\n" * 5)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        handle2, restored = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == snap.materialized_hash
        assert restored.materialized_hash == loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle2.handle_id)

    def test_restore_content_bytes_match_original(self, env):
        """Every page in the restored context must have the same content bytes
        as the corresponding page in the original loaded context."""
        content = b"exact byte-for-byte content must survive snapshot restore\n" * 8
        _, loaded = _load_one(env, content, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        # Build page_id -> content map from original
        orig_pages = {p.page_id: p.content for p in loaded.ordered_pages}

        handle2, restored = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )

        for rp in restored.ordered_pages:
            assert rp.page_id in orig_pages
            assert rp.content == orig_pages[rp.page_id]

        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle2.handle_id)


class TestSnapshotImmutability:
    """Snapshot page_bindings tuple must not be mutable."""

    def test_snapshot_page_bindings_tuple_cannot_be_appended(self, env):
        """Attempting to mutate the page_bindings tuple should raise (frozen pydantic)."""
        _, loaded = _load_one(env, b"immutable bindings content\n")
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        # page_bindings is a tuple — appending is impossible
        with pytest.raises((TypeError, AttributeError)):
            snap.page_bindings.append(  # type: ignore[attr-defined]
                PageBinding(
                    page_id="x", canonical_uri="y", artifact_id="z",
                    version=1, content_hash="a", page_hash="b",
                    byte_start=0, byte_end=1,
                )
            )


class TestSnapshotHandleLifecycle:
    """Snapshot validity across handle close."""

    def test_snapshot_valid_after_handle_closed(self, env):
        """A snapshot created from a context must remain restorable even after
        the original handle is closed."""
        handle, loaded = _load_one(env, b"content for closed handle snapshot test\n" * 3)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        # Close the original handle
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)

        # Snapshot must still restore successfully
        handle2, restored = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle2.handle_id)


class TestSnapshotIntegrity:
    """Corruption detection on restore."""

    def test_restore_unknown_snapshot_id_raises_snapshot_corrupt(self, env):
        """Restoring with a snapshot_id that does not exist in _snaps must raise
        ErrSnapshotCorrupt."""
        with pytest.raises(ErrSnapshotCorrupt):
            env["ctx_sdk"].restore_snapshot(
                pid=env["pid"], snapshot_id="nonexistent-snapshot-id-0000"
            )

    def test_restore_corrupt_page_binding_hash_raises_snapshot_corrupt(self, env):
        """Tampering with a PageBinding content_hash must cause restore to fail
        with ErrSnapshotCorrupt."""
        _, loaded = _load_one(env, b"corrupt hash test content\n" * 4, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        # Tamper with the first page binding's content_hash
        cb = snap.page_bindings[0]
        tampered = cb.model_copy(update={"content_hash": "0" * 64})
        bad_snap = snap.model_copy(update={"page_bindings": (tampered,)})

        # Inject the bad snapshot into the service's snap store
        env["ctx_svc"]._snaps[bad_snap.snapshot_id] = bad_snap

        with pytest.raises(ErrSnapshotCorrupt):
            env["ctx_sdk"].restore_snapshot(
                pid=env["pid"], snapshot_id=bad_snap.snapshot_id
            )


class TestSnapshotServiceRestart:
    """Snapshot round-trip across a simulated service restart."""

    def test_snapshot_roundtrip_across_service_restart(self, env):
        """Simulate a service restart: new ContextService wired to the same
        ArtifactFS, snapshot re-injected, restore succeeds."""
        content = b"service restart survival content\n" * 6
        page_size = 32
        _, loaded = _load_one(env, content, page_size=page_size)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        # Simulate restart: brand-new ContextService pointing at the same
        # underlying ArtifactFSService (via _ArtifactSupplier adapter)
        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=env["pid"]),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_sdk = ContextSDK(new_svc)

        # Re-inject snapshot into the new service
        new_svc._snaps[snap.snapshot_id] = snap

        handle2, restored = new_sdk.restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == loaded.materialized_hash
        new_sdk.close(pid=env["pid"], handle_id=handle2.handle_id)

    def test_snapshot_roundtrip_deterministic(self, env):
        """Same snapshot always round-trips to the same materialized_hash and
        same content bytes — repeatability."""
        content = b"deterministic replay content\n" * 5
        _, loaded = _load_one(env, content, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=env["pid"]),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap

        # Restore three times; all must produce identical materialized_hash
        hashes = []
        for _ in range(3):
            h, r = new_sdk.restore_snapshot(
                pid=env["pid"], snapshot_id=snap.snapshot_id
            )
            hashes.append(r.materialized_hash)
            new_sdk.close(pid=env["pid"], handle_id=h.handle_id)

        assert all(h == loaded.materialized_hash for h in hashes)


class TestSnapshotIdentity:
    """Multiple snapshots of the same context produce distinct IDs."""

    def test_multiple_snapshots_of_same_context_have_distinct_ids(self, env):
        """Snapshotting the same loaded context twice must yield two different
        snapshot_ids."""
        _, loaded = _load_one(env, b"distinct snapshot ids\n")

        snap1 = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        snap2 = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)

        assert snap1.snapshot_id
        assert snap2.snapshot_id
        assert snap1.snapshot_id != snap2.snapshot_id
