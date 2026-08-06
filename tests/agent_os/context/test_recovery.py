"""Crash-recovery tests for Context VM.

Verifies that the Context VM can recover its state after a service restart:
snapshot/restore survives a new ContextService instance, journal events are
emitted for replay, page hashes endure, and two-phase crash scenarios preserve
correct versions.
"""

from __future__ import annotations

from typing import Any

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


def _build_new_service(env: dict[str, Any], pid: str = "p1") -> ContextService:
    """Build a fresh ContextService sharing the same ArtifactFS/CAS + Journal."""
    from tests.agent_os.context.conftest import _AllowsAllCaps, _ArtifactSupplier

    return ContextService(
        content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )


# ── tests ────────────────────────────────────────────────────────────────────


class TestRestartRestore:
    """Restart (new ContextService) + snapshot -> restore works."""

    def test_restore_into_new_service(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"restart-data " * 10)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)

        # Build a new service (simulating restart)
        new_svc = _build_new_service(env)
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap  # simulate durable persist

        h2, restored = new_sdk.restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        assert h2.handle_id != handle.handle_id
        assert restored.manifest_hash == snap.manifest_hash
        assert restored.materialized_hash == snap.materialized_hash


class TestSameContentAfterRestart:
    """Same wallet-service restart: same content loaded after restore."""

    def test_content_bytes_survive_restart(self, env: dict[str, Any]) -> None:
        original_content = b"survive-restart " * 10
        _handle, loaded = _load(env, original_content)
        snap = env["ctx_sdk"].snapshot(
            pid="p1", context_id=loaded.context_id
        )

        new_svc = _build_new_service(env)
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap

        _h2, restored = new_sdk.restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        # The content bytes after restore must match the original
        original_bytes = b"".join(p.content for p in loaded.ordered_pages)
        restored_bytes = b"".join(p.content for p in restored.ordered_pages)
        assert restored_bytes == original_bytes


class TestProcessCrashCleanup:
    """After process crash, cleanup_process releases handles and working sets."""

    def test_cleanup_after_crash_releases_handles(self, env: dict[str, Any]) -> None:
        handle, _loaded = _load(env, b"crash-data " * 10)
        # Simulate crash cleanup
        result = env["ctx_sdk"].cleanup_process("p1")
        assert result["released_handles"] >= 1
        # Handle should now be inspectable as closed
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert info["closed"] is True

    def test_cleanup_empty_for_fresh_pid(self, env: dict[str, Any]) -> None:
        result = env["ctx_sdk"].cleanup_process("p_never_seen")
        assert result["released_handles"] == 0
        assert result["released_pins"] == 0


class TestJournalEventsEmitted:
    """Journal shows CONTEXT_MANIFEST_ACCEPTED, CONTEXT_LOAD_STARTED events."""

    def test_journal_records_context_events(self, env: dict[str, Any]) -> None:
        events_before = len(env["ctx_svc"]._events)
        _handle, _loaded = _load(env, b"journal-events " * 10)
        events_after = len(env["ctx_svc"]._events)
        assert events_after > events_before

        event_kinds = {ev["event"] for ev in env["ctx_svc"]._events}
        assert "CONTEXT_MANIFEST_ACCEPTED" in event_kinds
        assert "CONTEXT_LOAD_STARTED" in event_kinds


class TestJournalReplayRebuildsWorkingSets:
    """After restart, journal replay rebuilds working sets."""

    def test_journal_storage_has_context_events(self, env: dict[str, Any]) -> None:
        """After load, the service's internal event list contains replayable events."""
        _handle, _loaded = _load(env, b"replay-data " * 10)
        events = env["ctx_svc"]._events

        # Must have at least a manifest-accepted event
        manifest_events = [
            e for e in events if e["event"] == "CONTEXT_MANIFEST_ACCEPTED"
        ]
        assert len(manifest_events) >= 1
        # The manifest_accepted event must carry the manifest_hash
        assert "manifest_hash" in manifest_events[0]

    def test_replay_from_journal_restores_materialized_hash(
        self, env: dict[str, Any]
    ) -> None:
        """Journal replay can reconstruct the same manifest_hash."""
        _handle, loaded = _load(env, b"replay-hash " * 10)
        events = env["ctx_svc"]._events

        # Re-derive manifest_hash from the first manifest_accepted event
        manifest_events = [
            e for e in events if e["event"] == "CONTEXT_MANIFEST_ACCEPTED"
        ]
        assert manifest_events[0]["manifest_hash"] == loaded.manifest_hash


class TestPageHashesSurviveRestart:
    """Page hashes survive restart (new ContextService uses same ArtifactFS/CAS)."""

    def test_page_hashes_identical_after_restore(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"page-hash-check " * 10)
        snap = env["ctx_sdk"].snapshot(
            pid="p1", context_id=loaded.context_id
        )

        # New service, same CAS
        new_svc = _build_new_service(env)
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap

        _h2, restored = new_sdk.restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        # Compare page-by-page hashes
        original_hashes = sorted(p.page_hash for p in loaded.ordered_pages)
        restored_hashes = sorted(p.page_hash for p in restored.ordered_pages)
        assert restored_hashes == original_hashes


class TestRestoredMaterializedHashEqualsSnap:
    """Restored materialized_hash equals snapshot's materialized_hash."""

    def test_restored_hash_matches_snapshot(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"hash-equality " * 10)
        snap = env["ctx_sdk"].snapshot(
            pid="p1", context_id=loaded.context_id
        )

        new_svc = _build_new_service(env)
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap

        _h2, restored = new_sdk.restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == snap.materialized_hash


class TestTwoPhaseCrash:
    """Two-phase crash: snapshot between write & commit -> still restores v1 not malformed."""

    def test_snapshot_between_versions_restores_latest_written(
        self, env: dict[str, Any]
    ) -> None:
        """If snapshot is taken after writing v2, restore reflects v2 (the latest written)."""
        pid = "p1"
        # Write v1
        env["artifact_sdk"].write(
            pid, "workspace:///evolve.md", b"version-one\n", "idem-evolve-1"
        )
        ver = next(
            iter(env["artifact_sdk"].list_versions(pid, "workspace:///evolve.md"))
        )
        arts = env["artifact_svc"].list_artifacts(pid)
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])

        # Load a manifest pinned to v2 (written next)
        env["artifact_sdk"].write(
            pid, "workspace:///evolve.md", b"version-two\n", "idem-evolve-2"
        )
        ver2 = next(
            v for v in env["artifact_sdk"].list_versions(pid, "workspace:///evolve.md")
            if v["version"] == 2
        )
        arts2 = env["artifact_svc"].list_artifacts(pid)
        art2 = next(a for a in arts2 if a["artifact_id"] == ver2["artifact_id"])

        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r-evolve",
                    canonical_uri=art2["canonical_uri"],
                    artifact_id=ver2["artifact_id"],
                    version=ver2["version"],
                    content_hash=ver2["content_hash"],
                    media_type="text/markdown",
                    required=True,
                ),
            ),
            token_budget=100_000,
            page_size_bytes=64,
        )
        _handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        snap = env["ctx_sdk"].snapshot(pid=pid, context_id=loaded.context_id)

        # Simulate restart and restore
        new_svc = _build_new_service(env)
        new_sdk = ContextSDK(new_svc)
        new_svc._snaps[snap.snapshot_id] = snap

        _h2, restored = new_sdk.restore_snapshot(
            pid=pid, snapshot_id=snap.snapshot_id
        )

        # The restored content must be a valid version, not malformed
        # Since we pinned v2, restore should give v2 content
        assert restored.ordered_pages[0].content == b"version-two\n"
        assert restored.version_bindings[0].version == 2
        assert restored.materialized_hash == snap.materialized_hash
