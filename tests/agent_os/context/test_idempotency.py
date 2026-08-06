"""Idempotency tests for the Context VM.

Covers idempotent replay on load, snapshot, restore, close, pin, unpin,
and eviction. These tests assert that repeated operations with the same
idempotency key (or no-op repetition) return stable identifiers and do not
raise or produce divergent state.
"""

from __future__ import annotations

import pytest

from tests.agent_os.context.conftest import write_artifacts_and_build_manifest

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.errors import ErrIdempotentReplay, ErrHandleNotOwned
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator


# ── helper ──────────────────────────────────────────────────────────────────


def _mk(env, content, page_size=64, byte_budget=None, token_budget=100_000):
    uri = f"workspace:///x-{hash(content) % 10000}.md"
    env["artifact_sdk"].write(env["pid"], uri, content, f"i-{uri}-{len(content)}")
    ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    m = ContextManifest(
        owner_pid=env["pid"],
        refs=(ContentRef(
            ref_id="r", canonical_uri=art["canonical_uri"],
            artifact_id=ver["artifact_id"], version=ver["version"],
            content_hash=ver["content_hash"], media_type="text/markdown",
            priority=5, required=True,
        ),),
        token_budget=token_budget, byte_budget=byte_budget,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=m)


def _build_manifest(env, content, *, page_size=64, token_budget=100_000,
                    owner_pid="p1", required=True):
    """Write one artifact and build a version-pinned ContextManifest."""
    uri = f"workspace:///x-{hash(content) % 10000}.md"
    env["artifact_sdk"].write(env["pid"], uri, content, f"i-{uri}-{len(content)}")
    ver = next(iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
    arts = env["artifact_svc"].list_artifacts(env["pid"])
    art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
    return ContextManifest(
        owner_pid=owner_pid,
        refs=(ContentRef(
            ref_id="r", canonical_uri=art["canonical_uri"],
            artifact_id=ver["artifact_id"], version=ver["version"],
            content_hash=ver["content_hash"], media_type="text/markdown",
            priority=5, required=required,
        ),),
        token_budget=token_budget,
        page_size_bytes=page_size,
    )


# ── load idempotency ─────────────────────────────────────────────────────────


class TestLoadIdempotency:
    """load() with an idempotency_key must be a stable replay."""

    def test_same_pid_manifest_idem_returns_same_handle(self, env) -> None:
        """Calling load() with same pid+manifest+idem_key twice returns the
        same handle_id (idempotent replay)."""
        m = _build_manifest(env, b"idempotent load payload A")
        h1, _ = env["ctx_sdk"].load(
            pid=env["pid"], manifest=m, idempotency_key="abc")
        h2, _ = env["ctx_sdk"].load(
            pid=env["pid"], manifest=m, idempotency_key="abc")
        assert h1.handle_id == h2.handle_id

    def test_different_idem_key_produces_different_handle(self, env) -> None:
        """Different idempotency_key produces a different handle_id."""
        m = _build_manifest(env, b"idempotent load payload B")
        h1, _ = env["ctx_sdk"].load(
            pid=env["pid"], manifest=m, idempotency_key="key-one")
        h2, _ = env["ctx_sdk"].load(
            pid=env["pid"], manifest=m, idempotency_key="key-two")
        assert h1.handle_id != h2.handle_id

    def test_different_pid_for_same_manifest_idem_produces_different_handle(
        self, env
    ) -> None:
        """Loading with the same idem key but different caller pid yields
        distinct handles because the idempotency cache keys on (pid, ...)."""
        content = b"idempotent cross-pid payload"
        uri = f"workspace:///x-{hash(content) % 10000}.md"
        env["artifact_sdk"].write(
            env["pid"], uri, content, f"i-{uri}-{len(content)}")
        ver = next(
            iter(env["artifact_sdk"].list_versions(env["pid"], uri)))
        arts = env["artifact_svc"].list_artifacts(env["pid"])
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])

        ref_kwargs = dict(
            ref_id="r", canonical_uri=art["canonical_uri"],
            artifact_id=ver["artifact_id"], version=ver["version"],
            content_hash=ver["content_hash"], media_type="text/markdown",
            priority=5, required=True,
        )
        m_p1 = ContextManifest(
            owner_pid="p1", refs=(ContentRef(**ref_kwargs),),
            token_budget=100_000, page_size_bytes=64,
        )
        m_p2 = ContextManifest(
            owner_pid="p2", refs=(ContentRef(**ref_kwargs),),
            token_budget=100_000, page_size_bytes=64,
        )
        h1, _ = env["ctx_sdk"].load(
            pid="p1", manifest=m_p1, idempotency_key="shared")
        h2, _ = env["ctx_sdk"].load(
            pid="p2", manifest=m_p2, idempotency_key="shared")
        assert h1.handle_id != h2.handle_id


# ── snapshot idempotency ─────────────────────────────────────────────────────


class TestSnapshotIdempotency:
    """snapshot() must be a stable replay keyed by (pid, context_id, idem)."""

    def test_snapshot_same_pid_context_idem_returns_same_id(
        self, env
    ) -> None:
        """Snapshot is idempotent: same pid+snapshot_key on the same working
        set produces the same snapshot_id."""
        _h, loaded = _mk(env, b"snapshot payload for idem test")
        s1 = env["ctx_sdk"].snapshot(
            pid=env["pid"], context_id=loaded.context_id,
            idempotency_key="snap-x")
        s2 = env["ctx_sdk"].snapshot(
            pid=env["pid"], context_id=loaded.context_id,
            idempotency_key="snap-x")
        assert s1.snapshot_id == s2.snapshot_id

    def test_restore_same_pid_idem_returns_same_handle_id(
        self, env
    ) -> None:
        """Restoring a snapshot is idempotent: same pid+idem returns the same
        restored handle_id."""
        _h, loaded = _mk(env, b"snapshot restore idem payload")
        snap = env["ctx_sdk"].snapshot(
            pid=env["pid"], context_id=loaded.context_id,
            idempotency_key="snap-y")
        r1h, _ = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id,
            idempotency_key="rid-1")
        r2h, _ = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id,
            idempotency_key="rid-1")
        assert r1h.handle_id == r2h.handle_id

    def test_snapshot_after_content_change_produces_different_id(
        self, env
    ) -> None:
        """Idempotent snapshot after a content change (different manifest/
        context) produces a different snapshot_id even with the same idem key,
        because the idempotency cache keys on (pid, context_id, idem)."""
        h1, l1 = _mk(env, b"idempotent snapshot content v1")
        snap1 = env["ctx_sdk"].snapshot(
            pid=env["pid"], context_id=l1.context_id,
            idempotency_key="idem-z")
        h2, l2 = _mk(env, b"idempotent snapshot content v2-beta")
        snap2 = env["ctx_sdk"].snapshot(
            pid=env["pid"], context_id=l2.context_id,
            idempotency_key="idem-z")
        assert snap1.snapshot_id != snap2.snapshot_id


# ── close idempotency ────────────────────────────────────────────────────────


class TestCloseIdempotency:
    """close() must be a safe, idempotent no-op on repetition."""

    def test_second_close_succeeds(self, env) -> None:
        """Closing a handle is idempotent: the second close succeeds
        (returns True) without raising."""
        handle, _ = _mk(env, b"close idempotency payload")
        first = env["ctx_sdk"].close(
            pid=env["pid"], handle_id=handle.handle_id)
        assert first is True
        second = env["ctx_sdk"].close(
            pid=env["pid"], handle_id=handle.handle_id)
        assert second is True


# ── pin / unpin idempotency ─────────────────────────────────────────────────


class TestPinUnpinIdempotency:
    """pin/unpin must be safe under repetition."""

    def test_pin_same_page_twice_still_pinned_once(self, env) -> None:
        """Pin is idempotent: pinning the same page twice still results in
        the page being pinned exactly once (no duplicates)."""
        handle, loaded = _mk(env, b"pin idempotency content " * 4)
        pages = [p.page_id for p in loaded.ordered_pages]
        assert len(pages) >= 1
        first_pin = env["ctx_sdk"].pin(
            pid=env["pid"], handle_id=handle.handle_id,
            page_ids=[pages[0]])
        assert pages[0] in first_pin
        second_pin = env["ctx_sdk"].pin(
            pid=env["pid"], handle_id=handle.handle_id,
            page_ids=[pages[0]])
        assert pages[0] in second_pin
        # Must not be duplicated.
        assert second_pin.count(pages[0]) == 1
        # All unique.
        assert len(set(second_pin)) == len(second_pin)

    def test_unpin_non_pinned_page_does_not_error(self, env) -> None:
        """Unpin is idempotent: un-pinning a page that is not pinned does
        not raise and simply leaves state unchanged."""
        handle, loaded = _mk(env, b"unpin idempotency content " * 4)
        pages = [p.page_id for p in loaded.ordered_pages]
        assert len(pages) >= 1
        result = env["ctx_sdk"].unpin(
            pid=env["pid"], handle_id=handle.handle_id,
            page_ids=[pages[0]])
        # The never-pinned page must not appear as pinned.
        assert pages[0] not in result


# ── eviction idempotency ─────────────────────────────────────────────────────


class TestEvictionIdempotency:
    """evict() must be safe when called repeatedly."""

    def test_evict_already_evicted_ws_is_noop_or_empty(self, env) -> None:
        """Eviction on a working set whose pages are all required (and thus
        cannot be evicted) must not crash; repeated evicts are no-ops with
        empty evicted_pages."""
        handle, _loaded = _mk(
            env, b"evict idempotency content " * 2, page_size=64)
        first = env["ctx_svc"].evict(
            pid=env["pid"],
            working_set_id=handle.working_set_id,
            target_tokens=100_000,
        )
        assert first.get("evicted_pages") is not None
        assert first["evicted_pages"] == []
        second = env["ctx_svc"].evict(
            pid=env["pid"],
            working_set_id=handle.working_set_id,
            target_tokens=100_000,
        )
        assert second.get("evicted_pages") is not None
        assert second["evicted_pages"] == []
