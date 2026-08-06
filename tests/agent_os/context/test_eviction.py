"""Tests for Context VM eviction: token budget enforcement, pin/rule
protection, snapshot/restore after eviction, zero-token no-op, missing
working-set error, exceeded target, pinned-blocked reporting, and
deterministic ordering."""

from __future__ import annotations

import pytest

from tests.agent_os.context.conftest import write_artifacts_and_build_manifest

from lhos.agent_os.context.models import ContextManifest
from lhos.agent_os.context.errors import ErrInvalidManifest


# ── helpers ──────────────────────────────────────────────────────────────────


def _build_manifest(
    env: dict,
    *,
    artifacts: list[tuple[str, bytes, str]],
    required_map: dict[str, bool] | None = None,
    page_size: int = 64,
) -> ContextManifest:
    """Thin wrapper over the conftest helper with common defaults."""
    return write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=artifacts,
        page_size_bytes=page_size,
        required_map=required_map or {},
        token_budget=100_000,
    )


def _page_ids(loaded) -> list[str]:
    return [p.page_id for p in loaded.ordered_pages]


def _evict(env: dict, ws_id: str, target: int) -> dict:
    return env["ctx_svc"].evict(
        pid="p1", working_set_id=ws_id, target_tokens=target
    )


# ── tests ────────────────────────────────────────────────────────────────────


class TestEvictionWithAllPinned:
    """When every optional page is pinned, nothing can be evicted."""

    def test_evict_returns_empty_evicted_pages_if_all_pinned(
        self, env: dict
    ) -> None:
        artifacts = [
            ("artifact://ns-p1/a.md", b"A" * 64, "idem-a"),
            ("artifact://ns-p1/b.md", b"B" * 64, "idem-b"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/a.md": False,
                          "artifact://ns-p1/b.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        pages = _page_ids(loaded)
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=pages
        )
        result = _evict(env, handle.working_set_id, 100_000)
        assert result["evicted_pages"] == []
        assert result["tokens_freed"] == 0


class TestEvictionFreesTokens:
    """Eviction must report correct token counts from non-pinned pages."""

    def test_evict_frees_tokens_from_non_pinned_pages(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/c.md", b"C" * 64, "idem-c"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/c.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        result = _evict(env, handle.working_set_id, 10)
        assert result["tokens_freed"] > 0
        assert len(result["evicted_pages"]) >= 1

    def test_evict_zero_tokens_evicts_nothing(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/d.md", b"D" * 64, "idem-d"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/d.md": False},
        )
        handle, _loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        result = _evict(env, handle.working_set_id, 0)
        assert result["evicted_pages"] == []
        assert result["tokens_freed"] == 0


class TestRequiredPagesPreserved:
    """Pages marked required=True must never appear in eviction candidates."""

    def test_evict_preserves_required_pages(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/req.md", b"R" * 64, "idem-r"),
            ("artifact://ns-p1/opt.md", b"O" * 64, "idem-o"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/req.md": True,
                          "artifact://ns-p1/opt.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        page_map = {p.canonical_uri: p for p in loaded.ordered_pages}
        required_page = page_map["artifact://ns-p1/req.md"]
        result = _evict(env, handle.working_set_id, 100_000)
        assert required_page.page_id not in result["evicted_pages"]

    def test_evict_only_targets_required_false(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/r1.md", b"X" * 64, "idem-r1"),
            ("artifact://ns-p1/r2.md", b"Y" * 64, "idem-r2"),
            ("artifact://ns-p1/o1.md", b"Z" * 64, "idem-o1"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={
                "artifact://ns-p1/r1.md": True,
                "artifact://ns-p1/r2.md": True,
                "artifact://ns-p1/o1.md": False,
            },
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        optional_ids = {
            p.page_id
            for p in loaded.ordered_pages
            if not p.required
        }
        result = _evict(env, handle.working_set_id, 100_000)
        # Only optional pages were evicted.
        assert set(result["evicted_pages"]) <= optional_ids


class TestEvictionAndSnapshotRestore:
    """Eviction must not corrupt snapshot/restore integrity."""

    def test_snapshot_restore_works_after_eviction(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/e1.md", b"E" * 64, "idem-e1"),
            ("artifact://ns-p1/e2.md", b"F" * 64, "idem-e2"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/e1.md": False,
                          "artifact://ns-p1/e2.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        _evict(env, handle.working_set_id, 100_000)
        snap = env["ctx_sdk"].snapshot(
            pid="p1", context_id=loaded.context_id
        )
        assert snap.snapshot_id
        new_handle, new_loaded = env["ctx_sdk"].restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        assert new_handle.handle_id != handle.handle_id
        assert len(new_loaded.ordered_pages) == len(loaded.ordered_pages)


class TestEvictionErrors:
    """Invalid working-set lookup must raise."""

    def test_evict_on_nonexistent_working_set_raises(self, env: dict) -> None:
        with pytest.raises(ErrInvalidManifest):
            env["ctx_svc"].evict(
                pid="p1",
                working_set_id="totally-fake-ws-id",
                target_tokens=10,
            )


class TestPinDuringEviction:
    """Mixed pinned/optional pages: eviction must skip pinned pages."""

    def test_eviction_respects_pin(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/x.md", b"X" * 64, "idem-x"),
            ("artifact://ns-p1/y.md", b"Y" * 64, "idem-y"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/x.md": False,
                          "artifact://ns-p1/y.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        page_map = {p.canonical_uri: p for p in loaded.ordered_pages}
        pinned_page = page_map["artifact://ns-p1/x.md"].page_id
        other_page = page_map["artifact://ns-p1/y.md"].page_id
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pinned_page]
        )
        result = _evict(env, handle.working_set_id, 100_000)
        assert pinned_page not in result["evicted_pages"]
        assert other_page in result["evicted_pages"]


class TestEvictionTargetPressure:
    """Target tokens beyond available evicts everything evictable."""

    def test_target_exceeding_available_evicts_all_non_pinned(
        self, env: dict
    ) -> None:
        artifacts = [
            ("artifact://ns-p1/m1.md", b"A" * 64, "idem-m1"),
            ("artifact://ns-p1/m2.md", b"B" * 64, "idem-m2"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/m1.md": False,
                          "artifact://ns-p1/m2.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        pages = _page_ids(loaded)
        result = _evict(env, handle.working_set_id, 10_000_000)
        assert set(result["evicted_pages"]) == set(pages)


class TestPinnedBlockedReporting:
    """Eviction result must include the ``pinned_blocked`` field."""

    def test_pinned_pages_listed_separately_in_pinned_blocked(
        self, env: dict
    ) -> None:
        artifacts = [
            ("artifact://ns-p1/p.md", b"P" * 64, "idem-p"),
            ("artifact://ns-p1/q.md", b"Q" * 64, "idem-q"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={"artifact://ns-p1/p.md": False,
                          "artifact://ns-p1/q.md": False},
        )
        handle, loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        page_map = {p.canonical_uri: p for p in loaded.ordered_pages}
        pinned_page = page_map["artifact://ns-p1/p.md"].page_id
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pinned_page]
        )
        result = _evict(env, handle.working_set_id, 100_000)
        assert "pinned_blocked" in result
        # The pinned page is reported as blocked and is NOT in evicted_pages.
        assert pinned_page in result["pinned_blocked"]
        assert pinned_page not in result["evicted_pages"]


class TestEvictionDeterminism:
    """Same working set evicts the same pages across repeated runs."""

    def test_deterministic_eviction_order(self, env: dict) -> None:
        artifacts = [
            ("artifact://ns-p1/d1.md", b"M" * 64, "idem-d1"),
            ("artifact://ns-p1/d2.md", b"N" * 64, "idem-d2"),
            ("artifact://ns-p1/d3.md", b"O" * 64, "idem-d3"),
        ]
        manifest = _build_manifest(
            env, artifacts=artifacts,
            required_map={u: False for u, _, _ in artifacts},
        )
        handle, _loaded = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        first = _evict(env, handle.working_set_id, 100_000)
        second = _evict(env, handle.working_set_id, 100_000)
        assert first["evicted_pages"] == second["evicted_pages"]
        assert first["tokens_freed"] == second["tokens_freed"]
