"""Tests for Context VM pin/unpin semantics: pinned pages survive eviction,
ref-counted pinning, sorted-unique return values, unknown page tolerance,
pin release on close, and cross-PID rejection."""

from __future__ import annotations

import pytest

from tests.agent_os.context.conftest import write_artifacts_and_build_manifest

from lhos.agent_os.context.errors import ErrHandleNotOwned


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_with_pages(
    env: dict,
    *,
    count: int = 2,
    content_per_ref: bytes = b"payload-" * 8,
    required: bool = False,
    page_size: int = 64,
) -> tuple:
    """Build a manifest with ``count`` optional refs and load it.
    Returns (handle, loaded)."""
    artifacts = [
        (f"artifact://ns-p1/doc{i}.md", content_per_ref, f"idem-{i}")
        for i in range(count)
    ]
    manifest = write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=artifacts,
        page_size_bytes=page_size,
        required_map={uri: required for uri, _, _ in artifacts},
        token_budget=100_000,
    )
    return env["ctx_sdk"].load(pid="p1", manifest=manifest)


def _page_ids(loaded) -> list[str]:
    return [p.page_id for p in loaded.ordered_pages]


# ── tests ────────────────────────────────────────────────────────────────────


class TestPinBasics:
    """Pin/unpin state on the handle."""

    def test_pin_adds_page_to_handle_pinned_page_ids(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        assert len(pages) >= 1
        pinned = env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        assert pages[0] in pinned

    def test_pinned_page_survives_eviction(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        assert len(pages) >= 2
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        result = env["ctx_svc"].evict(
            pid="p1",
            working_set_id=handle.working_set_id,
            target_tokens=100_000,
        )
        assert pages[0] not in result["evicted_pages"]

    def test_unpin_removes_page_from_pinned_page_ids(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        pinned = env["ctx_sdk"].unpin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        assert pages[0] not in pinned

    def test_pin_returns_sorted_unique_page_ids(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env, count=3)
        pages = _page_ids(loaded)
        # Pin in reverse order plus a duplicate.
        pinned = env["ctx_sdk"].pin(
            pid="p1",
            handle_id=handle.handle_id,
            page_ids=[pages[2], pages[0], pages[0], pages[1]],
        )
        assert pinned == sorted(set(pages))

    def test_pin_unknown_page_id_does_not_crash(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        # Pinning a non-existent page should be accepted without error.
        pinned = env["ctx_sdk"].pin(
            pid="p1",
            handle_id=handle.handle_id,
            page_ids=["totally-bogus-page-id", pages[0]],
        )
        assert pages[0] in pinned
        assert "totally-bogus-page-id" in pinned


class TestRefCountedPinning:
    """Ref-counted pin/unpin semantics tracked by ContextService._pin_counts."""

    def test_pin_twice_unpin_once_still_pinned(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        svc = env["ctx_svc"]
        # Pin twice on the same page.
        svc.pin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        svc.pin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        assert svc._pin_counts.get(pages[0], 0) == 2
        # Unpin once — count goes to 1, page still effectively pinned.
        svc.unpin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        assert svc._pin_counts.get(pages[0], 0) == 1

    def test_unpin_only_truly_unpins_when_count_reaches_zero(
        self, env: dict
    ) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        svc = env["ctx_svc"]
        svc.pin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        svc.unpin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        assert svc._pin_counts.get(pages[0], 0) == 0


class TestPinBlocksEviction:
    """Pinned pages must never be evicted regardless of target token pressure."""

    def test_pinned_page_cannot_be_evicted_under_any_target(
        self, env: dict
    ) -> None:
        handle, loaded = _load_with_pages(env, count=3)
        pages = _page_ids(loaded)
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        result = env["ctx_svc"].evict(
            pid="p1",
            working_set_id=handle.working_set_id,
            target_tokens=1_000_000,
        )
        assert pages[0] not in result["evicted_pages"]

    def test_refcounted_pin_blocks_eviction_after_single_unpin(
        self, env: dict
    ) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        # Unpin once — count is still 1, page should survive eviction.
        env["ctx_sdk"].unpin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        result = env["ctx_svc"].evict(
            pid="p1",
            working_set_id=handle.working_set_id,
            target_tokens=100_000,
        )
        assert pages[0] not in result["evicted_pages"]


class TestPinLifecycle:
    """Pin counts are released when the handle is closed."""

    def test_loading_pinning_closing_releases_pin_count(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        svc = env["ctx_svc"]
        svc.pin(pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]])
        assert svc._pin_counts.get(pages[0], 0) == 1
        env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        assert svc._pin_counts.get(pages[0], 0) == 0

    def test_closing_handle_lifts_pin_protection(self, env: dict) -> None:
        handle, loaded = _load_with_pages(env)
        pages = _page_ids(loaded)
        env["ctx_sdk"].pin(
            pid="p1", handle_id=handle.handle_id, page_ids=[pages[0]]
        )
        env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        # After closing, the page should be evictable.
        # Re-open a new handle so we have a working_set_id to evict from.
        # Since pins are on the handle, evicting the original WS would now
        # succeed on p0 (it has no live handle with pin count).
        result = env["ctx_svc"].evict(
            pid="p1",
            working_set_id=handle.working_set_id,
            target_tokens=100_000,
        )
        assert pages[0] in result["evicted_pages"]


class TestCrossPidPinning:
    """Handles are process-bound; cross-PID pin/unpin must fail."""

    def test_cross_pid_pin_raises_err_handle_not_owned(self, env: dict) -> None:
        handle, _loaded = _load_with_pages(env)
        pages = _page_ids(_loaded)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].pin(
                pid="p2",
                handle_id=handle.handle_id,
                page_ids=[pages[0]],
            )
