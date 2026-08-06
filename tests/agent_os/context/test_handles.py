"""Tests for Context VM handle lifecycle: load, read, inspect, close,
working sets, process-scoping, and double-close idempotency."""

from __future__ import annotations

import pytest

from lhos.agent_os.context.errors import (
    ErrHandleClosed,
    ErrHandleNotOwned,
)
from tests.agent_os.context.conftest import write_artifacts_and_build_manifest

# ── helpers ──────────────────────────────────────────────────────────────────


def _load_single_ref(
    env: dict,
    *,
    uri: str = "artifact://ns-p1/doc1.md",
    content: bytes = b"hello world " * 100,
    page_size: int = 64,
) -> tuple:
    """Build a single-ref manifest and load it; returns (handle, loaded)."""
    manifest = write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=[(uri, content, f"idem-{uri}")],
        page_size_bytes=page_size,
        token_budget=100_000,
    )
    return env["ctx_sdk"].load(pid="p1", manifest=manifest)


# ── tests ────────────────────────────────────────────────────────────────────


class TestLoadAndRead:
    """Handle creation and basic read-back."""

    def test_load_returns_handle_with_non_empty_id(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        assert handle.handle_id
        assert isinstance(handle.handle_id, str)
        assert len(handle.handle_id) > 0

    def test_read_returns_context_matching_load(self, env: dict) -> None:
        handle, loaded = _load_single_ref(env)
        ctx = env["ctx_sdk"].read(pid="p1", handle_id=handle.handle_id)
        assert ctx.context_id == loaded.context_id
        assert ctx.manifest_id == loaded.manifest_id
        assert ctx.manifest_hash == loaded.manifest_hash
        assert ctx.working_set_id == loaded.working_set_id
        assert len(ctx.ordered_pages) == len(loaded.ordered_pages)
        for a, b in zip(ctx.ordered_pages, loaded.ordered_pages):
            assert a.page_id == b.page_id
            assert a.content == b.content
            assert a.page_hash == b.page_hash

    def test_inspect_returns_expected_keys(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        expected = {
            "context_id",
            "handle_id",
            "pid",
            "working_set_id",
            "manifest_id",
            "manifest_hash",
            "policy_id",
            "estimator_id",
            "page_count",
            "tokens_used",
            "token_budget",
            "bytes_used",
            "byte_budget",
            "omitted_count",
            "closed",
        }
        assert set(info.keys()) == expected

    def test_inspect_handle_id_matches(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert info["handle_id"] == handle.handle_id
        assert info["pid"] == "p1"

    def test_inspect_closed_flag_is_false_when_open(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert info["closed"] is False


class TestCloseLifecycle:
    """Close semantics and double-close idempotency."""

    def test_close_returns_true(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        result = env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        assert result is True

    def test_read_from_closed_handle_raises_err_handle_closed(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        with pytest.raises(ErrHandleClosed):
            env["ctx_sdk"].read(pid="p1", handle_id=handle.handle_id)

    def test_double_close_is_idempotent(self, env: dict) -> None:
        """Closing an already-closed handle does not raise and returns True."""
        handle, _loaded = _load_single_ref(env)
        first = env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        assert first is True
        # Second close on the same handle should be a no-op that still
        # returns True (idempotent).
        second = env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        assert second is True

    def test_inspect_after_close_shows_closed(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert info["closed"] is True


class TestWorkingSets:
    """Working-set listing and process isolation of the WS index."""

    def test_list_working_sets_at_least_one_after_load(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        wss = env["ctx_sdk"].list_working_sets(pid="p1")
        assert len(wss) >= 1
        ids = {ws["working_set_id"] for ws in wss}
        assert handle.working_set_id in ids

    def test_list_working_sets_reflects_state(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        wss = env["ctx_sdk"].list_working_sets(pid="p1")
        ws = next(w for w in wss if w["working_set_id"] == handle.working_set_id)
        assert ws["pid"] == "p1"
        assert ws["manifest_id"] == _loaded.manifest_id
        assert ws["selected_pages"] >= 1

    def test_list_working_sets_empty_for_fresh_pid(self, env: dict) -> None:
        """A PID that has never loaded anything has no working sets."""
        wss = env["ctx_sdk"].list_working_sets(pid="p999_fresh")
        assert wss == []


class TestProcessScoping:
    """Handles are bound to one PID; cross-PID access must fail."""

    def test_cross_pid_cannot_use_other_handles_read(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].read(pid="p2", handle_id=handle.handle_id)

    def test_cross_pid_cannot_use_other_handles_inspect(self, env: dict) -> None:
        handle, _loaded = _load_single_ref(env)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].inspect(pid="p2", handle_id=handle.handle_id)


class TestHandleIdentity:
    """Loading the same manifest twice yields distinct handles (no idem key)."""

    def test_same_manifest_two_loads_produce_different_handles(self, env: dict) -> None:
        manifest = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[
                ("artifact://ns-p1/doc1.md", b"alpha " * 50, "idem-a"),
            ],
            page_size_bytes=64,
            token_budget=100_000,
        )
        h1, _ = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        h2, _ = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        assert h1.handle_id != h2.handle_id

    def test_two_handles_share_pid_and_ws_but_are_distinct(self, env: dict) -> None:
        manifest = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[
                ("artifact://ns-p1/doc_same.md", b"same " * 50, "idem-s"),
            ],
            page_size_bytes=64,
            token_budget=100_000,
        )
        h1, _ = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        h2, _ = env["ctx_sdk"].load(pid="p1", manifest=manifest)
        # Both belong to p1
        assert h1.pid == "p1"
        assert h2.pid == "p1"
        # But are different handles
        assert h1.handle_id != h2.handle_id
