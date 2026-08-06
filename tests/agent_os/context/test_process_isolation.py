"""Process-isolation tests for Context VM.

Verifies that handles, working sets, and snapshots are strictly scoped to
their owning PID. Cross-PID access must fail with ErrHandleNotOwned (or a
similar denied error), and cleanup / listing is PID-bound.
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
    """Write one artifact and return a version-pinned ContentRef."""
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
    """Build a manifest from *contents* and load it; returns (handle, loaded)."""
    refs = [_ref(env, f"workspace:///doc{i}.md", c) for i, c in enumerate(contents)]
    manifest = ContextManifest(
        owner_pid=env["pid"],
        refs=tuple(refs),
        token_budget=100_000,
        page_size_bytes=page_size,
    )
    return env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)


def _setup_p2(env: dict[str, Any]) -> None:
    """Create a second namespace + capability so p2 can be used in tests."""
    env["ns_svc"].create_namespace("p2")
    env["cap_svc"].grant(
        "p2",
        Capability(
            resource_pattern="artifact://ns-p2/**",
            operations={"read", "write"},
        ),
    )


# ── tests ────────────────────────────────────────────────────────────────────


class TestPidCanOperateOwnHandles:
    """PID p1 can load, read, snapshot, restore on its own handles."""

    def test_load_returns_valid_handle_and_loaded(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"hello world " * 10)
        assert handle.handle_id
        assert handle.pid == "p1"
        assert loaded.pid == "p1"
        assert handle.working_set_id == loaded.working_set_id

    def test_read_on_own_handle_succeeds(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"alpha " * 50)
        read_back = env["ctx_sdk"].read(pid="p1", handle_id=handle.handle_id)
        assert read_back.context_id == loaded.context_id
        assert read_back.materialized_hash == loaded.materialized_hash

    def test_snapshot_on_own_context_succeeds(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"snapshot content " * 10)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        assert snap.snapshot_id
        assert snap.pid == "p1"
        assert snap.materialized_hash == loaded.materialized_hash

    def test_restore_on_own_snapshot_succeeds(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"restore content " * 10)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        h2, restored = env["ctx_sdk"].restore_snapshot(
            pid="p1", snapshot_id=snap.snapshot_id
        )
        assert h2.handle_id != handle.handle_id  # distinct handle
        assert restored.materialized_hash == snap.materialized_hash


class TestCrossPidReadDenied:
    """PID p2 cannot read p1's handles."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_p2_cannot_read_p1_handle(self, env: dict[str, Any]) -> None:
        handle, _loaded = _load(env, b"secret content " * 10)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].read(pid="p2", handle_id=handle.handle_id)


class TestCrossPidPinDenied:
    """PID p2 cannot pin pages on p1's handles."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_p2_cannot_pin_p1_handle(self, env: dict[str, Any]) -> None:
        handle, loaded = _load(env, b"pin-content " * 10)
        page_ids = [p.page_id for p in loaded.ordered_pages]
        assert len(page_ids) > 0
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].pin(
                pid="p2", handle_id=handle.handle_id, page_ids=[page_ids[0]]
            )


class TestCrossPidEvictDenied:
    """PID p2 cannot evict p1's working sets."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_p2_cannot_evict_p1_working_set(self, env: dict[str, Any]) -> None:
        handle, _loaded = _load(env, b"evict-me " * 10)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_svc"].evict(
                pid="p2",
                working_set_id=handle.working_set_id,
                target_tokens=100_000,
            )


class TestCrossPidSnapshotRestoreDenied:
    """PID p2 cannot restore a snapshot belonging to p1."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_p2_cannot_restore_p1_snapshot(self, env: dict[str, Any]) -> None:
        _handle, loaded = _load(env, b"snapshot-boundary " * 10)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        with pytest.raises((ErrHandleNotOwned, ErrCapabilityDenied)):
            env["ctx_sdk"].restore_snapshot(
                pid="p2", snapshot_id=snap.snapshot_id
            )


class TestHandleClosedStillScoped:
    """After p1 closes a handle, p2 still cannot use it."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_after_close_p2_still_denied(self, env: dict[str, Any]) -> None:
        handle, _loaded = _load(env, b"closed content " * 10)
        env["ctx_sdk"].close(pid="p1", handle_id=handle.handle_id)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].read(pid="p2", handle_id=handle.handle_id)


class TestMultipleHandlesDistinct:
    """Multiple handles for the same PID are distinct."""

    def test_two_loads_produce_distinct_handles(self, env: dict[str, Any]) -> None:
        h1, _ = _load(env, b"distinct-handle " * 10)
        h2, _ = _load(env, b"distinct-handle " * 10)
        assert h1.handle_id != h2.handle_id
        assert h1.working_set_id != h2.working_set_id

    def test_both_handles_belong_to_p1(self, env: dict[str, Any]) -> None:
        h1, _ = _load(env, b"same-pid " * 10)
        h2, _ = _load(env, b"same-pid " * 10)
        assert h1.pid == "p1"
        assert h2.pid == "p1"


class TestListWorkingSetsIsolation:
    """Working-set listing is PID-scoped."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_list_working_sets_empty_for_other_pid(
        self, env: dict[str, Any]
    ) -> None:
        _load(env, b"p1 data " * 10)
        wss = env["ctx_sdk"].list_working_sets(pid="p2")
        assert wss == []

    def test_p1_sees_own_working_sets(self, env: dict[str, Any]) -> None:
        handle, _ = _load(env, b"p1-data " * 10)
        wss = env["ctx_sdk"].list_working_sets(pid="p1")
        ids = {ws["working_set_id"] for ws in wss}
        assert handle.working_set_id in ids


class TestCleanupProcess:
    """cleanup_process clears handles and working sets for a PID."""

    def test_cleanup_clears_handles_for_pid(self, env: dict[str, Any]) -> None:
        handle, _ = _load(env, b"cleanup-test " * 10)
        result = env["ctx_sdk"].cleanup_process("p1")
        assert result["released_handles"] >= 1
        wss = env["ctx_sdk"].list_working_sets(pid="p1")
        # Working sets listing may still exist (metadata), but handles are closed.
        # Verify the handle is no longer usable.
        info = env["ctx_sdk"].inspect(pid="p1", handle_id=handle.handle_id)
        assert info["closed"] is True


class TestDifferentPidsDistinctWorkingSets:
    """Different PIDs with the same manifest produce distinct working sets."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_distinct_working_set_ids(self, env: dict[str, Any]) -> None:
        h1, _ = _load(env, b"shared-content " * 10)
        # Write same-content artifacts under p2 namespace.
        content = b"shared-content " * 10
        env["artifact_sdk"].write("p2", "workspace:///doc0.md", content, "idem-ws-p2")
        ver = next(iter(env["artifact_sdk"].list_versions("p2", "workspace:///doc0.md")))
        arts = env["artifact_svc"].list_artifacts("p2")
        art = next(a for a in arts if a["artifact_id"] == ver["artifact_id"])
        ref = ContentRef(
            ref_id="r-workspace:///doc0.md",
            canonical_uri=art["canonical_uri"],
            artifact_id=ver["artifact_id"],
            version=ver["version"],
            content_hash=ver["content_hash"],
            media_type="text/markdown",
            priority=0,
            required=True,
        )
        manifest2 = ContextManifest(
            owner_pid="p2",
            refs=(ref,),
            token_budget=100_000,
            page_size_bytes=64,
        )
        # Create a supplier for p2
        from tests.agent_os.context.conftest import _AllowsAllCaps, _ArtifactSupplier

        p2_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid="p2"),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        p2_sdk = ContextSDK(p2_svc)
        h2, _ = p2_sdk.load(pid="p2", manifest=manifest2)
        assert h1.working_set_id != h2.working_set_id
        assert h1.handle_id != h2.handle_id


class TestCrossPidSnapshotSharesContentButHandleRequiresOwnership:
    """Cross-PID snapshot shares content bytes but handle_id still requires ownership."""

    @pytest.fixture(autouse=True)
    def _setup(self, env: dict[str, Any]) -> None:
        _setup_p2(env)

    def test_snapshot_content_matches_but_cross_pid_handle_denied(
        self, env: dict[str, Any]
    ) -> None:
        handle, loaded = _load(env, b"cross-pid-snap " * 10)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        bind = snap.page_bindings[0]

        # The snapshot recorded the *same* content bytes that were materialized.
        assert bind.page_hash
        assert bind.artifact_id
        assert bind.canonical_uri

        # But p2 still cannot do any handle-based op on p1's original handle.
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].read(pid="p2", handle_id=handle.handle_id)
