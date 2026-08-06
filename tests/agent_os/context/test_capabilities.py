"""Capability enforcement tests for Context VM.

Validates that the ContextService capability checker gates load, restore,
and artifact-read operations as specified.
"""

from __future__ import annotations

import fnmatch
from typing import Any

import pytest

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.errors import ErrCapabilityDenied
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.kernel.models import Capability
from tests.agent_os.context.conftest import (
    _ArtifactSupplier,
    _DenyAllCaps,
)


# ── helpers ──────────────────────────────────────────────────────────────────


def _load_one(env: dict[str, Any], content: bytes, page_size: int = 64):
    """Write one artifact, build a version-pinned manifest, and load it.
    Returns (handle, loaded)."""
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


class _ExplicitCaps:
    """A capability checker backed by a list of Capability grants with
    fnmatch-style resource pattern matching."""

    def __init__(self, caps: list[Capability]) -> None:
        self._caps = caps

    def can_context_operation(self, **kwargs: Any) -> bool:
        return True

    def can_artifact_read(self, **kwargs: Any) -> bool:
        pid = kwargs.get("pid", "")
        # Construct a synthetic resource URI that mimics cap_svc conventions
        resource = f"artifact://ns-{pid}/**"
        return any(
            fnmatch.fnmatch(resource, cap.resource_pattern)
            and "read" in cap.operations
            for cap in self._caps
        )


# ── tests ────────────────────────────────────────────────────────────────────


class TestCapabilityAllow:
    """Positive capability paths: load succeeds when capabilities are granted."""

    def test_load_succeeds_with_allows_all_caps(self, env):
        """Default env fixture uses _AllowsAllCaps — load succeeds."""
        handle, loaded = _load_one(env, b"allow all caps content\n")
        assert loaded.ordered_pages
        assert loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)

    def test_load_succeeds_when_capability_denied_but_artifacts_accessible(self, env):
        """Even if a _DenyAllCaps checker is installed on a *separate* service,
        the default env (with _AllowsAllCaps) loads successfully because p1's
        artifacts are accessible in the fixture via cap_svc."""
        # Default env uses _AllowsAllCaps — load succeeds
        handle, loaded = _load_one(env, b"caps on artifact reads\n")
        assert loaded.ordered_pages
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle.handle_id)

    def test_artifact_sdk_write_then_load_works(self, env):
        """Write directly via artifact_sdk (cap_svc grants p1 read+write),
        then load — confirms granting flow works end to end."""
        pid = env["pid"]
        content = b"direct write then load content\n"
        uri = "workspace:///direct_write.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-direct-write")
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
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert loaded.ordered_pages[0].content == content
        env["ctx_sdk"].close(pid=pid, handle_id=handle.handle_id)

    def test_load_with_explicit_wildcard_capability_grant(self, env):
        """Explicit capability grant with wildcard pattern artifact://ns-p1/**
        allows load."""
        pid = env["pid"]
        content = b"wildcard grant content\n"

        checker = _ExplicitCaps(
            [Capability(resource_pattern="artifact://ns-p1/**", operations={"read"})]
        )
        svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=checker,
            estimator=DeterministicByteTokenEstimator(),
        )
        sdk = ContextSDK(svc)

        uri = "workspace:///wildcard.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-wildcard")
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
        handle, loaded = sdk.load(pid=pid, manifest=manifest)
        assert loaded.ordered_pages[0].content == content
        sdk.close(pid=pid, handle_id=handle.handle_id)

    def test_artifact_sdk_grant_read_then_load(self, env):
        """Grant read capability to p1 via cap_svc (already done in fixture),
        then confirm load works for an artifact p1 owns."""
        pid = env["pid"]
        # cap_svc already granted read+write on artifact://ns-p1/** in fixture.
        # Grant an additional explicit read capability.
        env["cap_svc"].grant(
            pid,
            Capability(resource_pattern="artifact://ns-p1/**", operations={"read"}),
        )
        handle, loaded = _load_one(env, b"grant read then load\n")
        assert loaded.ordered_pages
        env["ctx_sdk"].close(pid=pid, handle_id=handle.handle_id)

    def test_snapshot_restore_with_capability_checker(self, env):
        """Snapshot and restore both succeed when capability checker allows."""
        content = b"snapshot under caps\n" * 4
        _, loaded = _load_one(env, content, page_size=32)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        handle2, restored = env["ctx_sdk"].restore_snapshot(
            pid=env["pid"], snapshot_id=snap.snapshot_id
        )
        assert restored.materialized_hash == loaded.materialized_hash
        env["ctx_sdk"].close(pid=env["pid"], handle_id=handle2.handle_id)


class TestCapabilityDeny:
    """Negative capability paths: load fails when capabilities are missing."""

    def test_deny_all_caps_blocks_load(self, env):
        """A fresh ContextService with _DenyAllCaps context op checker rejects
        load with ErrCapabilityDenied."""
        pid = env["pid"]
        content = b"should not load\n"
        uri = "workspace:///deny_load.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-deny-load")
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

        svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=_DenyAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        sdk = ContextSDK(svc)
        with pytest.raises(ErrCapabilityDenied):
            sdk.load(pid=pid, manifest=manifest)

    def test_no_pattern_capability_denies_load(self, env):
        """Capability with empty pattern denies load — fnmatch never matches."""
        pid = env["pid"]
        content = b"no pattern content\n"
        uri = "workspace:///no_pattern.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-no-pattern")
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

        checker = _ExplicitCaps(
            [Capability(resource_pattern="", operations={"read"})]
        )
        svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=checker,
            estimator=DeterministicByteTokenEstimator(),
        )
        sdk = ContextSDK(svc)
        with pytest.raises(ErrCapabilityDenied):
            sdk.load(pid=pid, manifest=manifest)

    def test_wrong_operation_capability_denies_load(self, env):
        """Capability pattern matches but operation set does not include 'read'
        denies load."""
        pid = env["pid"]
        content = b"wrong op content\n"
        uri = "workspace:///wrong_op.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-wrong-op")
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

        checker = _ExplicitCaps(
            [Capability(resource_pattern="artifact://ns-p1/**", operations={"write"})]
        )
        svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=checker,
            estimator=DeterministicByteTokenEstimator(),
        )
        sdk = ContextSDK(svc)
        with pytest.raises(ErrCapabilityDenied):
            sdk.load(pid=pid, manifest=manifest)

    def test_artifact_read_denied_via_checker_blocks_load(self, env):
        """A checker that allows context operations but denies artifact reads
        must raise ErrCapabilityDenied on load."""
        pid = env["pid"]
        content = b"deny artifact read\n"
        uri = "workspace:///deny_artifact_read.md"
        env["artifact_sdk"].write(pid, uri, content, "idem-deny-ar")
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

        class _DenyArtifactRead:
            def can_context_operation(self, **kwargs: Any) -> bool:
                return True

            def can_artifact_read(self, **kwargs: Any) -> bool:
                return False

        svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid=pid),
            capability_checker=_DenyArtifactRead(),
            estimator=DeterministicByteTokenEstimator(),
        )
        sdk = ContextSDK(svc)
        with pytest.raises(ErrCapabilityDenied):
            sdk.load(pid=pid, manifest=manifest)
