"""End-to-end version-binding tests through ContextService.load()."""

from __future__ import annotations

from typing import Any

import pytest

from lhos.agent_os.context.errors import ErrInvalidContentHash
from lhos.agent_os.context.models import ContentRef, ContextManifest


def _write_versions(
    env: dict[str, Any],
    *,
    uri: str,
    contents: list[bytes],
) -> tuple[str, str, list[dict[str, Any]]]:
    """Write multiple versions of one artifact.

    Returns (canonical_uri, artifact_id, [{version, content_hash}, ...] sorted).
    """
    pid = env["pid"]
    for i, content in enumerate(contents):
        env["artifact_sdk"].write(pid, uri, content, f"vv-{uri}-{i}")
    rows = env["artifact_svc"].list_artifacts(pid)
    ws_name = uri.removeprefix("workspace:///")
    art_row = next(r for r in rows if r["canonical_uri"].endswith("/" + ws_name))
    versions = list(env["artifact_sdk"].list_versions(pid, uri))
    versions.sort(key=lambda v: v["version"])
    return art_row["canonical_uri"], art_row["artifact_id"], versions


class TestVersionBinding:
    """Version pinning: load pinned pages, integrity, snapshot/restore."""

    def test_load_pinned_to_v1_reads_v1_content(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///evolve.md",
            contents=[b"version-one\n", b"version-two-here\n"])
        v1 = versions[0]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v1["version"], content_hash=v1["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        _, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.ordered_pages[0].content == b"version-one\n"

    def test_load_pinned_to_v2_reads_v2_content(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///evolve2.md",
            contents=[b"v1\n", b"v2-content-data\n"])
        v2 = versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v2["version"], content_hash=v2["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        _, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.ordered_pages[0].content == b"v2-content-data\n"

    def test_version_pinning_integrity_across_writes(self, env):
        c, a, v1_versions = _write_versions(
            env=env, uri="workspace:///evolve3.md",
            contents=[b"original\n"])
        v1 = v1_versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v1["version"], content_hash=v1["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.ordered_pages[0].content == b"original\n"
        # write a third version
        env["artifact_sdk"].write(env["pid"], "workspace:///evolve3.md",
                                  b"later-version\n", "kv-ev3-later")
        # re-read original handle — should still serve v1
        reread = env["ctx_sdk"].read(pid=env["pid"], handle_id=h.handle_id)
        assert reread.ordered_pages[0].content == b"original\n"

    def test_version_bindings_reflect_pinned_version(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///evolve4.md",
            contents=[b"first-commit\n", b"second-commit\n", b"third-commit\n"])
        v3 = versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v3["version"], content_hash=v3["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        _, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert loaded.version_bindings[0].version == v3["version"]

    def test_different_versions_different_materialized_hash(self, env):
        pid = env["pid"]
        c, a, versions = _write_versions(
            env=env, uri="workspace:///vhash.md",
            contents=[b"AABB\n", b"XXYYZZ...\n"])
        v1 = versions[0]
        v2 = versions[-1]
        m1 = ContextManifest(
            owner_pid=pid,
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v1["version"], content_hash=v1["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        m2 = ContextManifest(
            owner_pid=pid,
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v2["version"], content_hash=v2["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        _, l1 = env["ctx_sdk"].load(pid=pid, manifest=m1)
        _, l2 = env["ctx_sdk"].load(pid=pid, manifest=m2)
        assert l1.materialized_hash != l2.materialized_hash

    def test_wrong_content_hash_raises(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///wh.md", contents=[b"good\n"])
        v = versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v["version"], content_hash="0" * 64,
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        with pytest.raises(ErrInvalidContentHash):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)

    def test_same_artifact_two_versions_in_one_manifest(self, env):
        pid = env["pid"]
        c, a, versions = _write_versions(
            env=env, uri="workspace:///mix.md",
            contents=[b"aaa-first\n", b"bbb-second-longer\n"])
        v1, v2 = versions[0], versions[-1]
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="as_v1", canonical_uri=c, artifact_id=a,
                    version=v1["version"], content_hash=v1["content_hash"],
                    media_type="text/plain", required=True,
                ),
                ContentRef(
                    ref_id="as_v2", canonical_uri=c, artifact_id=a,
                    version=v2["version"], content_hash=v2["content_hash"],
                    media_type="text/plain", required=True,
                ),
            ),
            token_budget=10_000, page_size_bytes=64,
        )
        _, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert len(loaded.ordered_pages) == 2
        versions_in_order = sorted(vb.version for vb in loaded.version_bindings)
        assert versions_in_order == [v1["version"], v2["version"]]
        by_version = {p.version: p for p in loaded.ordered_pages}
        assert by_version[v1["version"]].content == b"aaa-first\n"
        assert by_version[v2["version"]].content == b"bbb-second-longer\n"

    def test_snapshot_preserves_version_binding(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///snv.md",
            contents=[b"s1\n", b"s2-longer\n"])
        v2 = versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v2["version"], content_hash=v2["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        h, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"],
                                      context_id=loaded.context_id)
        assert any(b.version == v2["version"] for b in snap.page_bindings)

    def test_snapshot_materialized_hash_equals_original(self, env):
        c, a, versions = _write_versions(
            env=env, uri="workspace:///mhashcheck.md",
            contents=[b"materialized check content\n"])
        v = versions[-1]
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(ContentRef(
                ref_id="doc", canonical_uri=c, artifact_id=a,
                version=v["version"], content_hash=v["content_hash"],
                media_type="text/plain", required=True,
            ),),
            token_budget=10_000, page_size_bytes=64,
        )
        _, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        snap = env["ctx_sdk"].snapshot(pid=env["pid"],
                                      context_id=loaded.context_id)
        assert snap.materialized_hash == loaded.materialized_hash
