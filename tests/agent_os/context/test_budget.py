"""End-to-end budget enforcement tests through ContextService.load()."""

from __future__ import annotations

from typing import Any

import pytest

from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.errors import (
    ErrRequiredBudgetExceeded,
    ErrCapabilityDenied,
    ErrInvalidContentHash,
)


def _mk(
    env: dict[str, Any],
    *,
    uri: str,
    content: bytes,
    idem_key: str,
    media_type: str = "text/plain",
) -> tuple[str, str, int, str]:
    """Write an artifact and return (canonical_uri, artifact_id, version, content_hash)."""
    pid = env["pid"]
    env["artifact_sdk"].write(pid, uri, content, idem_key)

    # Discover artifact_id via list_artifacts (match on uri suffix).
    ws_name = uri.removeprefix("workspace:///")
    rows = env["artifact_svc"].list_artifacts(pid)
    art_row = next(
        r for r in rows
        if r["canonical_uri"] and r["canonical_uri"].endswith("/" + ws_name)
    )

    # Discover version + content_hash via list_versions.
    versions = env["artifact_sdk"].list_versions(pid, uri)
    vrow = next(iter(versions))

    return (art_row["canonical_uri"], art_row["artifact_id"], vrow["version"], vrow["content_hash"])


class TestBudget:
    """Token and byte budget enforcement across required/optional refs."""

    def test_single_required_fully_loaded(self, env):
        """Single required ref fully loaded returns one LoadedContext."""
        canonical_uri, art_id, version, content_hash = _mk(
            env=env, uri="workspace:///single.md", content=b"hello world\n",
            idem_key="k-single",
        )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=canonical_uri,
                    artifact_id=art_id, version=version,
                    content_hash=content_hash, media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)
        assert handle is not None
        assert loaded is not None
        assert len(loaded.ordered_pages) == 1
        assert loaded.ordered_pages[0].content == b"hello world\n"
        assert loaded.bytes_used == len(b"hello world\n")
        assert len(loaded.omitted_refs) == 0

    def test_token_budget_too_low_for_required(self, env):
        """token_budget too low for required raises ErrRequiredBudgetExceeded."""
        canonical_uri, art_id, version, content_hash = _mk(
            env=env, uri="workspace:///big_doc.md", content=b"x" * 512,
            idem_key="k-toklow",
        )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=canonical_uri,
                    artifact_id=art_id, version=version,
                    content_hash=content_hash, media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=1,  # way too low even for a single page
            page_size_bytes=64,
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)

    def test_byte_budget_too_low_for_required(self, env):
        """byte_budget too low for required raises ErrRequiredBudgetExceeded."""
        canonical_uri, art_id, version, content_hash = _mk(
            env=env, uri="workspace:///wide_doc.md", content=b"y" * 512,
            idem_key="k-bytelow",
        )
        manifest = ContextManifest(
            owner_pid=env["pid"],
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=canonical_uri,
                    artifact_id=art_id, version=version,
                    content_hash=content_hash, media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            byte_budget=10,  # way too low even for a single 64-byte page
            page_size_bytes=64,
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=env["pid"], manifest=manifest)

    def test_optional_under_budget_loaded_second_omitted(self, env):
        """Optional ref under budget loaded; 2nd optional that would blow budget is omitted."""
        pid = env["pid"]
        c1, a1, v1, h1 = _mk(
            env=env, uri="workspace:///opt_small.md", content=b"a" * 64,
            idem_key="k-smopt",
        )
        c2, a2, v2, h2 = _mk(
            env=env, uri="workspace:///opt_large.md", content=b"b" * 512,
            idem_key="k-lgopt",
        )
        # opt_small: 64 bytes / 64-page -> 1 page, 64 chars / 4 = 16 tokens
        # opt_large: 512 bytes / 64-page -> 8 pages, 128 tokens total
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c1, artifact_id=a1,
                    version=v1, content_hash=h1, media_type="text/plain",
                    required=True),
                ContentRef(
                    ref_id="r2", canonical_uri=c2, artifact_id=a2,
                    version=v2, content_hash=h2, media_type="text/plain",
                    required=False),
            ),
            token_budget=100,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert any(p.canonical_uri == c1 for p in loaded.ordered_pages)
        omitted_ids = [o.ref_id for o in loaded.omitted_refs]
        assert "r2" in omitted_ids

    def test_optional_over_budget_omitted_required_loads(self, env):
        """Optional ref over budget is omitted while required still loads."""
        pid = env["pid"]
        cr, ar, vr, hr = _mk(
            env=env, uri="workspace:///req_tiny.md", content=b"r" * 32,
            idem_key="k-req-only",
        )
        co, ao, vo, ho = _mk(
            env=env, uri="workspace:///opt_huge.md", content=b"o" * 1024,
            idem_key="k-opt-huge",
        )
        # required: 32 bytes / 64-page -> 1 page, 8 tokens
        # optional: 1024 bytes / 64-page -> 16 pages, 256 tokens
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="req", canonical_uri=cr, artifact_id=ar,
                    version=vr, content_hash=hr, media_type="text/plain",
                    required=True),
                ContentRef(
                    ref_id="opt", canonical_uri=co, artifact_id=ao,
                    version=vo, content_hash=ho, media_type="text/plain",
                    required=False),
            ),
            token_budget=50,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert any(p.canonical_uri == cr for p in loaded.ordered_pages)
        omitted_ids = [o.ref_id for o in loaded.omitted_refs]
        assert "opt" in omitted_ids

    def test_bytes_used_equals_sum_of_loaded_page_sizes(self, env):
        """bytes_used equals sum of loaded page sizes."""
        pid = env["pid"]
        content = b"A" * 128
        c, a, v, h = _mk(
            env=env, uri="workspace:///exact128.md", content=content,
            idem_key="k-bytesum",
        )
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c, artifact_id=a,
                    version=v, content_hash=h, media_type="text/plain",
                    required=True),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        expected = sum(p.size_bytes for p in loaded.ordered_pages)
        assert loaded.bytes_used == expected == 128

    def test_tokens_used_equals_sum_of_loaded_page_estimates(self, env):
        """tokens_used equals sum of loaded page estimated_tokens."""
        pid = env["pid"]
        content = b"B" * 128
        c, a, v, h = _mk(
            env=env, uri="workspace:///tok128.md", content=content,
            idem_key="k-toksum",
        )
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c, artifact_id=a,
                    version=v, content_hash=h, media_type="text/plain",
                    required=True),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        expected = sum(p.estimated_tokens for p in loaded.ordered_pages)
        assert loaded.tokens_used == expected
        # DeterministicByteTokenEstimator for text/plain ceil(char_count/4):
        # 128 chars / 4 = 32 tokens
        assert loaded.tokens_used == 32

    def test_tiny_page_size_creates_multiple_pages(self, env):
        """Loading with tiny page_size_bytes creates multiple pages for same artifact."""
        pid = env["pid"]
        content = b"Z" * 64
        c, a, v, h = _mk(
            env=env, uri="workspace:///paged64.md", content=content,
            idem_key="k-multpage",
        )
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c, artifact_id=a,
                    version=v, content_hash=h, media_type="text/plain",
                    required=True),
            ),
            token_budget=10_000,
            page_size_bytes=16,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert len(loaded.ordered_pages) == 4
        for p in loaded.ordered_pages:
            assert p.size_bytes == 16
            assert p.content == b"Z" * 16

    def test_mixed_required_optional_only_required_fits(self, env):
        """Mixed required/optional with only required fitting -> required loaded, optional omitted."""
        pid = env["pid"]
        cr, ar, vr, hr = _mk(
            env=env, uri="workspace:///req_mix.md", content=b"R" * 64,
            idem_key="k-req-mix",
        )
        co, ao, vo, ho = _mk(
            env=env, uri="workspace:///opt_mix.md", content=b"O" * 512,
            idem_key="k-opt-mix",
        )
        # required: 64 bytes -> 1 page, 16 tokens
        # optional: 512 bytes -> 8 pages, 128 tokens
        # budget 50: required fits (16), optional would be 16+128=144 > 50
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="req", canonical_uri=cr, artifact_id=ar,
                    version=vr, content_hash=hr, media_type="text/plain",
                    required=True),
                ContentRef(
                    ref_id="opt", canonical_uri=co, artifact_id=ao,
                    version=vo, content_hash=ho, media_type="text/plain",
                    required=False),
            ),
            token_budget=50,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        loaded_uris = [p.canonical_uri for p in loaded.ordered_pages]
        assert cr in loaded_uris
        assert co not in loaded_uris
        omitted_ids = [o.ref_id for o in loaded.omitted_refs]
        assert "opt" in omitted_ids

    def test_optional_just_fits_budget_boundary_is_deterministic(self, env):
        """100% Manifest where optional just fits budget boundary is deterministic."""
        pid = env["pid"]
        cr, ar, vr, hr = _mk(
            env=env, uri="workspace:///req_boundary.md", content=b"R" * 32,
            idem_key="k-bnd-req",
        )
        co, ao, vo, ho = _mk(
            env=env, uri="workspace:///opt_boundary.md", content=b"O" * 64,
            idem_key="k-bnd-opt",
        )
        # required: 32 bytes -> 8 tokens; optional: 64 bytes -> 16 tokens
        # budget 24 = exactly fits both
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="req", canonical_uri=cr, artifact_id=ar,
                    version=vr, content_hash=hr, media_type="text/plain",
                    required=True),
                ContentRef(
                    ref_id="opt", canonical_uri=co, artifact_id=ao,
                    version=vo, content_hash=ho, media_type="text/plain",
                    required=False),
            ),
            token_budget=24,
            page_size_bytes=64,
        )
        # First load
        h1, l1 = env["ctx_sdk"].load(pid=pid, manifest=manifest, idempotency_key="idem-bnd")
        # Second load with same manifest_hash + idem_key -> same materialized result
        h2, l2 = env["ctx_sdk"].load(pid=pid, manifest=manifest, idempotency_key="idem-bnd")

        # Both loads materialized fully (no omissions at the boundary)
        assert len(l1.omitted_refs) == 0
        assert len(l2.omitted_refs) == 0
        assert len(l1.ordered_pages) == len(l2.ordered_pages)
        # Both include the optional ref
        opt_loaded_1 = any(p.canonical_uri == co for p in l1.ordered_pages)
        opt_loaded_2 = any(p.canonical_uri == co for p in l2.ordered_pages)
        assert opt_loaded_1 and opt_loaded_2
        # Idempotency replay returned the same handle_id
        assert h1.handle_id == h2.handle_id
        assert l1.materialized_hash == l2.materialized_hash

    def test_empty_optional_works(self, env):
        """Empty optional (zero content) works."""
        pid = env["pid"]
        cr, ar, vr, hr = _mk(
            env=env, uri="workspace:///normal_req.md", content=b"data",
            idem_key="k-empty-req",
        )
        ce, ae, ve, he = _mk(
            env=env, uri="workspace:///empty_opt.md", content=b"",
            idem_key="k-empty-opt",
        )
        manifest = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="req", canonical_uri=cr, artifact_id=ar,
                    version=vr, content_hash=hr, media_type="text/plain",
                    required=True),
                ContentRef(
                    ref_id="opt", canonical_uri=ce, artifact_id=ae,
                    version=ve, content_hash=he, media_type="text/plain",
                    required=False),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        handle, loaded = env["ctx_sdk"].load(pid=pid, manifest=manifest)
        assert any(p.canonical_uri == cr for p in loaded.ordered_pages)
        # Empty optional should still be loaded (zero cost page)
        assert any(p.canonical_uri == ce for p in loaded.ordered_pages)
        # The empty page should have size_bytes == 0
        empty_page = next(p for p in loaded.ordered_pages if p.canonical_uri == ce)
        assert empty_page.size_bytes == 0
        assert empty_page.content == b""

    def test_materialized_hash_differs_with_different_content(self, env):
        """Materialized hash differs when different content used."""
        pid = env["pid"]
        c1, a1, v1, h1 = _mk(
            env=env, uri="workspace:///lhs.md", content=b"left content",
            idem_key="k-lhs",
        )
        c2, a2, v2, h2 = _mk(
            env=env, uri="workspace:///rhs.md", content=b"right content different",
            idem_key="k-rhs",
        )
        m1 = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c1, artifact_id=a1,
                    version=v1, content_hash=h1, media_type="text/plain",
                    required=True),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        m2 = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="r1", canonical_uri=c2, artifact_id=a2,
                    version=v2, content_hash=h2, media_type="text/plain",
                    required=True),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        _, l1 = env["ctx_sdk"].load(pid=pid, manifest=m1)
        _, l2 = env["ctx_sdk"].load(pid=pid, manifest=m2)
        assert l1.materialized_hash != l2.materialized_hash
