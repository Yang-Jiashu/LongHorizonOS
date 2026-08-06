"""Tests for Context VM core domain models."""

from __future__ import annotations

from lhos.agent_os.context.models import (
    ContentRef,
    ContextHandle,
    ContextManifest,
    ContextPage,
    ContextSnapshot,
    LoadedContext,
    LoadedPage,
    OmittedRef,
    PageBinding,
    VersionBinding,
    WorkingSet,
    _deterministic_hash,
)


class TestContentRef:
    def _make_ref(self, **overrides):
        base = dict(
            ref_id="r1",
            canonical_uri="artifact://ns-p1/a.md",
            artifact_id="aid-1",
            version=1,
            content_hash="h" * 64,
            media_type="text/markdown",
            priority=0,
            required=False,
            start_byte=None,
            end_byte=None,
        )
        base.update(overrides)
        return ContentRef(**base)

    def test_ref_hash_stable(self):
        a = self._make_ref()
        b = self._make_ref()
        assert a.ref_hash() == b.ref_hash()

    def test_ref_hash_changes_with_binding(self):
        base = self._make_ref()
        modified = self._make_ref(version=2)
        assert base.ref_hash() != modified.ref_hash()

    def test_ref_hash_differs_by_media_type(self):
        a = self._make_ref(media_type="text/markdown")
        b = self._make_ref(media_type="text/plain")
        assert a.ref_hash() != b.ref_hash()


class TestContextManifest:
    def _make_manifest(self, refs=None, manifest_id="mid", **over):
        refs = refs or []
        base = dict(
            manifest_id=manifest_id,
            owner_pid="p1",
            refs=tuple(refs),
            token_budget=10_000,
            page_size_bytes=64,
        )
        base.update(over)
        return ContextManifest(**base)

    def test_manifest_hash_stable(self):
        mk = lambda rid, aid, ch: ContentRef(
            ref_id=rid,
            canonical_uri=f"artifact://ns-p1/{rid}.md",
            artifact_id=aid,
            version=1,
            content_hash=ch,
            media_type="text/markdown",
        )
        r = mk("r1", "aid", "h" * 64)
        a = self._make_manifest(refs=[r])
        b = self._make_manifest(refs=[mk("r1", "aid", "h" * 64)])
        assert a.manifest_hash() == b.manifest_hash()

    def test_manifest_hash_independent_of_ref_order(self):
        mk = lambda rid, aid, ch: ContentRef(
            ref_id=rid,
            canonical_uri=f"artifact://ns-p1/{rid}.md",
            artifact_id=aid,
            version=1,
            content_hash=ch,
            media_type="text/markdown",
        )
        r1 = mk("r1", "aid-1", "h" * 64)
        r2 = mk("r2", "aid-2", "a" * 64)
        a = self._make_manifest(refs=[r1, r2])
        b = self._make_manifest(refs=[mk("r2", "aid-2", "a" * 64), mk("r1", "aid-1", "h" * 64)])
        assert a.manifest_hash() == b.manifest_hash()

    def test_manifest_hash_changes_with_budget(self):
        mk = lambda rid, aid, ch: ContentRef(
            ref_id=rid,
            canonical_uri=f"artifact://ns-p1/{rid}.md",
            artifact_id=aid,
            version=1,
            content_hash=ch,
            media_type="text/markdown",
        )
        r = mk("r1", "aid", "h" * 64)
        a = self._make_manifest(refs=[r], token_budget=10_000)
        b = self._make_manifest(refs=[mk("r1", "aid", "h" * 64)], token_budget=20_000)
        assert a.manifest_hash() != b.manifest_hash()


class TestContextPage:
    def test_page_fields_round_trip(self):
        p = ContextPage(
            page_id="pid",
            canonical_uri="artifact://ns/p1",
            artifact_id="aid",
            version=1,
            content_hash="c" * 64,
            page_hash="p" * 64,
            byte_start=0,
            byte_end=10,
            estimated_tokens=3,
            size_bytes=10,
            required=True,
            priority=0,
        )
        assert p.page_hash == "p" * 64
        assert p.size_bytes == 10
        assert p.estimated_tokens == 3


class TestWorkingSet:
    def test_default_state(self):
        ws = WorkingSet(
            pid="p1",
            manifest_id="mid",
            manifest_hash="h" * 64,
            policy_id="priority_stable_v1",
            token_budget=100,
            byte_budget=None,
            selected_page_ids=(),
            omitted_page_ids=(),
        )
        assert ws.state == "created"
        assert ws.tokens_used == 0
        assert ws.bytes_used == 0


class TestContextHandle:
    def test_handle_fields(self):
        h = ContextHandle(
            handle_id="h1", pid="p1", working_set_id="ws1", pinned_page_ids=("a", "b")
        )
        assert h.pid == "p1"
        assert h.closed_at is None
        assert h.pinned_page_ids == ("a", "b")


class TestLoadedPage:
    def test_loaded_page_has_page_hash(self):
        p = LoadedPage(
            page_id="pid",
            canonical_uri="artifact://ns/p1",
            artifact_id="aid",
            version=1,
            content_hash="c" * 64,
            page_hash="p" * 64,
            byte_start=0,
            byte_end=10,
            required=True,
            priority=0,
            media_type="text/plain",
            encoding="utf-8",
            estimated_tokens=3,
            size_bytes=0,
            content=b"hello",
        )
        assert p.page_hash == "p" * 64
        assert p.content == b"hello"


class TestOmittedRef:
    def test_omitted_ref(self):
        o = OmittedRef(ref_id="r1", reason="budget_exceeded", requested_tokens=42)
        assert o.reason == "budget_exceeded"
        assert o.requested_tokens == 42


class TestVersionBinding:
    def test_version_binding(self):
        vb = VersionBinding(
            page_id="pid",
            canonical_uri="artifact://ns/p1",
            artifact_id="aid",
            version=1,
            content_hash="c" * 64,
        )
        assert vb.version == 1
        assert vb.artifact_id == "aid"


class TestPageBinding:
    def test_page_binding(self):
        pb = PageBinding(
            page_id="pid",
            canonical_uri="artifact://ns/p1",
            artifact_id="aid",
            version=1,
            content_hash="c" * 64,
            page_hash="p" * 64,
            byte_start=0,
            byte_end=10,
        )
        assert pb.page_hash == "p" * 64


class TestContextSnapshot:
    def test_snapshot_fields(self):
        s = ContextSnapshot(
            pid="p1",
            manifest_hash="m" * 64,
            working_set_hash="w" * 64,
            materialized_hash="x" * 64,
            policy_id="priority_stable_v1",
            estimator_id="byte_x4_utf8_v1",
            page_bindings=(),
            tokens_used=5,
            bytes_used=20,
        )
        assert s.snapshot_id
        assert s.materialized_hash == "x" * 64


class TestLoadedContext:
    def test_materialized_hash_filled(self):
        p = LoadedPage(
            page_id="pid",
            canonical_uri="artifact://ns/p1",
            artifact_id="aid",
            version=1,
            content_hash="c" * 64,
            page_hash="p" * 64,
            byte_start=0,
            byte_end=10,
            required=True,
            priority=0,
            media_type="text/plain",
            encoding="utf-8",
            estimated_tokens=3,
            size_bytes=0,
            content=b"hello",
        )
        lc = LoadedContext(
            pid="p1",
            manifest_id="mid",
            manifest_hash="m" * 64,
            working_set_id="ws1",
            ordered_pages=(p,),
            token_budget=100,
            tokens_used=3,
            byte_budget=None,
            bytes_used=10,
            omitted_refs=(),
            version_bindings=(),
            materialized_hash="y" * 64,
        )
        assert lc.materialized_hash == "y" * 64


class TestDeterministicHash:
    def test_stable_output(self):
        a = _deterministic_hash(["foo", "bar", "baz"])
        b = _deterministic_hash(["foo", "bar", "baz"])
        assert a == b

    def test_distinct_from_different_inputs(self):
        a = _deterministic_hash(["foo"])
        b = _deterministic_hash(["bar"])
        assert a != b

    def test_length_is_sha256(self):
        h = _deterministic_hash(["x"])
        assert len(h) == 64
