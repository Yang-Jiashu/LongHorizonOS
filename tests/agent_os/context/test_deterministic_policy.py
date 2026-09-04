"""Tests for deterministic page-selection policy (spec Section 9).

Covers the six-level tie-breaker ordering in ``_ref_sort_key`` and
``sort_refs_deterministic``, budget enforcement and omission semantics in
``select_pages_v1``, and the ``manifest_hash_for`` helper.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.context.errors import (
    ErrInvalidPolicy,
    ErrRequiredBudgetExceeded,
)
from lhos.agent_os.context.models import (
    ContentRef,
    ContextManifest,
    ContextPage,
)
from lhos.agent_os.context.policies import (
    RefPages,
    _ref_sort_key,
    manifest_hash_for,
    select_pages_v1,
    sort_refs_deterministic,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ref(
    ref_id: str,
    *,
    canonical_uri: str = "artifact://ns/a",
    artifact_id: str = "aid",
    version: int = 1,
    content_hash: str = "c" * 64,
    media_type: str = "text/plain",
    priority: int = 0,
    required: bool = False,
    start_byte: int | None = None,
    end_byte: int | None = None,
) -> ContentRef:
    return ContentRef(
        ref_id=ref_id,
        canonical_uri=canonical_uri,
        artifact_id=artifact_id,
        version=version,
        content_hash=content_hash,
        media_type=media_type,
        priority=priority,
        required=required,
        start_byte=start_byte,
        end_byte=end_byte,
    )


def _page(
    page_id: str,
    *,
    canonical_uri: str = "artifact://ns/a",
    artifact_id: str = "aid",
    version: int = 1,
    content_hash: str = "c" * 64,
    page_hash: str = "p" * 64,
    byte_start: int = 0,
    byte_end: int = 64,
    estimated_tokens: int = 16,
    size_bytes: int = 64,
    required: bool = False,
    priority: int = 0,
) -> ContextPage:
    return ContextPage(
        page_id=page_id,
        canonical_uri=canonical_uri,
        artifact_id=artifact_id,
        version=version,
        content_hash=content_hash,
        byte_start=byte_start,
        byte_end=byte_end,
        page_hash=page_hash,
        estimated_tokens=estimated_tokens,
        size_bytes=size_bytes,
        required=required,
        priority=priority,
    )


def _manifest(
    *,
    token_budget: int = 10_000,
    byte_budget: int | None = None,
    policy_id: str = "priority_stable_v1",
) -> ContextManifest:
    return ContextManifest(
        owner_pid="p1",
        refs=(),
        token_budget=token_budget,
        byte_budget=byte_budget,
        policy_id=policy_id,
    )


def _rp(ref: ContentRef, *pages: ContextPage) -> RefPages:
    return RefPages(ref=ref, pages=pages)


# ---------------------------------------------------------------------------
# _ref_sort_key
# ---------------------------------------------------------------------------


class TestRefSortKey:
    def test_required_first_then_priority_descending(self):
        """Required refs always sort before optional; among equal
        required-status, higher priority sorts first (via negative)."""
        req_low = _ref("r1", required=True, priority=0)
        opt_high = _ref("r2", required=False, priority=99)
        assert _ref_sort_key(req_low) < _ref_sort_key(opt_high)

        # Two optionals with different priority: high first
        opt_lo = _ref("a", required=False, priority=1)
        opt_hi = _ref("b", required=False, priority=10)
        assert _ref_sort_key(opt_hi) < _ref_sort_key(opt_lo)

    def test_canonical_uri_lexical_tiebreaker(self):
        """When required + priority are equal, canonical_uri breaks
        the tie lexically (ascending)."""
        a = _ref("r1", canonical_uri="artifact://ns/a", priority=5)
        b = _ref("r2", canonical_uri="artifact://ns/b", priority=5)
        assert _ref_sort_key(a) < _ref_sort_key(b)

    def test_version_ascending_tiebreaker(self):
        """Same required + priority + uri → version ascending."""
        v1 = _ref("r1", version=1, priority=5)
        v2 = _ref("r2", version=2, priority=5)
        assert _ref_sort_key(v1) < _ref_sort_key(v2)

    def test_start_byte_then_ref_id_final_tiebreaker(self):
        """Same through version → start_byte ascending; if still
        identical, ref_id is the ultimate deterministic tiebreaker."""
        s0 = _ref("aaa", start_byte=0)
        s10 = _ref("aab", start_byte=10)
        assert _ref_sort_key(s0) < _ref_sort_key(s10)

        # Identical in every field except ref_id
        x = _ref("mmm", start_byte=5)
        y = _ref("nnn", start_byte=5)
        assert _ref_sort_key(x) < _ref_sort_key(y)


# ---------------------------------------------------------------------------
# sort_refs_deterministic
# ---------------------------------------------------------------------------


class TestSortRefsDeterministic:
    def test_required_first_regardless_of_priority(self):
        """A required, zero-priority ref always precedes any optional ref,
        no matter how high the optional's priority is."""
        refs = (
            _ref("opt_high", required=False, priority=100),
            _ref("req_low", required=True, priority=0),
            _ref("opt_mid", required=False, priority=50),
        )
        sorted_ids = [r.ref_id for r in sort_refs_deterministic(refs)]
        # required first, then optionals sorted by priority desc
        assert sorted_ids == ["req_low", "opt_high", "opt_mid"]

    def test_input_order_invariance(self):
        """Reordering the input tuple yields the exact same sorted output."""
        batch_a = (
            _ref("c", required=False, priority=3),
            _ref("a", required=True, priority=0),
            _ref("b", required=False, priority=5),
        )
        batch_b = (
            _ref("b", required=False, priority=5),
            _ref("c", required=False, priority=3),
            _ref("a", required=True, priority=0),
        )
        out_a = [r.ref_id for r in sort_refs_deterministic(batch_a)]
        out_b = [r.ref_id for r in sort_refs_deterministic(batch_b)]
        assert out_a == out_b
        assert out_a == ["a", "b", "c"]

    def test_six_level_ordering_complex(self):
        """A mixed set exercising all six tie-breakers yields the
        expected global order."""
        refs = (
            _ref("opt_z", required=False, priority=5, canonical_uri="artifact://ns/z", version=1),
            _ref("req_low", required=True, priority=0, canonical_uri="artifact://ns/z", version=2),
            _ref("opt_a", required=False, priority=10, canonical_uri="artifact://ns/a", version=1),
            _ref("req_hi", required=True, priority=100, canonical_uri="artifact://ns/a", version=1),
            _ref(
                "opt_dup", required=False, priority=10, canonical_uri="artifact://ns/a", version=1
            ),
        )
        ids = [r.ref_id for r in sort_refs_deterministic(refs)]
        # Required first: req_hi (pri=100, uri=a) before req_low (pri=0, uri=z).
        # Optional next:  opt_a/opt_dup (pri=10, uri=a) before opt_z (pri=5, uri=z).
        # opt_a/opt_dup tie-break by version (same), then start_byte (same),
        # then ref_id: "opt_a" < "opt_dup" lexically.
        assert ids == ["req_hi", "req_low", "opt_a", "opt_dup", "opt_z"]


# ---------------------------------------------------------------------------
# select_pages_v1
# ---------------------------------------------------------------------------


class TestSelectPagesV1:
    def test_raises_for_unknown_policy_id(self):
        """Any policy_id other than "priority_stable_v1" raises
        ErrInvalidPolicy."""
        manifest = _manifest(policy_id="does_not_exist")
        with pytest.raises(ErrInvalidPolicy):
            select_pages_v1(manifest=manifest, ref_pages=[])

    def test_required_exceeds_token_budget_raises(self):
        """When required pages alone exceed token_budget,
        ErrRequiredBudgetExceeded is raised."""
        manifest = _manifest(token_budget=10)
        ref = _ref("r1", required=True)
        rp = _rp(ref, _page("p1", estimated_tokens=50, size_bytes=10))
        with pytest.raises(ErrRequiredBudgetExceeded):
            select_pages_v1(manifest=manifest, ref_pages=[rp])

    def test_required_exceeds_byte_budget_raises(self):
        """When required pages alone exceed byte_budget,
        ErrRequiredBudgetExceeded is raised (even if tokens fit)."""
        manifest = _manifest(token_budget=10_000, byte_budget=100)
        ref = _ref("r1", required=True)
        rp = _rp(ref, _page("p1", estimated_tokens=5, size_bytes=500))
        with pytest.raises(ErrRequiredBudgetExceeded):
            select_pages_v1(manifest=manifest, ref_pages=[rp])

    def test_optional_overflow_fully_omitted_never_partial(self):
        """A ref that overflows budget is fully omitted; no individual
        pages from that ref bleed into the selected set."""
        manifest = _manifest(token_budget=25)
        ref = _ref("opt", required=False)
        rp = _rp(
            ref,
            _page("p1", estimated_tokens=20, size_bytes=30),
            _page("p2", estimated_tokens=20, size_bytes=30),
        )
        selected, omitted, tokens_used, _ = select_pages_v1(manifest=manifest, ref_pages=[rp])
        assert selected == []
        assert omitted == ["opt"]
        assert tokens_used == 0

    def test_optional_fits_is_included(self):
        """An optional ref whose full cost fits within both budgets is
        selected and counted."""
        manifest = _manifest(token_budget=100)
        ref = _ref("opt", required=False, priority=1)
        rp = _rp(ref, _page("p1", estimated_tokens=20, size_bytes=20))
        selected, omitted, tokens_used, bytes_used = select_pages_v1(
            manifest=manifest, ref_pages=[rp]
        )
        assert len(selected) == 1
        assert selected[0].page_id == "p1"
        assert omitted == []
        assert tokens_used == 20
        assert bytes_used == 20

    def test_byte_budget_enforced_for_optional(self):
        """An optional ref that fits the token budget but overflows the
        byte budget is still fully omitted."""
        manifest = _manifest(token_budget=10_000, byte_budget=50)
        ref = _ref("opt", required=False)
        rp = _rp(ref, _page("p1", estimated_tokens=1, size_bytes=200))
        selected, omitted, _, _ = select_pages_v1(manifest=manifest, ref_pages=[rp])
        assert selected == []
        assert omitted == ["opt"]

    def test_pages_preserve_all_original_fields(self):
        """Selected pages carry every field unchanged: page_id,
        byte range, hashes, costs, required/priority flags, and the
        default pinned/resident flags."""
        manifest = _manifest(token_budget=10_000)
        ref = _ref("r1", required=True, priority=7)
        page = ContextPage(
            page_id="mypage",
            canonical_uri="artifact://ns/doc",
            artifact_id="art123",
            version=3,
            content_hash="c" * 64,
            byte_start=128,
            byte_end=256,
            page_hash="h" * 64,
            estimated_tokens=32,
            size_bytes=128,
            required=True,
            priority=7,
        )
        rp = _rp(ref, page)
        selected, *_ = select_pages_v1(manifest=manifest, ref_pages=[rp])
        assert len(selected) == 1
        p = selected[0]
        assert p.page_id == "mypage"
        assert p.canonical_uri == "artifact://ns/doc"
        assert p.artifact_id == "art123"
        assert p.version == 3
        assert p.content_hash == "c" * 64
        assert p.byte_start == 128
        assert p.byte_end == 256
        assert p.page_hash == "h" * 64
        assert p.estimated_tokens == 32
        assert p.size_bytes == 128
        assert p.required is True
        assert p.priority == 7
        assert p.pinned is False
        assert p.resident is False

    def test_selection_is_deterministic_across_runs(self):
        """Passing the same manifest + same pre-sorted ref_pages always
        returns an identical (pages, omitted, totals) tuple."""
        manifest = _manifest(token_budget=60)
        refs = sort_refs_deterministic(
            (
                _ref("r1", required=True, priority=0),
                _ref("r2", required=False, priority=10),
                _ref("r3", required=False, priority=5),
            )
        )
        ref_pages = [
            _rp(refs[0], _page("p_r1", estimated_tokens=20, required=True, priority=0)),
            _rp(refs[1], _page("p_r2", estimated_tokens=20, required=False, priority=10)),
            _rp(refs[2], _page("p_r3", estimated_tokens=20, required=False, priority=5)),
        ]
        a = select_pages_v1(manifest=manifest, ref_pages=ref_pages)
        b = select_pages_v1(manifest=manifest, ref_pages=ref_pages)
        assert a == b
        selected_a, omit_a, tok_a, byte_a = a
        # Pre-sorted order is required first, then optionals by priority desc
        assert [p.page_id for p in selected_a] == ["p_r1", "p_r2", "p_r3"]
        assert omit_a == []
        assert tok_a == 60
        assert byte_a == 3 * 64


# ---------------------------------------------------------------------------
# manifest_hash_for
# ---------------------------------------------------------------------------


class TestManifestHashFor:
    def test_returns_same_value_as_manifest_hash(self):
        """manifest_hash_for is a thin wrapper returning
        manifest.manifest_hash()."""
        m = _manifest(token_budget=42, byte_budget=99)
        assert manifest_hash_for(m) == m.manifest_hash()

    def test_result_is_stable_across_calls(self):
        """Calling manifest_hash_for repeatedly on the same manifest
        returns the same hash (and matches manifest_hash at every call)."""
        m = _manifest(token_budget=7, byte_budget=None, policy_id="priority_stable_v1")
        h1 = manifest_hash_for(m)
        h2 = manifest_hash_for(m)
        assert h1 == h2
        assert h1 == m.manifest_hash()
        assert h1 == manifest_hash_for(m)
