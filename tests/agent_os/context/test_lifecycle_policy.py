"""Tests for the recoverability_residual_lifecycle_v1 selection policy.

Adopted ideas under test:
- Recoverability (CWL, arXiv 2606.11213): at equal priority a recoverable ref
  is omitted before a non-recoverable one.
- Residual utility / lifecycle-aware eviction (TokenPilot, arXiv 2606.17016):
  refs in the declared read-set are kept before refs whose relevance expired;
  prefix stability is reported so callers can reason about KV-cache invalidation.

The suite also asserts that priority_stable_v1 behaviour is *unchanged* (a
regression there would corrupt the meaning of durable snapshots).
"""

from __future__ import annotations

from typing import Any

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
    LIFECYCLE_POLICY_ID,
    PRIORITY_STABLE_POLICY_ID,
    LifecycleSelection,
    PrefixStability,
    RefPages,
    analyze_prefix_stability,
    read_set_from_metadata,
    select_pages,
    select_pages_lifecycle_v1,
    select_pages_v1,
    sort_ref_pages_lifecycle,
)
from tests.agent_os.context.conftest import write_artifacts_and_build_manifest

# ---------------------------------------------------------------------------
# Helpers (pure-policy level — no estimator, explicit costs)
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
    recoverable: bool = False,
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
        recoverable=recoverable,
        start_byte=start_byte,
        end_byte=end_byte,
    )


def _page(
    page_id: str,
    *,
    estimated_tokens: int = 10,
    size_bytes: int = 40,
    required: bool = False,
    priority: int = 0,
) -> ContextPage:
    return ContextPage(
        page_id=page_id,
        canonical_uri="artifact://ns/a",
        artifact_id="aid",
        version=1,
        content_hash="c" * 64,
        byte_start=0,
        byte_end=size_bytes,
        page_hash="p" * 64,
        estimated_tokens=estimated_tokens,
        size_bytes=size_bytes,
        required=required,
        priority=priority,
    )


def _manifest(
    *,
    token_budget: int = 10_000,
    byte_budget: int | None = None,
    policy_id: str = LIFECYCLE_POLICY_ID,
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
# Determinism across ref insertion orders
# ---------------------------------------------------------------------------


class TestLifecycleDeterminism:
    def test_selection_independent_of_ref_pages_order(self):
        """Passing ref_pages in any order yields the identical selection.

        The policy sorts internally, so caller ordering cannot matter.
        """
        manifest = _manifest(token_budget=25)
        rps = [
            _rp(_ref("r_req", required=True, priority=0), _page("p_req", estimated_tokens=5)),
            _rp(_ref("r_hi", priority=10), _page("p_hi", estimated_tokens=10)),
            _rp(_ref("r_lo", priority=1), _page("p_lo", estimated_tokens=10)),
            _rp(_ref("r_rec", priority=5, recoverable=True), _page("p_rec", estimated_tokens=10)),
        ]

        forward = select_pages_lifecycle_v1(manifest=manifest, ref_pages=list(rps))
        reverse = select_pages_lifecycle_v1(manifest=manifest, ref_pages=list(reversed(rps)))

        assert [p.page_id for p in forward.selected_pages] == [
            p.page_id for p in reverse.selected_pages
        ]
        assert forward.omitted_ref_ids == reverse.omitted_ref_ids
        assert forward.tokens_used == reverse.tokens_used
        assert forward.bytes_used == reverse.bytes_used

    def test_repeated_calls_are_identical(self):
        manifest = _manifest(token_budget=15)
        rps = [
            _rp(_ref("a", priority=5), _page("pa", estimated_tokens=10)),
            _rp(_ref("b", priority=5, recoverable=True), _page("pb", estimated_tokens=10)),
        ]
        a = select_pages_lifecycle_v1(manifest=manifest, ref_pages=list(rps))
        b = select_pages_lifecycle_v1(manifest=manifest, ref_pages=list(rps))
        assert a == b


# ---------------------------------------------------------------------------
# Recoverability ordering (CWL 2606.11213)
# ---------------------------------------------------------------------------


class TestRecoverabilityOrdering:
    def test_recoverable_omitted_before_non_recoverable_at_equal_priority(self):
        """At equal priority, the recoverable ref is dropped even though its
        canonical_uri sorts first — proving recoverability outranks the lexical
        tie-break, not the other way around."""
        manifest = _manifest(token_budget=20)
        rec = _ref("rec", canonical_uri="artifact://ns/a", priority=5, recoverable=True)
        norec = _ref("norec", canonical_uri="artifact://ns/z", priority=5, recoverable=False)
        rps = [
            _rp(rec, _page("p_rec", estimated_tokens=20)),
            _rp(norec, _page("p_norec", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(manifest=manifest, ref_pages=rps)
        assert [p.page_id for p in result.selected_pages] == ["p_norec"]
        assert result.omitted_ref_ids == ("rec",)

    def test_priority_dominates_recoverability(self):
        """A high-priority recoverable ref is still kept over a low-priority
        non-recoverable one — recoverability only breaks equal-priority ties."""
        manifest = _manifest(token_budget=20)
        hi_rec = _ref("hi_rec", priority=10, recoverable=True)
        lo_norec = _ref("lo_norec", priority=1, recoverable=False)
        rps = [
            _rp(lo_norec, _page("p_lo", estimated_tokens=20)),
            _rp(hi_rec, _page("p_hi", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(manifest=manifest, ref_pages=rps)
        assert [p.page_id for p in result.selected_pages] == ["p_hi"]
        assert result.omitted_ref_ids == ("lo_norec",)

    def test_equal_recoverability_falls_back_to_lexical_uri(self):
        """With recoverability equal, the canonical_uri tie-break decides."""
        manifest = _manifest(token_budget=20)
        a = _ref("ra", canonical_uri="artifact://ns/a", priority=5, recoverable=False)
        z = _ref("rz", canonical_uri="artifact://ns/z", priority=5, recoverable=False)
        rps = [
            _rp(z, _page("p_z", estimated_tokens=20)),
            _rp(a, _page("p_a", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(manifest=manifest, ref_pages=rps)
        assert [p.page_id for p in result.selected_pages] == ["p_a"]
        assert result.omitted_ref_ids == ("rz",)


# ---------------------------------------------------------------------------
# Residual-utility ordering (TokenPilot 2606.17016)
# ---------------------------------------------------------------------------


class TestResidualUtilityOrdering:
    def test_expired_relevance_omitted_before_needed(self):
        """A ref absent from the declared read-set (relevance expired) is
        dropped before one that is present, even though its uri sorts first."""
        manifest = _manifest(token_budget=20)
        expired = _ref("expired", canonical_uri="artifact://ns/a", priority=5)
        needed = _ref("needed", canonical_uri="artifact://ns/z", priority=5)
        rps = [
            _rp(expired, _page("p_expired", estimated_tokens=20)),
            _rp(needed, _page("p_needed", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(manifest=manifest, ref_pages=rps, read_set={"needed"})
        assert [p.page_id for p in result.selected_pages] == ["p_needed"]
        assert result.omitted_ref_ids == ("expired",)

    def test_residual_utility_outranks_recoverability(self):
        """Precedence check: a needed-but-recoverable ref is kept over an
        expired-but-non-recoverable one."""
        manifest = _manifest(token_budget=20)
        expired_norec = _ref("expired_norec", canonical_uri="artifact://ns/a", priority=5)
        needed_rec = _ref(
            "needed_rec", canonical_uri="artifact://ns/z", priority=5, recoverable=True
        )
        rps = [
            _rp(expired_norec, _page("p_en", estimated_tokens=20)),
            _rp(needed_rec, _page("p_nr", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(
            manifest=manifest, ref_pages=rps, read_set={"needed_rec"}
        )
        assert [p.page_id for p in result.selected_pages] == ["p_nr"]
        assert result.omitted_ref_ids == ("expired_norec",)

    def test_none_read_set_treats_every_ref_as_relevant(self):
        """read_set=None neutralises the residual-utility term."""
        manifest = _manifest(token_budget=40)
        rps = [
            _rp(_ref("a", priority=5), _page("pa", estimated_tokens=20)),
            _rp(_ref("b", priority=5), _page("pb", estimated_tokens=20)),
        ]
        result = select_pages_lifecycle_v1(manifest=manifest, ref_pages=rps, read_set=None)
        assert result.omitted_ref_ids == ()
        assert len(result.selected_pages) == 2


# ---------------------------------------------------------------------------
# Required fail-closed
# ---------------------------------------------------------------------------


class TestRequiredFailClosed:
    def test_required_exceeds_token_budget_raises(self):
        manifest = _manifest(token_budget=10)
        rp = _rp(_ref("r1", required=True), _page("p1", estimated_tokens=50, required=True))
        with pytest.raises(ErrRequiredBudgetExceeded):
            select_pages_lifecycle_v1(manifest=manifest, ref_pages=[rp])

    def test_required_exceeds_byte_budget_raises(self):
        manifest = _manifest(token_budget=10_000, byte_budget=100)
        rp = _rp(
            _ref("r1", required=True),
            _page("p1", estimated_tokens=5, size_bytes=500, required=True),
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            select_pages_lifecycle_v1(manifest=manifest, ref_pages=[rp])

    def test_recoverable_required_ref_is_never_dropped(self):
        """recoverable=True must not weaken the required fail-closed guarantee."""
        manifest = _manifest(token_budget=10)
        rp = _rp(
            _ref("r1", required=True, recoverable=True),
            _page("p1", estimated_tokens=50, required=True),
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            select_pages_lifecycle_v1(manifest=manifest, ref_pages=[rp])


# ---------------------------------------------------------------------------
# Prefix stability (KV-cache continuity signal)
# ---------------------------------------------------------------------------


class TestPrefixStability:
    def test_initial_selection_is_stable(self):
        s = analyze_prefix_stability(None, ("a", "b"))
        assert s == PrefixStability(prefix_stable=True, kind="initial", first_divergence_index=None)

    def test_identical_order_is_stable(self):
        s = analyze_prefix_stability(("a", "b"), ("a", "b"))
        assert s.prefix_stable is True
        assert s.kind == "identical"

    def test_append_only_is_stable(self):
        s = analyze_prefix_stability(("a", "b"), ("a", "b", "c"))
        assert s.prefix_stable is True
        assert s.kind == "append"
        assert s.first_divergence_index is None

    def test_tail_truncation_is_stable(self):
        s = analyze_prefix_stability(("a", "b", "c"), ("a", "b"))
        assert s.prefix_stable is True
        assert s.kind == "truncate"

    def test_mid_sequence_change_is_unstable(self):
        s = analyze_prefix_stability(("a", "b", "c"), ("a", "x", "c"))
        assert s.prefix_stable is False
        assert s.kind == "prefix_changed"
        assert s.first_divergence_index == 1

    def test_head_change_is_unstable_at_index_zero(self):
        s = analyze_prefix_stability(("a", "b"), ("x", "b"))
        assert s.prefix_stable is False
        assert s.first_divergence_index == 0

    def test_policy_reports_append_as_stable(self):
        """Growing the budget so a new lower-priority page appends to the tail
        preserves the prefix."""
        rps = [
            _rp(_ref("a", priority=10), _page("pa", estimated_tokens=10)),
            _rp(_ref("b", priority=5), _page("pb", estimated_tokens=10)),
        ]
        first = select_pages_lifecycle_v1(manifest=_manifest(token_budget=10), ref_pages=rps)
        assert [p.page_id for p in first.selected_pages] == ["pa"]
        assert first.prefix.kind == "initial"

        prev_order = [p.page_id for p in first.selected_pages]
        second = select_pages_lifecycle_v1(
            manifest=_manifest(token_budget=20),
            ref_pages=rps,
            previous_page_order=prev_order,
        )
        assert [p.page_id for p in second.selected_pages] == ["pa", "pb"]
        assert second.prefix.prefix_stable is True
        assert second.prefix.kind == "append"

    def test_policy_reports_reorder_as_prefix_change(self):
        """Swapping priorities reorders the selected head and invalidates the
        cached prefix."""
        rps_v1 = [
            _rp(_ref("a", priority=10), _page("pa", estimated_tokens=10)),
            _rp(_ref("b", priority=5), _page("pb", estimated_tokens=10)),
        ]
        first = select_pages_lifecycle_v1(manifest=_manifest(token_budget=100), ref_pages=rps_v1)
        assert [p.page_id for p in first.selected_pages] == ["pa", "pb"]

        rps_v2 = [
            _rp(_ref("a", priority=5), _page("pa", estimated_tokens=10)),
            _rp(_ref("b", priority=10), _page("pb", estimated_tokens=10)),
        ]
        second = select_pages_lifecycle_v1(
            manifest=_manifest(token_budget=100),
            ref_pages=rps_v2,
            previous_page_order=[p.page_id for p in first.selected_pages],
        )
        assert [p.page_id for p in second.selected_pages] == ["pb", "pa"]
        assert second.prefix.prefix_stable is False
        assert second.prefix.kind == "prefix_changed"
        assert second.prefix.first_divergence_index == 0


# ---------------------------------------------------------------------------
# read_set_from_metadata
# ---------------------------------------------------------------------------


class TestReadSetFromMetadata:
    def test_absent_returns_none(self):
        m = ContextManifest(owner_pid="p1", refs=(), token_budget=10)
        assert read_set_from_metadata(m) is None

    def test_list_returns_frozenset(self):
        m = ContextManifest(
            owner_pid="p1", refs=(), token_budget=10, metadata={"read_set": ["a", "b"]}
        )
        assert read_set_from_metadata(m) == frozenset({"a", "b"})

    def test_bare_string_is_single_ref(self):
        m = ContextManifest(owner_pid="p1", refs=(), token_budget=10, metadata={"read_set": "solo"})
        assert read_set_from_metadata(m) == frozenset({"solo"})

    def test_non_iterable_degrades_to_none(self):
        m = ContextManifest(owner_pid="p1", refs=(), token_budget=10, metadata={"read_set": 123})
        assert read_set_from_metadata(m) is None

    def test_non_string_members_are_ignored(self):
        m = ContextManifest(
            owner_pid="p1", refs=(), token_budget=10, metadata={"read_set": ["a", 1, "b"]}
        )
        assert read_set_from_metadata(m) == frozenset({"a", "b"})


# ---------------------------------------------------------------------------
# Dispatch + priority_stable_v1 unchanged
# ---------------------------------------------------------------------------


class TestDispatchAndV1Unchanged:
    def _v1_scenario(self):
        manifest = _manifest(token_budget=60, policy_id=PRIORITY_STABLE_POLICY_ID)
        ref_pages = [
            _rp(_ref("r1", required=True, priority=0), _page("p_r1", estimated_tokens=20)),
            _rp(_ref("r2", priority=10), _page("p_r2", estimated_tokens=20)),
            _rp(_ref("r3", priority=5), _page("p_r3", estimated_tokens=20)),
        ]
        return manifest, ref_pages

    def test_dispatch_routes_v1_identically(self):
        manifest, ref_pages = self._v1_scenario()
        direct = select_pages_v1(manifest=manifest, ref_pages=ref_pages)
        routed = select_pages(manifest=manifest, ref_pages=ref_pages)
        assert routed == direct
        selected, omitted, tokens, byts = routed
        assert [p.page_id for p in selected] == ["p_r1", "p_r2", "p_r3"]
        assert omitted == []
        assert tokens == 60

    def test_v1_ignores_recoverable_field(self):
        """priority_stable_v1 must not read the new recoverable field — its
        result is identical whether or not refs are marked recoverable, so
        existing snapshots keep their meaning."""
        manifest = _manifest(token_budget=20, policy_id=PRIORITY_STABLE_POLICY_ID)
        plain = [
            _rp(_ref("a", canonical_uri="artifact://ns/a", priority=5), _page("pa")),
            _rp(_ref("b", canonical_uri="artifact://ns/b", priority=5), _page("pb")),
        ]
        marked = [
            _rp(
                _ref("a", canonical_uri="artifact://ns/a", priority=5, recoverable=True),
                _page("pa"),
            ),
            _rp(
                _ref("b", canonical_uri="artifact://ns/b", priority=5, recoverable=True),
                _page("pb"),
            ),
        ]
        assert select_pages_v1(manifest=manifest, ref_pages=plain) == select_pages_v1(
            manifest=manifest, ref_pages=marked
        )

    def test_recoverable_does_not_change_manifest_or_ref_hash(self):
        """The recoverable flag is deliberately excluded from the identity
        hashes so durable priority_stable_v1 snapshots are byte-stable."""
        base = _ref("r1", recoverable=False)
        rec = _ref("r1", recoverable=True)
        assert base.ref_hash() == rec.ref_hash()

        m_base = ContextManifest(owner_pid="p1", manifest_id="m", refs=(base,), token_budget=10)
        m_rec = ContextManifest(owner_pid="p1", manifest_id="m", refs=(rec,), token_budget=10)
        assert m_base.manifest_hash() == m_rec.manifest_hash()

    def test_dispatch_rejects_unknown_policy(self):
        manifest = _manifest(token_budget=10, policy_id="does_not_exist")
        with pytest.raises(ErrInvalidPolicy):
            select_pages(manifest=manifest, ref_pages=[])

    def test_lifecycle_fn_rejects_v1_manifest(self):
        manifest = _manifest(token_budget=10, policy_id=PRIORITY_STABLE_POLICY_ID)
        with pytest.raises(ErrInvalidPolicy):
            select_pages_lifecycle_v1(manifest=manifest, ref_pages=[])

    def test_as_v1_tuple_shape(self):
        sel = LifecycleSelection(
            selected_pages=(_page("p1"),),
            omitted_ref_ids=("r2",),
            tokens_used=10,
            bytes_used=40,
            prefix=PrefixStability(prefix_stable=True, kind="initial", first_divergence_index=None),
        )
        pages, omitted, tokens, byts = sel.as_v1_tuple()
        assert [p.page_id for p in pages] == ["p1"]
        assert omitted == ["r2"]
        assert tokens == 10
        assert byts == 40


# ---------------------------------------------------------------------------
# sort_ref_pages_lifecycle direct
# ---------------------------------------------------------------------------


class TestSortRefPagesLifecycle:
    def test_full_keep_order(self):
        rps = [
            _rp(_ref("opt_rec", priority=5, recoverable=True), _page("p1")),
            _rp(_ref("req", required=True, priority=0), _page("p2")),
            _rp(_ref("opt_norec", priority=5, recoverable=False), _page("p3")),
            _rp(_ref("opt_hi", priority=10), _page("p4")),
        ]
        ordered = sort_ref_pages_lifecycle(rps, read_set=None)
        assert [rp.ref.ref_id for rp in ordered] == ["req", "opt_hi", "opt_norec", "opt_rec"]


# ---------------------------------------------------------------------------
# End-to-end through ContextService (dispatch + read-set from metadata)
# ---------------------------------------------------------------------------


def _lifecycle_manifest(
    env: dict[str, Any],
    *,
    recoverable_map: dict[str, bool] | None = None,
    read_set: list[str] | None = None,
    token_budget: int = 10,
    required_default: bool = False,
) -> ContextManifest:
    """Write two 40-byte artifacts and build a lifecycle-policy manifest.

    Each ref costs 10 tokens (ceil(40/4)); ref_ids are 'ra' and 'rz'.
    """
    pid = env["pid"]
    base = write_artifacts_and_build_manifest(
        env=env,
        pid=pid,
        artifacts=[
            ("workspace:///a.txt", b"x" * 40, "idem-a"),
            ("workspace:///z.txt", b"y" * 40, "idem-z"),
        ],
        token_budget=token_budget,
        page_size_bytes=64,
        required_map={
            "workspace:///a.txt": required_default,
            "workspace:///z.txt": required_default,
        },
        priority_map={"workspace:///a.txt": 5, "workspace:///z.txt": 5},
        ref_id_map={"workspace:///a.txt": "ra", "workspace:///z.txt": "rz"},
    )
    recoverable_map = recoverable_map or {}
    refs = tuple(
        r.model_copy(update={"recoverable": recoverable_map.get(r.ref_id, False)})
        for r in base.refs
    )
    metadata = {"read_set": read_set} if read_set is not None else {}
    return base.model_copy(
        update={"refs": refs, "policy_id": LIFECYCLE_POLICY_ID, "metadata": metadata}
    )


class TestLifecycleThroughService:
    def test_recoverable_ref_is_omitted_end_to_end(self, env: dict[str, Any]) -> None:
        """Budget fits one 10-token ref; the recoverable one (ra) is dropped
        despite sorting first lexically."""
        manifest = _lifecycle_manifest(env, recoverable_map={"ra": True}, token_budget=10)
        _handle, loaded = env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])
        assert len(loaded.ordered_pages) == 1
        assert loaded.ordered_pages[0].canonical_uri.endswith("/z.txt")
        assert {o.ref_id for o in loaded.omitted_refs} == {"ra"}

    def test_read_set_from_metadata_drives_selection(self, env: dict[str, Any]) -> None:
        """With both refs non-recoverable, a declared read-set of {'rz'} keeps
        rz and omits ra — flipping the default lexical outcome."""
        manifest = _lifecycle_manifest(env, read_set=["rz"], token_budget=10)
        _handle, loaded = env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])
        assert len(loaded.ordered_pages) == 1
        assert loaded.ordered_pages[0].canonical_uri.endswith("/z.txt")
        assert {o.ref_id for o in loaded.omitted_refs} == {"ra"}

    def test_service_selection_is_deterministic(self, env: dict[str, Any]) -> None:
        manifest = _lifecycle_manifest(env, recoverable_map={"ra": True}, token_budget=10)
        _h1, l1 = env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])
        _h2, l2 = env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])
        assert [p.page_id for p in l1.ordered_pages] == [p.page_id for p in l2.ordered_pages]
        assert {o.ref_id for o in l1.omitted_refs} == {o.ref_id for o in l2.omitted_refs}

    def test_required_fail_closed_end_to_end(self, env: dict[str, Any]) -> None:
        manifest = _lifecycle_manifest(env, token_budget=5, required_default=True)
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])

    def test_snapshot_records_lifecycle_policy_id(self, env: dict[str, Any]) -> None:
        """The distinct policy_id propagates into the durable snapshot so its
        provenance is unambiguous."""
        manifest = _lifecycle_manifest(env, recoverable_map={"ra": True}, token_budget=10)
        _handle, loaded = env["ctx_svc"].load(manifest=manifest, caller_pid=env["pid"])
        snap = env["ctx_svc"].snapshot(pid=env["pid"], context_id=loaded.context_id)
        assert snap.policy_id == LIFECYCLE_POLICY_ID
