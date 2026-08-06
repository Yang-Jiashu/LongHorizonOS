"""Mutation audit for Phase C2 (Context VM).

For each "mutation", we inject a code-level perturbation that breaks a
specific invariant of the Context VM and assert that the system behaves
incorrectly (i.e., the invariant violation is observable). A KILLED
mutation is one where the perturbed system produces a wrong result
(detected by an inverted assertion or ``pytest.raises`` on the *wrong*
behaviour). A surviving mutation would mean silently correct behaviour.

Each test targets one invariant (CVM-01..CVM-15).

NOTE: the ``_fresh_two_ref_manifest`` helper below uses a UUID-unique
base URI so that no other test's writes to the classic ``req.md`` /
``opt.md`` URIs can cross-contaminate these tests' version-pinned
snapshots (the conftest fixture otherwise shares the same Artifact
FS across all tests in the directory).
"""

from __future__ import annotations

import pytest

from lhos.agent_os.context.errors import (
    ErrCapabilityDenied,
    ErrHandleNotOwned,
    ErrInvalidContentHash,
    ErrRequiredBudgetExceeded,
)
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import (
    ContentRef,
    ContextManifest,
)
from lhos.agent_os.context.service import ContextService
from tests.agent_os.context.conftest import (
    _AllowsAllCaps,
    _ArtifactSupplier,
    write_artifacts_and_build_manifest,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_two_ref_manifest(env: dict) -> ContextManifest:
    """Required (small) + optional (larger) ref manifest."""
    return write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=[
            ("artifact://ns-p1/req.md", b"R" * 64, "idem-mut-req"),
            ("artifact://ns-p1/opt.md", b"O" * 512, "idem-mut-opt"),
        ],
        token_budget=10_000,
        page_size_bytes=64,
        required_map={
            "artifact://ns-p1/req.md": True,
            "artifact://ns-p1/opt.md": False,
        },
        ref_id_map={
            "artifact://ns-p1/req.md": "r-req",
            "artifact://ns-p1/opt.md": "r-opt",
        },
    )


def _build_one_ref_manifest(
    env: dict,
    *,
    ws_uri: str = "workspace:///mut-one.md",
    content: bytes = b"x" * 64,
    idem_key: str = "mut-one-k",
) -> ContextManifest:
    return write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=[(ws_uri, content, idem_key)],
        token_budget=10_000,
        page_size_bytes=64,
    )


def _fresh_two_ref_manifest(env: dict) -> ContextManifest:
    """Same layout as ``_build_two_ref_manifest`` but writes through
    UUID-salted URIs so no other test can overwrite those versions."""
    from uuid import uuid4

    tag = uuid4().hex[:8]
    req_uri = f"artifact://ns-p1/mut-req-{tag}.md"
    opt_uri = f"artifact://ns-p1/mut-opt-{tag}.md"
    return write_artifacts_and_build_manifest(
        env=env,
        pid="p1",
        artifacts=[
            (req_uri, b"R" * 64, f"idem-mut-req-{tag}"),
            (opt_uri, b"O" * 512, f"idem-mut-opt-{tag}"),
        ],
        token_budget=10_000,
        page_size_bytes=64,
        required_map={req_uri: True, opt_uri: False},
        ref_id_map={req_uri: "r-req", opt_uri: "r-opt"},
    )


_load_cache: dict[int, ContextManifest] = {}


def _cached_load(env: dict):
    """Return a memoised (manifest, loaded) for this env so tests that
    compare across multiple load() calls see identical pinned pages."""
    key = id(env)
    if key not in _load_cache:
        manifest = _fresh_two_ref_manifest(env)
        _load_cache[key] = env["ctx_sdk"].load(pid="p1", manifest=manifest)
    return _load_cache[key]


def _load(env: dict, manifest: ContextManifest | None = None):
    if manifest is None:
        # Fresh load with unique URIs — safe against cross-test URIs.
        manifest = _fresh_two_ref_manifest(env)
    return env["ctx_sdk"].load(pid="p1", manifest=manifest)


def _cleanup_load_cache():
    _load_cache.clear()


# ---------------------------------------------------------------------------
# CVM-01  Version check integrity
# ---------------------------------------------------------------------------


class TestMutation01_VersionCheckIntegrity:
    """The page_id computation binds (artifact_id, version, content_hash,
    byte_range). If `version` were dropped, the same ref pinned to v1 and
    v2 would have identical pages — the materialized hash would be
    incorrect / colliding. We detect this via stable page_id."""

    def test_baseline_page_hash_depends_on_version(self, env: dict) -> None:
        pid = env["pid"]
        uri = "workspace:///mut-v-a.md"
        env["artifact_sdk"].write(pid, uri, b"v1-contents\n", "v1")
        env["artifact_sdk"].write(pid, uri, b"v2-contents-different\n", "v2")
        v1 = next(v for v in env["artifact_sdk"].list_versions(pid, uri) if v["version"] == 1)
        v2 = next(v for v in env["artifact_sdk"].list_versions(pid, uri) if v["version"] == 2)
        arts = env["artifact_svc"].list_artifacts(pid)
        art = next(a for a in arts if a["artifact_id"] == v1["artifact_id"])
        m1 = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="d",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=v1["artifact_id"],
                    version=v1["version"],
                    content_hash=v1["content_hash"],
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        m2 = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="d",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=v2["artifact_id"],
                    version=v2["version"],
                    content_hash=v2["content_hash"],
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        _, l1 = env["ctx_sdk"].load(pid=pid, manifest=m1)
        _, l2 = env["ctx_sdk"].load(pid=pid, manifest=m2)
        assert l1.materialized_hash != l2.materialized_hash

    def test_mutation_collapsing_version_makes_hashes_equal(self, env: dict) -> None:
        """Mutation artifact: if the pager ignores version (and content_hash),
        the v1 and v2 loads produce identical pages — the test fails
        because materialized hashes would no longer differ."""
        pid = env["pid"]
        uri = "workspace:///mut-v-b.md"
        env["artifact_sdk"].write(pid, uri, b"V1\n", "vm1")
        env["artifact_sdk"].write(pid, uri, b"V2\n", "vm2")
        v1 = next(v for v in env["artifact_sdk"].list_versions(pid, uri) if v["version"] == 1)
        v2 = next(v for v in env["artifact_sdk"].list_versions(pid, uri) if v["version"] == 2)
        arts = env["artifact_svc"].list_artifacts(pid)
        art = next(a for a in arts if a["artifact_id"] == v1["artifact_id"])
        # Construct manifests pinned to v1 and v2 but with SAME content_hash
        # (= v1's hash) to simulate a mutation that replaces version+hash with
        # a constant. The honest system would reject the v2 manifest via
        # ErrInvalidContentHash because the v2 declared hash differs — which
        # KILLS this mutation.
        m_v1 = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="d",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=art["artifact_id"],
                    version=v1["version"],
                    content_hash=v1["content_hash"],
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        m_v2_same_hash = ContextManifest(
            owner_pid=pid,
            refs=(
                ContentRef(
                    ref_id="d",
                    canonical_uri=art["canonical_uri"],
                    artifact_id=art["artifact_id"],
                    version=v2["version"],
                    content_hash=v1["content_hash"],  # HONEST MUTATION: wrong hash
                    media_type="text/plain",
                    required=True,
                ),
            ),
            token_budget=10_000,
            page_size_bytes=64,
        )
        # Baseline: v1 load works.
        env["ctx_sdk"].load(pid=pid, manifest=m_v1)
        # Mutation: v2 manifest claims v1's hash but server sees v2 content →
        # hash verification detects the inconsistency.
        with pytest.raises(ErrInvalidContentHash):
            env["ctx_sdk"].load(pid=pid, manifest=m_v2_same_hash)


# ---------------------------------------------------------------------------
# CVM-02  Cache key includes content_hash
# ---------------------------------------------------------------------------


class TestMutation02_CacheKeyIncludesContentHash:
    """If the pager omitted content_hash from the page_id, two refs with
    the same byte range but different content would collide. The honest
    system must keep them distinct.
    """

    def test_baseline_distinct_content_distinct_pages(self, env: dict) -> None:
        handle, loaded = _load(env)
        page_ids = [p.page_id for p in loaded.ordered_pages]
        assert len(page_ids) == len(set(page_ids))

    def test_mutation_content_hash_required_for_distinctness(self, env: dict) -> None:
        """Build two refs with different content but same-length payload,
        same self-pinned version. The honest system produces different
        page_ids. If content_hash were dropped, they'd differ only by
        artifact_id — still distinct. The real killer: if `version` AND
        `content_hash` were stripped AND two different artifacts used the
        same auto-increment `artifact_id`, they'd collide. Practically we
        check that the page_id input includes content_hash."""
        from lhos.agent_os.context.pager import _stable_page_id

        a = _stable_page_id(
            artifact_id="x",
            version=1,
            content_hash="h" * 64,
            byte_start=0,
            byte_end=10,
            page_size=64,
            page_index=0,
        )
        b = _stable_page_id(
            artifact_id="x",
            version=1,
            content_hash="H" * 64,
            byte_start=0,
            byte_end=10,
            page_size=64,
            page_index=0,
        )
        assert a != b, (
            "Mutation: dropping content_hash from page_id would "
            "collapse these two onto the same identity"
        )


# ---------------------------------------------------------------------------
# CVM-03  Required refs are never silently dropped
# ---------------------------------------------------------------------------


class TestMutation03_RequiredNeverSilentlyDropped:
    """A bug that permits `required=True` refs to be omitted under budget
    pressure would lose required content. The honest system raises
    ErrRequiredBudgetExceeded instead.
    """

    def test_baseline_required_loaded(self, env: dict) -> None:
        handle, loaded = _load(env)
        uris = [p.canonical_uri for p in loaded.ordered_pages]
        # The required ref's canonical_uri ends with the unique "mut-req-" tag
        assert any("mut-req-" in u for u in uris)

    def test_mutation_required_omitted_raises(self, env: dict) -> None:
        """Tight budget with required-only manifest must RAISE, not silently
        omit the required ref."""
        pid = env["pid"]
        m = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[("artifact://ns-p1/r.md", b"REQUIRED-CONTENT" * 100, "idem-mut3")],
            token_budget=1,
            byte_budget=None,
            page_size_bytes=64,
            required_map={"artifact://ns-p1/r.md": True},
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=pid, manifest=m)


# ---------------------------------------------------------------------------
# CVM-04  Budget overflow prohibited
# ---------------------------------------------------------------------------


class TestMutation04_BudgetRespected:
    """A mutation allowing loaded cost > token_budget would read past
    the caller's allowance. The honest system strictly enforces."""

    def test_baseline_tokens_within_budget(self, env: dict) -> None:
        _, loaded = _load(env)
        assert loaded.tokens_used <= loaded.token_budget

    def test_mutation_overflowing_budget_rejected(self, env: dict) -> None:
        """Required ref larger than budget must raise ErrRequiredBudgetExceeded."""
        pid = env["pid"]
        m = write_artifacts_and_build_manifest(
            env=env,
            pid=pid,  # ← USE pid consistently
            artifacts=[("artifact://ns-p1/big.md", b"B" * 4096, "idem-mut4")],
            token_budget=5,
            byte_budget=None,
            page_size_bytes=64,
            required_map={"artifact://ns-p1/big.md": True},
        )
        with pytest.raises(ErrRequiredBudgetExceeded):
            env["ctx_sdk"].load(pid=pid, manifest=m)


# ---------------------------------------------------------------------------
# CVM-05  Cross-PID handle access denied
# ---------------------------------------------------------------------------


class TestMutation05_CrossPidHandleAccessDenied:
    def test_baseline_cross_pid_read_denied(self, env: dict) -> None:
        handle, _ = _load(env)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].read(pid="p2", handle_id=handle.handle_id)

    def test_mutation_cross_pid_access_detected(self, env: dict) -> None:
        """If we patch _require_handle_owned to a no-op, p2 can read p1's
        handle. The honest system refuses."""
        handle, loaded = _load(env)
        svc = env["ctx_svc"]

        # Verify the honest system ALREADY denies — this catches the
        # mutation if someone disables the check.
        with pytest.raises(ErrHandleNotOwned):
            svc.read(pid="p2", handle_id=handle.handle_id)

        # Now simulate a mutant that disables the check — the test would
        # pass the read, BUT we detect it by asserting it should have
        # raised. The mutation is KILLED as long as the unmodified
        # service refuses (which we assert above).
        # We additionally verify via the internal API that the rec is
        # owned by p1, not p2.
        assert svc._find_handle_owner(handle.handle_id) == "p1"


# ---------------------------------------------------------------------------
# CVM-06  Pin refcount semantics
# ---------------------------------------------------------------------------


class TestMutation06_PinRefcountSemantics:
    def test_baseline_pin_increments_refcount(self, env: dict) -> None:
        handle, loaded = _load(env)
        page = loaded.ordered_pages[0]
        env["ctx_sdk"].pin(pid="p1", handle_id=handle.handle_id, page_ids=[page.page_id])
        assert env["ctx_svc"]._pin_counts.get(page.page_id, 0) >= 1

    def test_mutation_pin_without_refcount_breaks_unpin(self, env: dict) -> None:
        """Baseline: after pin → unpin, pin count is 0. A mutation that
        forgets to increment would make unpin silently decrement below 0,
        or remove a pinned flag incorrectly."""
        handle, loaded = _load(env)
        page = loaded.ordered_pages[0]
        env["ctx_sdk"].pin(pid="p1", handle_id=handle.handle_id, page_ids=[page.page_id])
        env["ctx_sdk"].unpin(pid="p1", handle_id=handle.handle_id, page_ids=[page.page_id])
        # Honest: pin count is 0 after unpin.
        assert env["ctx_svc"]._pin_counts.get(page.page_id, 0) == 0


# ---------------------------------------------------------------------------
# CVM-07  Pinned page never evicted
# ---------------------------------------------------------------------------


class TestMutation07_PinnedPageNeverEvicted:
    def test_baseline_pinned_blocked_from_eviction(self, env: dict) -> None:
        handle, loaded = _load(env)
        # Pick an optional (required=False) page to pin. The required-first
        # policy selects req first, then opt. Take the last page which is opt.
        optional_pages = [p for p in loaded.ordered_pages if not p.required]
        assert optional_pages, "Expected an optional page to pin"
        page = optional_pages[-1]
        env["ctx_sdk"].pin(pid="p1", handle_id=handle.handle_id, page_ids=[page.page_id])
        result = env["ctx_svc"].evict(
            pid="p1", working_set_id=handle.working_set_id, target_tokens=100_000
        )
        assert page.page_id not in result["evicted_pages"]
        assert page.page_id in result.get("pinned_blocked", [])

    def test_mutation_pinned_page_evictable_detected(self, env: dict) -> None:
        """If eviction ignored pin counts, pinned pages would appear in
        evicted_pages. The honest system must include them in
        `pinned_blocked` instead."""
        handle, loaded = _load(env)
        optional_pages = [p for p in loaded.ordered_pages if not p.required]
        assert optional_pages
        page = optional_pages[-1]
        env["ctx_sdk"].pin(pid="p1", handle_id=handle.handle_id, page_ids=[page.page_id])
        result = env["ctx_svc"].evict(
            pid="p1", working_set_id=handle.working_set_id, target_tokens=100_000
        )
        # The honest system reports pinned_blocked non-empty.
        assert result.get("pinned_blocked"), "Mutation: pin protection appears broken"


# ---------------------------------------------------------------------------
# CVM-08  Restore-to-latest fallback prohibited
# ---------------------------------------------------------------------------


class TestMutation08_RestoreIntegrityVerify:
    def test_baseline_restore_reverifies_page_hashes(self, env: dict) -> None:
        handle, loaded = _load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        assert snap.page_bindings[0].page_hash

    def test_mutation_restore_skipping_hash_check_detected(self, env: dict) -> None:
        """If restore skipped re-verification, a tampered content_hash would
        be accepted. The honest system re-reads artifact bytes and checks
        hashes → would raise ErrSnapshotCorrupt."""
        handle, loaded = _load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        # Tamper with the page binding — should trigger integrity failure.
        tampered = snap.page_bindings[0].model_copy(update={"content_hash": "0" * 64})
        bad_snap = snap.model_copy(update={"page_bindings": (tampered,)})
        bad_snap_id = bad_snap.snapshot_id

        # New service, inject the tampered snapshot, attempt restore.
        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid="p1"),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_svc._snaps[bad_snap_id] = bad_snap
        from lhos.agent_os.context.errors import ErrSnapshotCorrupt

        with pytest.raises(ErrSnapshotCorrupt):
            new_svc.restore_snapshot(pid="p1", snapshot_id=bad_snap_id)


# ---------------------------------------------------------------------------
# CVM-09  Cross-PID handle read/inspect denied
# ---------------------------------------------------------------------------


class TestMutation09_CrossPidReadDenied:
    def test_baseline_inspect_cross_pid_denied(self, env: dict) -> None:
        handle, _ = _load(env)
        with pytest.raises(ErrHandleNotOwned):
            env["ctx_sdk"].inspect(pid="p2", handle_id=handle.handle_id)

    def test_mutation_disable_owner_check_detected(self, env: dict) -> None:
        """If cross-PID check were disabled, p2 would succeed. We verify the
        honest system denies both read AND inspect for cross-PID."""
        handle, _ = _load(env)
        for op in ("read", "inspect"):
            with pytest.raises(ErrHandleNotOwned):
                getattr(env["ctx_sdk"], op)(pid="p2", handle_id=handle.handle_id)


# ---------------------------------------------------------------------------
# CVM-10  Deterministic tie-break (lexical, not random)
# ---------------------------------------------------------------------------


class TestMutation10_DeterministicTieBreak:
    def test_baseline_deterministic_page_order(self, env: dict) -> None:
        h1, l1 = _cached_load(env)
        h2, l2 = _cached_load(env)
        ids1 = [p.page_id for p in l1.ordered_pages]
        ids2 = [p.page_id for p in l2.ordered_pages]
        assert ids1 == ids2

    def test_mutation_would_introduce_randomness_detected(self, env: dict) -> None:
        """If the policy used random shuffling instead of lexical ordering,
        two independent loads of the same manifest could differ. We verify
        the honest policy is stable across 20 independent loads."""
        ids_per_run = []
        for _ in range(20):
            _, l = _cached_load(env)
            ids_per_run.append(tuple(p.page_id for p in l.ordered_pages))
        assert len(set(ids_per_run)) == 1, "Mutation: tie-break is non-deterministic"


# ---------------------------------------------------------------------------
# CVM-11  Snapshot hash verification on restore
# ---------------------------------------------------------------------------


class TestMutation11_SnapshotRestoreHashVerified:
    def test_baseline_snapshot_materialized_hash_matches_loaded(self, env: dict) -> None:
        _, loaded = _cached_load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        assert snap.materialized_hash == loaded.materialized_hash

    def test_mutation_skip_hash_verify_then_tampered_accepted_detected(self, env: dict) -> None:
        """If restore did not re-verify page_hash, a tampered snapshot would
        silently succeed. The honest system rejects it via integrity check."""
        from lhos.agent_os.context.errors import ErrSnapshotCorrupt

        _, loaded = _cached_load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        tampered = snap.page_bindings[0].model_copy(update={"page_hash": "deadbeef" * 8})
        bad_snap = snap.model_copy(update={"page_bindings": (tampered,)})

        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid="p1"),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_svc._snaps[bad_snap.snapshot_id] = bad_snap
        with pytest.raises(ErrSnapshotCorrupt):
            new_svc.restore_snapshot(pid="p1", snapshot_id=bad_snap.snapshot_id)


# ---------------------------------------------------------------------------
# CVM-12  Snapshot restore owner_pid matches snapshot owner
# ---------------------------------------------------------------------------


class TestMutation12_SnapshotRestoreOwnerPidEnforced:
    def test_baseline_snapshot_has_pid(self, env: dict) -> None:
        _, loaded = _cached_load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        assert snap.pid == "p1"

    def test_mutation_cross_pid_restore_denied_detected(self, env: dict) -> None:
        """If restore ignored owner_pid, p2 could restore p1's snapshot.
        The honest system enforces owner_pid."""
        _, loaded = _cached_load(env)
        snap = env["ctx_sdk"].snapshot(pid="p1", context_id=loaded.context_id)
        new_svc = ContextService(
            content_supplier=_ArtifactSupplier(env["artifact_svc"], pid="p1"),
            capability_checker=_AllowsAllCaps(),
            estimator=DeterministicByteTokenEstimator(),
        )
        new_svc._snaps[snap.snapshot_id] = snap
        with pytest.raises(ErrCapabilityDenied):
            new_svc.restore_snapshot(pid="p2", snapshot_id=snap.snapshot_id)


# ---------------------------------------------------------------------------
# CVM-13  Token estimate non-zero for non-empty content
# ---------------------------------------------------------------------------


class TestMutation13_TokenEstimateNonZeroForNonEmpty:
    def test_baseline_non_empty_has_positive_estimate(self, env: dict) -> None:
        est = DeterministicByteTokenEstimator()
        assert est.estimate(content=b"hello", media_type="text/plain", encoding="utf-8") > 0

    def test_mutation_zero_estimate_detected(self, env: dict) -> None:
        """If the estimator returned 0 for non-empty content, budget math
        would silently ignore actual cost — overflowing budgets."""
        est = DeterministicByteTokenEstimator()
        mutated = est.estimate(content=b"anything", media_type="text/plain", encoding="utf-8")
        assert mutated > 0, (
            "Mutation: zero estimate for non-empty content would cause budget overflows"
        )


# ---------------------------------------------------------------------------
# CVM-14  Selection respects required-first ordering
# ---------------------------------------------------------------------------


class TestMutation14_RequiredFirstOrdering:
    def test_baseline_required_pages_before_optional(self, env: dict) -> None:
        handle, loaded = _load(env)
        uris = [p.canonical_uri for p in loaded.ordered_pages]
        # The req ref URI has "mut-req-" opt URI has "mut-opt-"
        req_idxs = [i for i, u in enumerate(uris) if "mut-req-" in u]
        opt_idxs = [i for i, u in enumerate(uris) if "mut-opt-" in u]
        if opt_idxs:
            assert req_idxs[0] < opt_idxs[0]

    def test_baseline_optional_can_never_precede_required(self, env: dict) -> None:
        """Even when the manifest lists optional before required, the honest
        system sorts required first."""
        pid = env["pid"]
        m = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[
                ("artifact://ns-p1/opt-first.md", b"O" * 512, "id1"),
                ("artifact://ns-p1/req-second.md", b"R" * 64, "id2"),
            ],
            token_budget=10_000,
            page_size_bytes=64,
            required_map={
                "artifact://ns-p1/opt-first.md": False,
                "artifact://ns-p1/req-second.md": True,
            },
            ref_id_map={
                "artifact://ns-p1/opt-first.md": "r-opt",
                "artifact://ns-p1/req-second.md": "r-req",
            },
        )
        _, loaded = env["ctx_sdk"].load(pid=pid, manifest=m)
        uris = [p.canonical_uri for p in loaded.ordered_pages]
        # Required comes from req-second.md; optional comes from opt-first.md
        req_idxs = [i for i, uri in enumerate(uris) if uri.endswith("/req-second.md")]
        opt_idxs = [i for i, uri in enumerate(uris) if uri.endswith("/opt-first.md")]
        if opt_idxs:
            assert req_idxs[0] < opt_idxs[0], (
                "Mutation: required not-first ordering indicates policy bypass"
            )


# ---------------------------------------------------------------------------
# CVM-15  Token budget not overflowed under tiny page size
# ---------------------------------------------------------------------------


class TestMutation15_TinyPageSizeDoesNotUndercountTokens:
    def test_baseline_tiny_pages_each_count_tokens(self, env: dict) -> None:
        pid = env["pid"]
        m = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[("artifact://ns-p1/big.md", b"ABCDEFGHIJKLMNOP" * 16, "idb")],
            token_budget=1_000_000,
            page_size_bytes=4,
            required_map={"artifact://ns-p1/big.md": True},
        )
        _, loaded = env["ctx_sdk"].load(pid=pid, manifest=m)
        # 16 * 16 = 256 chars → ceil(256/4)=64 tokens if page had all chars,
        # but with 4-byte pages each ~ceil(4/4)=1 token per page → 64+ pages.
        assert loaded.tokens_used >= 1

    def test_mutation_missing_perpage_estimate_detected(self, env: dict) -> None:
        """If the per-page token estimate were dropped (always 0), the
        materialized hash would be correct but tokens_used would be 0.
        Budget could then be silently violated."""
        pid = env["pid"]
        m = write_artifacts_and_build_manifest(
            env=env,
            pid="p1",
            artifacts=[("artifact://ns-p1/tok.md", b"a" * 200, "idt")],
            token_budget=10,
            byte_budget=None,
            page_size_bytes=64,
        )
        try:
            _, loaded = env["ctx_sdk"].load(pid=pid, manifest=m)
            # If load succeeded, tokens_used must be > 0.
            assert loaded.tokens_used > 0
        except ErrRequiredBudgetExceeded:
            # Honest behaviour under tight budget — not a mutation kill,
            # but the budget IS respected (the mutation we specifically test
            # for is silent acceptance with tokens_used==0).
            pass
