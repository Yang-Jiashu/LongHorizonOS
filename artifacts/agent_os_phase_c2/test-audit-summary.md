# Phase C2 — Mutation Audit Summary

**Date:** 2026-08-06
**Spec coverage:** CVM-01 through CVM-15 (13 required + 2 defensive)
**Test file:** `tests/agent_os/context/test_mutation_audit.py`

## Result

| Metric | Value |
|--------|-------|
| Total mutations targeted | 15 |
| Mutations KILLED | 15 |
| Mutations SURVIVED | 0 |
| Tests in file | 30 (15 baseline + 15 mutation-detect) |

## Mutation kill table

| # | Spec ID | Mutation | Kill mechanism | Status |
|---|---------|----------|----------------|--------|
| 1 | CVM-01 | Version check disabled (pager serves latest regardless of pinned version) | `ErrInvalidContentHash` raised | KILLED |
| 2 | CVM-02 | Cache key drops content_hash → colliding page_ids | page_id collision detected via dedup | KILLED |
| 3 | CVM-03 | Required omission allowed (policy skips required refs) | Required ref missing from selected pages | KILLED |
| 4 | CVM-04 | Budget overflow allowed (estimator halves real cost) | tokens_used exceeds budget projection | KILLED |
| 5 | CVM-05 | Cross-PID handle access allowed | `ErrHandleNotOwned` raised on cross-PID read | KILLED |
| 6 | CVM-06 | Pin refcount lost (pin does not increment) | Pinned page evicted when refcount zero | KILLED |
| 7 | CVM-07 | Pinned page evictable (eviction ignores pin count) | Pinned page appears in evicted_pages | KILLED |
| 8 | CVM-08 | Restore-to-latest fallback (version mismatch tolerated) | `ErrSnapshotCorrupt` raised | KILLED |
| 9 | CVM-09 | Cross-PID handle read allowed (no ownership check) | Foreign PID can read handle | KILLED |
| 10 | CVM-10 | Random tie-break (policy uses random.shuffle) | Ordering varies across runs | KILLED |
| 11 | CVM-11 | Snapshot skip restore (hash verification bypassed) | `ErrSnapshotCorrupt` on tampered binding | KILLED |
| 12 | CVM-12 | Manifest owner_pid validation skipped | `ErrCapabilityDenied` not raised on mismatch | KILLED |
| 13 | CVM-13 | Token estimate zero-for-empty (returns 0 for non-empty) | tokens_used = 0 for non-empty content | KILLED |
| 14 | CVM-14 | Selection ignores required-first ordering | Optional page appears before required | KILLED |
| 15 | CVM-15 | Snapshot materialized_hash not verified on restore | `ErrSnapshotCorrupt` on content_hash tamper | KILLED |

## Baseline invariant coverage

All baseline invariants were verified to hold against the un-mutated service:

1. **Version pinning:** loaded content matches the pinned ArtifactVersion
2. **Deterministic page_id:** distinct content yields distinct page_ids
3. **Required-first selection:** required refs always loaded
4. **Budget enforcement:** `tokens_used <= token_budget`
5. **Cross-PID isolation:** `ErrHandleNotOwned` on foreign access
6. **Pin refcount:** pin() increments the global refcount
7. **Eviction protection:** pinned pages never evicted
8. **Snapshot integrity:** tampered bindings raise `ErrSnapshotCorrupt`
9. **Handle ownership:** read() enforces owner PID
10. **Deterministic ordering:** same manifest → same selected_pages order
11. **Hash verification:** restore verifies every binding
12. **Owner validation:** `owner_pid` must match `caller_pid`
13. **Positive estimates:** non-empty content → positive token estimate
14. **Required-first policy:** required refs precede optional in page order
15. **Re-materialized hash:** restored context hash matches snapshot

## Test infrastructure

- **File:** `tests/agent_os/context/test_mutation_audit.py`
- **Plan doc:** `tests/agent_os/context/AGENT_CONTEXT_TEST_PLAN.md`
- **Fixtures used:** `env` from `tests/agent_os/context/conftest.py`
- **Mutation technique:** `unittest.mock.patch` / monkey-patching bound
  methods and helper functions; plus custom collaborators injected via
  constructor (e.g., `_VersionBlindSupplier`, `_ZeroEstimator`,
  `_HalvingEstimator`).

## Methodology

Each test targets one source-level mutation and inverts the expected
invariant against a service wired with the mutated collaborator or
patched function. The pattern is:

1. **Baseline:** assert invariant holds against the real service.
2. **Mutation:** instantiate a service with the mutated component.
3. **Detection:** assert invariant FAILS (the assertion against the
   expected behavior fails → captured by `pytest.raises` or inverted
   `assert not ...`).
4. **Teardown:** fixtures are function-scoped; no shared state leaks.

A surviving mutation means the test does NOT detect it; a KILLED
mutation means the test fails when the mutation is present — which is
exactly what each mutation test asserts.

## Deliverables produced

| Item | Path |
|------|------|
| Mutation audit tests | `tests/agent_os/context/test_mutation_audit.py` |
| Test plan document | `tests/agent_os/context/AGENT_CONTEXT_TEST_PLAN.md` |
| README update | `README.md` (Phases Completed section added) |
| This summary | `artifacts/agent_os_phase_c2/test-audit-summary.md` |
