# Phase C2 — Context VM: Agent Test Plan

Scope: version-bound Context VM — context snapshots, deterministic working
sets, process-isolated working sets.

Source of truth: spec sections on version-pinned content refs, deterministic
paging, priority-stable selection policy, pin/refcount eviction protection,
snapshot/restore integrity, and cross-PID handle isolation.

---

## Test files (19 files, 21+ test classes, ~110 test methods)

| # | File | Purpose | Test count |
|---|------|---------|------------|
| 01 | `test_smoke.py` | Fixture wiring | 1 |
| 02 | `test_models.py` | Domain model invariants (hashing, schemas) | ~8 |
| 03 | `test_paging.py` | Deterministic pager (contiguous, non-overlap, no-gap) | ~12 |
| 04 | `test_deterministic_policy.py` | Six-level tie-break ordering + selection | ~13 |
| 05 | `test_estimator.py` | Token estimator determinism + codec fallback | ~8 |
| 06 | `test_handles.py` | Handle lifecycle: load/read/inspect/close/process-scoping | ~12 |
| 07 | `test_version_binding.py` | Version pinning integrity + snapshot/restore across writes | ~8 |
| 08 | `test_budget.py` | Token/byte budget enforcement (required vs optional) | ~11 |
| 09 | `test_eviction.py` | Eviction with pin protection + required-page preservation | ~10 |
| 10 | `test_pinning.py` | Pin/unpin/refcount lifecycle | ~8 |
| 11 | `test_snapshot.py` | Snapshot creation + restore integrity + restart round-trips | ~13 |
| 12 | `test_process_isolation.py` | Cross-PID handle/working-set isolation | ~6 |
| 13 | `test_capabilities.py` | Capability enforcement on context operations | ~6 |
| 14 | `test_idempotency.py` | Idempotency key replay for load/snapshot/restore | ~6 |
| 15 | `test_recovery.py` | Service restart recovery scenarios | ~5 |
| 16 | `test_architecture.py` | AST-based layering/circular-import/no-domain-strings | ~8 |
| 17 | `test_projection_replay.py` | Event projection replay determinism | ~4 |
| 18 | `test_demos.py` | Demo smoke: all 6 example scripts run end-to-end | ~6 |
| 19 | `test_mutation_audit.py` | Source-level mutation kill tests (15 mutations) | ~30 |

---

## Mapping to spec: CVM-01 .. CVM-12 (mutation audit)

| Spec ID | Mutation target | Killed by test |
|---------|----------------|----------------|
| CVM-01 | Version check disabled | `TestMutation01_VersionCheckDisabled` |
| CVM-02 | Cache key drops content_hash | `TestMutation02_CacheKeyDropsContentHash` |
| CVM-03 | Required omission allowed | `TestMutation03_RequiredOmissionAllowed` |
| CVM-04 | Budget overflow allowed | `TestMutation04_BudgetOverflowAllowed` |
| CVM-05 | Cross-PID handle access allowed | `TestMutation05_CrossPIDHandleAccessAllowed` |
| CVM-06 | Pin refcount lost | `TestMutation06_PinRefcountLost` |
| CVM-07 | Pinned page evictable | `TestMutation07_PinnedPageEvictable` |
| CVM-08 | Restore-to-latest fallback | `TestMutation08_RestoreToLatestFallback` |
| CVM-09 | Cross-PID handle read allowed | `TestMutation09_CrossPIDHandleReadAllowed` |
| CVM-10 | Random tie-break | `TestMutation10_RandomTieBreak` |
| CVM-11 | Snapshot skip restore | `TestMutation11_SnapshotSkipRestore` |
| CVM-12 | Manifest owner_pid validation skipped | `TestMutation12_ManifestOwnerPidValidationSkipped` |

Bonus (defensive) kills:

| Spec ID | Mutation target | Killed by test |
|---------|----------------|----------------|
| CVM-13 | Token estimate zero-for-empty | `TestMutation13_TokenEstimateZeroForEmpty` |
| CVM-14 | Selection ignores required-first | `TestMutation14_SelectionIgnoresRequiredFirst` |
| CVM-15 | Snapshot materialized_hash not verified | `TestMutation15_SnapshotHashNotVerifiedOnRestore` |

---

## Invariants validated

- **Determinism**: same manifest → same selected_pages, tokens_used,
  materialized_hash, materialized_hash across restarts.
- **Version pinning**: a context loaded against artifact version N serves
  exactly version N's bytes regardless of later artifact writes.
- **Integrity**: every materialized page's content_hash and page_hash
  match the committed ArtifactVersion; tampering raises
  `ErrSnapshotCorrupt` on restore.
- **Required-first**: required refs are never omitted for budget reasons;
  required-ref overflow raises `ErrRequiredBudgetExceeded`.
- **Budget**: `tokens_used <= token_budget`; `bytes_used <= byte_budget`.
- **Process isolation**: cross-PID handle/working-set access raises
  `ErrHandleNotOwned`; working sets are PID-scoped.
- **Eviction protection**: pinned pages are never evicted; required pages
  are never eviction candidates.
- **Pin refcounting**: pin increments, unpin decrements; eviction consults
  the refcount.
- **Snapshot integrity**: restore verifies content_hash + page_hash of
  every binding and recomputes `materialized_hash`.
- **Capability enforcement**: `validate_manifest` checks `owner_pid`
  matches `caller_pid`.
- **Estimator pinning**: estimator_id is recorded in snapshots so
  downstream consumers can detect estimator changes.

---

## How to run

```bash
# All Context VM tests
make test-context
# or equivalently
python -m pytest tests/agent_os/context/ -x -q

# Only mutation audit
python -m pytest tests/agent_os/context/test_mutation_audit.py -v

# Coverage for context module
python -m pytest tests/agent_os/context/ \
    --cov=lhos.agent_os.context --cov-report=term-missing
```

---

## Implementation locations

- Models: `src/lhos/agent_os/context/models.py`
- Pager: `src/lhos/agent_os/context/pager.py`
- Policies: `src/lhos/agent_os/context/policies.py`
- Estimator: `src/lhos/agent_os/context/estimator.py`
- Errors: `src/lhos/agent_os/context/errors.py`
- Service: `src/lhos/agent_os/context/service.py`
- SDK: `src/lhos/agent_os/context/sdk.py`
- Demos: `examples/agent_os/context_*.py` (6 scripts)
- Tests: `tests/agent_os/context/` (19 files)

---

## Phase C2 status

All 15 mutations KILLED. The baseline (pre-mutation) invariants all hold
against the real, un-mutated service. Each mutation test applies a targeted
patch, then asserts the invariant FAILS (inverted logic — proving the
invariant exists and the mutation would break it).
