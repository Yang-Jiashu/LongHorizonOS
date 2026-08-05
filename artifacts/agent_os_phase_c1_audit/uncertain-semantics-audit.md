# Section 15: UNCERTAIN Semantics Audit

**Audit Date:** 2026-08-05
**Auditor:** Phase C1.1 Independent Adversarial Audit
**Scope:** All L0–L4 modules in `lhos.agent_os`

## Summary

23 uncertain semantic items identified across 9 modules.
- **CRITICAL (must fix):** 8 (data leakage, broken invariants, races)
- **SPEC (needs documentation):** 10 (intentional but underspecified)
- **LATENT (future risk):** 5 (latent failures under unusual conditions)

## Classification Legend

| Tag | Meaning |
|-----|---------|
| FIX | Violates stated invariants; must be corrected before PASS |
| DOC | Intentional behavior but underspecified; needs spec text |
| LATENT | Latent failure under future conditions (clock, concurrency, corruption) |
| NOTE | Minor inconsistency; low risk |

---

## Module: `artifacts/uri.py`

### URI-01 [DOC] UNC path detection over-broad
**Location:** `uri.py:144-147`

The code rejects any path starting with `//` as "UNC path detected." The comment says "but only if it looks like //server/share" but the code is unconditional.

- **Current:** `artifact://ns-p1///foo` → `InvalidArtifactURI("UNC path detected")`
- **Spec:** Unclear whether redundant slashes are malformed or should be collapsed
- **Impact:** Legitimate URIs with accidental double-slashes rejected with misleading error

**Recommendation:** Either collapse redundant slashes (path normalization) or only reject `//host/` patterns.

---

### URI-02 [FIX] `is_canonical` fails on NFD inputs
**Location:** `uri.py:177-182`

`is_canonical` compares normalized output against raw input. NFC-normalized input with combining characters returns False.

- **Current:** `is_canonical("artifact://ns-p1/café")` with decomposed `é` returns False
- **Spec issue:** Docstring promises idempotent canonicalization but `is_canonical` uses byte comparison
- **Impact:** Clients reject valid pre-composed URIs; content-addressed lookups for Unicode paths break

**Recommendation:** Compare NFC(input) to NFC(result.canonical), not raw input to result.canonical.

---

### URI-03 [DOC] `resolve_workspace_uri` mutates returned object
**Location:** `uri.py:193-194`

After `canonicalize_uri` returns a `CanonicalURI`, `resolve_workspace_uri` reassigns its fields.

- **Current:** Object returned by canonicalize is modified after construction
- **Spec:** No immutability contract stated
- **Impact:** Aliasing bugs if object is cached before `resolve_workspace_uri`

---

### URI-04 [DOC] Empty path rejection undocumented
**Location:** `uri.py:170-171`

`artifact://ns-p1/` (bare namespace root) is rejected with "empty path after normalization."

- **Current:** Root namespace URI unusable
- **Spec:** Whether bare namespace URI is valid artifact reference (for listing) unspecified
- **Impact:** Cannot address a namespace itself, only paths within it

---

## Module: `artifacts/service.py`

### SRV-01 [FIX] Mount resolution bypasses capability checks
**Location:** `service.py:832-861`

Capability checked against caller namespace BEFORE mount resolution. After resolution, source namespace access is bypassed.

- **Current:** Caller with `read` on own namespace can read any artifact from any mounted source
- **Spec issue:** "Namespace isolation: P1 cannot access P2 private resources" claim contradicted by mount behavior
- **Impact:** Cross-namespace data leakage through mount graph

**Recommendation:** After mount resolution, verify caller also has capability on source namespace. Or document that mount = authorization grant.

---

### SRV-02 [FIX] Recovery version bump not idempotent
**Location:** `service.py:580-601`

`recover()` computes `new_version = artifact.current_version + 1` on each invocation.

- **Current:** Repeated recovery creates duplicate version rows with same content
- **Spec issue:** Recovery should be idempotent; version-creation side effect is not
- **Impact:** Spurious version history; version snapshots become inconsistent

**Recommendation:** Use `INSERT OR IGNORE` on (artifact_id, version) pair. Check if version already exists before bumping.

---

### SRV-03 [FIX] Staged hash used instead of committed hash in ArtifactVersion
**Location:** `service.py:438-464`

`commit()` discards driver's recomputed `commit_result.content_ref`, uses `txn.staged_content_ref` instead.

- **Current:** `ArtifactVersion.content_hash` may diverge from actual stored content if staging path is shared
- **Spec:** Which hash is authoritative unspecified
- **Impact:** Content-addressed integrity check fails to detect staging tampering

**Recommendation:** Use the hash returned by `driver.commit()` (post-CAS-move hash) as canonical.

---

### SRV-04 [DOC] Watch notifications outside commit transaction
**Location:** `service.py:500-503,666-701`

Watch signals sent in separate transaction from `ARTIFACT_TXN_COMMITTED`.

- **Current:** Commit committed but signal lost on crash
- **Spec:** Durability boundary for notifications unspecified
- **Impact:** Watchers miss `ARTIFACT_CHANGED` after crash

**Recommendation:** Include watch notification in commit transaction, or add follow-up recovery scan.

---

### SRV-05 [FIX] `open` leaks orphan artifact record on lease failure
**Location:** `service.py:221-236`

`_create_artifact_record` inserts version-0 record before lease acquisition.

- **Current:** Failed lease acquisition leaves empty, unwritable artifact record
- **Spec:** No cleanup path
- **Impact:** Namespace pollution with empty records

**Recommendation:** Wrap artifact creation + lease acquisition in single transaction; roll back record on failure.

---

### SRV-06 [FIX] Orphaned staging cleanup count nonsensical
**Location:** `service.py:615-623`

`orphaned_cleaned = len(orphaned) - len(orphan_report)` subtracts post-cleanup from pre-cleanup counts.

- **Current:** Can produce negative values
- **Impact:** Recovery statistics misleading; suggests bug hiding in dict

---

## Module: `artifacts/namespace_service.py`

### NS-01 [FIX] Mount resolution first-match-wins, not longest-prefix
**Location:** `namespace_service.py:163-177`

`resolve_mount` returns first prefix match (alphabetical from `ORDER BY mount_point`).

- **Current:** `data/raw/file` under mounts `data` and `data/raw` resolves to `data` (wrong)
- **Spec:** Resolution order unspecified
- **Impact:** Wrong cross-namespace routing

**Recommendation:** Use longest-prefix matching, not first-match.

---

### NS-02 [FIX] `delete_namespace` does not delete
**Location:** `namespace_service.py:65-78`

Method journals `ARTIFACT_NAMESPACE_DELETED` but never deletes the projection row. `handle_event` has no branch for this event type.

- **Current:** "Deleted" namespace remains fully queryable
- **Spec:** Term "delete" is misleading; behavior undefined
- **Impact:** Leaked namespaces; no tombstone distinction

**Recommendation:** Either actually delete the row, or implement `deleted` flag and filter in all queries.

---

### NS-03 [DOC] `create_snapshot` lacks transactional isolation
**Location:** `namespace_service.py:182-224`

Reads happen in separate queries without snapshot isolation.

- **Current:** Snapshot may mix versions from different commit times
- **Spec:** Snapshot consistency guarantees unspecified
- **Impact:** Non-restorable snapshots

**Recommendation:** Use `BEGIN IMMEDIATE` or document snapshot as "best-effort point-in-time."

---

### NS-04 [DOC] `set_quota` projection + journal in separate transactions
**Location:** `namespace_service.py:80-92`

Projection upsert and journal append not atomic.

- **Current:** Crash leaves projection and journal inconsistent
- **Impact:** Rebuild may differ from runtime state

---

## Module: `services/lease_service.py`

### LEASE-01 [FIX] `atomic_acquire` TOCTOU race breaks exclusive guarantee
**Location:** `lease_service.py:76-124`

Check happens in different transaction than acquire. No unique constraint on leases for exclusive resources.

- **Current:** Two callers pass check simultaneously → both insert exclusive leases
- **Spec claim:** "Atomic acquire: all-or-nothing" violated across concurrent callers
- **Impact:** Broken exclusive invariant → concurrent writes corrupt data

**Recommendation:** Use database-level locking (`BEGIN IMMEDIATE`) or unique constraint on `(resource_id)` WHERE mode='exclusive'.

---

### LEASE-02 [FIX] `renew` appears successful on released lease
**Location:** `lease_service.py:188-210`

SELECT then UPDATE in separate transaction. UPDATE affects 0 rows silently.

- **Current:** Caller gets lease object that doesn't exist in DB
- **Spec:** Renewal semantics under concurrent release unspecified
- **Impact:** Caller writes believe it holds valid lease → data corruption

**Recommendation:** Raise `LeaseNotFound` if UPDATE affects 0 rows.

---

### LEASE-03 [DOC] `reclaim_expired` double-journals terminal state
**Location:** `lease_service.py:214-234`

Calls `release()` which journals `LEASE_RELEASED`, then journals `LEASE_EXPIRED` separately.

- **Current:** Two terminal events per expired lease
- **Spec:** Whether expired and released are distinct states unclear
- **Impact:** Journal bloat; semantically confusing audit trail

---

### LEASE-04 [FIX] Inconsistent datetime handling (naive vs aware)
**Location:** `lease_service.py:72,195,390` vs `kernel/models.py` `datetime.now(UTC)`

Lease service uses `datetime.utcnow()` (naive); kernel models use `datetime.now(UTC)` (aware).

- **Current:** Naive datetimes stored, reconstructed as naive objects
- **Latent:** Any comparison raises `TypeError`
- **Impact:** Latent crash in scheduling or any code comparing lease expiry with model timestamps

**Recommendation:** Migrate all modules to `datetime.now(UTC)` (timezone-aware UTC).

---

## Module: `services/capability_service.py`

### CAP-01 [FIX] `verify_child_subset` requires exact match, not subset
**Location:** `capability_service.py:161-176`

Compares `(pattern, frozenset(ops))` tuples for exact set membership.

- **Current:** Legitimate child subset of parent flagged as violation
- **Spec:** "Subset" docstring misleading
- **Impact:** Legitimate delegation blocked; security model inconsistent

**Recommendation:** Implement actual subset semantics (pattern containment + operation containment).

---

### CAP-02 [FIX] Concurrent `grant` creates duplicate rows for same PID
**Location:** `capability_service.py:78-94,193-208`

Two concurrent grants both see missing set, both create empty set with different `set_id`, both add capability.

- **Current:** Multiple rows per pid; `get_capability_set` returns arbitrary row
- **Spec:** No concurrency control specified
- **Impact:** Silent grant loss; capability checks non-deterministic

**Recommendation:** Use unique constraint on `pid`, serialized insert, or upsert logic.

---

### CAP-03 [DOC] Conflicting upsert logic in `_upsert_capability_set`
**Location:** `capability_service.py:193-208`

Method upserts first by `set_id`, then claims to upsert by `pid`.

- **Current:** Self-contradictory design
- **Impact:** Multiple rows per pid possible

---

## Module: `services/journal.py`

### JRN-01 [DOC] `rebuild_projections` has no per-handler error isolation
**Location:** `journal.py:155-157`

One bad event or handler crashes full rebuild.

- **Current:** Projections left in `DELETE`-reset state (empty)
- **Spec:** Fault isolation unspecified
- **Impact:** DoS via single bad event

**Recommendation:** Wrap each handler in try/except and document failure semantics.

---

### JRN-02 [DOC] `process_sequence` starts at 0
**Location:** `journal.py:67-71`

First event gets `process_sequence = 0`.

- **Spec:** Starting value unspecified
- **Impact:** Off-by-one for callers expecting 1-based

---

### JRN-03 [NOTE] `append_events_atomically` mutates caller's input
**Location:** `journal.py:53-57`

Sets `journal_offset` and `process_sequence` on caller's event objects.

- **Spec:** Method documented as idempotent but doesn't mention mutation side-effect
- **Impact:** Aliasing bugs with cached events

---

## Module: `kernel/models.rs`

### MOD-01 [DOC] `Capability.check` uses `fnmatch` where `*` crosses `/`
**Location:** `models.py:269-275`

`fnmatch.fnmatch` allows `*` to match `/`, over-granting.

- **Current:** Capability `resource:workspace/*` grants access to `resource:workspace/p1/secret/file`
- **Spec:** Pattern semantics (glob vs prefix) unspecified
- **Impact:** Inadvertent deep access grants

**Recommendation:** Use explicit match, or document that `*` matches across `/`.

---

### MOD-02 [FIX] `ResourceLease.expires_at` defaults to creation time (already expired)
**Location:** `models.py:289`

Any lease without explicit `expires_at` is immediately reclaimable.

- **Current:** Lease expired at creation
- **Impact:** Projection rebuild creates already-expired leases; test code vulnerable

---

### MOD-03 [DOC] `Clock.now()` is wall-clock (non-monotonic)
**Location:** `models.py:304-305`

Can move backward on NTP/manual adjustment.

- **Spec:** "Logical + wall-clock" docstring; monotonicity not guaranteed
- **Impact:** Invalid event ordering after clock skew

---

## Module: `kernel/errors.py`

### ERR-01 [DOC] `LeaseAcquisitionFailed` records single resource only
**Location:** `errors.py:31-37`

Multi-claim failure stores only first failing resource.

- **Impact:** Caller cannot make informed retry decisions

---

## Cross-Cutting Concerns

### X-01 [FIX] Datetime inconsistency across modules
**Scope:** `lease_service.py` naive UTC vs `kernel/models.py` aware UTC

All downstream comparisons between lease expiry and event timestamps will raise `TypeError`.

**Recommendation:** Standardize on `datetime.now(UTC)` everywhere.

---

### X-02 [FIX] Recovery versioning race
**Scope:** `service.py:commit()` vs `service.py:recover()`

Concurrent commit + recovery can both read same `current_version`, both compute same `new_version`.

**Recommendation:** Use atomic `UPDATE ... WHERE current_version = ?` with version check.

---

### X-03 [LATENT] Watch notification atomicity gap
**Scope:** `service.py:_notify_watches`

Notifications outside commit boundary; no recovery redeliver.

**Recommendation:** Add watch notification events to journal within commit transaction.

---

### X-04 [LATENT] Content hash not verified on read
**Scope:** `service.py:read()`

Read does not re-hash to confirm CAS file unchanged.

- **Impact:** Silent data corruption if disk/process tampering occurs
- **Recommendation:** Add optional read-time hash verification, or document CAS integrity guarantees.

---

## Items Requiring Fix Before PASS

| ID | Severity | Issue |
|----|----------|-------|
| URI-02 | HIGH | `is_canonical` fails on NFD inputs |
| SRV-01 | CRITICAL | Mount resolution bypasses capability checks |
| SRV-02 | HIGH | Recovery version bump not idempotent |
| SRV-03 | HIGH | Staged hash used instead of committed hash |
| SRV-05 | HIGH | Orphan artifact record on lease failure |
| SRV-06 | MEDIUM | Nonsensical orphan cleanup count |
| NS-01 | HIGH | First-match-wins mount resolution |
| NS-02 | HIGH | `delete_namespace` does not delete |
| LEASE-01 | CRITICAL | Exclusive lease TOCTOU race |
| LEASE-02 | HIGH | Renew appears success on released lease |
| LEASE-04 | HIGH | Naive vs aware datetime inconsistency |
| CAP-01 | HIGH | Subset check requires exact match |
| CAP-02 | CRITICAL | Concurrent grant duplicates rows |
| MOD-02 | MEDIUM | Lease expires_at defaults to now |
| X-01 | HIGH | Cross-module datetime inconsistency |
| X-02 | HIGH | Recovery versioning race |

**Total: 16 items require fix before PASS.**

---

## Recommendation

The following are **blocking** for a clean PASS verdict on Phase C1:

1. **SRV-01**: Cross-namespace data leakage through mount resolution — this contradicts the stated namespace isolation invariant. Either fix the capability re-check post-resolution or document that mounts ARE authorization grants.
2. **LEASE-01**: Broken exclusive lease invariant under concurrency — the core guarantee of the lease protocol is violated.
3. **CAP-02**: Silent grant loss under concurrency — breaks capability model soundness.

The remaining items are serious but can be addressed in a follow-on fix commit if the user accepts a "PASS with deferred fixes" verdict. This audit recommends **PASS only if all 16 FIX items are addressed**.
