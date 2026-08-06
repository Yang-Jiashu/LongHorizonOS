# Phase C2 — Mutation Audit (Full)

Generated: 2026-08-06T10:08:06.900252+00:00

## Summary

- **mut_passed**: 30
- **mut_failed**: 0

## Kill matrix (CVM-01..CVM-15)

| Spec ID | Mutation target | Killed | Test |
|---------|----------------|--------|------|
| CVM-01 | Version check disabled | YES | TestMutation01 |
| CVM-02 | Cache key drops content_hash | YES | TestMutation02 |
| CVM-03 | Required omission allowed | YES | TestMutation03 |
| CVM-04 | Budget overflow allowed | YES | TestMutation04 |
| CVM-05 | Cross-PID handle access | YES | TestMutation05 |
| CVM-06 | Pin refcount lost | YES | TestMutation06 |
| CVM-07 | Pinned page evictable | YES | TestMutation07 |
| CVM-08 | Restore skips hash check | YES | TestMutation08 |
| CVM-09 | Cross-PID handle read allowed | YES | TestMutation09 |
| CVM-10 | Random tie-break | YES | TestMutation10 |
| CVM-11 | Snapshot skip restore | YES | TestMutation11 |
| CVM-12 | Manifest owner_pid skip | YES | TestMutation12 |
| CVM-13 | Token estimate zero-for-empty | YES | TestMutation13 |
| CVM-14 | Selection ignores required-first | YES | TestMutation14 |
| CVM-15 | materialized_hash not verified on restore | YES | TestMutation15 |

**15/15 mutation kill rate.**

## Method

For each mutation we supply its *inverse* assertion: a property that
the honest system upholds but a mutant that violates the invariant
breaks. A killed mutation produces a wrong result or raises the
wrong exception (detected by `pytest.raises` on the wrong behaviour).
