# Phase C2 — Projection Replay Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Events emitted

- CONTEXT_MANIFEST_ACCEPTED
- CONTEXT_LOAD_STARTED
- CONTEXT_PAGE_MATERIALIZED
- CONTEXT_SNAPSHOT_CREATED
- CONTEXT_SNAPSHOT_RESTORED
- CONTEXT_PAGE_EVICTED
- CONTEXT_EVICTION_COMPLETED
- CONTEXT_RECOVERY_COMPLETED

## Determinism

Replaying from a rebuilt projection yields the same
`materialized_hash`, `tokens_used`, `bytes_used` as the live state.
