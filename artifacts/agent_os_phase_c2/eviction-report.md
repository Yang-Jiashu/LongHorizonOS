# Phase C2 — Eviction Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Deterministic order

`priority_stable_v1.select_pages_v1` + evict() walk pages in
priority order with lexical tie-break; output is the same for the
same inputs.

## Protection

- Required pages never appear in candidates.
- Pinned pages never appear in candidates; they are reported in
  `pinned_blocked`.
- Eviction stops once `freed_tokens >= target_tokens`.
- Eviction never frees more than needed to hit target.
