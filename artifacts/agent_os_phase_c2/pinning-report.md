# Phase C2 — Pinning Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Refcount semantics

- `pin(page_id)` increments refcount.
- `unpin(page_id)` decrements refcount.
- Multiple pins increment independently; only at refcount 0 can
  the page be evicted.
- Pin of a non-existent page raises `ErrInvalidManifest`.
- Pin returns True when refcount transitions 0→1.
