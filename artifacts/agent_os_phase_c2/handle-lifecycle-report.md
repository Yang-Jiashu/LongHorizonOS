# Phase C2 — Handle Lifecycle Report

Generated: 2026-08-06T10:08:06.900252+00:00

Tests: `test_handles.py`, `test_pinning.py`, `test_eviction.py`.

## Invariants

- Handle is owned by the loader PID.
- Close is idempotent.
- Inspect no longer raises on closed handle.
- Read after close raises `ErrInvalidManifest`.
- Cross-PID inspect/read raises `ErrHandleNotOwned`.
- Working-set lookup is scoped by (pid, working_set_id).
