# Phase C2 — SDK Facade Report

Generated: 2026-08-06T10:08:06.900252+00:00

## ContextSDK public API

- `load(pid, manifest, idem="") → (ContextHandle, LoadedContext)`
- `inspect(pid, handle_id) → ContextHandle`
- `read(pid, handle_id, ...) → bytes`
- `snapshot(pid, context_id, idem="") → ContextSnapshot`
- `restore(pid, snapshot_id, idem="") → (ContextHandle, LoadedContext)`
- `evict(pid, working_set_id, target_tokens) → dict`
- `pin(pid, page_id) → bool`
- `unpin(pid, page_id) → bool`

Public surface matches the package `__init__.py` exports and the
README.
