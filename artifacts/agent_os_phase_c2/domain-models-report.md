# Phase C2 — Domain Models Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Models in `src/lhos/agent_os/context/models.py`

| Model | Purpose | Key fields |
|-------|---------|------------|
| `ContentRef` | Pinned content pointer | ref_id, canonical_uri, artifact_id, version, content_hash, media_type, priority, required, start_byte, end_byte |
| `ContextManifest` | Load request | owner_pid, refs, token_budget, byte_budget, page_size_bytes, manifest_id |
| `ContextPage` | Page metadata | page_id, canonical_uri, artifact_id, version, content_hash, byte_start, byte_end, required, priority, media_type, encoding, estimated_tokens |
| `WorkingSet` | Page set + budgets | working_set_id, context_id, selected_ids, tokens_used, bytes_used |
| `ContextHandle` | Loaded context ref | handle_id, pid, manifest_hash, working_set_id, pinned_page_ids, created_at |
| `LoadedContext` | Materialized result | context_id, handle_id, ordered_pages, materialized_hash, token_budget, tokens_used, byte_budget, bytes_used, estimator_id |
| `ContextSnapshot` | Immutable snapshot | snapshot_id, pid, context_id, page_bindings, materialized_hash, parent_snapshot_id, created_at |
| `LoadedPage` | Materialized page | page_id, canonical_uri, artifact_id, version, content_hash, page_hash, byte_start, byte_end, size_bytes, required, priority, media_type, encoding, estimated_tokens, content |

## Errors in `src/lhos/agent_os/context/errors.py`

- `ErrInvalidManifest`
- `ErrDuplicateRefId`
- `ErrInvalidContentHash`
- `ErrRequiredBudgetExceeded`
- `ErrBudgetExceeded`
- `ErrHandleNotOwned`
- `ErrSnapshotCorrupt`
- `ErrCapabilityDenied`
- `ErrInvalidRange`

## Hashing

`_deterministic_hash` uses SHA-256 over a stable str-join. `materialized_hash`
includes the ordered content bytes (`.hex()`) to distinguish content
refactorings.

## Page id

`_stable_page_id` derives a deterministic id from artifact_id + byte
range (UUID5-like). Same manifest → same page ids.
