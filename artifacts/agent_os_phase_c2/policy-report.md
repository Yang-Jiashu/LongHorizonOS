# Phase C2 — Policy Report (`priority_stable_v1`)

Generated: 2026-08-06T10:08:06.900252+00:00

## Sort key

`(-priority, not required, estimated_tokens, byte_start, page_id)`

Tie-break: lexical page_id (NOT random).

## Selection algorithm

- Walk sorted pages; accumulate tokens and bytes.
- Skip optional pages that would exceed budget.
- Required pages are never skipped.
- Required-ref overflow raises `ErrRequiredBudgetExceeded`.

## Determinism

Same input → same selected_pages on every call.
