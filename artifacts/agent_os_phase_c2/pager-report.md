# Phase C2 — Deterministic Pager Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Invariants

- Contiguous, non-overlapping, no-gap ranges covering `[0, len(bytes)]`.
- Byte ranges are stable for the same input.
- Empty content produces a single empty range `[0, 0]`.
- Page boundary crossings raise `ErrInvalidRange`.
- `page_size_bytes` parameterisation produces identical page count
  for the same input.
