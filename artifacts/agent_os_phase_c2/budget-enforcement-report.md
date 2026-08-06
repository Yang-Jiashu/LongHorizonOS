# Phase C2 — Budget Enforcement Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Token budget

- Required refs always included; optional refs drop last.
- Required-ref overflow raises `ErrRequiredBudgetExceeded`.
- manifest_hash is stable for the same input.

## Byte budget

- `tokens_used <= token_budget`
- `bytes_used <= byte_budget`
- Refs selected by `priority_stable_v1` in priority order,
  stable lexical tie-break.

## Idempotency

Two loads with the same `(pid, manifest_hash, idempotency_key)` return
the same handle (pointer equality).
