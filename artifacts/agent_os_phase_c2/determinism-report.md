# Phase C2 — Determinism Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Summary

- **determinism_passed**: 12
- **determinism_failed**: 0

## Scenarios

| Scenario | N runs | Status |
|----------|--------|--------|
| Same manifest, same service | 100 | verified |
| Same manifest, restart service | 20 | verified |
| Same manifest, rebuild projections | 20 | verified |
| Shuffled input (5 runs) | 5 x 20 | verified |

Test file: `tests/agent_os/context/test_determinism.py`.

## Invariants checked per run

- `tokens_used` equal
- `materialized_hash` equal
- selected_pages order identical
- byte offsets identical

Any divergence fails the scenario.
