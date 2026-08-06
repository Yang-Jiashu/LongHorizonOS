# Phase C2 — SIGKILL Recovery Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Summary

- **Test file**: `tests/agent_os/context/test_sigkill_recovery.py`
- **sigkill_passed**: 5
- **sigkill_failed**: 0

## Scenarios (5 × 20 runs each)

| # | Scenario |
|---|----------|
| 1 | SIGKILL mid-load |
| 2 | SIGKILL mid-snapshot |
| 3 | SIGKILL mid-eviction |
| 4 | SIGKILL mid-restore |
| 5 | After restart all handles valid |

## Oracle

- No partial state leaks across restarts
- All recovered contexts have identical `materialized_hash`
- Journal events are exactly-once idempotent
