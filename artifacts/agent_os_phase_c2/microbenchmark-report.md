# Phase C2 — Microbenchmark Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Summary

- **Test file**: `tests/agent_os/context/test_microbenchmarks.py`
- **micro_passed**: 7
- **micro_failed**: 0

## Workloads

| # | Operation | Count |
|---|-----------|-------|
| 1 | Manifest validation | 1000 |
| 2 | Page splitting | 1000 |
| 3 | Policy selection | 1000 |
| 4 | Snapshot creation | 100 |
| 5 | Snapshot restore | 100 |
| 6 | Eviction pass | 100 |
| 7 | Journal event projection | 100 |

Each benchmark reports p50 / p95 / p99 latency (µs/operation) plus
median throughput. Gate: p50 < soft ceiling, p99 < hard ceiling.
