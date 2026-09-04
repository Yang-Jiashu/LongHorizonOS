# Incremental VPG History Benchmark

This is a deterministic, offline storage/consistency benchmark for sequential
small VPG patches. It measures durable entity-revision rows and the persisted
`READY_FRONTIER_UPDATED` event payload separately from end-to-end commit time.

## Reproduce

```bash
python scripts/benchmark_vpg_incremental_history.py --check
```

The default workload commits one new Task node per patch at
`N = 100, 200, 400`. The checker verifies:

- one durable node revision per newly added Task;
- historical snapshots reconstruct the expected latest projection;
- no unexpected edge revisions;
- snapshot-header counts and hashes remain consistent;
- entity-history payload stays within a linear budget; and
- READY frontier event payload stays within a linear budget.

The checked-in reference output is:

```text
artifacts/benchmark_results/vpg-incremental-history-2026-08-12-frontier-summary-final.json
```

## Reference result

| Patches | Node-history rows | History payload | READY frontier event payload | Database | Total commit time |
|---:|---:|---:|---:|---:|---:|
| 100 | 100 | 35,274 B | 11,892 B | 483,328 B | 0.895 s |
| 200 | 200 | 70,874 B | 23,892 B | 888,832 B | 4.032 s |
| 400 | 400 | 142,074 B | 47,892 B | 1,638,400 B | 17.202 s |

The old full-copy history baseline at `N=400` would contain 80,200 history
rows. Entity-revision history therefore reduces history-row count by 99.50% on
this workload. READY frontier events are persisted as a constant-size
`count` + SHA-256 summary (`summary-v1`); older databases containing a full
JSON list remain readable.

When a new compact event is read through `get_events()`, its full
`ready_frontier` tuple is intentionally empty and
`ready_frontier_count`/`ready_frontier_hash` carry the durable audit summary.
Use `query_ready_frontier(graph_id)` to derive the current ordered frontier
from the authoritative projection. Legacy full-list event rows still populate
`ready_frontier`.

## Interpretation and limits

These numbers establish linear durable-history and frontier-event payload
growth for this workload. They **do not** establish linear end-to-end commit
latency: each commit still decodes, copies, derives, validates, and hashes a
full candidate projection, and semantic commits are serialized by the
single-host SQLite writer. The elapsed times are one local reference run, not a
production latency or throughput guarantee.
