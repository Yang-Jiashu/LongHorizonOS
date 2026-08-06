# Phase C2 — Recovery Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Scenarios

- Service restart, no journals in flight.
- Service restart after partial eviction.
- Service restart after partial snapshot.
- Service restart with stale projection (projection cleared, replay).

## Oracle

Any recovered context has identical `materialized_hash` to the prior
state.
