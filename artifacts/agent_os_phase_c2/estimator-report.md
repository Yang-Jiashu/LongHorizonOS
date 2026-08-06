# Phase C2 — Token Estimator Report

Generated: 2026-08-06T10:08:06.900252+00:00

## DeterministicByteTokenEstimator

- estimator_id = `byte_x4_utf8_v1`
- estimate = ceil(decodeable characters / 4); fallback returns 1 byte.
- Pure function, no internal state.
- Stable across snapshots — estimator_id recorded alongside
  materialized_hash.
