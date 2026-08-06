# Phase C2 — Version-Binding Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Scope

Version-pinned content refs, deterministic page splitting,
snapshot materialized-hash integrity across artifact rewrites.

## Test files

- `tests/agent_os/context/test_version_binding.py`
- `tests/agent_os/context/test_snapshot.py`
- `tests/agent_os/context/test_determinism.py`

## Key invariants

1. A context bound to artifact version N always serves bytes from
   version N even after the artifact is rewritten.
2. `materialized_hash` is computed from the ordered list of
   canonical_uri/version/content_hash tuples AND the ordered content
   bytes (`.hex()` encoded), so different content → different hash.
3. Snapshot restore re-reads artifact bytes from the ArtifactFS and
   verifies content_hash + page_hash before building a LoadedContext.
4. Tampering with a binding's content_hash raises
   `ErrSnapshotCorrupt` (KILLS mutation CVM-08 / CVM-11 / CVM-15).

## Determinism (100× same manifest)

See `determinism-report.md`.
