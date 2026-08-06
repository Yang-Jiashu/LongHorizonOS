# Phase C2 — Adversarial Manifest Corpus

Generated: 2026-08-06T10:08:06.900252+00:00

## Summary

- **Test file**: `tests/agent_os/context/test_adversarial.py`
- **adversarial_passed**: 516
- **adversarial_failed**: 0

## Corpus size

- 500 random manifests
- 17 edge-case manifests (duplicate ref_id, wrong hash, empty
  content, page ranges, oversized content, negative budget, nested
  refs, missing version, zero-byte pages, high-priority ordering,
  deeply nested required chains, pinning storm, snapshot storm,
  eviction under pressure, cross-PID capability, projection replay,
  idempotency storm).

## Manifest coverage

Each random manifest exercises:
- manifest validation (`validate_manifest`)
- pager (`split_into_pages`)
- policy (`select_pages_v1`)
- capability checks (`_cap.can_context_operation`)
- snapshot/restore save path
- eviction candidate selection
- journal event emission

## Oracles

- `ErrDuplicateRefId` for duplicate refs
- `ErrInvalidContentHash` for wrong hash
- `ErrRequiredBudgetExceeded` for overflow
- `ErrInvalidManifest` for missing version
- Snapshot restore rejects tampered bindings (`ErrSnapshotCorrupt`)
- Cross-PID access raises `ErrHandleNotOwned`
