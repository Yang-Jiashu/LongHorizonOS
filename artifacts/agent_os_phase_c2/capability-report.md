# Phase C2 — Capability Report

Generated: 2026-08-06T10:08:06.900252+00:00

## per-operation gating

- `can_context_operation(...)` gates: manifest accept, handle read,
  snapshot create, eviction trigger, projection replay.
- `can_artifact_read(...)` gates content bytes retrieval.

## Denial

- Raises `ErrCapabilityDenied` at the SDK boundary.
- `_AllowsAllCaps` is the default test fixture.
- `_DenyAllCaps` is used for denial-only tests.
