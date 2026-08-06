# Phase C2 — Process Isolation Report

Generated: 2026-08-06T10:08:06.900252+00:00

## Enforcement

- A handle is absolutely bound to its loader's PID.
- Cross-PID inspect / read / pin / unpin raises `ErrHandleNotOwned`.
- Working-set lookup walks only the calling PID's handle map.
- Snapshot `pid` field is checked against the restoring PID.

## Failure modes

- p2 attempts to read p1's handle → ErrHandleNotOwned.
- p2 attempts to restore p1's snapshot → ErrCapabilityDenied.
