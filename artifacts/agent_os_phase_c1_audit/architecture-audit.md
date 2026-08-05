# Architecture Boundary Audit

> Verify kernel/runtimes/harnesses separation and driver isolation.

## Import Graph Checks

| Rule | Result |
|------|--------|
| Kernel does NOT import artifacts | PASS |
| Kernel does NOT import runtimes | PASS |
| Kernel does NOT import harnesses | PASS |
| Artifact service does NOT import VPG/Planner/Task | PASS |
| StorageDriver does NOT import JournalService | PASS |
| StorageDriver does NOT import ProcessService | PASS |
| StorageDriver does NOT import LeaseService | PASS |
| StorageDriver does NOT import SignalService | PASS |
| SDK does NOT expose host paths | PASS |
| Driver does NOT call ProcessService | PASS |
| Driver does NOT acquire/release Kernel Lease | PASS |
| Driver does NOT send Signal | PASS |

## Minor Findings

| Finding | Severity |
|---------|----------|
| 5 demo files import `LocalArtifactStorageDriver` directly (for setup) | LOW — standalone demos, not production code |
| `examples/agent_os/` artifacts perform plumbing (not direct FS access) | OK |
| `sdk/client.py` imports driver for composition root | OK |

## Verdict

PASS — Layer boundaries are correctly enforced at the framework layer. No cross-layer violations in production code.
