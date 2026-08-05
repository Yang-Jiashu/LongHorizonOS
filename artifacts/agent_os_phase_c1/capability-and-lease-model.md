# Capability and Lease Model

> Component: Artifact FS + Kernel integration
> Status: Implemented
> Date: 2026-08-05

## 1. Purpose

Control which process can do what to which artifact.

## 2. Capabilities

| Capability | Meaning |
|-----------|---------|
| `read` | Read any version of the artifact |
| `write` | Create new versions (requires handle) |
| `delete` | Soft-delete the entire series |
| `mount` | Allow this namespace to be mounted by others |
| `grant` | Delegate capability to another process |

## 3. Lease

Write handle holds an exclusive lease on the artifact series.
- Only one writer per series at a time
- Lease TTL (default: 60s, renewable)
- Process crash → lease expires → next writer can acquire

## 4. Mount + Capability

Cross-namespace access requires BOTH:
1. **Mount visibility**: target_pid has a mount pointing to source namespace
2. **Capability**: target_pid has the required capability on the target artifact

Mount modes:
- `shared_readonly`: can read, cannot write
- `shared_readwrite`: can read and write (requires explicit `write` cap)
- `copy_on_write`: read creates local copy on first write

## 5. Enforcement Order

```
1. Parse URI → canonical form
2. Check capability (read/write) for (pid, canonical_uri)
3. Resolve mount chain if uri is from another namespace
4. Verify handle ownership (opened_by_pid == pid)
5. Perform operation
```

## 6. Error Hierarchy

| Error | Condition |
|-------|-----------|
| `CapabilityDenied` | pid lacks capability |
| `HandleNotOwned` | pid doesn't own the handle |
| `ArtifactNotFound` | Artifact doesn't exist |
| `VersionConflict` | expected_version mismatch |
| `VersionNotFound` | Pinned version doesn't exist |
| `QuotaExceeded` | Write would exceed namespace quota |
| `LeaseRequired` | Write without handle/lease |
| `InvalidURI` | URI fails canonicalization |
| `PathTraversalError` | Canonicalization catches escape |
| `ArtifactDeleted` | Artifact was soft-deleted |

## 7. Files

- `src/lhos/agent_os/artifacts/errors.py` — Error classes
- `src/lhos/agent_os/artifacts/service.py` — Capability checks
- `src/lhos/agent_os/artifacts/namespace_service.py` — Mount management
