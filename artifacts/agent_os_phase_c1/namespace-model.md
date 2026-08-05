# Namespace Model — Artifact FS

> Component: NamespaceService
> Status: Implemented
> Date: 2026-08-05

## 1. Purpose

Provide per-process URI isolation. Each process gets its own namespace
by default, preventing accidental cross-contamination.

## 2. Namespace ID Convention

```
ns-{pid}
```

Example: Process `p123` has namespace `ns-p123`.

## 3. Namespace Lifecycle

```
create_namespace(pid)
  → ArtifactNamespace created with owner_pid=pid
  → Default quota (unlimited or configurable)

get_namespace(pid)
  → Returns namespace or auto-creates
```

## 4. Mount

A mount makes one namespace's artifacts visible under a sub-path
of another namespace.

```
mount(target_pid, mount_point, source_pid, source_prefix="", mode="shared_readonly")
  → NamespaceMount record
  → target can now access source's artifacts at artifact://ns-target/mount_point/...
```

### 4.1 Mount Resolution

When resolving a URI, the service checks:
1. Is the URI within the caller's namespace? → direct access
2. Is the URI under a mount point? → resolve through mount to source namespace
3. Otherwise → not found

### 4.2 Mount Modes

| Mode | Read | Write |
|------|------|-------|
| `shared_readonly` | ✓ | ✗ |
| `shared_readwrite` | ✓ | ✓ (requires write cap) |
| `copy_on_write` | ✓ (first read may copy) | ✓ (on first write, promotes to local) |

## 5. Snapshot

Immutable point-in-time capture of namespace state:
- Which artifacts exist
- Which version each artifact is at

Used by VPG for evidence binding.

## 6. Quota

Per-namespace byte limit. Default: unlimited.
When set, writes that would exceed are rejected with `QuotaExceeded`.

## 7. Files

- `src/lhos/agent_os/artifacts/namespace_service.py` — NamespaceService
- `src/lhos/agent_os/artifacts/models.py` — ArtifactNamespace, NamespaceMount, NamespaceSnapshot
