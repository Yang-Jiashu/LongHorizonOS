# Architecture Audit — Phase C1

> Date: 2026-08-05

## Layer Dependencies

```
L4 Semantic Runtime (runtimes/)      ← does NOT import artifacts
     ↑
L3 System Services (services/)       ← imports artifacts, projections
     ↑
L2 Microkernel (kernel/)             ← does NOT import artifacts
     ↑
L1 Drivers (drivers/)                ← does NOT import services
```

## Import Graph Audit

| Module | Imports | Forbidden? |
|--------|---------|------------|
| `artifacts/models.py` | None (pure data) | No |
| `artifacts/uri.py` | None (pure parsing) | No |
| `artifacts/service.py` | kernel.errors, artifacts.*, drivers | No |
| `artifacts/projections.py` | kernel.storage | No |
| `drivers/local_artifact_storage.py` | only pathlib, os, hashlib | Does NOT import kernel ✓ |
| `sdk/artifact_sdk.py` | artifacts, namespace_service | No |

## No Circular Dependencies

No module in `artifacts/` or `drivers/` imports from `kernel/` directly
(except `kernel.errors` which is pure data, safe import).

`kernel/` does not import `artifacts/`.
`harnesses/` (L5) would not import `artifacts/` directly.

## Cargo Cult Check

All imports in artifact code are used. No "just in case" imports.

## Future VPG Integration Point

VPG Phase will become consumer of Artifact FS:
- Evidence binds to ArtifactVersion
- GraphPatch operations use Artifact URIs
- Snapshot support uses NamespaceSnapshot

Artifact FS remains unaware of VPG.
