# Phase C1 Deliverables Report — Versioned Artifact File System and Namespace

## Overview

Phase C1 implements a minimal yet semantically complete file system and process resource view for Agent OS. It provides Agent Processes with isolated, versioned, and permission-controlled access to artifacts via the Agent OS SDK, abstracting away host system details.

## Git Commits

| # | Commit | Description |
|---|--------|-------------|
| 1 | `c97feec` | Define artifact and namespace domain models |
| 2 | `d00f28e` | Canonical artifact URI parser with path traversal defense |
| 3 | `c3edfde` | Versioned local artifact storage driver with CAS |
| 4 | `c065fad` | Atomic write transactions with recovery and journal replay |
| 5 | `cde004d` | Namespace mounts, snapshots, and copy-on-write semantics |
| 6 | `9d6fa8a` | Artifact watches with signal delivery, quota enforcement, and SDK API |
| 7 | `c9e56dd` | Adversarial tests, demo script, and microbenchmarks |

**Base tag**: `agent-os-phase-b-audit-v1` → `68927b3`

## Source Files

### Core Modules (2,247 lines)

| File | Lines | Description |
|------|-------|-------------|
| `src/lhos/agent_os/artifacts/models.py` | 246 | Pydantic domain models: ArtifactRecord, ArtifactVersion, ArtifactHandle, WriteTransaction, ArtifactNamespace, NamespaceMount, NamespaceSnapshot, ArtifactWatch |
| `src/lhos/agent_os/artifacts/errors.py` | 181 | Custom exceptions: ArtifactNotFound, VersionConflict, QuotaExceeded, PathTraversalRejected, IdempotencyConflict, etc. |
| `src/lhos/agent_os/artifacts/uri.py` | 196 | Canonical URI parser: percent-decode, NFC, path traversal rejection, Windows path rejection |
| `src/lhos/agent_os/artifacts/projections.py` | 480 | SQLite projection read/write helpers for all artifact tables |
| `src/lhos/agent_os/artifacts/namespace_service.py` | 253 | NamespaceService: create/delete namespaces, mounts, snapshots, quota management |
| `src/lhos/agent_os/artifacts/service.py` | 890 | ArtifactFSService: read/write/commit/abort, handle management, recovery, watch signals, quota enforcement |

### SDK (343 lines)

| File | Lines | Description |
|------|-------|-------------|
| `src/lhos/agent_os/sdk/artifact_sdk.py` | 192 | High-level ArtifactSDK facade: read, write, list, stat, open, close, mount, snapshot, watch |
| `src/lhos/agent_os/sdk/client.py` | 150 | `create_kernel_with_artifacts()` factory wiring all services |

### Storage Driver (389 lines)

| File | Lines | Description |
|------|-------|-------------|
| `src/lhos/agent_os/drivers/local_artifact_storage.py` | 389 | Content-addressable storage (CAS): staging, atomic commit, dedup, recovery |

### Schema (300 lines)

| File | Lines | Description |
|------|-------|-------------|
| `src/lhos/agent_os/storage/schema.py` | 300 | DDL for 9 artifact tables + 11 indexes |

## Test Files (2,324 lines)

| File | Lines | Tests | Description |
|------|-------|-------|-------------|
| `test_uri_canonicalization.py` | 191 | 22 | URI parsing, normalization, path traversal rejection |
| `test_local_storage_driver.py` | 250 | 21 | CAS staging, commit, abort, recovery, dedup |
| `test_artifact_service.py` | 399 | 30 | Read, write, commit, abort, version conflict, idempotency, recovery, projection rebuild |
| `test_mounts_snapshots.py` | 221 | 24 | Mount creation, resolution, shared_readonly, COW, snapshots, projection rebuild |
| `test_watches_quota_sdk.py` | 397 | 28 | Watch signal delivery, quota enforcement, SDK API |
| `test_adversarial.py` | 480 | 28 | Concurrent writes, COW isolation, handle leaks, quota bypass, mount traversal, idempotency replay |
| `test_benchmark.py` | 183 | 7 | Microbenchmarks: writes, reads, version updates, mount reads, snapshots, watches, COW |
| `demo.py` | 202 | — | 6-scenario demo script |

**Artifact test total**: 162 tests (all passing)

## Test Counts

| Scope | Count |
|-------|-------|
| Phase C1 artifact tests | 162 |
| Phase B agent_os tests | 207 |
| Agent OS total | 369 |
| Legacy tests | 380 |
| **Full suite** | **749** |

All 749 tests pass. Ruff clean. Mypy clean (35 source files).

## Implemented Features

### 1. Canonical URI System
- `artifact://<namespace-id>/<path>` canonical form
- `workspace:///` → `artifact://ns-<pid>/` resolution
- Percent-decode (once), Unicode NFC normalization
- Path traversal (`..`) rejection, Windows drive/UNC path rejection
- Backslash rejection, NUL/control character rejection
- Idempotent canonicalization

### 2. Content-Addressable Storage (CAS)
- SHA-256 content addressing: `cas/<hash[:2]>/<hash>`
- Staging directory for in-progress writes
- Atomic commit via `os.rename` (with cross-device fallback)
- Content deduplication
- Orphaned staging file recovery

### 3. Versioned Artifacts
- Immutable committed versions (version numbers start at 1, strictly increment)
- Parent version tracking
- Version pinning (read handles pin specific versions)
- Soft delete (mark as deleted, preserve versions)
- Metadata (content_hash, size_bytes, committed_by_pid, committed_action_id)

### 4. Atomic Write Transactions
- Begin → Stage → Commit/Abort protocol
- Optimistic concurrency control (expected_version)
- Idempotency keys (replay returns same result)
- States: open → staged → committed/aborted/conflicted/uncertain
- Journal event recording for all state transitions

### 5. Crash Recovery
- UNCERTAIN transaction resolution (check CAS for committed content)
- Orphaned staging file cleanup
- Projection rebuild from journal events
- Recovery is idempotent

### 6. Namespace Isolation
- Per-process private namespace: `ns-<pid>`
- Quota management (bytes, max_open_handles)
- Usage reporting (total_bytes, artifact_count, version_count, quota_used_pct)

### 7. Namespace Mounts
- **shared_readonly**: Target can read source namespace artifacts
- **copy_on_write**: Target reads from source, writes create local copy
- **private**: Mount only visible to target (defined, not heavily used)
- **shared_readwrite**: Defined but deferred (not required for C1 gate)
- Mount point normalization and path resolution
- Unmount support

### 8. Namespace Snapshots
- Immutable manifest of artifact URI → version
- Content refs captured for each version
- Snapshots survive journal rebuild
- Snapshot retrieval by ID

### 9. Artifact Watches
- Register watch by URI prefix
- ARTIFACT_CHANGED signal delivered to watching processes on commit
- Writer process excluded from self-notification
- Prefix matching (only matching URIs trigger signals)
- Multiple watchers supported
- Unwatch (deactivate)
- Watches survive journal rebuild

### 10. SDK API
- `ArtifactSDK` facade class
- File operations: read, read_text, write, stat, list, list_versions, delete, exists
- Handle operations: open, close, close_all
- Namespace operations: create_namespace, get_namespace, set_quota, get_usage
- Mount operations: mount, unmount, list_mounts
- Snapshot operations: snapshot, get_snapshot
- Watch operations: watch, unwatch, list_watches
- Recovery: recover()
- `create_kernel_with_artifacts()` factory for full integration

## Security Properties

- **Path traversal**: Rejected at URI canonicalization layer
- **Symlink TOCTOU**: CAS uses direct file I/O, no symlink following
- **Namespace isolation**: Processes cannot access other namespaces without explicit mounts
- **Quota enforcement**: Checked at stage time, cannot be bypassed by commit
- **Handle exhaustion**: Max 64 open handles per process (configurable)
- **Idempotency replay**: Aborted/conflicted transactions raise on replay
- **Version rollback**: Old expected_version rejected after new commit

## Benchmark Results

| Operation | Throughput | Latency |
|-----------|-----------|---------|
| Sequential writes | ~1,600 ops/s | 0.62ms/op |
| Sequential reads | ~16,700 ops/s | 0.06ms/op |
| Version updates | ~1,600 ops/s | 0.62ms/op |
| Mount read-through | ~15,700 ops/s | 0.06ms/op |
| Snapshot creation (50 artifacts) | — | 1.01ms |
| Write + watch signal | ~1,500 ops/s | 0.67ms/op |
| COW write (local copy) | ~1,750 ops/s | 0.57ms/op |

## Adversarial Test Coverage

- Concurrent write conflicts (optimistic concurrency)
- COW isolation (writes don't leak to source)
- Handle leak and exhaustion
- Quota bypass attempts
- Mount path traversal
- Idempotency replay attacks
- Delete-then-read races
- Version rollback attempts
- Namespace isolation violations
- Watch signal injection and leakage
- Crash recovery edge cases

## Prohibitions Compliance

- ✅ No real LLM/Browser/Shell integration
- ✅ No VPG migration
- ✅ No Context VM
- ✅ No real network I/O
- ✅ All storage is local filesystem + SQLite
- ✅ All models are Pydantic BaseModels
- ✅ All operations are synchronous (no async in artifact layer)

## Working Directory Status

CLEAN — `git status --short` returns empty output after commit.
