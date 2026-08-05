# Artifact FS — Technical Specification (Phase C1)

> Component: L3 System Services
> Status: Implemented
> Date: 2026-08-05

## 1. Purpose

Provide versioned, content-addressed artifact storage with namespace isolation,
capability enforcement, lease protection, and atomic writes.

## 2. Core Objects

### 2.1 ArtifactRecord

Metadata for a single artifact (URI-addressed, version-series container).

| Field | Type | Semantics |
|-------|------|-----------|
| `artifact_id` | str | UUID, stable |
| `namespace_id` | str | Owner namespace |
| `artifact_uri` | str | Canonical URI |
| `latest_version` | int | Current committed version (0 = no version) |
| `created_at`, `updated_at` | datetime | Timestamps |
| `owner_pid` | str | Creating process |

### 2.2 ArtifactVersion

An immutable committed version of an artifact.

| Field | Type | Semantics |
|-------|------|-----------|
| `version` | int | 1-based, strictly monotonic |
| `artifact_id` | str | Parent |
| `content_hash` | str | SHA-256 hex of content |
| `size_bytes` | int | Content size |
| `idempotency_key` | str | Enables idempotent writes |
| `committed_at` | datetime | Commit time |
| `committed_by_pid` | str | Writer process |

### 2.3 WriteTransaction

Tracks the lifecycle of a write operation.

| Field | Type | Semantics |
|-------|------|-----------|
| `transaction_id` | str | UUID |
| `artifact_id` | str | Target |
| `expected_version` | int | For optimistic concurrency |
| `staged_uri` | str | Driver staging path |
| `target_uri` | str | Final destination |
| `status` | enum | pending / committed / aborted / uncertain |
| `idempotency_key` | str | Dedup key |

### 2.4 ArtifactHandle

File-descriptor-like access to an artifact.

| Field | Type | Semantics |
|-------|------|-----------|
| `handle_id` | str | UUID |
| `artifact_id` | str | Target |
| `opened_by_pid` | str | Owner process |
| `mode` | str | "read" or "write" |
| `opened_version` | int | Pinned version (read) |
| `lease_id` | str | Exclusive lease (write) |

### 2.5 ArtifactNamespace

Per-process URI isolation boundary.

| Field | Type | Semantics |
|-------|------|-----------|
| `namespace_id` | str | "ns-{pid}" |
| `owner_pid` | str | Owner process |
| `quota_bytes` | int | Storage limit |
| `created_at` | datetime | Creation time |

### 2.6 NamespaceMount

Cross-namespace visibility with capability control.

| Field | Type | Semantics |
|-------|------|-----------|
| `mount_id` | str | UUID |
| `target_pid` | str | Consumer |
| `target_prefix` | str | Mount point |
| `source_namespace_id` | str | Provider |
| `source_prefix` | str | Source sub-tree |
| `mode` | str | "shared_readonly" / "copy_on_write" |

## 3. URI Scheme

### 3.1 Artifact URI

```
artifact://<namespace_id>/<path>
```

Examples:
- `artifact://ns-p1/report.md`
- `artifact://ns-researcher/data/experiment.json`
- `artifact://ns-reviewer/shared/templates/doc.md`

### 3.2 Local Path Mapping

```
<store_root>/blobs/<content_hash>   — Content-addressed blobs
<store_root>/staging/<txn_id>       — Pending write staging
```

### 3.3 Canonical URI Normals

- Percent-decode exactly once
- Unicode NFC normalization
- Path segment collapse (`//` → `/`, `./` → removed)
- Reject `.` and `..` segments (path traversal defense)
- Reject empty segments, control characters, NUL

### 3.4 Defenses

- No drive letters (`c:`, `d:`)
- No backslash conversion
- No UNC paths (`//host/share`)
- No symlinks in storage tree (driver verify)
- No absolute host paths exposed to processes

## 4. Write Protocol

### 4.1 Optimistic Concurrency

```
WRITE_ARTIFACT:
  1. Parse and canonicalize URI
  2. Resolve namespace via mount chain
  3. Check capability (read or write) for pid on target
  4. If expected_version specified ≠ current_version → VERSION_CONFLICT
  5. Acquire write lease (exclusive, TTL-based)
  6. Generate transaction_id, idempotency_key
  7. Stage content to temporary location
  8. Commit via atomic rename
  9. Release lease on abort; keep on commit until all handles closed
```

### 4.2 Idempotency

System records `(pid, artifact_id, idempotency_key)` on successful commit.
Repeat calls within retention window return existing version without creating a new one.

### 4.3 Visibility

Staged content is invisible to readers. Only after atomic commit (which swaps
references) does new version become visible. Readers with pinned version handle
are unaffected by commit.

## 5. Version Monotonicity

- First write → version 1
- Each successful commit increases latest_version by exactly 1
- No version holes
- No version reuse after delete (delete is soft; version history preserved)

## 6. Handle Semantics

| Handle | Behavior |
|--------|----------|
| Read | Pins version; new writes don't affect pinned read |
| Write | Exclusive lease; second writer blocked |

## 7. Recovery Protocol

```
RECOVER:
  1. Query all transactions in "pending" state
  2. For each, inspect driver transaction marker:
     - COMMITTED → create ArtifactVersion, emit ArtifactVersionCommitted event
     - ABORTED → clean staging, mark transaction aborted
     - UNKNOWN → mark transaction UNCERTAIN (no external call made)
  3. Rebuild projections from full event stream (optional)
  4. Close orphaned handles (process no longer alive)
```

## 8. Projection Rebuild

```
REPLAY_ALL:
  1. Projections.clear()
  2. For each event in journal order:
     - NamespaceCreated → projections.apply_namespace_created(...)
     - NamespaceMounted → projections.apply_namespace_mounted(...)
     - ArtifactCreated → projections.apply_artifact_created(...)
     - ArtifactVersionCommitted → projections.apply_version_committed(...)
     - HandleOpened → projections.apply_handle_opened(...)
     - TransactionAborted → projections.apply_transaction_aborted(...)
  3. Projections now match event-sourced truth
```

## 9. Naming Convention

Artifact namespace IDs follow `ns-{pid}` convention. URIs are always
canonical `artifact://` form. The `workspace://` scheme is SDK-level sugar
that maps to `artifact://ns-{caller_pid}/...`.

## 10. Files

- `src/lhos/agent_os/artifacts/models.py` — Core objects
- `src/lhos/agent_os/artifacts/uri.py` — Canonical URI + defenses
- `src/lhos/agent_os/artifacts/errors.py` — Error hierarchy
- `src/lhos/agent_os/artifacts/namespace_service.py` — Namespace + Mounts
- `src/lhos/agent_os/artifacts/service.py` — ArtifactFSService
- `src/lhos/agent_os/artifacts/projections.py` — Read models
- `src/lhos/agent_os/drivers/local_artifact_storage.py` — CAS driver
