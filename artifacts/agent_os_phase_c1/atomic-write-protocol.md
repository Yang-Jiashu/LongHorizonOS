# Atomic Write Protocol — Artifact FS

> Component: Storage Driver + ArtifactFSService
> Status: Implemented
> Date: 2026-08-05

## 1. Goal

Ensure readers see only fully committed versions, never partial writes,
even under crash conditions.

## 2. Protocol

### 2.1 Multi-Version Concurrency Control (MVCC)

Each artifact has:
- A latest version number (atomic counter per artifact series)
- Each version maps to a content-addressed blob (hash)
- Staging area is separate from committed area

### 2.2 Write Sequence

```
1. Writer opens handle (acquires exclusive lease)
2. Writer stages content to temporary blob: <staging_dir>/<txn_id>
3. fsync staged blob
4. Writer commits:
   a. Write transaction marker: COMMITTED (with content_hash)
   b. fsync marker file
   c. Atomic append to journal: ArtifactVersionCommitted
   d. Update ArtifactRecord.latest_version
5. Reader reads committed version: <blob_dir>/<content_hash>
```

### 2.3 Atomicity Guarantee

- If crash before step 4: staging blob becomes orphan → GC later
- If crash at 4a (marker written): recovery creates version from marker
- If crash at 4b (journal append): recovery from journal sees event → consistent
- If crash after 4: full version visible to readers

### 2.4 Visibility

Staging blobs are not indexed by artifact URI. Readers can only access content
through committed version blob references. Commit is a single metadata update
that atomically swaps the "latest version" pointer.

## 3. Idempotency

Commit records `(pid, idempotency_key)` → `version`. Retention window:
24 hours (configurable). Duplicate commits within window return the existing
version without incrementing.

## 4. Content-Addressed Storage

Blobs are stored as `<root>/blobs/<sha256>`. Advantage:
- Natural deduplication
- Integrity verification on read (re-hash and compare)
- Crash-safe: blob content is immutable once written

## 5. Files

- `src/lhos/agent_os/drivers/local_artifact_storage.py` — staging, commit, read
- `src/lhos/agent_os/artifacts/service.py` — transaction orchestration
