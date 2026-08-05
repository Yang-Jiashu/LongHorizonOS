# Journal and Projection Model

> Component: Artifact FS Event Sourcing
> Status: Implemented
> Date: 2026-08-05

## 1. Design

Artifact FS is event-sourced. All state changes are appended to the
journal before being applied to projections.

## 2. Events (see JournalService)

| Event | Payload |
|-------|---------|
| `namespace_created` | namespace_id, owner_pid, quota_bytes |
| `namespace_mounted` | mount_id, target_pid, source_ns_id, mode |
| `namespace_unmounted` | mount_id |
| `artifact_created` | artifact_id, namespace_id, artifact_uri |
| `version_committed` | artifact_id, version, content_hash, size, key |
| `artifact_deleted` | artifact_id |
| `handle_opened` | handle_id, artifact_id, pid, mode |
| `handle_closed` | handle_id |
| `watch_registered` | watch_id, pid, uri_prefix |
| `watch_triggered` | watch_id, artifact_uri, new_version |
| `transaction_started` | txn_id, artifact_id, expected_version |
| `transaction_committed` | txn_id, version |
| `transaction_aborted` | txn_id, reason |

Events are immutable and append-only.

## 3. Projections

Read-model tables in SQLite for efficient queries:
- `projection_namespaces`
- `projection_mounts`
- `projection_artifacts`
- `projection_artifact_versions`
- `projection_handles`

## 4. Replay

```
ArtifactProjections.clear()
for event in JournalService.list_events(order=ASC):
    apply(event)
```

Apply functions are deterministic. Replay produces identical state.

## 5. Rebuild Use Cases

1. **Crash recovery**: After journal replay, verify in-flight transactions
2. **New read replica**: Stand up new projection database from same journal
3. **Schema migration**: Replay into new projection schema

## 6. Implementation

- `src/lhos/services/journal.py` — JournalService
- `src/lhos/agent_os/artifacts/projections.py` — ArtifactProjections
