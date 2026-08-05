# Crash Recovery Report — Artifact FS

> Date: 2026-08-05

## Protocol

Artifact FS uses a write-ahead transaction protocol:

1. **Stage**: Write content to staging blob
2. **Intent**: Record transaction marker (STAGED)
3. **Commit**: Atomically update version pointer + journal event
4. **Orphan cleanup**: Remove staging blobs of old aborted/committed txns

## Recovery Cases

| Crash Point | Recovery Action |
|------------|-----------------|
| After stage, before commit | Transaction found in STAGED state → clean staging → mark aborted |
| During commit | If journal event missing, inspect driver marker → if committed recreate version |
| After commit | Data consistent, idempotent on replay |
| Unknown (disk failure) | Mark UNCERTAIN — do not retry external call |

## Tests

- `tests/agent_os/artifacts/test_recovery.py` — projection rebuild
- `tests/agent_os/artifacts/test_demo.py::demo_recovery` — idempotency check

## Idempotent Re-commit

Replaying a committed transaction (same idempotency_key) does NOT create a
new version. Returns existing version (HTTP 200-like semantics).

## Projection Rebuild

After journal replay for new projections:
```
delete from projection_namespaces;
delete from projection_mounts;
...
for event in ordered_events:
    apply(event to projections)
```

Result: projections identical to state before restart.

## Files

- `src/lhos/agent_os/artifacts/service.py` — recover() method
- `tests/agent_os/artifacts/test_recovery.py`
- `examples/agent_os/crash_recovery.py`
