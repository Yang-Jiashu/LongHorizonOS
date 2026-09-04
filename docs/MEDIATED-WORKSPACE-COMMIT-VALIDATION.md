# Mediated workspace commit validation

`WorkspaceProvenanceGateway` provides a bounded provenance boundary for
filesystem access. Reads and writes performed through the gateway are recorded
in the bound `ExecutionContext` with a canonical path, byte hash, optional
version, and gateway identity.

## Commit-time read check

Before semantic Evidence commit, the SDK now re-reads the latest mediated
workspace read-set and compares the current byte hashes:

```python
report = gateway.validate_read_set_current()
if not report.current:
    # stale, deleted, unknown, or truncated observation
    ...

gateway.require_read_set_current()  # raises on a non-current set
```

The built-in `AgentOS.run()` and `run_async()` paths invoke this check as an
additional pre-commit fence. A changed or deleted file is quarantined as
`STALE_COGNITION` / `READ_SET_UNAVAILABLE` and cannot publish `VERIFIED`
Evidence.

## Deliberate boundary

This is a **mediated, single-host, point-in-time** check:

- it covers only reads that crossed this `WorkspaceProvenanceGateway`;
- direct `open`/`Path`/shell/browser/network access remains outside coverage;
- it is not a filesystem lock and does not make workspace bytes, Facts, VPG,
  and external side effects one atomic transaction;
- the validation is bounded to 256 resources per gateway by the SDK commit path
  (the public helper accepts 1–4096).

Facts/Artifact version and hash guards remain the semantic authority. This
extra check closes the narrower race where a workspace file is changed before
commit but its next Artifact/Facts version has not yet been registered.

