# Phase C2 — Demo: snapshot_restore

File: `examples/agent_os/context_snapshot_restore.py`

Gate: `test_demos.py::TestDemoScripts::test_snapshot_restore_demo`

## Behavior

- load a context
- snapshot it
- restart the context service (`new_svc`)
- restore snapshot in fresh service
- materialized_hash restored equals original
