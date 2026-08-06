# Phase C2 — Demos Spec

Generated: 2026-08-06T10:08:06.900252+00:00

## Flagship demos (5)

1. `context_basic_load.py` — load + inspect + close happy path.
2. `context_budget_eviction.py` — token_budget + eviction.
3. `context_version_pinning.py` — cross-write pin.
4. `context_snapshot_restore.py` — restart restore.
5. `context_process_isolation.py` — cross-PID denial.

## Runtime gate

`tests/agent_os/context/test_demos.py::TestDemoScripts`
- **demo_passed**: 5
- **demo_failed**: 0

## How to run

```
python -m examples.agent_os.context_basic_load
python -m examples.agent_os.context_budget_eviction
python -m examples.agent_os.context_version_pinning
python -m examples.agent_os.context_snapshot_restore
python -m examples.agent_os.context_process_isolation
```
