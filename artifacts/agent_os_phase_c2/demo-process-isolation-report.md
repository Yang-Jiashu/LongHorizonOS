# Phase C2 — Demo: process_isolation

File: `examples/agent_os/context_process_isolation.py`

Gate: `test_demos.py::TestDemoScripts::test_process_isolation_demo`

## Behavior

- p1 loads context, gets handle
- p2 attempts to inspect the same handle
- System raises `ErrHandleNotOwned`
- p2 load attempt with same ref_id but wrong pid also denied
