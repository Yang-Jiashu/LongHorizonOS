# Phase C2 — Demo: version_pinning

File: `examples/agent_os/context_version_pinning.py`

Gate: `test_demos.py::TestDemoScripts::test_version_pinning_demo`

## Behavior

- write artifact v1, then rewrite to v2
- build manifest pinned to v1
- load and verify bytes equal v1 (not v2)
- page_hash check matches v1
