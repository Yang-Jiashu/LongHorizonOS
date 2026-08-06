# Phase C2 — Demo: budget_eviction

File: `examples/agent_os/context_budget_eviction.py`

Gate: `test_demos.py::TestDemoScripts::test_budget_eviction_demo`

## Behavior

- write multiple artifacts with diverging priorities
- set tight token_budget to force optional ref omission
- verify selected_pages drop within budget (required preserved)
- run eviction pass with `target_tokens`
- report evicted_pages / tokens_freed / pinned_blocked
