# Phase C1 Baseline Version — Audit-Passed Phase B

## Tags

- **Phase B original tag**: `agent-os-phase-b-v0` → `23d2f1b`
- **Phase B.1 audit tag**: `agent-os-phase-b-audit-v1` → `68927b3`

## Git Commit

```
68927b3 fix(agent-os): Phase B.1 audit — journal rebuild, signal replay, deterministic timestamps
23d2f1b chore: milestone 2/3 leftover changes
498663f test(agent-os): 137 tests, 5 demo scenarios, and demo runner script
17f6b61 feat(agent-os): kernel loop, dispatcher, drivers, programs, SDK, signals
fcdfb08 feat(agent-os): action, capability, and lease services
132c3db feat(agent-os): storage layer, journal service, and process service
9769188 feat(agent-os): kernel domain models — PCB, ACB, state machines, errors
```

## Audit Bugs Fixed (3)

### Bug 1: event_cursor Not Restored During Journal Rebuild
- **File**: `src/lhos/agent_os/services/process_service.py`
- **Root Cause**: `handle_event` did not restore `event_cursor` from journal offset during replay
- **Fix**: Added `pcb.event_cursor = ev.journal_offset` in PROCESS_STATE_CHANGED handler

### Bug 2: Non-Deterministic finished_at Timestamps
- **File**: `src/lhos/agent_os/services/action_service.py`
- **Root Cause**: `handle_event` used `datetime.utcnow()` during replay instead of event timestamp
- **Fix**: Changed to use `ev.created_at` for `finished_at` when processing replayed events

### Bug 3: SIGNAL_CONSUMED Events Not Handled During Replay
- **File**: `src/lhos/agent_os/services/signal_service.py`
- **Root Cause**: `handle_event` had no handler for `SIGNAL_CONSUMED` events
- **Fix**: Added handler to mark signals as consumed during journal replay

## Test Counts

| Scope | Count |
|-------|-------|
| Original Phase B tests | 137 |
| Audit tests (Phase B.1) | 70 |
| Agent OS total | 207 |
| Legacy tests | 380 |
| **Full suite** | **587** |

All 587 tests pass. Ruff clean. Mypy clean (26 source files).

## Working Directory Status

CLEAN — `git status --short` returns empty output after commit.
