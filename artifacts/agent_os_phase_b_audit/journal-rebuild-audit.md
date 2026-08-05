# Journal Rebuild Audit — Phase B

## Objective

Verify that all projection state can be rebuilt from the Journal alone, with no reliance on in-memory state, and that replay is deterministic.

## Test File

`tests/agent_os/test_audit_journal_rebuild.py` — 5 tests, all PASSED.

## Invariants Verified

### 1. All Projections Rebuild from Journal After Restart

**Test**: `test_all_projections_rebuild_from_journal_after_restart`

**Methodology**:
1. Run a Demo A scenario (spawn → submit model action → process event → exit) on a file-based SQLite DB.
2. Take a full snapshot of all projection tables (processes, actions, leases, signals, program_states).
3. Close the kernel and **delete all projection tables** via direct SQL `DELETE FROM`.
4. Reopen the DB and call `rebuild_from_journal(db_path)`.
5. Compare the rebuilt state against the snapshot.

**Result**: ✅ PASS

- `journal_count`: identical (events persisted in `journal_events` table, not affected by projection deletion).
- `journal_offset`: identical (restored from `journal_meta` table).
- `processes`: PID, state, program_id, exit_code all match. `event_cursor` > 0 (restored from journal replay).
- `actions`: action_id, state, device_type, operation all match.
- `leases`: count matches (both empty after process exit).
- `signals`: signal_id, consumed state all match.

### 2. Replay Does Not Depend on In-Memory State

**Test**: `test_replay_does_not_depend_on_in_memory_state`

**Methodology**:
1. Run a demo on file-based DB.
2. Close kernel (all in-memory state lost).
3. Create a **brand new** kernel object via `rebuild_from_journal(db_path)` — no shared state.
4. Verify process state is EXITED, actions are COMMITTED.

**Result**: ✅ PASS — Rebuilt kernel has no reference to the original kernel's in-memory state.

### 3. Replay Is Deterministic Across Three Rebuilds

**Test**: `test_replay_is_deterministic_across_three_rebuilds`

**Methodology**:
1. Run a multi-action demo (model action + device action).
2. Close kernel.
3. Rebuild from journal **3 times**, each with a fresh kernel object.
4. Serialize full snapshot to JSON (with `sort_keys=True`).
5. Assert all 3 snapshots are byte-identical.

**Result**: ✅ PASS — All 3 rebuilds produced identical state.

**Key fix during audit**: `ActionService.handle_event` was using `datetime.utcnow()` for `finished_at` during replay. Fixed to use `ev.created_at` (the journal event's timestamp), ensuring deterministic timestamps.

### 4. Replay Preserves Consumed Signal State

**Test**: `test_replay_preserves_consumed_signal_state`

**Methodology**:
1. Run a demo that generates and consumes signals.
2. Record `consumed` count before closing.
3. Rebuild from journal.
4. Verify `consumed` count matches.

**Result**: ✅ PASS

**Key fix during audit**: `SignalService.handle_event` was not handling `SIGNAL_CONSUMED` events during replay. Fixed to process these events and restore `consumed=True` state.

### 5. Replay Preserves UNCERTAIN Action State

**Test**: `test_replay_preserves_uncertain_action_state`

**Methodology**:
1. Configure driver with `crash_after_effect` behavior.
2. Submit a NON_REVERSIBLE action — it goes UNCERTAIN after recovery.
3. Close kernel.
4. Rebuild from journal.
5. Verify UNCERTAIN action state is preserved.

**Result**: ✅ PASS — UNCERTAIN state is terminal and survives journal replay.

## Bugs Found and Fixed

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| `event_cursor` not restored after rebuild | `ProcessService.handle_event` did not restore `event_cursor` from journal offset | Added `pcb.event_cursor = ev.journal_offset` in handle_event for PROCESS_STATE_CHANGED events |
| `finished_at` timestamps non-deterministic across rebuilds | `ActionService.handle_event` used `datetime.utcnow()` during replay | Changed to use `ev.created_at` for `finished_at` when processing replayed events |
| `SIGNAL_CONSUMED` events not processed during replay | `SignalService.handle_event` had no handler for `SIGNAL_CONSUMED` | Added handler to mark signals as consumed during replay |

## Conclusion

The Journal is the single source of truth. All projection tables can be deleted and rebuilt from the Journal with identical, deterministic results. No in-memory state is required for correct replay.
