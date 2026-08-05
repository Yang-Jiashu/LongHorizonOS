# BLOCKED Zero-Polling Audit — Phase B

## Objective

Verify that BLOCKED processes are never polled — their `program.step()` is not called, and model drivers are not dispatched, while the process waits for a signal.

## Test File

`tests/agent_os/test_audit_blocked_polling.py` — 3 tests, all PASSED.

## Invariants Verified

### 1. BLOCKED Process Has Zero Program Polling

**Test**: `test_blocked_process_has_zero_program_polling`

**Methodology**:
1. Register a `PendingDriver` that returns `status="running"` on dispatch (action never completes).
2. Wrap the program in a `CountingProgram` that tracks `step_count`.
3. Submit a device action → process goes BLOCKED.
4. Tick multiple times — verify `step_count` stays at 1 (no polling).
5. Manually send a matching `ACTION_COMPLETED` signal.
6. Tick again — verify `step_count` increases (process wakes).

**Result**: ✅ PASS

| Tick | step_count | State | Notes |
|------|-----------|-------|-------|
| 1 | 1 | BLOCKED | Submit action, process blocks |
| 2 | 1 | BLOCKED | Driver returns "running", no polling |
| 3 | 1 | BLOCKED | Recovery tick, still no polling |
| 4 (after signal) | ≥2 | RUNNING→EXITED | Signal wakes process |

### 2. BLOCKED Process Has Zero Model Polling

**Test**: `test_blocked_process_has_zero_model_polling`

**Methodology**:
1. Register a `CountingModelDriver` that tracks `dispatch_count`.
2. Register a `PendingDriver` that returns "running".
3. Submit a device action → process goes BLOCKED.
4. Tick — verify `model_driver.dispatch_count == 0`.

**Result**: ✅ PASS — No model driver calls while process is BLOCKED.

### 3. Matching Completion Wakes Exactly Once

**Test**: `test_matching_completion_wakes_exactly_once`

**Methodology**:
1. Run a normal demo (submit → process → exit).
2. Verify `step_count == 3` (submit + process_event + exit).
3. Tick 5 more times — verify `step_count` stays at 3.

**Result**: ✅ PASS — Process wakes exactly once on signal, then exits. No spurious re-scheduling after exit.

## Known Phase B Limitation

During the audit, a known limitation was identified:

- When a device action goes UNCERTAIN (due to `crash_after_effect`), the kernel generates an `ACTION_UNCERTAIN` signal.
- The BLOCKED process's `wait_condition` expects `ACTION_COMPLETED`.
- `ACTION_UNCERTAIN` does not match `ACTION_COMPLETED`, so the process stays BLOCKED.
- This means the process never wakes up on UNCERTAIN/FAILED action outcomes.

**Impact**: A process blocked on an action that goes UNCERTAIN or FAILED will remain blocked indefinitely unless manually woken.

**Mitigation**: This is a known Phase B limitation. The kernel should also wake processes on `ACTION_UNCERTAIN` and `ACTION_FAILED` signals. This is documented as a future improvement, not a bug in the audit's scope.

## Implementation Notes

The kernel's `tick()` method:
1. Selects a READY process (not BLOCKED).
2. Calls `program.step()` on the selected process.
3. BLOCKED processes are excluded from `list_ready()`.

This ensures BLOCKED processes are never selected for execution, and their `step()` is never called.

## Conclusion

BLOCKED zero-polling is verified:
- ✅ `program.step()` is never called while BLOCKED.
- ✅ Model driver `dispatch()` is never called while BLOCKED.
- ✅ Process wakes exactly once on matching signal completion.
- ✅ No spurious re-scheduling after process exit.
- ⚠️ Known limitation: UNCERTAIN/FAILED action outcomes don't wake blocked processes (documented, not a regression).
