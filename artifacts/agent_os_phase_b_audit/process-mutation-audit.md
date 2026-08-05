# Process State Machine Mutation Audit — Phase B

## Objective

Verify that Process state machine invariants hold — terminal processes cannot resume, BLOCKED requires a wait condition, and scheduler exclusivity is maintained.

## Test File

`tests/agent_os/test_audit_process_mutation.py` — 10 tests, all PASSED.

## Process States

| State | Terminal? | Meaning |
|-------|-----------|---------|
| `CREATED` | No | Just spawned, not yet ready |
| `READY` | No | Waiting to be scheduled |
| `RUNNING` | No | Currently executing |
| `BLOCKED` | No | Waiting for a signal/event |
| `SUSPENDED` | No | Paused by kernel |
| `EXITED` | **Yes** | Normal termination |
| `FAILED` | **Yes** | Abnormal termination |

## Invariants Verified

### Mutation A: BLOCKED Without Wait Condition Must Raise

**Test**: `test_blocked_requires_wait_condition`

**Methodology**: Attempt to transition RUNNING → BLOCKED without a `wait_condition`. Then attempt with a wait_condition.

**Result**: ✅ PASS — `WaitConditionMissing` raised without wait_condition; transition succeeds with one.

### Mutation B: Terminal Process Cannot Resume

**Test**: `test_terminal_process_cannot_resume`

**Methodology**: Attempt EXITED → READY transition.

**Result**: ✅ PASS — `TerminalStateError` raised.

### Mutation C: Failed Process Cannot Step

**Test**: `test_failed_process_cannot_step`

**Methodology**: Attempt FAILED → RUNNING transition.

**Result**: ✅ PASS — `TerminalStateError` raised.

### Mutation D: One Process Has At Most One Active Step

**Test**: `test_one_process_has_at_most_one_active_step`

**Methodology**: Transition a process READY → RUNNING, then attempt RUNNING → RUNNING (self-transition).

**Result**: ✅ PASS — `IllegalStateTransition` raised for self-transition. No RUNNING → RUNNING transition exists.

### Mutation E: Suspended Process Not Scheduled

**Test**: `test_suspended_process_is_not_scheduled`

**Methodology**: Create READY and SUSPENDED processes. Call `list_ready()`.

**Result**: ✅ PASS — Only READY process appears in the list.

### Scheduler Exclusion: BLOCKED Not in Ready List

**Test**: `test_blocked_process_not_in_ready_list`

**Result**: ✅ PASS

### Scheduler Exclusion: EXITED Not in Ready List

**Test**: `test_exited_process_not_in_ready_list`

**Result**: ✅ PASS

### Scheduler Exclusion: FAILED Not in Ready List

**Test**: `test_failed_process_not_in_ready_list`

**Result**: ✅ PASS

## Mutation Detection Tests

### Wait Condition Guard Detection

**Test**: `test_mutation_blocked_without_wait_condition_detected`

**Methodology**: Verify the wait_condition guard exists by asserting `WaitConditionMissing` is raised.

**Result**: ✅ PASS — The guard is in `validate_process_transition()` and is checked before the transition table.

### Terminal Check Defense-in-Depth

**Test**: `test_mutation_exited_can_resume_detected`

**Methodology**: Temporarily add `EXITED→READY` to `_PROCESS_TRANSITIONS`. Verify `TerminalStateError` is still raised because terminal check happens before transition lookup.

**Result**: ✅ PASS — Same defense-in-depth pattern as Action state machine.

## Implementation Notes

The process state machine shares the same **two-layer defense** as the action state machine:
1. **Terminal state check** (first): EXITED and FAILED are terminal — no transitions allowed.
2. **Transition table lookup** (second): For non-terminal states, check `_PROCESS_TRANSITIONS`.

Additionally, BLOCKED has a **guard condition**: `wait_condition` must be provided. This is a code-level check, not a table entry, making it harder to accidentally bypass.

## Conclusion

Process state machine invariants are solid:
- ✅ Terminal states (EXITED, FAILED) cannot transition to anything.
- ✅ BLOCKED requires a wait_condition (no blind blocking).
- ✅ No RUNNING → RUNNING self-transition (one active step per process).
- ✅ SUSPENDED, BLOCKED, EXITED, FAILED processes are excluded from `list_ready()`.
- ✅ Defense-in-depth: terminal check is independent of transition table.
- ✅ Mutation tests verify breaking the table doesn't bypass guards.
