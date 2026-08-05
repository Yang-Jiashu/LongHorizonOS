# SIGKILL Recovery Audit — Phase B

## Objective

Verify crash recovery using real SIGKILL signals in separate subprocesses. Each crash scenario kills the kernel process, then reopens the database in a new process to verify recovery.

## Test File

`tests/agent_os/test_audit_sigkill.py` — 5 tests, all PASSED.

## Methodology

Each test:
1. Writes a Python script that creates a kernel, runs it to a specific crash point, then calls `os.kill(os.getpid(), signal.SIGKILL)`.
2. Runs the script in a subprocess — verifies exit code is `-9` (SIGKILL).
3. Writes a recovery script that reopens the DB via `rebuild_from_journal(db_path)`, runs a recovery tick, and dumps state to JSON.
4. Runs the recovery script — verifies exit code is `0`.
5. Reads the JSON result and verifies invariants.

## Crash Scenarios

### Crash A: Durable Intent Committed, Driver Not Yet Executed

**Test**: `test_crash_a_durable_intent_no_driver`

**Scenario**: Process submits a PURE action. The action is ADMITTED (durable intent in journal) but the driver hasn't dispatched yet. Process is killed.

**Recovery**: Reopen DB, run recovery tick.

**Verification**:
- Process exists in projections.
- Action exists with a valid state (committed, failed, uncertain, admitted, or running).
- No data corruption.

**Result**: ✅ PASS — Action recovered to a valid state after rebuild.

### Crash B: IDEMPOTENT Side Effect, Completion Not Committed

**Test**: `test_crash_b_idempotent_recovery`

**Scenario**: Process submits an IDEMPOTENT action. Driver dispatches and crashes after effect. Action is RUNNING. Process is killed before recovery.

**Recovery**: Reopen DB, run recovery tick. Since the original driver's effect store is lost, `inspect()` returns "unknown". The action transitions to UNCERTAIN (safe behavior).

**Verification**:
- Action is NOT stuck in "running" state.
- Action is in a terminal state (committed, uncertain, or failed).

**Result**: ✅ PASS — Action correctly recovered to terminal state.

### Crash C: NON_REVERSIBLE Side Effect, Unknown Inspect

**Test**: `test_crash_c_non_reversible_uncertain`

**Scenario**: Process submits a NON_REVERSIBLE action. Driver dispatches and crashes after effect. Action is RUNNING. Process is killed.

**Recovery**: Reopen DB, run recovery tick. `inspect()` returns "unknown". Since the side effect is non-reversible and we can't confirm completion, the action goes UNCERTAIN.

**Verification**:
- At least one action is in UNCERTAIN state.
- No auto-retry of non-reversible actions.

**Result**: ✅ PASS — NON_REVERSIBLE actions correctly go UNCERTAIN on crash recovery.

### Crash D: Action Committed, Lease Released

**Test**: `test_crash_d_committed_lease_released`

**Scenario**: Process submits a PURE action with an exclusive resource claim. Action dispatches and commits. Lease is released in the same tick. Process is killed after the tick completes.

**Recovery**: Reopen DB, run recovery tick.

**Verification**:
- `lease_count == 0` — No active leases after recovery.
- Leases were either released before crash or reclaimed during recovery.

**Result**: ✅ PASS — No lease leaks after crash + recovery.

### Crash E: Signal Generated But Not Consumed

**Test**: `test_crash_e_signal_consumed_once`

**Scenario**: Kernel sends a signal to a non-existent PID. Process is killed before the signal is consumed.

**Recovery**: Reopen DB, run recovery tick.

**Verification**:
- `signal_count >= 1` — Signal survived the crash.
- At least 1 unconsumed signal (target process doesn't exist, so no consumer).

**Result**: ✅ PASS — Signals are durable and survive crash + rebuild.

## Side-Effect Classification Matrix

| Side-Effect Class | Crash After Effect | Inspect Result | Recovery Action |
|-------------------|-------------------|----------------|-----------------|
| PURE | N/A (no side effect) | N/A | Re-execute safely |
| IDEMPOTENT | Yes | "unknown" | UNCERTAIN (can't confirm) |
| NON_REVERSIBLE | Yes | "unknown" | UNCERTAIN (must not retry) |

## Implementation Notes

- SIGKILL cannot be caught — the process dies immediately. No cleanup code runs.
- All durable state is in SQLite (journal_events, journal_meta) — survives SIGKILL.
- In-memory state (driver effect stores, cached PCBs) is lost — recovery must handle this.
- `rebuild_from_journal()` replays all events and reconstructs projections.
- Recovery tick handles RUNNING actions by calling `inspect()` on the driver.
- If `inspect()` returns "unknown" (driver effect store lost), action goes UNCERTAIN.

## Conclusion

Real SIGKILL recovery is verified:
- ✅ All 5 crash scenarios recover correctly.
- ✅ Durable intents survive crash (journal is persistent).
- ✅ IDEMPOTENT actions go UNCERTAIN when inspect is unknown (safe).
- ✅ NON_REVERSIBLE actions go UNCERTAIN (never auto-retried).
- ✅ Leases are released after crash + recovery.
- ✅ Signals survive crash and remain durable.
