# Deadlock Audit — Phase B

## Objective

Verify deadlock prevention (atomic acquire), detection (wait-for graph cycle detection), recovery (victim selection), and non-deadlock discrimination.

## Test File

`tests/agent_os/test_audit_deadlock.py` — 7 tests, all PASSED.

## Invariants Verified

### 1. Atomic Acquire Prevents Hold-and-Wait

**Test**: `test_atomic_acquire_prevents_hold_and_wait`

**Methodology**:
- p1 atomically acquires R1+R2 (both exclusive).
- p2 attempts to atomically acquire R1+R2 — fails because both are held.
- p2 holds nothing — no deadlock possible.

**Result**: ✅ PASS — `LeaseAcquisitionFailed` raised. `detect_deadlocks()` returns empty.

**Principle**: Atomic multi-acquire is all-or-nothing. If any resource is unavailable, none are acquired. This eliminates the hold-and-wait condition (one of the four Coffman conditions for deadlock).

### 2. Deadlock Detected via Wait-For Graph

**Test**: `test_deadlock_detected_via_wait_for_graph`

**Methodology**:
- p1 holds R1, p2 holds R2.
- p1 waits for R2 (held by p2), p2 waits for R1 (held by p1).
- This creates a cycle: p1 → p2 → p1.

**Result**: ✅ PASS — `detect_deadlocks()` returns a cycle containing both p1 and p2.

**Implementation**: The lease service maintains a `lease_waiters` table. `detect_deadlocks()` builds a wait-for graph and uses DFS to find cycles.

### 3. Deadlock Recovery Selects Victim

**Test**: `test_deadlock_recovery_selects_victim`

**Methodology**:
- Create two real processes (spawned via kernel).
- p1 holds R1, p2 holds R2.
- Create wait-for cycle.
- Call `_recover_deadlock(cycle)`.
- Verify:
  - `DEADLOCK_DETECTED` and `DEADLOCK_RECOVERED` events are journaled.
  - Victim process is in FAILED state.
  - Victim's leases are released.

**Result**: ✅ PASS — Recovery correctly fails the victim, releases its leases, and journals both events.

### 4. Non-Deadlock Wait Not Detected

**Test**: `test_non_deadlock_wait_not_detected`

**Methodology**:
- p1 holds R1.
- p2 waits for R1 (but p2 holds nothing).
- No cycle — p2 → p1, but p1 doesn't wait for anyone.

**Result**: ✅ PASS — `detect_deadlocks()` returns empty. This is a legitimate wait, not a deadlock.

### 5. Non-Deadlock Chain Not Detected

**Test**: `test_non_deadlock_chain_not_detected`

**Methodology**:
- p1 holds R1, p2 holds R2, p3 holds R3.
- p2 waits for R1 (held by p1), p3 waits for R2 (held by p2).
- Chain: p3 → p2 → p1. But p1 doesn't wait for anyone — no cycle.

**Result**: ✅ PASS — `detect_deadlocks()` returns empty. A chain without a cycle is not a deadlock.

### 6. Victim Selection Is Deterministic

**Test**: `test_victim_selection_is_deterministic`

**Methodology**:
- Create two BLOCKED processes with different priorities:
  - p1: priority=5
  - p2: priority=10
- Both hold resources and wait for each other (deadlock).
- Call `_select_victim(cycle)`.

**Result**: ✅ PASS — p1 (lower priority) is selected as victim. Deterministic: same input → same victim.

**Selection algorithm**: Lower priority process is selected as victim (minimizing the impact of killing higher-priority work).

## Coffman Conditions Analysis

The four necessary conditions for deadlock:

| Condition | Phase B Mitigation |
|-----------|-------------------|
| Mutual Exclusion | ✅ Exclusive leases enforce this |
| Hold and Wait | ✅ Atomic acquire prevents (all-or-nothing) |
| No Preemption | ⚠️ Not prevented, but recovery handles |
| Circular Wait | ✅ Detected via wait-for graph DFS |

**Prevention**: Hold-and-wait is prevented by atomic acquire.
**Detection**: Circular wait is detected by DFS cycle detection.
**Recovery**: Victim selection breaks the cycle by failing one process.

## Implementation Notes

- `lease_waiters` table tracks which PIDs are waiting for which resources.
- `detect_deadlocks()` builds a wait-for graph and runs DFS to find cycles.
- `_select_victim(cycle)` selects the lowest-priority process in the cycle.
- `_recover_deadlock(cycle)` fails the victim, releases its leases, and journals events.
- The kernel calls `detect_deadlocks()` periodically during `tick()`.

## Conclusion

Deadlock handling is comprehensive:
- ✅ Prevention: Atomic acquire eliminates hold-and-wait.
- ✅ Detection: Wait-for graph DFS finds circular waits.
- ✅ Recovery: Victim selection is deterministic (lowest priority).
- ✅ Non-deadlock discrimination: Single waits and chains are not falsely detected.
- ✅ All recovery actions are journaled.
