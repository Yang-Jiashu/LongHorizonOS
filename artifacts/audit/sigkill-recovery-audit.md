# Milestone 1G: SIGKILL Recovery Audit Report

## Summary

All 5 crash points passed real SIGKILL recovery testing. The runtime correctly
recovers from process death at every critical point in the execution lifecycle.

**Result: PASS (5/5)**

## Test Configuration

- **Crash mechanism**: Real `SIGKILL` signal (not `SimulatedCrashError`)
- **Checkpoint type**: Filesystem (tar snapshot + manifest)
- **Config**: `restore_on_crash=True`, `restore_on_failure=True`, `after_verified_node=True`
- **Task**: 3-node chain (n1 → n2 → n3), crash injected on n2
- **Patching**: Controller crash points patch `_inject_crash_once` to hang after
  writing the fire-once `CRASH_INJECTED` event; worker crash points wrap
  `FakeWorker.execute` to catch `SimulatedCrashError` and hang.

## Results

| # | Crash Point | Flag | Status | Attempts (n2) | Checkpoint Restored | CRASH_INJECTED |
|---|------------|------|--------|---------------|---------------------|----------------|
| 1 | after_lease_before_execution | crash_before_execution | PASS | 1 | 0 | 1 |
| 2 | during_tool_execution | crash_on_attempt | PASS | 2 | 1 | 0 |
| 3 | after_tool_side_effect_before_event | crash_after_tool_calls | PASS | 2 | 1 | 0 |
| 4 | after_claim_before_verification | crash_before_verification | PASS | 2 | 0 | 1 |
| 5 | after_verified_before_commit | crash_after_verified | PASS | 1 | 0 | 1 |

## Detailed Analysis

### 1. after_lease_before_execution (crash_before_execution)

- **State before resume**: n1=verified, n2=ready (never started)
- **Recovery**: No RUNNING nodes; CRASH_INJECTED event prevents re-crash
- **After resume**: n2 executes once (attempt=1), all 3 nodes verified
- **Output**: n2.txt created on resume, hash consistent

### 2. during_tool_execution (crash_on_attempt)

- **State before resume**: n1=verified, n2=running (EXECUTION_STARTED committed)
- **Recovery**: RUNNING → FAILED, checkpoint restored (workspace rolled back)
- **After resume**: n2 re-executed (attempt=2), `crash_on_attempt=1` doesn't match
- **Output**: n2.txt created on re-execution, no duplicate tool calls
- **Idempotency**: Checkpoint restore changes generation, tools re-execute with new keys

### 3. after_tool_side_effect_before_event (crash_after_tool_calls)

- **State before resume**: n1=verified, n2=running, n2.txt exists (tool completed)
- **Recovery**: RUNNING → FAILED, checkpoint restored (n2.txt deleted)
- **After resume**: n2 re-executed (attempt=2), tool calls re-execute (generation=1)
- **Output**: n2.txt hash identical before and after (74b2605a7d193a5d)
- **Idempotency**: 5 tool_call_completed events, no duplicate keys
- **Note**: This is the key idempotency path — tool side-effects happened but
  the claim was never persisted. Checkpoint restore + generation-keyed replay
  ensures correct re-execution.

### 4. after_claim_before_verification (crash_before_verification)

- **State before resume**: n1=verified, n2=claimed_done (CLAIM_SUBMITTED committed)
- **Recovery**: CLAIMED_DONE → FAILED (no checkpoint restore for non-RUNNING nodes)
- **After resume**: n2 re-executed (attempt=2), CRASH_INJECTED prevents re-crash
- **Output**: n2.txt existed before and after (same hash), tool calls replayed
- **Idempotency**: 4 tool_call_completed events, no duplicate keys
- **Note**: No checkpoint restore because node was CLAIMED_DONE, not RUNNING.
  Tool calls replayed via idempotency keys (no generation change).

### 5. after_verified_before_commit (crash_after_verified)

- **State before resume**: n1=verified, n2=verified (fully committed)
- **Recovery**: VERIFIED nodes not touched by recovery
- **After resume**: n3 executes normally, all 3 nodes verified
- **Output**: n2.txt existed before and after (same hash)
- **Note**: Verified work is never repeated. n2 attempt_count=1 (not re-executed).

## Idempotency Verification

- **No duplicate tool call keys** across all 5 crash points
- **Output hash consistency**: All output files exist with correct content after resume
- **Event log integrity**: No duplicate `TOOL_CALL_COMPLETED` events with same key

## Key Findings

1. **Fire-once crash injection works correctly**: The `CRASH_INJECTED` event
   ensures controller crash points don't re-crash on resume.

2. **Checkpoint restore + generation keys**: For worker crash points (2, 3),
   checkpoint restore changes the generation count, which modifies idempotency
   keys. This correctly forces tool re-execution after environment rollback.

3. **CLAIMED_DONE recovery**: For crash point 4, the node transitions
   CLAIMED_DONE → FAILED → READY → RUNNING → CLAIMED_DONE → VERIFIED.
   Tool calls are replayed (not re-executed) because no checkpoint restore
   occurred (no generation change).

4. **VERIFIED nodes are never re-executed**: For crash point 5, the verified
   node's attempt_count remains 1, confirming no re-execution.

5. **SQLite WAL mode survives SIGKILL**: All events committed before the crash
   are properly persisted and available on resume.
