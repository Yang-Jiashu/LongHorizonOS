# Milestone 1F: Manual Mutation Audit Report

## Methodology

Created branch `audit/manual-mutations` from the audit HEAD. For each mutation:
1. Applied a single code change
2. Ran relevant tests
3. Recorded failing tests and failure reasons
4. Reverted the change with `git checkout -- <file>`

After all mutations, restored to main and deleted the branch.

## Mutation Results

### Mutation 1: Allow CLAIMED_DONE without Evidence to become VERIFIED

- **Modified file**: `src/lhos/infrastructure/db/sqlite_graph_store.py`
- **Change**: Removed the `EvidenceRequiredError` check when transitioning to VERIFIED
- **Failing tests**:
  - `test_store_rejects_verified_without_evidence` — FAILED: `DID NOT RAISE EvidenceRequiredError`
- **Expected failures**: State machine tests, Verification Gate tests, integration test
- **Actual failures**: 1 test
- **Assessment**: The evidence invariant is enforced at the store level. The verification gate synthesizes evidence when none exists, so most tests still pass. The store-level test correctly catches the mutation. However, additional tests should verify that the gate doesn't synthesize fake evidence when the verifier returns none.

### Mutation 2: Disable invalidation propagation after upstream artifact change

- **Modified file**: `src/lhos/runtime/controller.py`
- **Change**: Commented out `self._reconciler.reconcile_event(run_id, event)` in `_update_produced_artifacts`
- **Failing tests**:
  - `test_constraint_change_must_invalidates_and_reverification_updates_artifact` — FAILED: `KeyError: 'artifact_version'`
- **Expected failures**: Invalidation tests, branch isolation tests, local repair tests
- **Actual failures**: 1 test
- **Assessment**: The mutation only disabled one of two invalidation paths (artifact-change-driven). Environment-event-driven invalidation still works. More tests are needed to specifically test the artifact-change path in isolation.

### Mutation 3: Remove tool idempotency check

- **Modified file**: `src/lhos/runtime/tool_runtime.py`
- **Change**: Disabled idempotency key replay and key requirement
- **Failing tests**:
  - `test_crash_during_tool_execution` — FAILED: `assert 0 == 1` (incomplete_tool_calls not detected)
  - `test_crash_after_tool_completion_before_claim_uses_idempotency` — FAILED: `assert 1 == 0` (tool was re-executed instead of replayed)
- **Expected failures**: Duplicate tool call test, crash recovery idempotency test
- **Actual failures**: 2 tests
- **Assessment**: Idempotency tests are effective. They correctly detect both the missing replay and the missing incomplete-call detection.

### Mutation 4: Cost-aware Scheduler degrades to FIFO

- **Modified file**: `src/lhos/runtime/cost_aware_scheduler.py`
- **Change**: Replaced `select()` method with FIFO ordering
- **Initial failing tests**: 0 (all tests passed!)
- **Root cause**: Test data had nodes with same `ready_at` and alphabetical ID ordering that matched cost-aware preference
- **Action taken**: Added `test_cost_aware_picks_critical_path_over_earlier_ready` test
- **After adding test**: 1 test failure — `test_cost_aware_picks_critical_path_over_earlier_ready` correctly detects FIFO degradation
- **Assessment**: Original tests were insufficient. New test creates a scenario where FIFO and cost-aware pick different nodes.

### Mutation 5: Resume re-executes already VERIFIED nodes

- **Modified file**: `src/lhos/runtime/recovery.py`
- **Change**: Added code to reset VERIFIED nodes to PENDING during recovery
- **Failing tests**:
  - `test_crash_before_node_execution` — FAILED: `InvalidStateTransition: illegal transition verified -> pending`
  - `test_crash_during_tool_execution` — FAILED: same
  - `test_crash_after_tool_completion_before_claim_uses_idempotency` — FAILED: same
  - `test_crash_before_verification_recovers_claimed_node` — FAILED: same
  - `test_crash_after_verified_commit_does_not_repeat_work` — FAILED: same
  - `test_kill_mid_run_then_resume_does_not_reexecute_verified` — FAILED: same
- **Expected failures**: Attempt count test, no repeated verified work test, recovery overhead test
- **Actual failures**: 6 tests
- **Assessment**: The state machine correctly rejects VERIFIED → PENDING transitions. All crash recovery tests fail because the mutation raises an exception. Tests are effective.

### Mutation 6: Event Replay ignores Evidence

- **Modified file**: `src/lhos/graph/projection.py`
- **Change**: Commented out evidence replay in `rebuild_projection()`
- **Failing tests**:
  - `test_rebuild_from_events_is_identical` — FAILED: evidence count mismatch
  - `test_tiny_repository_task_end_to_end` — FAILED: projection hash mismatch
- **Expected failures**: Replay equivalence test, graph hash test, evidence reconstruction test
- **Actual failures**: 2 tests
- **Assessment**: Tests are effective. They correctly detect missing evidence during replay through both count comparison and hash comparison.

## Summary

| Mutation | Modified File | Tests Failed | Tests Effective? |
|---|---|---|---|
| 1. VERIFIED without Evidence | sqlite_graph_store.py | 1 | Yes (minimal) |
| 2. Disable invalidation | controller.py | 1 | Yes (minimal) |
| 3. Remove idempotency | tool_runtime.py | 2 | Yes |
| 4. FIFO degradation | cost_aware_scheduler.py | 0 → 1 (after fix) | Fixed |
| 5. Re-execute VERIFIED | recovery.py | 6 | Yes |
| 6. Skip evidence replay | projection.py | 2 | Yes |

## Issues Found and Fixed

1. **Mutation 4 — Scheduler tests insufficient**: The original tests did not distinguish between cost-aware and FIFO scheduling because test nodes had the same `ready_at` and alphabetical ordering matched cost-aware preference. Added `test_cost_aware_picks_critical_path_over_earlier_ready` which creates a scenario where FIFO and cost-aware pick different nodes.

## Recommendations

1. **Mutation 1**: Add tests that verify the verification gate does not synthesize fake evidence when the verifier returns none (only when the verifier returns evidence with an empty list).
2. **Mutation 2**: Add tests that specifically test artifact-change-driven invalidation in isolation (not mixed with environment events).
3. All other mutations are adequately covered by existing tests.
