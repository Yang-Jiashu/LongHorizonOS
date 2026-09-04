from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk.compute_usage import (
    AttemptUsageRecord,
    AttemptUsageState,
    InvalidUsageTransition,
    UnknownAttemptError,
    UsageConflictError,
    UsageIdentityError,
    UsageLedger,
    UsageVector,
)


def vector(**overrides: int) -> UsageVector:
    values = {
        "tokens": 10,
        "wall_time_ms": 20,
        "cost_microusd": 30,
        "context_tokens": 40,
        "verification_tokens": 50,
    }
    values.update(overrides)
    return UsageVector(**values)


def test_usage_vector_is_strict_non_negative_and_additive() -> None:
    with pytest.raises((ValidationError, ValueError)):
        UsageVector(tokens=-1)
    with pytest.raises((ValidationError, ValueError)):
        UsageVector(tokens=True)  # type: ignore[arg-type]
    with pytest.raises((ValidationError, ValueError)):
        UsageVector(tokens=1.5)  # type: ignore[arg-type]

    left = vector(tokens=2)
    right = vector(tokens=3)
    assert left.plus(right).tokens == 5
    assert left.plus(right).wall_time_ms == 40
    assert vector(tokens=2).minus_clamped(vector(tokens=9)).tokens == 0


def test_identity_requires_all_non_empty_strings() -> None:
    with pytest.raises((UsageIdentityError, ValidationError, ValueError)):
        AttemptUsageRecord(
            goal_id=" ",
            task_id="task",
            attempt_id="attempt",
            state=AttemptUsageState.ESTIMATED,
            estimated=vector(),
        )
    with pytest.raises((UsageIdentityError, ValidationError, ValueError)):
        AttemptUsageRecord(
            goal_id="goal",
            task_id=7,  # type: ignore[arg-type]
            attempt_id="attempt",
            state=AttemptUsageState.ESTIMATED,
            estimated=vector(),
        )


def test_record_shape_requires_state_specific_usage() -> None:
    with pytest.raises((ValidationError, ValueError)):
        AttemptUsageRecord(
            goal_id="g",
            task_id="t",
            attempt_id="a",
            state=AttemptUsageState.ESTIMATED,
        )
    with pytest.raises((ValidationError, ValueError)):
        AttemptUsageRecord(
            goal_id="g",
            task_id="t",
            attempt_id="a",
            state=AttemptUsageState.RESERVED,
            reserved=vector(),
            measured=vector(),
        )
    with pytest.raises((ValidationError, ValueError)):
        AttemptUsageRecord(
            goal_id="g",
            task_id="t",
            attempt_id="a",
            state=AttemptUsageState.COMMITTED,
        )


def test_lifecycle_separates_reservation_from_measured_consumption() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=100))
    ledger = ledger.reserve("g", "t", "a", vector(tokens=80))
    reserved = ledger.aggregate(goal_id="g")
    assert reserved.estimated.tokens == 100
    assert reserved.reserved.tokens == 80
    assert reserved.active_reserved.tokens == 80
    assert reserved.measured.tokens == 0

    ledger = ledger.record_measured("g", "t", "a", vector(tokens=17))
    measured = ledger.aggregate(goal_id="g")
    assert measured.reserved.tokens == 80
    assert measured.active_reserved.tokens == 0
    assert measured.measured.tokens == 17
    assert measured.committed.tokens == 0

    ledger = ledger.commit("g", "t", "a")
    committed = ledger.aggregate(goal_id="g")
    assert committed.measured.tokens == 17
    assert committed.committed.tokens == 17
    assert committed.failed.tokens == 0


def test_failed_stale_and_cancelled_measured_usage_are_disjoint_buckets() -> None:
    ledger = UsageLedger.empty()
    ledger = ledger.record_estimate("g", "t1", "a1", vector(tokens=5))
    ledger = ledger.record_measured("g", "t1", "a1", vector(tokens=2))
    ledger = ledger.fail("g", "t1", "a1")

    ledger = ledger.record_estimate("g", "t2", "a2", vector(tokens=5))
    ledger = ledger.mark_stale("g", "t2", "a2", measured=vector(tokens=3))

    ledger = ledger.record_estimate("g", "t3", "a3", vector(tokens=5))
    ledger = ledger.cancel("g", "t3", "a3", measured=UsageVector.zero())

    aggregate = ledger.by_goal("g")
    assert aggregate.measured.tokens == 5
    assert aggregate.failed.tokens == 2
    assert aggregate.stale.tokens == 3
    assert aggregate.cancelled.tokens == 0
    assert aggregate.terminal_measured == aggregate.measured


def test_exact_duplicate_add_is_idempotent_and_conflict_fails_closed() -> None:
    record = AttemptUsageRecord(
        goal_id="g",
        task_id="t",
        attempt_id="a",
        state=AttemptUsageState.ESTIMATED,
        estimated=vector(tokens=12),
    )
    ledger = UsageLedger.empty().add(record)
    duplicate = ledger.add(record.model_copy())
    assert duplicate is ledger
    assert duplicate.canonical_hash == ledger.canonical_hash

    with pytest.raises(UsageConflictError):
        ledger.add(record.model_copy(update={"estimated": vector(tokens=13)}))

    with pytest.raises((UsageConflictError, ValidationError, ValueError)):
        UsageLedger(records=(record, record.model_copy(update={"estimated": vector(tokens=13)})))


def test_duplicate_transitions_are_idempotent_but_different_payload_conflicts() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=9))
    ledger = ledger.reserve("g", "t", "a", vector(tokens=8))
    assert ledger.reserve("g", "t", "a", vector(tokens=8)) is ledger
    with pytest.raises(UsageConflictError):
        ledger.reserve("g", "t", "a", vector(tokens=7))

    ledger = ledger.record_measured("g", "t", "a", vector(tokens=4))
    assert ledger.record_measured("g", "t", "a", vector(tokens=4)) is ledger
    with pytest.raises(UsageConflictError):
        ledger.record_measured("g", "t", "a", vector(tokens=5))


def test_invalid_and_unknown_transitions_fail_closed() -> None:
    with pytest.raises(UnknownAttemptError):
        UsageLedger.empty().reserve("g", "t", "a", vector())

    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector())
    with pytest.raises(InvalidUsageTransition):
        ledger.transition("g", "t", "a", AttemptUsageState.COMMITTED, measured=vector())

    ledger = ledger.record_measured("g", "t", "a", vector())
    ledger = ledger.commit("g", "t", "a")
    with pytest.raises(InvalidUsageTransition):
        ledger.fail("g", "t", "a", measured=vector())


def test_direct_measured_attempt_can_skip_reservation_but_not_measurement() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=1))
    ledger = ledger.record_measured("g", "t", "a", vector(tokens=2))
    assert ledger.by_attempt("g", "t", "a").measured.tokens == 2
    ledger = ledger.commit("g", "t", "a")
    assert ledger.records[0].reserved is None


def test_terminal_transition_requires_authoritative_measured_usage() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=1))
    with pytest.raises(InvalidUsageTransition, match="requires explicit measured usage"):
        ledger.cancel("g", "t", "a")
    cancelled = ledger.cancel("g", "t", "a", measured=UsageVector.zero())
    assert cancelled.by_goal("g").cancelled.is_zero()


def test_historical_estimate_reservation_and_measurement_cannot_be_rewritten() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=10))
    with pytest.raises(UsageConflictError, match="cannot rewrite estimated"):
        ledger.transition(
            "g",
            "t",
            "a",
            AttemptUsageState.RESERVED,
            estimated=vector(tokens=11),
            reserved=vector(tokens=8),
        )

    ledger = ledger.reserve("g", "t", "a", vector(tokens=8))
    with pytest.raises(UsageConflictError, match="cannot rewrite reserved"):
        ledger.transition(
            "g",
            "t",
            "a",
            AttemptUsageState.MEASURED,
            reserved=vector(tokens=7),
            measured=vector(tokens=4),
        )

    ledger = ledger.record_measured("g", "t", "a", vector(tokens=4))
    with pytest.raises(UsageConflictError, match="cannot rewrite measured"):
        ledger.commit("g", "t", "a", measured=vector(tokens=5))


def test_scope_aggregates_are_deterministic_and_filterable() -> None:
    ledger = UsageLedger.empty()
    ledger = ledger.record_estimate("g2", "t", "a2", vector(tokens=2))
    ledger = ledger.record_measured("g2", "t", "a2", vector(tokens=1))
    ledger = ledger.record_estimate("g1", "t", "a1", vector(tokens=4))
    ledger = ledger.record_measured("g1", "t", "a1", vector(tokens=3))
    all_usage = ledger.aggregate()
    assert all_usage.attempt_count == 2
    assert all_usage.estimated.tokens == 6
    assert all_usage.measured.tokens == 4
    assert ledger.by_task("g1", "t").measured.tokens == 3
    assert ledger.by_attempt("g2", "t", "a2").measured.tokens == 1

    reversed_ledger = UsageLedger(
        records=tuple(reversed(ledger.records)),
    )
    assert reversed_ledger.records == ledger.records
    assert reversed_ledger.canonical_hash == ledger.canonical_hash


def test_canonical_hash_changes_on_semantic_change_not_insertion_order() -> None:
    first = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=1))
    second = UsageLedger.empty().record_estimate("g", "t", "a", vector(tokens=2))
    assert first.canonical_hash != second.canonical_hash
    assert first.by_goal("g").canonical_hash != second.by_goal("g").canonical_hash


def test_frozen_models_reject_mutation() -> None:
    ledger = UsageLedger.empty().record_estimate("g", "t", "a", vector())
    with pytest.raises(ValidationError):
        ledger.records = ()  # type: ignore[misc]


def test_mapping_inputs_are_supported_but_unknown_dimensions_fail() -> None:
    ledger = UsageLedger.empty().record_estimate(
        "g",
        "t",
        "a",
        {"tokens": 4, "wall_time_ms": 1},
    )
    assert ledger.records[0].estimated is not None
    assert ledger.records[0].estimated.tokens == 4
    with pytest.raises((ValidationError, ValueError)):
        UsageVector(tokens=1, not_a_dimension=2)  # type: ignore[call-arg]
