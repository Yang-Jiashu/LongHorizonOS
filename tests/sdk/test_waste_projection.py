"""Read-only five-waste projection tests.

Each test constructs one waste condition from durable Scheduler state and
asserts the projection detects and attributes it, plus a test that the
unobservable dimension is reported unavailable rather than as a fabricated
zero, and that a genuine measured zero stays distinct from unavailable.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from lhos.runtimes.multi_agent import (
    AgentSnapshot,
    AttemptState,
    ComputationCost,
    ResourceBinding,
    ScheduledExecutionAttempt,
)
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    UsageVector,
    WasteDimension,
    WasteProjection,
    build_waste_projection,
)

_STARTED = datetime(2026, 1, 1, tzinfo=UTC)


def _os_with_tasks(*task_ids: str) -> tuple[AgentOS, Goal, str]:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker"))
    goal = Goal("waste-goal")
    for task_id in task_ids:
        goal.task(task_id, agent="worker")
    graph_id = os_._compile_goal(goal)
    return os_, goal, graph_id


def _attempt(
    graph_id: str,
    *,
    task: str,
    claim: str,
    attempt_id: str,
    epoch: int,
    number: int,
    state: AttemptState,
    reads: tuple[tuple[str, str, int], ...] | None = None,
    cost: ComputationCost | None = None,
    agent: str = "worker",
    process: str = "p1",
) -> ScheduledExecutionAttempt:
    snapshot: AgentSnapshot | None = None
    if reads is not None or cost is not None:
        snapshot = AgentSnapshot(
            agent_id=agent,
            process_id=process,
            task_id=task,
            claim_id=claim,
            attempt_id=attempt_id,
            graph_id=graph_id,
            graph_version=1,
            semantic_epoch=epoch,
            context_identity=None,
            read_set=tuple(
                ResourceBinding(
                    operation="read",
                    resource_uri=uri,
                    artifact_id=artifact,
                    version=version,
                    content_hash="a" * 64,
                    source="context_vm",
                )
                for uri, artifact, version in (reads or ())
            ),
            write_set=(),
            progress=0.5,
            cost=cost or ComputationCost(),
            started_at=_STARTED,
            captured_at=_STARTED,
            state=state,
        )
    return ScheduledExecutionAttempt(
        attempt_id=attempt_id,
        graph_id=graph_id,
        graph_version=1,
        semantic_epoch=epoch,
        task_id=task,
        claim_id=claim,
        agent_id=agent,
        process_id=process,
        attempt_number=number,
        state=state,
        started_at=_STARTED,
        agent_snapshot=snapshot,
    )


def _inject(os_: AgentOS, attempts: list[ScheduledExecutionAttempt]) -> None:
    core = os_.scheduler._s
    with core._schedule_lock:
        core._attempts = attempts


def _set_ledger(
    os_: AgentOS,
    goal_id: str,
    entries: tuple[tuple[str, str, UsageVector], ...],
) -> None:
    ledger = os_.usage_ledger
    for task_id, key, measured in entries:
        ledger = ledger.record_estimate(goal_id, task_id, key, measured)
        ledger = ledger.record_measured(goal_id, task_id, key, measured)
    os_._usage_ledger = ledger


def test_repeated_reasoning_same_epoch_is_detected_and_attributed() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        reads = (("vpg://req", "req", 3),)
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.FAILED,
                    reads=reads,
                ),
                _attempt(
                    gid,
                    task="t",
                    claim="c1",
                    attempt_id="a1",
                    epoch=1,
                    number=1,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                    reads=reads,
                ),
            ],
        )
        _set_ledger(
            os_,
            goal.goal_id,
            (("t", "c1", UsageVector(tokens=90, cost_microusd=7, wall_time_ms=250)),),
        )

        projection = build_waste_projection(os_, goal)
        repeated = projection.repeated_reasoning

        assert repeated.observable
        assert repeated.total_count == 1
        assert repeated.total_tokens == 90
        assert repeated.total_cost_microusd == 7
        assert repeated.total_wall_time_ms == 250
        assert [item.task_id for item in repeated.by_task] == ["t"]
        assert repeated.by_task[0].count == 1
        # Both attempts carry snapshots with an identical read-set, so the
        # unchanged-input refinement is confirmed (no note is added).
        assert repeated.unavailable == ()
        # A same-epoch repeat is not premature parallelism or rework.
        assert projection.premature_parallelism.total_count == 0
        assert projection.stale_rework.total_count == 0
    finally:
        os_.close()


def test_repeated_reasoning_without_snapshot_flags_unchanged_input_unavailable() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.FAILED,
                ),
                _attempt(
                    gid,
                    task="t",
                    claim="c1",
                    attempt_id="a1",
                    epoch=1,
                    number=1,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                ),
            ],
        )

        projection = build_waste_projection(os_, goal)
        repeated = projection.repeated_reasoning

        # The repeat is still counted from attempt state alone ...
        assert repeated.observable
        assert repeated.total_count == 1
        # ... but with no AgentSnapshot the "unchanged input" claim is unavailable.
        assert any(item.name == "unchanged_read_set" for item in repeated.unavailable)
        # No measured ledger entry exists, so magnitude is unavailable, not zero.
        assert repeated.total_tokens is None
        assert any(item.name == "tokens" for item in repeated.unavailable)
    finally:
        os_.close()


def test_premature_parallelism_from_stale_cognition_quarantine() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.STALE_COGNITION,
                    cost=ComputationCost(input_tokens=40, output_tokens=10, elapsed_ms=300),
                ),
            ],
        )
        _set_ledger(
            os_,
            goal.goal_id,
            (("t", "c0", UsageVector(tokens=50, cost_microusd=4, wall_time_ms=300)),),
        )

        projection = build_waste_projection(os_, goal)
        premature = projection.premature_parallelism

        assert premature.observable
        assert premature.total_count == 1
        assert premature.total_tokens == 50
        assert [item.task_id for item in premature.by_task] == ["t"]
        assert "a0" in premature.by_task[0].detail_ids
        # A quarantined dispatch is not counted as repeated reasoning or rework.
        assert projection.repeated_reasoning.total_count == 0
        assert projection.stale_rework.total_count == 0
    finally:
        os_.close()


def test_stale_rework_across_epochs_charges_discarded_prior_attempt() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        _inject(
            os_,
            [
                # Superseded prior attempt at epoch 1 (discarded after invalidation).
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                ),
                # Re-execution at the newer epoch 2 (the current, non-waste attempt).
                _attempt(
                    gid,
                    task="t",
                    claim="c1",
                    attempt_id="a1",
                    epoch=2,
                    number=1,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                ),
            ],
        )
        _set_ledger(
            os_,
            goal.goal_id,
            (
                ("t", "c0", UsageVector(tokens=120, cost_microusd=9, wall_time_ms=400)),
                ("t", "c1", UsageVector(tokens=110, cost_microusd=8, wall_time_ms=380)),
            ),
        )

        projection = build_waste_projection(os_, goal)
        rework = projection.stale_rework

        assert rework.observable
        # Only the superseded prior attempt is rework; the latest epoch is not.
        assert rework.total_count == 1
        assert rework.total_tokens == 120
        assert rework.total_cost_microusd == 9
        assert [item.task_id for item in rework.by_task] == ["t"]
        assert projection.repeated_reasoning.total_count == 0
        assert projection.premature_parallelism.total_count == 0
    finally:
        os_.close()


def test_reread_context_detected_but_magnitude_is_unavailable() -> None:
    os_, goal, gid = _os_with_tasks("t1", "t2")
    try:
        reads = (("vpg://shared", "shared", 3),)
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t1",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                    reads=reads,
                ),
                _attempt(
                    gid,
                    task="t2",
                    claim="c1",
                    attempt_id="a1",
                    epoch=1,
                    number=0,
                    state=AttemptState.SUCCEEDED_OPERATIONALLY,
                    reads=reads,
                ),
            ],
        )

        projection = build_waste_projection(os_, goal)
        reread = projection.reread_context

        assert reread.observable
        # The same agent materialized shared@v3 in a later attempt it already read.
        assert reread.total_count == 1
        assert [item.task_id for item in reread.by_task] == ["t2"]
        assert reread.by_task[0].detail_ids == ("vpg://shared",)
        # Occurrence is observable; the byte/token magnitude is explicitly not.
        assert reread.total_tokens is None
        assert reread.by_task[0].tokens is None
        assert any(item.name == "tokens" for item in reread.unavailable)
    finally:
        os_.close()


def test_wrong_direction_continuation_is_unavailable_not_zero() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.RUNNING,
                ),
            ],
        )

        projection = build_waste_projection(os_, goal)
        wrong = projection.wrong_direction_continuation

        # The dimension cannot be observed from durable state: it is reported
        # unavailable, never as a fabricated measured zero.
        assert wrong.observable is False
        assert wrong.total_count is None
        assert wrong.total_tokens is None
        assert wrong.total_cost_microusd is None
        assert wrong.total_wall_time_ms is None
        assert wrong.reason
        assert "graph_version" in wrong.reason
        assert wrong.unavailable
        assert wrong.by_task == ()
    finally:
        os_.close()


def test_measured_zero_is_distinct_from_unavailable() -> None:
    os_, goal, _ = _os_with_tasks("t")
    try:
        # Compiled goal, nothing dispatched: no attempts, no snapshots.
        projection = build_waste_projection(os_, goal)

        # Attempt-derived dimensions are observable with a genuine measured zero.
        for report in (
            projection.repeated_reasoning,
            projection.premature_parallelism,
            projection.stale_rework,
        ):
            assert report.observable
            assert report.total_count == 0
            assert report.total_tokens == 0
            assert report.unavailable == ()

        # Re-read cannot be observed with no snapshots -> unavailable, not zero.
        assert projection.reread_context.observable is False
        assert projection.reread_context.total_count is None
        assert projection.reread_context.reason

        # Wrong-direction is structurally unavailable regardless of attempts.
        assert projection.wrong_direction_continuation.observable is False
        assert projection.wrong_direction_continuation.total_count is None
    finally:
        os_.close()


def test_projection_is_deterministic_and_side_effect_free() -> None:
    os_, goal, gid = _os_with_tasks("t")
    try:
        _inject(
            os_,
            [
                _attempt(
                    gid,
                    task="t",
                    claim="c0",
                    attempt_id="a0",
                    epoch=1,
                    number=0,
                    state=AttemptState.STALE_COGNITION,
                ),
            ],
        )
        before = tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts)

        first = build_waste_projection(os_, goal)
        second = build_waste_projection(os_, goal.goal_id)

        assert isinstance(first, WasteProjection)
        assert first == second
        assert first.projection_hash == second.projection_hash
        assert first.as_dict() == second.as_dict()
        # Observation never mutates durable Scheduler state.
        assert tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts) == before
        # The public dimension accessor resolves both enum and string keys.
        assert first.dimension("premature_parallelism") is first.premature_parallelism
        assert first.dimension(WasteDimension.STALE_REWORK) is first.stale_rework
    finally:
        os_.close()


def test_uncompiled_goal_fails_closed() -> None:
    os_ = AgentOS(":memory:")
    try:
        with pytest.raises(ConfigurationError):
            build_waste_projection(os_, Goal("never-compiled"))
    finally:
        os_.close()


def test_read_only_reopen_reports_attempt_dimensions_unavailable(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    manifest = tmp_path / "run.json"
    os_ = AgentOS(str(db))
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("persisted-goal")
        goal.task("task", agent="worker")
        os_._compile_goal(goal)
        os_.save_run(str(manifest))
    finally:
        os_.close()

    reopened = AgentOS.open_run(str(manifest))
    try:
        projection = build_waste_projection(reopened, "persisted-goal")

        for report in (
            projection.repeated_reasoning,
            projection.premature_parallelism,
            projection.stale_rework,
            projection.reread_context,
        ):
            assert report.observable is False
            assert report.total_count is None
            assert report.reason == "scheduler durable state is not loaded by read-only AgentOS"
    finally:
        reopened.close()
