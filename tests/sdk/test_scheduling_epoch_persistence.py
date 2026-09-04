"""Durable SchedulingEpoch audit integration.

SchedulingEpoch is an immutable WHAT/WHEN proposal.  Persisting it must be
durable and idempotent, but must not acquire claims, leases, or mutate VPG
state.  These tests exercise the public AgentOS facade plus the scheduler
journal boundary.
"""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent.durable_state import SchedulerStateStore
from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.sdk import AgentOS, ConfigurationError, Goal


def _compiled_goal(os_: AgentOS, goal_id: str = "epoch-persistence-goal") -> Goal:
    goal = Goal(goal_id)
    goal.task("task-a", agent="")
    os_._compile_goal(goal)
    return goal


def _epoch_event(os_: AgentOS):
    return next(
        event
        for event in os_.scheduler.events
        if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
    )


def test_plan_frontier_persist_is_durable_across_reopen_and_idempotent(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_)
        epoch = os_.plan_frontier(goal, epoch_id=7, max_parallelism=2, persist=True)
        first = _epoch_event(os_)
        assert first.graph_id == epoch.graph_id
        assert first.graph_version == epoch.graph_version
        assert first.decision_hash == epoch.decision_hash

        # Replaying the same immutable proposal must not append a duplicate.
        replay = os_.plan_frontier(goal.goal_id, epoch_id=7, max_parallelism=2, persist=True)
        assert replay == epoch
        assert [
            event
            for event in os_.scheduler.events
            if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
        ] == [first]
    finally:
        os_.close()

    # The event must survive a fresh durable-store load, including hash-chain
    # verification performed by SchedulerStateStore.load().
    store = SchedulerStateStore(db)
    try:
        state = store.load()
        persisted = [
            event
            for event in state.events
            if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
        ]
        assert len(persisted) == 1
        assert persisted[0].event_id == first.event_id
        assert persisted[0].decision_hash == first.decision_hash
    finally:
        store.close()


def test_plan_frontier_default_is_read_only_and_persist_opt_in(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch-opt-in.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-opt-in-goal")

        epoch = os_.plan_frontier(goal, epoch_id=1)
        assert epoch.selected_task_ids == ("task-a",)
        assert not any(
            event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
            for event in os_.scheduler.events
        )

        os_.plan_frontier(goal, epoch_id=1, persist=True)
        assert (
            len(
                [
                    event
                    for event in os_.scheduler.events
                    if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
                ]
            )
            == 1
        )
    finally:
        os_.close()


def test_plan_frontier_persist_fails_closed_in_read_only_mode(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch-read-only.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-read-only-goal")
        os_._read_only = True
        with pytest.raises(
            ConfigurationError,
            match="read-only AgentOS cannot persist SchedulingEpoch audits",
        ):
            os_.plan_frontier(goal, epoch_id=2, persist=True)
    finally:
        os_.close()


def test_conflicting_scheduling_epoch_event_identity_is_rejected(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch-conflict.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-conflict-goal")
        os_.plan_frontier(goal, epoch_id=3, persist=True)
        event = _epoch_event(os_)
        conflicting = event.model_copy(update={"decision_hash": "f" * 64})
        with pytest.raises(ValueError, match="conflicting scheduler event id"):
            os_.scheduler.record_event(conflicting)
        assert sum(item.event_id == event.event_id for item in os_.scheduler.events) == 1
    finally:
        os_.close()


def test_scheduling_epoch_metadata_is_bounded(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch-bounded.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-bounded-goal")
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        long_id = "x" * 500
        os_.scheduler.record_scheduling_epoch(
            graph_id=gid,
            graph_version=1,
            epoch_id=9,
            policy_id="bounded-test",
            decision_hash="a" * 64,
            candidate_task_ids=[f"task-{index:04d}" for index in range(400)] + [long_id],
            selected_task_ids=[f"selected-{index:04d}" for index in range(400)],
            deferred_task_ids=[f"deferred-{index:04d}" for index in range(400)],
        )
        metadata = _epoch_event(os_).metadata
        assert metadata["task_id_lists_truncated"] is True
        assert metadata["task_id_list_limit"] == 256
        assert metadata["task_id_max_length"] == 160
        for key in ("candidate_task_ids", "selected_task_ids", "deferred_task_ids"):
            values = metadata[key]
            assert len(values) <= 256
            assert all(len(value) <= 160 for value in values)
    finally:
        os_.close()


@pytest.mark.parametrize("invalid", ["not-a-digest", "0" * 63, "g" * 64])
def test_scheduling_epoch_rejects_malformed_decision_hash(tmp_path, invalid: str) -> None:
    db = tmp_path / "scheduling-epoch-invalid-hash.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-invalid-hash-goal")
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        expected = (
            "graph_id, policy_id, and decision_hash must be non-empty"
            if not invalid
            else "64-character SHA-256"
        )
        with pytest.raises(ValueError, match=expected):
            os_.scheduler.record_scheduling_epoch(
                graph_id=gid,
                graph_version=1,
                epoch_id=1,
                policy_id="bounded-test",
                decision_hash=invalid,
            )
    finally:
        os_.close()


def test_scheduling_epoch_rejects_empty_decision_hash(tmp_path) -> None:
    db = tmp_path / "scheduling-epoch-empty-hash.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_, "epoch-empty-hash-goal")
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        with pytest.raises(ValueError, match="must be non-empty"):
            os_.scheduler.record_scheduling_epoch(
                graph_id=gid,
                graph_version=1,
                epoch_id=1,
                policy_id="bounded-test",
                decision_hash="",
            )
    finally:
        os_.close()
