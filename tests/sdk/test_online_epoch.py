"""Focused tests for Scheduler-backed online computation epochs."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ConfigurationError,
    HarnessSessionIdentity,
    OnlineEpochScheduleResult,
    OnlineEpochStatus,
)


def _compiled_goal(os_: AgentOS, goal_id: str, executor) -> object:
    os_.add_agent(Agent("worker", executor=executor))
    goal = os_.goal(goal_id)
    goal.task("task-a", agent="worker")
    os_._compile_goal(goal)
    return goal


def test_online_epoch_plan_only_does_not_claim_or_execute() -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-plan-only", executed.append)

        result = os_.schedule_online_epoch(goal)

        assert result.status is OnlineEpochStatus.PLANNED_ONLY
        assert result.selected_task_ids == ("task-a",)
        assert result.dispatches == ()
        assert result.scheduler_invoked is False
        assert result.claims_retained is False
        assert executed == []
        assert os_.scheduler.active_claim_for_task("task-a", result.graph_id) is None
    finally:
        os_.close()


def test_online_epoch_scheduler_admission_auto_releases_without_execution() -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-auto-release", executed.append)

        result = os_.schedule_online_epoch(goal, plan_only=False)

        assert result.status is OnlineEpochStatus.CLAIMS_RELEASED
        assert result.scheduler_invoked is True
        assert len(result.dispatches) == 1
        dispatch = result.dispatches[0]
        assert dispatch.task_id == "task-a"
        assert dispatch.claim_id
        assert dispatch.attempt_id
        assert dispatch.lease_id
        assert dispatch.lease_fencing_token >= 1
        assert result.released_claim_ids == (dispatch.claim_id,)
        assert result.retained_claim_ids == ()
        assert result.release_required is False
        assert executed == []
        assert os_.scheduler.active_claim_for_task("task-a", result.graph_id) is None
        attempt = os_.scheduler.attempt_for_claim(dispatch.claim_id)
        assert attempt is not None
        assert attempt.state.value == "failed"
        assert attempt.error == "online_epoch_auto_release"
    finally:
        os_.close()


def test_ownerless_start_cannot_register_harness_before_scheduler_ownership() -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-harness-fence", executed.append)
        graph_id = os_._goal_gid[goal.goal_id]
        graph_version = os_.runtime_state(goal).progress.graph_version
        ownerless = CallableHarnessAdapter(
            HarnessSessionIdentity(
                session_id="ownerless-session",
                graph_id=graph_id,
                graph_version=graph_version,
                semantic_epoch=0,
                task_id="task-a",
                agent_id="worker",
                claim_id="missing-claim",
                attempt_id="missing-attempt",
            ),
            executor=lambda: executed.append("ownerless"),
        )
        with pytest.raises(ConfigurationError, match="not an active Scheduler Claim"):
            os_.register_harness(ownerless)

        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        assert result.status is OnlineEpochStatus.CLAIMS_ACQUIRED
        assert result.release_required is True
        dispatch = result.dispatches[0]
        fenced = CallableHarnessAdapter(
            HarnessSessionIdentity(
                session_id="fenced-session",
                graph_id=dispatch.graph_id,
                graph_version=dispatch.graph_version,
                semantic_epoch=dispatch.semantic_epoch,
                task_id=dispatch.task_id,
                agent_id=dispatch.agent_id,
                claim_id=dispatch.claim_id,
                attempt_id=dispatch.attempt_id,
            ),
            executor=lambda: executed.append("fenced"),
        )

        assert os_.register_harness(fenced) is fenced
        assert executed == []
        assert os_.unregister_harness("fenced-session", claim_id=dispatch.claim_id)

        released = os_.release_online_epoch(result)
        assert released.complete is True
        assert released.released_claim_ids == (dispatch.claim_id,)
        assert os_.scheduler.active_claim_for_task("task-a", graph_id) is None
        assert executed == []
    finally:
        os_.close()


def test_online_epoch_uses_selected_tasks_as_scheduler_advisory_filter() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = os_.goal("online-selected-filter")
        goal.task("a", agent="worker")
        goal.task("b", agent="worker")
        os_._compile_goal(goal)

        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
            max_parallelism=1,
        )

        assert result.selected_task_ids == ("a",)
        assert tuple(item.task_id for item in result.dispatches) == ("a",)
        assert os_.scheduler.active_claim_for_task("b", result.graph_id) is None
        cleanup = os_.release_online_epoch(result)
        assert cleanup.complete
    finally:
        os_.close()


def test_online_epoch_fails_closed_and_releases_when_graph_changes_after_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[str] = []
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-graph-race", executed.append)
        original_run_pass = os_.scheduler.run_pass
        original_version = os_._vpg_surface.current_graph_version

        def racing_run_pass(*args, **kwargs):
            scheduled = original_run_pass(*args, **kwargs)
            monkeypatch.setattr(
                os_._vpg_surface,
                "current_graph_version",
                lambda graph_id: original_version(graph_id) + 1,
            )
            return scheduled

        monkeypatch.setattr(os_.scheduler, "run_pass", racing_run_pass)
        result = os_.schedule_online_epoch(goal, plan_only=False)

        assert result.status is OnlineEpochStatus.GRAPH_CHANGED
        assert result.scheduler_invoked is True
        assert len(result.dispatches) == 1
        assert result.released_claim_ids == (result.dispatches[0].claim_id,)
        assert result.retained_claim_ids == ()
        assert os_.scheduler.active_claim_for_task("task-a", result.graph_id) is None
        assert executed == []
    finally:
        os_.close()


def test_online_epoch_auto_release_reports_cleanup_required_on_release_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-cleanup-error", lambda: None)
        original_release = os_.scheduler.release_task

        def failing_release(*args, **kwargs):
            raise RuntimeError("simulated release failure")

        monkeypatch.setattr(os_.scheduler, "release_task", failing_release)
        result = os_.schedule_online_epoch(goal, plan_only=False)

        assert result.status is OnlineEpochStatus.CLEANUP_REQUIRED
        assert result.release_required is True
        assert len(result.retained_claim_ids) == 1

        monkeypatch.setattr(os_.scheduler, "release_task", original_release)
        cleanup = os_.release_online_epoch(result)
        assert cleanup.complete is True
        assert cleanup.released_claim_ids == result.retained_claim_ids
    finally:
        os_.close()


def test_release_online_epoch_is_idempotent_for_terminal_claim() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "online-release-idempotent", lambda: None)
        result = os_.schedule_online_epoch(goal, plan_only=False, keep_claims=True)
        claim_id = result.retained_claim_ids[0]

        first = os_.release_online_epoch(result)
        second = os_.release_online_epoch(result)

        assert first.complete is True
        assert first.released_claim_ids == (claim_id,)
        assert second.complete is True
        assert second.released_claim_ids == ()
        assert second.already_terminal_claim_ids == (claim_id,)
        assert second.not_released_claim_ids == ()
    finally:
        os_.close()


def test_schedule_result_rejects_dispatch_from_another_graph() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "dto-cross-graph", lambda: None)
        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        payload = result.model_dump(mode="python")
        dispatch = dict(payload["dispatches"][0])
        dispatch["graph_id"] = "different-graph"
        payload["dispatches"] = (dispatch,)

        with pytest.raises(ValidationError, match="dispatch graph_id"):
            OnlineEpochScheduleResult.model_validate(payload)
    finally:
        os_.close()


def test_schedule_result_rejects_dispatch_for_unselected_task() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "dto-unselected", lambda: None)
        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        payload = result.model_dump(mode="python")
        payload["selected_task_ids"] = ()

        with pytest.raises(ValidationError, match="must be selected"):
            OnlineEpochScheduleResult.model_validate(payload)
    finally:
        os_.close()


def test_schedule_result_rejects_incomplete_claim_ownership_partition() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "dto-partition", lambda: None)
        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        payload = result.model_dump(mode="python")
        payload["status"] = OnlineEpochStatus.CLAIMS_RELEASED
        payload["retained_claim_ids"] = ()
        payload["released_claim_ids"] = ()
        payload["claims_retained"] = False

        with pytest.raises(
            ValidationError,
            match="partition dispatched Claims",
        ):
            OnlineEpochScheduleResult.model_validate(payload)
    finally:
        os_.close()


def test_schedule_result_rejects_scheduler_dispatch_on_plan_only_status() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = _compiled_goal(os_, "dto-plan-status", lambda: None)
        result = os_.schedule_online_epoch(
            goal,
            plan_only=False,
            keep_claims=True,
        )
        payload = result.model_dump(mode="python")
        payload["status"] = OnlineEpochStatus.PLANNED_ONLY
        payload["scheduler_invoked"] = False
        payload["retained_claim_ids"] = ()
        payload["released_claim_ids"] = ()
        payload["claims_retained"] = False

        with pytest.raises(
            ValidationError,
            match="dispatches must invoke the Scheduler",
        ):
            OnlineEpochScheduleResult.model_validate(payload)
    finally:
        os_.close()
