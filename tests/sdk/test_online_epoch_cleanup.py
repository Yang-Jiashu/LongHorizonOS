"""Failure-boundary tests for one-shot online epoch ownership cleanup."""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import AttemptState
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk import Agent, AgentOS, Goal, SchedulingError, VerificationOutcome


@pytest.mark.asyncio
async def test_post_admission_observation_failure_releases_exact_claim_and_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("online-post-admission-failure")
        goal.task(
            "task",
            agent="worker",
            verify=lambda: VerificationOutcome(
                passed=True,
                artifact_id="artifact",
                version=1,
                content="artifact-v1",
            ),
        )

        def fail_observation(graph_id: str) -> dict[str, int]:
            del graph_id
            raise RuntimeError("post-admission observation failed")

        monkeypatch.setattr(os_.scheduler._s, "observe_vpg", fail_observation)

        with pytest.raises(SchedulingError, match="scheduler pass failed"):
            await os_.execute_online_epoch(goal)

        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        assert executed == []
        assert os_.scheduler.active_claim_for_task("task", gid) is None
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.attempts[-1].state is AttemptState.FAILED
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(
                claim_resource_uri(gid, "task")
            )
            == []
        )
    finally:
        os_.close()


def test_run_pass_cleanup_failure_adds_exact_claim_id_to_root_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compensation errors must remain observable without replacing the root error."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("online-cleanup-error-observable")
        goal.task("task", agent="worker")
        os_._compile_goal(goal)
        original_release = os_.scheduler._s.release_task

        def fail_observation(graph_id: str) -> dict[str, int]:
            del graph_id
            raise RuntimeError("post-admission observation failed")

        def fail_release(*args: object, **kwargs: object) -> bool:
            del args, kwargs
            raise RuntimeError("lease release failed")

        monkeypatch.setattr(os_.scheduler._s, "observe_vpg", fail_observation)
        monkeypatch.setattr(os_.scheduler._s, "release_task", fail_release)

        with pytest.raises(RuntimeError, match="post-admission observation failed") as caught:
            os_.scheduler.run_pass(os_._gid_for(goal.goal_id) or "")

        notes = getattr(caught.value, "__notes__", ())
        assert notes
        claim_id = os_.scheduler.claims[-1].claim_id
        assert claim_id in notes[-1]
        assert "lease release failed" in notes[-1]
        # The injected release failure is intentionally fail-closed: ownership
        # remains visible for reconciliation rather than being reported gone.
        assert os_.scheduler.active_claim_for_task("task", os_._gid_for(goal.goal_id)) is not None
        assert os_.scheduler.claims[-1].state.value == "active"

        # Restore the real release path so fixture cleanup cannot mask the
        # assertion above or leave the temporary database with a live lease.
        monkeypatch.setattr(os_.scheduler._s, "release_task", original_release)
        assert os_.scheduler.release_task(
            os_._gid_for(goal.goal_id) or "",
            "task",
            expected_claim_id=claim_id,
            reason="test_cleanup",
        )
    finally:
        os_.close()


def test_run_pass_compensation_does_not_release_replacement_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replacement owner must survive cleanup of the failed pass's claim."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("online-replacement-fence")
        goal.task("task", agent="worker")
        os_._compile_goal(goal)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        original_observe = os_.scheduler._s.observe_vpg
        old_claim_id: str | None = None
        replacement_claim_id: str | None = None

        def race_then_fail(graph_id: str) -> dict[str, int]:
            nonlocal old_claim_id, replacement_claim_id
            claim = os_.scheduler.active_claim_for_task("task", graph_id)
            assert claim is not None
            old_claim_id = claim.claim_id
            assert os_.scheduler.release_task(
                graph_id,
                "task",
                expected_claim_id=old_claim_id,
                reason="test_replacement_race",
            )
            replacement = os_.scheduler.schedule_once(graph_id)
            assert replacement.dispatched
            replacement_claim_id = replacement.dispatched[0]["claim_id"]
            raise RuntimeError("post-admission race")

        monkeypatch.setattr(os_.scheduler._s, "observe_vpg", race_then_fail)
        with pytest.raises(RuntimeError, match="post-admission race"):
            os_.scheduler.run_pass(gid)

        assert old_claim_id is not None
        assert replacement_claim_id is not None
        active = os_.scheduler.active_claim_for_task("task", gid)
        assert active is not None
        assert active.claim_id == replacement_claim_id
        # Compensation is fenced to the original claim and therefore cannot
        # release the replacement owner.
        assert active.claim_id != old_claim_id
        assert active.state.value == "active"

        monkeypatch.setattr(os_.scheduler._s, "observe_vpg", original_observe)
        assert os_.scheduler.release_task(
            gid,
            "task",
            expected_claim_id=replacement_claim_id,
            reason="test_cleanup",
        )
    finally:
        os_.close()
