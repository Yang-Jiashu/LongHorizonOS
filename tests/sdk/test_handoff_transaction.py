from __future__ import annotations

from lhos.sdk import (
    Agent,
    AgentOS,
    OnlineEpochStatus,
    OwnershipHandoffStatus,
)


def _retained() -> tuple[AgentOS, object]:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker-a"))
    os_.add_agent(Agent("worker-b"))
    goal = os_.goal("handoff-transaction-sdk")
    goal.task("task", agent="worker-a")
    os_._compile_goal(goal)
    epoch = os_.schedule_online_epoch(goal, plan_only=False, keep_claims=True)
    assert epoch.status is OnlineEpochStatus.CLAIMS_ACQUIRED
    return os_, epoch


def test_agent_os_exposes_bounded_handoff_intent_protocol() -> None:
    os_, epoch = _retained()
    try:
        dispatch = epoch.dispatches[0]
        prepared = os_.prepare_handoff(
            dispatch.graph_id,
            dispatch.task_id,
            source_claim_id=dispatch.claim_id,
            replacement_agent_id="worker-b",
            expected_attempt_id=dispatch.attempt_id,
            expected_semantic_epoch=dispatch.semantic_epoch,
            handoff_id="sdk-handoff-1",
            action="preempt",
        )
        assert prepared.status is OwnershipHandoffStatus.PREPARED

        committed = os_.commit_handoff(prepared.intent)
        assert committed.status is OwnershipHandoffStatus.COMMITTED
        assert committed.transferred

        replay = os_.commit_handoff(prepared.intent.model_dump(mode="json"))
        assert replay.status is OwnershipHandoffStatus.REPLAYED
        assert replay.replacement_claim_id == committed.replacement_claim_id
    finally:
        os_.close()


def test_agent_os_handoff_wrapper_rejects_read_only_runtime() -> None:
    # The public wrapper must not accidentally bypass the Scheduler's write
    # authority when an AgentOS instance was opened read-only.
    import pytest

    with pytest.raises(Exception, match="read-only AgentOS"):
        AgentOS(":memory:", read_only=True)
