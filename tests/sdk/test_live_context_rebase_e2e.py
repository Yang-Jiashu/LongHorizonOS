"""Authority-backed end-to-end tests for the bounded live rebase façade."""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256

import pytest

from lhos.runtimes.multi_agent.models import AgentSnapshot, ResourceBinding
from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ContextGraphChange,
    ContextGraphDelta,
    ContextRebaseAction,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessSessionIdentity,
    OwnershipHandoffStatus,
)


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _binding(artifact_id: str) -> ResourceBinding:
    return ResourceBinding(
        operation="read",
        resource_uri=f"vpg://{artifact_id}",
        artifact_id=artifact_id,
        version=1,
        content_hash=_hash(f"{artifact_id}@1"),
    )


def _live_session(
    *read_artifact_ids: str,
) -> tuple[AgentOS, object, object, object, CallableHarnessAdapter, list[str]]:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker"))
    goal = os_.goal("live-context-rebase")
    goal.task("task", agent="worker")
    os_._compile_goal(goal)
    epoch = os_.schedule_online_epoch(
        goal,
        plan_only=False,
        keep_claims=True,
    )
    dispatch = epoch.dispatches[0]

    assert os_.scheduler.bind_attempt_context_snapshot(
        dispatch.claim_id,
        snapshot_id="ctx-task-v1",
        manifest_id="manifest-task-v1",
        manifest_hash=_hash("manifest-task-v1"),
        working_set_hash=_hash("working-set-task-v1"),
        materialized_hash=_hash("materialized-task-v1"),
    )
    attempt = os_.scheduler.attempt_for_claim(dispatch.claim_id)
    assert attempt is not None
    snapshot = AgentSnapshot.from_attempt(
        attempt,
        context_bindings=tuple(_binding(item) for item in read_artifact_ids),
    )
    assert os_.scheduler.bind_agent_snapshot(dispatch.claim_id, snapshot)

    calls: list[str] = []

    def _start(*_args: object) -> HarnessHookOutcome:
        calls.append("start")
        return HarnessHookOutcome(progress=0.1)

    def _continue(*_args: object) -> HarnessHookOutcome:
        calls.append("continue")
        return HarnessHookOutcome(progress=0.2)

    harness = CallableHarnessAdapter(
        HarnessSessionIdentity(
            session_id="session-task",
            graph_id=dispatch.graph_id,
            graph_version=dispatch.graph_version,
            semantic_epoch=dispatch.semantic_epoch,
            task_id=dispatch.task_id,
            agent_id=dispatch.agent_id,
            claim_id=dispatch.claim_id,
            attempt_id=dispatch.attempt_id,
        ),
        start=_start,
        continue_handler=_continue,
        rebase=lambda *_args: HarnessHookOutcome(progress=0.05),
    )
    os_.register_harness(harness)

    # A live rebase decision must be planned against the authoritative VPG
    # version, not an invented future version. Advance the graph through its
    # normal empty derived-state transaction while retaining the old Attempt.
    refreshed = os_._vpg.refresh_derived_state(
        dispatch.graph_id,
        author_pid="live-rebase-test",
        reason="authoritative graph advance for live rebase",
        expected_graph_version=dispatch.graph_version,
    )
    assert refreshed.committed_graph_version == dispatch.graph_version + 1
    return os_, goal, epoch, dispatch, harness, calls


def _api_delta(graph_id: str) -> ContextGraphDelta:
    return ContextGraphDelta(
        graph_id=graph_id,
        changes=(
            ContextGraphChange(
                artifact_id="api",
                old_version=1,
                new_version=2,
                new_content_hash=_hash("api@2"),
            ),
        ),
        coverage="complete",
    )


def _plan(os_: AgentOS, goal: object, dispatch: object):
    return os_.plan_live_context_rebase(
        goal,
        task_id=dispatch.task_id,
        claim_id=dispatch.claim_id,
        agent_id=dispatch.agent_id,
        graph_delta=_api_delta(dispatch.graph_id),
        target_graph_version=dispatch.graph_version + 1,
        reason="API changed",
    )


@pytest.mark.asyncio
async def test_live_reuse_applies_once_and_same_plan_replays_idempotently() -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session("docs")
    try:
        plan = _plan(os_, goal, dispatch)

        first = await os_.apply_live_context_rebase(plan)
        revision_after_first = harness.snapshot.revision
        replay = await os_.apply_live_context_rebase(plan)

        assert plan.decision.action is ContextRebaseAction.REUSE
        assert plan.decision.control_request is not None
        assert plan.decision.control_request.operation is HarnessOperation.START
        assert first.applied is True
        assert first.replayed is False
        assert first.refused is False
        assert replay.applied is True
        assert replay.replayed is True
        assert replay.refused is False
        assert replay.harness_result == first.harness_result
        assert harness.snapshot.revision == revision_after_first == 1
        assert calls == ["start"]
    finally:
        os_.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reads", "expected_action"),
    (
        (("api", "auth"), ContextRebaseAction.REBASE),
        (("api",), ContextRebaseAction.FULL_RELOAD),
    ),
)
async def test_changed_read_fences_old_harness_and_admits_fresh_attempt(
    reads: tuple[str, ...],
    expected_action: ContextRebaseAction,
) -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session(*reads)
    try:
        plan = _plan(os_, goal, dispatch)
        result = await os_.apply_live_context_rebase(plan)

        assert plan.decision.action is expected_action
        assert result.applied is True
        assert result.refused is False
        assert result.ownership_unchanged is False
        assert result.handoff_attempted is True
        assert result.handoff_id
        assert result.replacement_claim_id
        assert result.replacement_attempt_id
        assert "fresh Attempt admitted" in result.reason
        assert harness.snapshot.revision == 0
        assert calls == []
        assert os_.harness_for_claim(dispatch.claim_id) is None
        active = os_.scheduler.active_claim_for_task(
            dispatch.task_id,
            dispatch.graph_id,
        )
        assert active is not None
        assert active.claim_id == result.replacement_claim_id
        assert active.claim_id != dispatch.claim_id
        assert os_.scheduler.attempt_for_claim(active.claim_id).attempt_id == (
            result.replacement_attempt_id
        )
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_live_rebase_handoff_replays_after_source_detach() -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session("api")
    try:
        plan = _plan(os_, goal, dispatch)
        first = await os_.apply_live_context_rebase(plan)
        second = await os_.apply_live_context_rebase(plan)

        assert first.applied is True
        assert first.handoff_attempted is True
        assert second.applied is True
        assert second.replayed is True
        assert second.handoff_replayed is True
        assert second.handoff_id == first.handoff_id
        assert second.replacement_claim_id == first.replacement_claim_id
        assert second.replacement_attempt_id == first.replacement_attempt_id
        assert calls == []
        active = os_.scheduler.active_claim_for_task(
            dispatch.task_id,
            dispatch.graph_id,
        )
        assert active is not None
        assert active.claim_id == first.replacement_claim_id
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_live_rebase_replay_survives_unrelated_graph_advance_after_commit() -> None:
    """Durable idempotency replay must not be masked by later graph progress."""

    os_, goal, _epoch, dispatch, harness, calls = _live_session("api")
    try:
        plan = _plan(os_, goal, dispatch)
        first = await os_.apply_live_context_rebase(plan)
        assert first.applied is True

        current = int(os_.vpg.get_graph(dispatch.graph_id).current_version)
        refreshed = os_.vpg.refresh_derived_state(
            dispatch.graph_id,
            author_pid="live-rebase-replay-test",
            reason="unrelated graph advance after handoff",
            expected_graph_version=current,
        )
        assert refreshed.committed_graph_version == current + 1

        replay = await os_.apply_live_context_rebase(plan)

        assert replay.applied is True
        assert replay.replayed is True
        assert replay.handoff_replayed is True
        assert replay.replacement_claim_id == first.replacement_claim_id
        assert replay.replacement_attempt_id == first.replacement_attempt_id
        assert harness.snapshot.revision == 0
        assert calls == []
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_live_rebase_surfaces_durable_in_doubt_recovery_after_source_detach(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A crash gap must remain actionable instead of becoming generic fence loss."""

    os_, goal, _epoch, dispatch, harness, calls = _live_session("api")
    try:
        plan = _plan(os_, goal, dispatch)
        recovery = type(
            "RecoveryWitness",
            (),
            {
                "status": OwnershipHandoffStatus.IN_DOUBT,
                "intent": type(
                    "Intent",
                    (),
                    {
                        "graph_id": plan.graph_id,
                        "task_id": plan.task_id,
                        "source_claim_id": plan.claim_id,
                        "source_attempt_id": plan.attempt_id,
                        "source_agent_id": plan.agent_id,
                        "replacement_agent_id": plan.agent_id,
                        "source_graph_version": plan.source_graph_version,
                        "source_semantic_epoch": plan.source_semantic_epoch,
                        "action": "rebase",
                    },
                )(),
                "source_lease_released": None,
                "replacement_claim_id": None,
                "replacement_attempt_id": None,
                "reason": "crash gap after source release",
            },
        )()
        monkeypatch.setattr(
            os_,
            "_live_claim",
            lambda *_args, **_kwargs: None,
        )
        monkeypatch.setattr(
            os_,
            "recover_handoff",
            lambda _handoff_id: recovery,
        )

        result = await os_.apply_live_context_rebase(plan)

        assert result.applied is False
        assert result.refused is True
        assert result.recovery_required is True
        assert result.handoff_result is recovery
        assert result.ownership_unchanged is False
        assert "crash gap after source release" in result.reason
        assert harness.snapshot.revision == 0
        assert calls == []
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_live_rebase_marks_recovery_required_when_witness_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session("api")
    try:
        plan = _plan(os_, goal, dispatch)
        monkeypatch.setattr(
            os_,
            "_live_claim",
            lambda *_args, **_kwargs: None,
        )

        def _raise(_handoff_id: str) -> object:
            raise RuntimeError("journal unavailable")

        monkeypatch.setattr(os_, "recover_handoff", _raise)

        result = await os_.apply_live_context_rebase(plan)

        assert result.applied is False
        assert result.refused is True
        assert result.recovery_required is True
        assert "journal unavailable" in result.reason
        assert harness.snapshot.revision == 0
        assert calls == []
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_claim_and_lease_loss_after_planning_fails_closed() -> None:
    os_, goal, epoch, dispatch, harness, calls = _live_session("docs")
    try:
        plan = _plan(os_, goal, dispatch)
        released = os_.release_online_epoch(epoch)
        assert released.released_claim_ids == (dispatch.claim_id,)

        result = await os_.apply_live_context_rebase(plan)

        assert result.applied is False
        assert result.refused is True
        assert "Claim/Lease fence" in result.reason
        assert harness.snapshot.revision == 0
        assert calls == []
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_unrelated_harness_revision_change_after_planning_fails_closed() -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session("docs")
    try:
        plan = _plan(os_, goal, dispatch)
        other = await os_.control_harness(
            dispatch.claim_id,
            HarnessOperation.START,
            request_id="independent-start",
            reason="another controller won",
        )
        assert other.applied

        result = await os_.apply_live_context_rebase(plan)

        assert result.applied is False
        assert result.refused is True
        assert result.replayed is False
        assert "identity changed since planning" in result.reason
        assert harness.snapshot.revision == 1
        assert calls == ["start"]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_agent_snapshot_change_after_planning_fails_closed() -> None:
    os_, goal, _epoch, dispatch, harness, calls = _live_session("docs")
    try:
        plan = _plan(os_, goal, dispatch)
        attempt = os_.scheduler.attempt_for_claim(dispatch.claim_id)
        assert attempt is not None and attempt.agent_snapshot is not None
        old = attempt.agent_snapshot
        updated = old.model_copy(
            update={
                "captured_at": old.captured_at + timedelta(microseconds=1),
                "progress": 0.25,
            }
        )
        assert os_.scheduler.bind_agent_snapshot(dispatch.claim_id, updated)

        result = await os_.apply_live_context_rebase(plan)

        assert result.applied is False
        assert result.refused is True
        assert "identity changed since planning" in result.reason
        assert harness.snapshot.revision == 0
        assert calls == []
    finally:
        os_.close()
