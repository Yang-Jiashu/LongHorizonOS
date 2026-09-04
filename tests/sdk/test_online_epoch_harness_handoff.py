"""Focused tests for retained online-epoch Claim -> Harness handoff."""

from __future__ import annotations

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ConfigurationError,
    HarnessHookOutcome,
    HarnessSessionIdentity,
    OnlineEpochHarnessHandoffStatus,
    OnlineEpochStatus,
)


def _retained_epoch() -> tuple[AgentOS, object]:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker"))
    goal = os_.goal("online-harness-handoff")
    goal.task("task-a", agent="worker")
    os_._compile_goal(goal)
    result = os_.schedule_online_epoch(
        goal,
        plan_only=False,
        keep_claims=True,
    )
    assert result.status is OnlineEpochStatus.CLAIMS_ACQUIRED
    return os_, result


def _adapter(dispatch, *, session_id: str = "session-a") -> CallableHarnessAdapter:
    return CallableHarnessAdapter(
        HarnessSessionIdentity(
            session_id=session_id,
            graph_id=dispatch.graph_id,
            graph_version=dispatch.graph_version,
            semantic_epoch=dispatch.semantic_epoch,
            task_id=dispatch.task_id,
            agent_id=dispatch.agent_id,
            claim_id=dispatch.claim_id,
            attempt_id=dispatch.attempt_id,
        ),
        start=lambda _request, _snapshot: HarnessHookOutcome(progress=0.1),
    )


def test_retained_epoch_handoff_binds_exact_harness_and_is_replayable() -> None:
    os_, result = _retained_epoch()
    try:
        dispatch = result.dispatches[0]
        harness = _adapter(dispatch)

        first = os_.handoff_online_epoch_to_harness(
            result,
            {dispatch.claim_id: harness},
        )

        assert first.status is OnlineEpochHarnessHandoffStatus.BOUND
        assert first.complete
        assert first.bound_claim_ids == (dispatch.claim_id,)
        assert first.replayed_claim_ids == ()
        assert first.bindings[0].claim_id == dispatch.claim_id
        assert first.bindings[0].lease_id == dispatch.lease_id
        assert os_.harness_for_claim(dispatch.claim_id) is harness

        replay = os_.handoff_online_epoch_to_harness(
            result,
            {dispatch.claim_id: harness},
        )
        assert replay.status is OnlineEpochHarnessHandoffStatus.REPLAYED
        assert replay.replayed_claim_ids == (dispatch.claim_id,)
        assert replay.result_hash
    finally:
        os_.close()


def test_handoff_preflight_rejects_identity_mismatch_without_releasing_claim() -> None:
    os_, result = _retained_epoch()
    try:
        dispatch = result.dispatches[0]
        wrong = CallableHarnessAdapter(
            HarnessSessionIdentity(
                session_id="wrong-session",
                graph_id=dispatch.graph_id,
                graph_version=dispatch.graph_version,
                semantic_epoch=dispatch.semantic_epoch,
                task_id=dispatch.task_id,
                agent_id=dispatch.agent_id,
                claim_id=dispatch.claim_id,
                attempt_id="wrong-attempt",
            ),
            start=lambda _request, _snapshot: HarnessHookOutcome(),
        )

        handoff = os_.handoff_online_epoch_to_harness(
            result,
            {dispatch.claim_id: wrong},
        )

        assert handoff.status is OnlineEpochHarnessHandoffStatus.REFUSED
        assert handoff.refused_claim_ids == (dispatch.claim_id,)
        assert (
            os_.scheduler.active_claim_for_task(
                dispatch.task_id,
                result.graph_id,
            )
            is not None
        )
        assert os_.harness_for_claim(dispatch.claim_id) is None
    finally:
        os_.close()


def test_handoff_rejects_terminal_claim_and_does_not_touch_replacement_owner() -> None:
    os_, result = _retained_epoch()
    try:
        dispatch = result.dispatches[0]
        cleanup = os_.release_online_epoch(result)
        assert cleanup.complete
        harness = _adapter(dispatch, session_id="late-session")

        handoff = os_.handoff_online_epoch_to_harness(
            result,
            {dispatch.claim_id: harness},
        )

        assert handoff.status is OnlineEpochHarnessHandoffStatus.REFUSED
        assert handoff.refused_claim_ids == (dispatch.claim_id,)
        assert os_.harness_for_claim(dispatch.claim_id) is None
        assert (
            os_.scheduler.active_claim_for_task(
                dispatch.task_id,
                result.graph_id,
            )
            is None
        )
    finally:
        os_.close()


def test_handoff_requires_exact_mapping_and_preserves_ownership_on_error() -> None:
    os_, result = _retained_epoch()
    try:
        dispatch = result.dispatches[0]
        with pytest.raises(ConfigurationError, match="cover exactly"):
            os_.handoff_online_epoch_to_harness(result, {})

        assert (
            os_.scheduler.active_claim_for_task(
                dispatch.task_id,
                result.graph_id,
            )
            is not None
        )
    finally:
        os_.close()
