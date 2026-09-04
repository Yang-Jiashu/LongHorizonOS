"""Integration tests for the AgentOS Harness control-plane bridge.

The Harness is an execution unit; Scheduler Claims/Attempts remain the
ownership authority and no Harness transition may publish semantic Evidence.
"""

from __future__ import annotations

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ConfigurationError,
    Goal,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionIdentity,
)


def _active_claim() -> tuple[AgentOS, Goal, object, object]:
    os_ = AgentOS(":memory:")
    os_.add_agent(Agent("worker"))
    goal = Goal("harness-bridge-goal")
    goal.task("task-a", agent="worker")
    graph_id = os_._compile_goal(goal)
    scheduled = os_.scheduler.run_pass(graph_id, max_claims=1)
    assert len(scheduled.dispatched) == 1
    claim = os_.scheduler.active_claim_for_task("task-a", graph_id)
    assert claim is not None
    attempt = os_.scheduler.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    return os_, goal, claim, attempt


def _harness_for(claim, attempt) -> CallableHarnessAdapter:
    return CallableHarnessAdapter(
        HarnessSessionIdentity(
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            semantic_epoch=attempt.semantic_epoch,
            task_id=claim.task_id,
            agent_id=claim.agent_id,
            claim_id=claim.claim_id,
            attempt_id=attempt.attempt_id,
        ),
        start=lambda _request, _snapshot: HarnessHookOutcome(progress=0.25),
        checkpoint=lambda _request, _snapshot: HarnessHookOutcome(
            checkpoint_id="cp-1",
            progress=0.5,
        ),
    )


def test_harness_control_is_fenced_and_does_not_change_scheduler_ownership() -> None:
    os_, _goal, claim, attempt = _active_claim()
    try:
        harness = _harness_for(claim, attempt)
        os_.register_harness(harness)
        before_claim = claim.model_dump(mode="json")
        before_attempt = attempt.model_dump(mode="json")

        result = os_.control_harness_sync(
            claim.claim_id,
            HarnessOperation.START,
            request_id="harness-start-once",
            reason="begin next execution epoch",
        )

        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state.value == "running"
        assert claim.model_dump(mode="json") == before_claim
        assert attempt.model_dump(mode="json") == before_attempt
        events = [
            event for event in os_.scheduler.events if event.event_type.value == "harness_control"
        ]
        assert len(events) == 1
        assert events[0].claim_id == claim.claim_id
        assert events[0].attempt_id == attempt.attempt_id
        assert events[0].metadata["request_id"] == "harness-start-once"

        # The adapter's idempotency result is replayed and the bridge does not
        # append a second event for the same exact request.
        replay = os_.control_harness_sync(
            claim.claim_id,
            HarnessOperation.START,
            request_id="harness-start-once",
            reason="begin next execution epoch",
        )
        assert replay == result
        assert (
            len(
                [
                    event
                    for event in os_.scheduler.events
                    if event.event_type.value == "harness_control"
                ]
            )
            == 1
        )
    finally:
        os_.close()


def test_harness_registration_rejects_nonmatching_claim_attempt_identity() -> None:
    os_, _goal, claim, attempt = _active_claim()
    try:
        wrong = CallableHarnessAdapter(
            HarnessSessionIdentity(
                graph_id=claim.graph_id,
                graph_version=claim.graph_version,
                semantic_epoch=attempt.semantic_epoch,
                task_id=claim.task_id,
                agent_id=claim.agent_id,
                claim_id=claim.claim_id,
                attempt_id="different-attempt",
            ),
            start=lambda _request, _snapshot: HarnessHookOutcome(),
        )
        with pytest.raises(ConfigurationError, match="does not match"):
            os_.register_harness(wrong)
    finally:
        os_.close()


def test_harness_registration_is_single_owner_per_claim() -> None:
    os_, _goal, claim, attempt = _active_claim()
    try:
        first = _harness_for(claim, attempt)
        second = _harness_for(claim, attempt)
        os_.register_harness(first)
        with pytest.raises(ConfigurationError, match="already has Harness"):
            os_.register_harness(second)
        assert os_.harness_for_claim(claim.claim_id) is first
        assert os_.unregister_harness(first.snapshot.identity.session_id)
        assert os_.harness_for_claim(claim.claim_id) is None
    finally:
        os_.close()


def test_harness_control_requires_live_registered_claim() -> None:
    os_ = AgentOS(":memory:")
    try:
        with pytest.raises(ConfigurationError, match="no Harness"):
            os_.control_harness_sync("missing-claim", HarnessOperation.START)
    finally:
        os_.close()


def test_harness_control_reopens_from_durable_event_history(tmp_path) -> None:
    """A restart restores only the bounded session projection and idempotency.

    The fresh adapter's hook must not run during registration or replay: the
    journal restores logical Harness state, not arbitrary callback memory.
    """

    db = tmp_path / "harness-reopen.sqlite"
    writer = AgentOS(str(db))
    writer.add_agent(Agent("worker"))
    goal = Goal("harness-reopen-goal")
    goal.task("task-a", agent="worker")
    graph_id = writer._compile_goal(goal)
    assert writer.scheduler.run_pass(graph_id, max_claims=1).dispatched
    durable_claim = writer.scheduler.active_claim_for_task("task-a", graph_id)
    assert durable_claim is not None
    durable_attempt = writer.scheduler.attempt_for_claim(durable_claim.claim_id)
    assert durable_attempt is not None
    first = _harness_for(durable_claim, durable_attempt)
    writer.register_harness(first)
    applied = writer.control_harness_sync(
        durable_claim.claim_id,
        HarnessOperation.START,
        request_id="durable-start-once",
    )
    assert applied.status is HarnessResultStatus.APPLIED
    event_count = len(
        [event for event in writer.scheduler.events if event.event_type.value == "harness_control"]
    )
    session_identity = first.snapshot.identity
    writer.close()

    calls: list[str] = []
    reopened = AgentOS(str(db))
    try:
        replacement = CallableHarnessAdapter(
            session_identity,
            start=lambda _request, _snapshot: (
                calls.append("start"),
                HarnessHookOutcome(progress=0.99),
            )[1],
        )
        reopened.register_harness(replacement)
        assert replacement.snapshot.state.value == "running"
        assert replacement.snapshot.revision == 1
        assert replacement.snapshot.progress == 0.25
        assert calls == []

        replay = reopened.control_harness_sync(
            durable_claim.claim_id,
            HarnessOperation.START,
            request_id="durable-start-once",
        )
        assert replay == applied
        assert calls == []
        assert (
            len(
                [
                    event
                    for event in reopened.scheduler.events
                    if event.event_type.value == "harness_control"
                ]
            )
            == event_count
        )
    finally:
        reopened.close()


def test_harness_reopen_rejects_replacement_session_without_handoff(tmp_path) -> None:
    """A new session cannot silently take over a Claim with durable history."""

    db = tmp_path / "harness-replacement.sqlite"
    writer = AgentOS(str(db))
    writer.add_agent(Agent("worker"))
    goal = Goal("harness-replacement-goal")
    goal.task("task-a", agent="worker")
    graph_id = writer._compile_goal(goal)
    assert writer.scheduler.run_pass(graph_id, max_claims=1).dispatched
    claim = writer.scheduler.active_claim_for_task("task-a", graph_id)
    assert claim is not None
    attempt = writer.scheduler.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    first = _harness_for(claim, attempt)
    writer.register_harness(first)
    writer.control_harness_sync(
        claim.claim_id,
        HarnessOperation.START,
        request_id="replacement-start",
    )
    writer.close()

    reopened = AgentOS(str(db))
    try:
        replacement_identity = HarnessSessionIdentity(
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            semantic_epoch=attempt.semantic_epoch,
            task_id=claim.task_id,
            agent_id=claim.agent_id,
            claim_id=claim.claim_id,
            attempt_id=attempt.attempt_id,
        )
        replacement = CallableHarnessAdapter(
            replacement_identity,
            start=lambda _request, _snapshot: HarnessHookOutcome(),
        )
        # The new identity gets a new session_id by default; durable history is
        # therefore intentionally not transferable without a handoff protocol.
        assert replacement_identity.session_id != first.snapshot.identity.session_id
        with pytest.raises(ConfigurationError, match="another or legacy session"):
            reopened.register_harness(replacement)
    finally:
        reopened.close()
