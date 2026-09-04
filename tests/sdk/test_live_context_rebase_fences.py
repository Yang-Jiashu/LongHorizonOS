"""Fail-closed identity fences for the bounded live Context-rebase façade."""

from __future__ import annotations

from datetime import UTC

import pytest

from lhos.runtimes.multi_agent.models import (
    AgentSnapshot,
    AttemptState,
    ContextIdentity,
    ScheduledExecutionAttempt,
    TaskClaim,
)
from lhos.sdk import (
    Agent,
    AgentOS,
    CallableHarnessAdapter,
    ConfigurationError,
    ContextGraphDelta,
    Goal,
    HarnessHookOutcome,
    HarnessSessionIdentity,
)


def _active_runtime() -> tuple[AgentOS, Goal, TaskClaim, ScheduledExecutionAttempt]:
    runtime = AgentOS(":memory:")
    runtime.add_agent(Agent("worker"))
    goal = Goal("live-rebase-fences")
    goal.task("task-a", agent="worker")
    graph_id = runtime._compile_goal(goal)
    epoch = runtime.schedule_online_epoch(
        goal,
        plan_only=False,
        keep_claims=True,
    )
    assert epoch.dispatches
    dispatch = epoch.dispatches[0]
    claim = runtime.scheduler.active_claim_for_task(dispatch.task_id, graph_id)
    assert claim is not None
    attempt = runtime.scheduler.attempt_for_claim(claim.claim_id)
    assert attempt is not None
    assert runtime.scheduler.bind_attempt_context_snapshot(
        claim.claim_id,
        snapshot_id="context-1",
        manifest_id="manifest-1",
        manifest_hash="a" * 64,
        working_set_hash="b" * 64,
        materialized_hash="c" * 64,
    )
    identity = ContextIdentity.from_attempt(attempt)
    assert identity is not None
    snapshot = AgentSnapshot(
        agent_id=attempt.agent_id,
        process_id=attempt.process_id,
        task_id=attempt.task_id,
        claim_id=attempt.claim_id,
        attempt_id=attempt.attempt_id,
        graph_id=attempt.graph_id,
        graph_version=attempt.graph_version,
        semantic_epoch=attempt.semantic_epoch,
        context_identity=identity,
        started_at=attempt.started_at,
        captured_at=attempt.started_at.replace(tzinfo=UTC),
        state=AttemptState.DISPATCHED,
    )
    assert runtime.scheduler.bind_agent_snapshot(claim.claim_id, snapshot)
    harness = CallableHarnessAdapter(
        HarnessSessionIdentity(
            graph_id=claim.graph_id,
            graph_version=claim.graph_version,
            semantic_epoch=attempt.semantic_epoch,
            task_id=claim.task_id,
            agent_id=claim.agent_id,
            claim_id=claim.claim_id,
            attempt_id=attempt.attempt_id,
        ),
        start=lambda _request, _snapshot: HarnessHookOutcome(progress=0.1),
        continue_handler=lambda _request, _snapshot: HarnessHookOutcome(progress=0.2),
    )
    runtime.register_harness(harness)
    return runtime, goal, claim, attempt


def _empty_delta(graph_id: str) -> ContextGraphDelta:
    return ContextGraphDelta(graph_id=graph_id, coverage="partial")


def test_live_rebase_plan_rejects_target_ahead_of_authoritative_vpg() -> None:
    runtime, goal, claim, _attempt = _active_runtime()
    try:
        current = runtime.vpg.get_graph(claim.graph_id).current_version
        with pytest.raises(
            ConfigurationError,
            match="equal the current authoritative VPG version",
        ):
            runtime.plan_live_context_rebase(
                goal,
                task_id=claim.task_id,
                claim_id=claim.claim_id,
                agent_id=claim.agent_id,
                graph_delta=_empty_delta(claim.graph_id),
                target_graph_version=current + 1,
            )
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_live_rebase_apply_rejects_tampered_process_identity_without_control() -> None:
    runtime, goal, claim, attempt = _active_runtime()
    try:
        current = runtime.vpg.get_graph(claim.graph_id).current_version
        plan = runtime.plan_live_context_rebase(
            goal,
            task_id=claim.task_id,
            claim_id=claim.claim_id,
            agent_id=claim.agent_id,
            graph_delta=_empty_delta(claim.graph_id),
            target_graph_version=current,
        )
        tampered = plan.model_copy(update={"process_id": "forged-process"})

        result = await runtime.apply_live_context_rebase(tampered)

        assert result.refused is True
        assert result.applied is False
        assert "identity" in result.reason.lower()
        harness = runtime.harness_for_claim(claim.claim_id)
        assert harness is not None
        assert harness.snapshot.state.value == "created"
        assert attempt.process_id != tampered.process_id
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_live_rebase_apply_rejects_graph_version_tamper_before_harness() -> None:
    runtime, goal, claim, _attempt = _active_runtime()
    try:
        current = runtime.vpg.get_graph(claim.graph_id).current_version
        plan = runtime.plan_live_context_rebase(
            goal,
            task_id=claim.task_id,
            claim_id=claim.claim_id,
            agent_id=claim.agent_id,
            graph_delta=_empty_delta(claim.graph_id),
            target_graph_version=current,
        )
        tampered = plan.model_copy(update={"graph_version": current + 1})

        result = await runtime.apply_live_context_rebase(tampered)

        assert result.refused is True
        assert result.applied is False
        assert "integrity" in result.reason.lower()
        harness = runtime.harness_for_claim(claim.claim_id)
        assert harness is not None
        assert harness.snapshot.state.value == "created"
    finally:
        runtime.close()
