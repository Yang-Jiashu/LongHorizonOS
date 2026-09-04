"""Focused end-to-end tests for the bounded Context/Harness rebase bridge."""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

import pytest

from lhos.runtimes.multi_agent.models import (
    AgentSnapshot,
    AttemptState,
    ResourceBinding,
)
from lhos.sdk.context_delta import (
    ContextGraphChange,
    ContextGraphDelta,
    ContextRebaseAction,
)
from lhos.sdk.harness import (
    CallableHarnessAdapter,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionIdentity,
)
from lhos.sdk.rebase_runtime import (
    CommitFreshnessStatus,
    RebaseRuntimeBridge,
    apply_rebase_runtime,
    plan_rebase_runtime,
    validate_read_set_freshness,
)


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _binding(
    artifact_id: str,
    *,
    version: int = 1,
    operation: str = "read",
) -> ResourceBinding:
    return ResourceBinding(
        operation=operation,
        resource_uri=f"vpg://{artifact_id}",
        artifact_id=artifact_id,
        version=version,
        content_hash=_hash(f"{artifact_id}@{version}"),
    )


def _agent(task_id: str, *reads: ResourceBinding) -> AgentSnapshot:
    return AgentSnapshot(
        agent_id=f"agent-{task_id}",
        process_id=f"process-{task_id}",
        task_id=task_id,
        claim_id=f"claim-{task_id}",
        attempt_id=f"attempt-{task_id}",
        graph_id="graph-1",
        graph_version=1,
        semantic_epoch=0,
        read_set=reads,
        started_at=datetime.now(UTC),
        state=AttemptState.RUNNING,
    )


def _harness_for(
    agent: AgentSnapshot,
    *,
    start: bool = True,
) -> CallableHarnessAdapter:
    identity = HarnessSessionIdentity(
        graph_id=agent.graph_id,
        graph_version=agent.graph_version,
        semantic_epoch=agent.semantic_epoch,
        task_id=agent.task_id,
        agent_id=agent.agent_id,
        claim_id=agent.claim_id,
        attempt_id=agent.attempt_id,
    )
    harness = CallableHarnessAdapter(
        identity,
        start=lambda _request, _snapshot: HarnessHookOutcome(progress=0.2),
        continue_handler=lambda _request, _snapshot: HarnessHookOutcome(progress=0.4),
        rebase=lambda _request, _snapshot: HarnessHookOutcome(progress=0.1),
    )
    if start:
        started = harness.control_sync(harness.make_request(HarnessOperation.START))
        assert started.status is HarnessResultStatus.APPLIED
    return harness


def _api_delta() -> ContextGraphDelta:
    return ContextGraphDelta(
        graph_id="graph-1",
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


def test_changed_api_rebases_only_the_session_that_read_it() -> None:
    affected = _agent("backend", _binding("api"), _binding("auth"))
    unaffected = _agent("docs", _binding("docs"), _binding("auth"))
    affected_harness = _harness_for(affected)
    unaffected_harness = _harness_for(unaffected)
    bridge = RebaseRuntimeBridge()

    affected_decision = bridge.plan(
        affected,
        graph_delta=_api_delta(),
        new_graph_version=2,
        harness=affected_harness,
    )
    unaffected_decision = bridge.plan(
        unaffected,
        graph_delta=_api_delta(),
        new_graph_version=2,
        harness=unaffected_harness,
    )

    assert affected_decision.action is ContextRebaseAction.REBASE
    assert affected_decision.affected_ref_ids == ("vpg://api",)
    assert affected_decision.control_request is not None
    assert affected_decision.control_request.operation is HarnessOperation.REBASE
    assert unaffected_decision.action is ContextRebaseAction.REUSE
    assert unaffected_decision.affected_ref_ids == ()
    assert unaffected_decision.control_request is not None
    assert unaffected_decision.control_request.operation is HarnessOperation.CONTINUE


@pytest.mark.asyncio
async def test_unaffected_session_continues_through_harness_control() -> None:
    agent = _agent("docs", _binding("docs"))
    harness = _harness_for(agent, start=False)
    started = await harness.control(harness.make_request(HarnessOperation.START))
    assert started.status is HarnessResultStatus.APPLIED
    decision = plan_rebase_runtime(
        agent,
        graph_delta=_api_delta(),
        new_graph_version=2,
        harness=harness,
    )

    result = await apply_rebase_runtime(decision, harness=harness)

    assert decision.action is ContextRebaseAction.REUSE
    assert result.harness_result is not None
    assert result.harness_result.status is HarnessResultStatus.APPLIED
    assert result.harness_result.operation is HarnessOperation.CONTINUE
    assert result.harness_result.after.state.value == "running"


def test_stale_read_set_is_rejected_before_commit() -> None:
    agent = _agent("backend", _binding("api"))
    freshness = validate_read_set_freshness(
        agent,
        current_graph_version=2,
        graph_delta=_api_delta(),
    )

    assert freshness.status is CommitFreshnessStatus.STALE
    assert freshness.rejected is True
    assert freshness.allowed is False
    assert freshness.stale_ref_ids == ("vpg://api",)


@pytest.mark.parametrize(
    "delta",
    (
        ContextGraphDelta(
            graph_id="graph-1",
            changes=(
                ContextGraphChange(
                    artifact_id="api",
                    old_version=1,
                    new_version=2,
                    new_content_hash=_hash("api@2"),
                ),
            ),
            # A partial delta is not a complete account of the graph change.
            coverage="partial",
        ),
        ContextGraphDelta(
            graph_id="graph-1",
            # Even an empty partial delta cannot prove that an unchanged
            # binding stayed current when the graph advanced.
            coverage="partial",
        ),
    ),
    ids=("partial-with-change", "partial-without-enumeration"),
)
def test_graph_advance_with_partial_delta_fails_closed(
    delta: ContextGraphDelta,
) -> None:
    """A version advance requires complete delta coverage for freshness."""

    agent = _agent("backend", _binding("api"))
    freshness = validate_read_set_freshness(
        agent,
        current_graph_version=2,
        graph_delta=delta,
    )

    assert freshness.status is CommitFreshnessStatus.BLOCKED
    assert freshness.allowed is False
    assert freshness.rejected is True
    assert freshness.observed_graph_version == 1
    assert freshness.current_graph_version == 2
    assert "not complete" in freshness.reason


def test_partial_delta_is_compatible_when_graph_version_does_not_advance() -> None:
    """A same-version, no-change observation remains a valid no-op."""

    agent = _agent("backend", _binding("api"))
    freshness = validate_read_set_freshness(
        agent,
        current_graph_version=1,
        graph_delta=ContextGraphDelta(
            graph_id="graph-1",
            coverage="partial",
        ),
    )

    assert freshness.status is CommitFreshnessStatus.FRESH


def test_unknown_delta_blocks_reuse_and_emits_no_harness_request() -> None:
    agent = _agent("backend", _binding("api"))
    harness = _harness_for(agent)
    unknown = ContextGraphDelta(
        graph_id="graph-1",
        known=False,
        coverage="unknown",
        reason="watcher could not enumerate API dependencies",
    )

    decision = RebaseRuntimeBridge().plan(
        agent,
        graph_delta=unknown,
        new_graph_version=2,
        harness=harness,
    )

    assert decision.action is ContextRebaseAction.BLOCKED
    assert decision.freshness.status is CommitFreshnessStatus.BLOCKED
    assert decision.control_request is None
    assert decision.plan.blocked is True


def test_agent_from_a_newer_graph_than_runtime_fails_closed() -> None:
    agent = _agent("future", _binding("api")).model_copy(update={"graph_version": 4})

    freshness = validate_read_set_freshness(
        agent,
        current_graph_version=3,
        graph_delta=ContextGraphDelta(graph_id="graph-1", coverage="complete"),
    )
    decision = RebaseRuntimeBridge().plan(
        agent,
        graph_delta=ContextGraphDelta(graph_id="graph-1", coverage="complete"),
        new_graph_version=3,
    )

    assert freshness.status is CommitFreshnessStatus.BLOCKED
    assert decision.action is ContextRebaseAction.BLOCKED
    assert "newer" in freshness.reason


def test_unknown_read_binding_blocks_even_when_delta_is_complete() -> None:
    unknown_read = ResourceBinding(
        operation="read",
        resource_uri="tool://opaque",
        known=False,
    )
    agent = _agent("opaque", unknown_read)
    freshness = validate_read_set_freshness(
        agent,
        current_graph_version=1,
        graph_delta=ContextGraphDelta(graph_id="graph-1", coverage="complete"),
    )

    assert freshness.status is CommitFreshnessStatus.BLOCKED
    assert freshness.unknown_ref_ids == ("tool://opaque",)


@pytest.mark.asyncio
async def test_rebase_can_invoke_explicit_non_atomic_handoff_callback() -> None:
    agent = _agent("backend", _binding("api"), _binding("auth"))
    harness = _harness_for(agent, start=False)
    started = await harness.control(harness.make_request(HarnessOperation.START))
    assert started.status is HarnessResultStatus.APPLIED
    calls: list[str] = []

    def handoff(decision: Any) -> dict[str, str]:
        calls.append(decision.attempt_id)
        return {"status": "replacement-admitted"}

    bridge = RebaseRuntimeBridge(handoff_callback=handoff)
    decision = bridge.plan(
        agent,
        graph_delta=_api_delta(),
        new_graph_version=2,
        harness=harness,
    )
    result = await bridge.apply(decision, harness=harness)

    assert result.handoff_attempted is True
    assert result.non_atomic_handoff is True
    assert result.handoff_result == {"status": "replacement-admitted"}
    assert calls == [agent.attempt_id]
    assert "non-atomic" in result.reason


def test_full_reload_is_selected_when_every_context_binding_changes() -> None:
    agent = _agent("single", _binding("api"))
    decision = RebaseRuntimeBridge().plan(
        agent,
        graph_delta=_api_delta(),
        new_graph_version=2,
    )

    assert decision.action is ContextRebaseAction.FULL_RELOAD
    assert decision.plan.reload_ref_ids == ("vpg://api",)
    assert decision.plan.preserve_ref_ids == ()
