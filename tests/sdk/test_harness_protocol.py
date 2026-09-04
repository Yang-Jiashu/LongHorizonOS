"""Harness/session control-plane contract tests."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    CallableHarnessAdapter,
    HarnessControlRequest,
    HarnessHookOutcome,
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionIdentity,
    HarnessSessionState,
)


def _identity() -> HarnessSessionIdentity:
    return HarnessSessionIdentity(
        session_id="session-1",
        graph_id="graph-1",
        graph_version=4,
        semantic_epoch=2,
        task_id="task-1",
        agent_id="agent-1",
        claim_id="claim-1",
        attempt_id="attempt-1",
    )


def test_stateful_harness_supports_control_lifecycle_and_rebase_identity() -> None:
    seen: list[HarnessOperation] = []

    def start(request, snapshot):
        seen.append(request.operation)
        return HarnessHookOutcome(progress=0.25)

    def checkpoint(request, snapshot):
        seen.append(request.operation)
        return HarnessHookOutcome(checkpoint_id="cp-1", progress=0.5)

    async def continue_handler(request, snapshot):
        seen.append(request.operation)
        await asyncio.sleep(0)
        return HarnessHookOutcome(progress=0.7)

    def rebase(request, snapshot):
        seen.append(request.operation)
        return HarnessHookOutcome(progress=0.8)

    harness = CallableHarnessAdapter(
        _identity(),
        start=start,
        continue_handler=continue_handler,
        checkpoint=checkpoint,
        rebase=rebase,
    )
    assert set(harness.capabilities.operations) == {
        HarnessOperation.START,
        HarnessOperation.CONTINUE,
        HarnessOperation.CHECKPOINT,
        HarnessOperation.REBASE,
    }
    assert harness.snapshot.state is HarnessSessionState.CREATED

    started = harness.control_sync(harness.make_request(HarnessOperation.START))
    assert started.status is HarnessResultStatus.APPLIED
    assert started.after.state is HarnessSessionState.RUNNING
    assert started.after.progress == 0.25

    checkpointed = harness.control_sync(harness.make_request(HarnessOperation.CHECKPOINT))
    assert checkpointed.after.state is HarnessSessionState.CHECKPOINTED
    assert checkpointed.after.checkpoint_id == "cp-1"

    continued = harness.control_sync(harness.make_request(HarnessOperation.CONTINUE))
    assert continued.after.state is HarnessSessionState.RUNNING
    assert continued.after.checkpoint_id is None

    rebased = harness.control_sync(
        harness.make_request(
            HarnessOperation.REBASE,
            target_graph_version=5,
            target_semantic_epoch=3,
            reason="API changed",
        )
    )
    assert rebased.after.state is HarnessSessionState.RUNNING
    assert rebased.after.identity.graph_version == 5
    assert rebased.after.identity.semantic_epoch == 3
    assert seen == [
        HarnessOperation.START,
        HarnessOperation.CHECKPOINT,
        HarnessOperation.CONTINUE,
        HarnessOperation.REBASE,
    ]


def test_preempt_is_explicitly_cooperative_and_terminal() -> None:
    harness = CallableHarnessAdapter(
        _identity(),
        start=lambda request, snapshot: HarnessHookOutcome(),
        preempt=lambda request, snapshot: HarnessHookOutcome(
            checkpoint_id="cp-before-preempt", progress=0.4
        ),
    )
    assert harness.capabilities.preemption_mode == "cooperative"

    started = harness.control_sync(harness.make_request(HarnessOperation.START))
    assert started.after.state is HarnessSessionState.RUNNING

    result = harness.control_sync(
        harness.make_request(HarnessOperation.PREEMPT, reason="semantic interrupt")
    )
    assert result.applied
    assert result.after.state is HarnessSessionState.PREEMPTED
    assert result.after.checkpoint_id == "cp-before-preempt"

    rejected = harness.control_sync(
        HarnessControlRequest(
            request_id="continue-after-preempt",
            operation=HarnessOperation.CONTINUE,
            session=result.after.identity,
            expected_revision=result.after.revision,
        )
    )
    assert rejected.status is HarnessResultStatus.UNSUPPORTED
    assert "support" in rejected.message


def test_unsupported_operation_and_stale_identity_fail_closed() -> None:
    harness = CallableHarnessAdapter(
        _identity(),
        start=lambda request, snapshot: HarnessHookOutcome(),
    )
    started_request = harness.make_request(HarnessOperation.START)
    started = harness.control_sync(started_request)
    assert started.after.state is HarnessSessionState.RUNNING

    unsupported = harness.control_sync(harness.make_request(HarnessOperation.CHECKPOINT))
    assert unsupported.status is HarnessResultStatus.UNSUPPORTED
    assert unsupported.before == unsupported.after == harness.snapshot

    stale = started_request.model_copy(
        update={
            "request_id": "stale-revision",
            "expected_revision": 99,
        }
    )
    stale_result = harness.control_sync(stale)
    assert stale_result.status is HarnessResultStatus.REJECTED
    assert "revision" in stale_result.message

    wrong_session = harness.make_request(HarnessOperation.CONTINUE).model_copy(
        update={
            "request_id": "wrong-owner",
            "session": harness.snapshot.identity.model_copy(update={"claim_id": "other-claim"}),
        }
    )
    wrong_result = harness.control_sync(wrong_session)
    assert wrong_result.status is HarnessResultStatus.REJECTED
    assert "identity" in wrong_result.message


def test_request_idempotency_replays_without_invoking_hook_twice() -> None:
    calls = 0

    def start(request, snapshot):
        nonlocal calls
        calls += 1
        return HarnessHookOutcome(progress=0.2)

    harness = CallableHarnessAdapter(_identity(), start=start)
    request = harness.make_request(HarnessOperation.START, request_id="start-once")
    first = harness.control_sync(request)
    replay = harness.control_sync(request)

    assert first == replay
    assert calls == 1
    assert harness.snapshot.revision == 1

    altered = request.model_copy(update={"payload": {"different": True}})
    conflict = harness.control_sync(altered)
    assert conflict.status is HarnessResultStatus.REJECTED
    assert "different control request" in conflict.message
    assert calls == 1


def test_legacy_callable_adapter_is_one_shot_and_preserves_task_id() -> None:
    seen: list[str] = []
    harness = CallableHarnessAdapter(_identity(), executor=lambda task_id: seen.append(task_id))
    assert harness.capabilities.operations == (HarnessOperation.START,)
    result = harness.control_sync(harness.make_request(HarnessOperation.START))
    assert result.after.state is HarnessSessionState.COMPLETED
    assert result.after.progress == 1.0
    assert seen == ["task-1"]

    second = harness.control_sync(harness.make_request(HarnessOperation.START))
    assert second.status is HarnessResultStatus.REJECTED


def test_rebase_requires_forward_target() -> None:
    with pytest.raises(ValidationError, match="REBASE requires"):
        HarnessControlRequest(
            operation=HarnessOperation.REBASE,
            session=_identity(),
            expected_revision=0,
        )

    harness = CallableHarnessAdapter(
        _identity(),
        start=lambda request, snapshot: HarnessHookOutcome(),
        rebase=lambda request, snapshot: HarnessHookOutcome(),
    )
    harness.control_sync(harness.make_request(HarnessOperation.START))
    backwards = harness.control_sync(
        harness.make_request(
            HarnessOperation.REBASE,
            target_graph_version=4,
            target_semantic_epoch=2,
        )
    )
    assert backwards.status is HarnessResultStatus.REJECTED
    assert "advance" in backwards.message
