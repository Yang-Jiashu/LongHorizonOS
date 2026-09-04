"""AgentOS integration tests for the explicit side-effect boundary."""

from __future__ import annotations

import pytest

from lhos.provenance import EffectRequest
from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    ExecutionContext,
    Goal,
    VerificationOutcome,
    create_execution_context,
)


def _pass() -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id="effect-result",
        version=1,
        content="ok",
    )


class _Gateway:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.requests: list[EffectRequest] = []
        self.result = result
        self.error = error

    def submit(self, request: EffectRequest):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        if self.result is not None:
            return self.result
        return {
            "effect_id": request.effect_id,
            "status": "completed",
            "action_id": f"action-{request.effect_id}",
            "idempotency_key": request.declaration.idempotency_key,
        }


def _submit_effect(context) -> VerificationOutcome:
    context.declare_effect(
        "publish",
        side_effect_class="idempotent",
        resource_uri="sink://release",
        idempotency_key="publish-v1",
        operation="write",
    )
    context.submit_effect("publish", "write", arguments={"version": 1})
    return _pass()


def _assert_released(os_: AgentOS, gid: str) -> None:
    assert os_.scheduler.claims[-1].state.value == "released"
    assert (
        os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
        == []
    )


def test_secure_context_rejects_legacy_executor_api() -> None:
    with pytest.raises(ConfigurationError, match="context_v1"):
        create_execution_context(
            "G",
            "T",
            executor_api="legacy_task_id",
            secure_mode=True,
        )


def test_sdk_exports_execution_context_for_secure_callbacks() -> None:
    context = create_execution_context(
        "G",
        "T",
        executor_api="context_v1",
        secure_mode=True,
    )
    assert isinstance(context, ExecutionContext)
    assert context.secure_mode is True


def test_secure_agentos_completed_gateway_receipt_can_reach_verified() -> None:
    gateway = _Gateway()
    os_ = AgentOS(":memory:", secure_mode=True, action_gateway=gateway)
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=_submit_effect,
                executor_api="context_v1",
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker")

        result = os_.run(goal, max_dispatches=1)

        assert result.verified == ["T"]
        assert len(gateway.requests) == 1
        request = gateway.requests[0]
        assert request.task_id == "T"
        assert request.claim_id
        assert request.attempt_id
        assert request.declaration is not None
        assert request.declaration.idempotency_key == "publish-v1"
    finally:
        os_.close()


def test_secure_agentos_rejects_legacy_callback_before_user_code_and_releases() -> None:
    calls: list[str] = []

    def legacy(task_id: str) -> VerificationOutcome:
        calls.append(task_id)
        return _pass()

    os_ = AgentOS(":memory:", secure_mode=True, action_gateway=_Gateway())
    try:
        os_.add_agent(Agent("worker", executor=legacy))
        goal = Goal("G")
        goal.task("T", agent="worker")

        with pytest.raises(ConfigurationError, match="context_v1"):
            os_.run(goal, max_dispatches=1)

        gid = os_._gid_for("G")
        assert gid is not None
        assert calls == []
        assert os_.result(gid).task_states["T"] == "unverified"
        _assert_released(os_, gid)
    finally:
        os_.close()


async def test_run_async_secure_mode_rejects_legacy_executor_before_user_code() -> None:
    calls: list[str] = []

    async def legacy(task_id: str) -> VerificationOutcome:
        calls.append(task_id)
        return _pass()

    os_ = AgentOS(":memory:", secure_mode=True, action_gateway=_Gateway())
    try:
        os_.add_agent(Agent("worker", executor=legacy))
        goal = Goal("G")
        goal.task("T", agent="worker")

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        gid = os_._gid_for("G")
        assert gid is not None
        assert calls == []
        assert result.task_states["T"] == "unverified"
        assert any("context_v1" in failure for failure in result.failures)
        _assert_released(os_, gid)
    finally:
        os_.close()


@pytest.mark.parametrize(
    ("result", "error"),
    [
        (None, RuntimeError("gateway timeout")),
        (
            {
                "effect_id": "wrong-effect",
                "status": "completed",
                "action_id": "action-wrong",
                "idempotency_key": "publish-v1",
            },
            None,
        ),
        (
            {
                "effect_id": "publish",
                "status": "uncertain",
                "action_id": "action-publish",
                "idempotency_key": "publish-v1",
            },
            None,
        ),
        (
            {
                "effect_id": "publish",
                "status": "rejected",
                "action_id": "action-publish",
                "idempotency_key": "publish-v1",
            },
            None,
        ),
    ],
)
def test_untrustworthy_gateway_result_never_reaches_verified_and_releases(
    result,
    error,
) -> None:
    os_ = AgentOS(
        ":memory:",
        secure_mode=True,
        action_gateway=_Gateway(result=result, error=error),
    )
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=_submit_effect,
                executor_api="context_v1",
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker")

        run_result = os_.run(goal, max_dispatches=1)
        gid = os_._gid_for("G")

        assert gid is not None
        assert run_result.task_states["T"] == "unverified"
        assert run_result.verified == []
        _assert_released(os_, gid)
    finally:
        os_.close()


async def test_run_async_gateway_rejection_never_reaches_verified_and_releases() -> None:
    gateway = _Gateway(
        result={
            "effect_id": "publish",
            "status": "rejected",
            "action_id": "action-publish",
            "idempotency_key": "publish-v1",
        }
    )
    os_ = AgentOS(":memory:", secure_mode=True, action_gateway=gateway)
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=_submit_effect,
                executor_api="context_v1",
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker")

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)
        gid = os_._gid_for("G")

        assert gid is not None
        assert result.task_states["T"] == "unverified"
        assert result.verified == []
        assert result.failures
        _assert_released(os_, gid)
    finally:
        os_.close()
