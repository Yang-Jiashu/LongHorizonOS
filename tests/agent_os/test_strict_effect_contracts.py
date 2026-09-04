"""Strict admission tests for explicit Action effect contracts."""

from __future__ import annotations

from typing import Any

import pytest

from lhos.agent_os.kernel.models import (
    ActionState,
    KernelEvent,
    ProcessState,
    RecoveryPolicy,
    SideEffectClass,
    SubmitActionRequest,
)
from lhos.agent_os.programs.base import ProgramStepResult
from lhos.agent_os.sdk.client import create_kernel, create_kernel_with_artifacts
from lhos.sdk import AgentOS


class _MissingContractProgram:
    program_id = "missing-effect-contract"

    async def step(
        self,
        state: dict[str, Any],
        event: KernelEvent | None,
    ) -> ProgramStepResult:
        del event
        return ProgramStepResult(
            new_state={**state, "submitted": True},
            request=SubmitActionRequest(
                pid=state["pid"],
                device_type="model/mock",
                operation="generate",
            ),
        )


@pytest.mark.asyncio
async def test_strict_submit_rejects_implicit_contract_before_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kernel = create_kernel(":memory:", strict_effect_contracts=True)
    try:
        pid = await kernel.spawn(type("Program", (), {"program_id": "strict-reject"})())
        claims = [{"resource_id": "resource:strict/R1", "mode": "exclusive"}]

        def unexpected(*_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("strict rejection must happen before admission services")

        monkeypatch.setattr(kernel._capability_service, "enforce", unexpected)
        monkeypatch.setattr(kernel._action_service, "submit", unexpected)
        monkeypatch.setattr(kernel._lease_service, "atomic_acquire", unexpected)

        request = SubmitActionRequest(
            pid=pid,
            device_type="model/mock",
            operation="generate",
            resource_claims=claims,
        )
        event = await kernel._dispatcher.dispatch(request)

        assert event is not None
        assert event.event_type == "ACTION_REJECTED"
        assert event.payload == {
            "reason": "effect_contract_required",
            "request_id": request.request_id,
            "missing_fields": ["side_effect_class", "recovery_policy"],
            "device_type": "model/mock",
            "operation": "generate",
        }
        assert kernel._action_service.list_by_pid(pid) == []
        assert kernel._lease_service.list_all_leases() == []
        assert kernel._lease_service.list_waiters("resource:strict/R1") == []
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_strict_submit_accepts_explicit_pure_retry_and_completes() -> None:
    kernel = create_kernel(":memory:", strict_effect_contracts=True)
    try:
        pid = await kernel.spawn(type("Program", (), {"program_id": "strict-accept"})())
        request = SubmitActionRequest(
            pid=pid,
            device_type="model/mock",
            operation="generate",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.RETRY,
        )

        event = await kernel._dispatcher.dispatch(request)
        assert event is not None
        assert event.event_type == "ACTION_READY_FOR_DISPATCH"

        await kernel._dispatch_pending_actions()

        actions = kernel._action_service.list_by_pid(pid)
        assert len(actions) == 1
        assert actions[0].state == ActionState.COMMITTED
        assert actions[0].side_effect_class == SideEffectClass.PURE
        assert actions[0].recovery_policy == RecoveryPolicy.RETRY
    finally:
        kernel.close()


@pytest.mark.asyncio
async def test_strict_process_rejection_returns_ready_without_waiting_on_old_action() -> None:
    kernel = create_kernel(":memory:", strict_effect_contracts=True)
    try:
        program = _MissingContractProgram()
        pid = await kernel.spawn(program)
        old_action = kernel._action_service.submit(
            pid,
            "model/mock",
            "old-operation",
            side_effect_class=SideEffectClass.PURE,
            recovery_policy=RecoveryPolicy.RETRY,
        )
        kernel._action_service.admit(old_action.action_id)
        kernel._action_service.mark_intent_durable(old_action.action_id, [])
        kernel._action_service.dispatch(old_action.action_id)
        kernel._action_service.commit(old_action.action_id, {"old": True})

        pcb = kernel._process_service.get_process(pid)
        assert pcb is not None
        await kernel._run_process_step(pcb)

        current = kernel._process_service.get_process(pid)
        assert current is not None
        assert current.state == ProcessState.READY
        assert current.wait_condition is None
        assert kernel._action_service.list_by_pid(pid) == [
            kernel._action_service.get_action(old_action.action_id)
        ]
        rejected = [
            event
            for event in kernel._journal.read_all()
            if event.pid == pid and event.event_type == "ACTION_REJECTED"
        ]
        assert len(rejected) == 1
    finally:
        kernel.close()


def test_kernel_constructors_wire_strict_mode(tmp_path) -> None:
    kernel = create_kernel(":memory:", strict_effect_contracts=True)
    artifact_kernel = None
    try:
        assert kernel._strict_effect_contracts is True
        assert kernel._dispatcher._strict_effect_contracts is True

        artifact_kernel, _ = create_kernel_with_artifacts(
            ":memory:",
            tmp_path / "cas",
            strict_effect_contracts=True,
        )
        assert artifact_kernel._strict_effect_contracts is True
        assert artifact_kernel._dispatcher._strict_effect_contracts is True
    finally:
        kernel.close()
        if artifact_kernel is not None:
            artifact_kernel.close()


def test_secure_agentos_wires_strict_kernel_and_explicit_verification_action() -> None:
    os_ = AgentOS(":memory:", secure_mode=True)
    try:
        assert os_.kernel._strict_effect_contracts is True
        assert os_.kernel._dispatcher._strict_effect_contracts is True

        action_id = os_._facts.commit_action("strict-verification-action")
        action = os_.kernel._action_service.get_action(action_id)
        assert action is not None
        assert action.state == ActionState.COMMITTED
        assert action.side_effect_class == SideEffectClass.PURE
        assert action.recovery_policy == RecoveryPolicy.RETRY
    finally:
        os_.close()
