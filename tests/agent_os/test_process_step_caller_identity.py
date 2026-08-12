"""Authenticated caller binding at the program-to-kernel syscall boundary."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from lhos.agent_os.kernel.models import (
    ActionState,
    CancelActionRequest,
    Capability,
    ExitRequest,
    InspectActionRequest,
    KernelEvent,
    KernelRequest,
    ProcessState,
    ReleaseResourceRequest,
    SpawnRequest,
    SubmitActionRequest,
)
from lhos.agent_os.programs.base import ProgramStepResult
from lhos.agent_os.sdk.client import create_kernel


class _OneShotProgram:
    def __init__(
        self,
        program_id: str,
        request_factory: Callable[[str], KernelRequest],
    ) -> None:
        self._program_id = program_id
        self._request_factory = request_factory

    @property
    def program_id(self) -> str:
        return self._program_id

    async def step(
        self,
        state: dict[str, Any],
        event: KernelEvent | None,
    ) -> ProgramStepResult:
        del event
        caller_pid = state["pid"]
        return ProgramStepResult(
            new_state={**state, "issued": True},
            request=self._request_factory(caller_pid),
        )


@pytest.fixture
def kernel():
    instance = create_kernel(":memory:")
    try:
        yield instance
    finally:
        instance.close()


async def _spawn_and_run(kernel, program: _OneShotProgram) -> str:
    pid = await kernel.spawn(program)
    pcb = kernel._process_service.get_process(pid)
    assert pcb is not None
    await kernel._run_process_step(pcb)
    return pid


def _spawn_victim(kernel):
    return kernel._process_service.spawn(program_id="victim")


def _make_admitted_action_with_lease(kernel, victim_pid: str):
    claim = {"resource_id": "resource:R1", "mode": "exclusive"}
    lease = kernel._lease_service.atomic_acquire(victim_pid, [claim])[0]
    action = kernel._action_service.submit(
        victim_pid,
        "model/mock",
        "generate",
        resource_claims=[claim],
    )
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(action.action_id, [lease.lease_id])
    return action, lease


@pytest.mark.asyncio
async def test_forged_submit_pid_is_rebound_to_executing_process(kernel) -> None:
    victim = _spawn_victim(kernel)
    program = _OneShotProgram(
        "forged-submit",
        lambda _caller: SubmitActionRequest(
            pid=victim.pid,
            device_type="model/mock",
            operation="generate",
        ),
    )

    attacker_pid = await _spawn_and_run(kernel, program)

    assert kernel._action_service.list_by_pid(victim.pid) == []
    attacker_actions = kernel._action_service.list_by_pid(attacker_pid)
    assert len(attacker_actions) == 1
    assert attacker_actions[0].pid == attacker_pid
    assert attacker_actions[0].state == ActionState.ADMITTED


@pytest.mark.asyncio
async def test_forged_inspect_pid_cannot_read_victim_action(kernel) -> None:
    victim = _spawn_victim(kernel)
    action = kernel._action_service.submit(victim.pid, "model/mock", "generate")
    kernel._action_service.admit(action.action_id)
    kernel._action_service.mark_intent_durable(action.action_id, [])
    kernel._action_service.dispatch(action.action_id)
    kernel._action_service.commit(action.action_id, result={"secret": "victim-only"})
    program = _OneShotProgram(
        "forged-inspect",
        lambda _caller: InspectActionRequest(pid=victim.pid, action_id=action.action_id),
    )

    attacker_pid = await _spawn_and_run(kernel, program)

    failures = [
        event
        for event in kernel._journal.read_all()
        if event.pid == attacker_pid and event.event_type == "ACTION_INSPECT_FAILED"
    ]
    assert len(failures) == 1
    assert failures[0].payload == {"action_id": action.action_id, "reason": "not_found"}
    assert not {"state", "result", "error"} & failures[0].payload.keys()


@pytest.mark.asyncio
async def test_forged_cancel_pid_cannot_cancel_victim_action(kernel) -> None:
    victim = _spawn_victim(kernel)
    action, lease = _make_admitted_action_with_lease(kernel, victim.pid)
    program = _OneShotProgram(
        "forged-cancel",
        lambda _caller: CancelActionRequest(pid=victim.pid, action_id=action.action_id),
    )

    await _spawn_and_run(kernel, program)

    current = kernel._action_service.get_action(action.action_id)
    assert current is not None
    assert current.state == ActionState.ADMITTED
    assert kernel._lease_service.get_lease(lease.lease_id) is not None


@pytest.mark.asyncio
async def test_forged_release_pid_cannot_release_victim_lease(kernel) -> None:
    victim = _spawn_victim(kernel)
    lease = kernel._lease_service.atomic_acquire(
        victim.pid,
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )[0]
    program = _OneShotProgram(
        "forged-release",
        lambda _caller: ReleaseResourceRequest(
            pid=victim.pid,
            lease_ids=[lease.lease_id],
        ),
    )

    await _spawn_and_run(kernel, program)

    assert kernel._lease_service.get_lease(lease.lease_id) is not None


@pytest.mark.asyncio
async def test_forged_exit_pid_exits_attacker_not_victim(kernel) -> None:
    victim = _spawn_victim(kernel)
    victim_lease = kernel._lease_service.atomic_acquire(
        victim.pid,
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )[0]
    program = _OneShotProgram(
        "forged-exit",
        lambda _caller: ExitRequest(pid=victim.pid, exit_code="forged"),
    )
    attacker_pid = await kernel.spawn(program)
    attacker_lease = kernel._lease_service.atomic_acquire(
        attacker_pid,
        [{"resource_id": "resource:R2", "mode": "exclusive"}],
    )[0]
    attacker = kernel._process_service.get_process(attacker_pid)
    assert attacker is not None

    await kernel._run_process_step(attacker)

    attacker = kernel._process_service.get_process(attacker_pid)
    victim_after = kernel._process_service.get_process(victim.pid)
    assert attacker is not None
    assert attacker.state == ProcessState.EXITED
    assert attacker.exit_code == "forged"
    assert victim_after is not None
    assert victim_after.state == ProcessState.READY
    assert victim_after.exit_code is None
    assert kernel._lease_service.get_lease(attacker_lease.lease_id) is None
    assert kernel._lease_service.get_lease(victim_lease.lease_id) is not None


@pytest.mark.asyncio
async def test_forged_spawn_parent_is_rejected_without_creating_child(kernel) -> None:
    victim = _spawn_victim(kernel)
    program = _OneShotProgram(
        "forged-spawn-parent",
        lambda _caller: SpawnRequest(
            pid=victim.pid,
            parent_pid=victim.pid,
            program_id="forged-child",
        ),
    )
    before = {process.pid for process in kernel._process_service.list_all()}

    attacker_pid = await _spawn_and_run(kernel, program)

    spawned_after = [
        process
        for process in kernel._process_service.list_all()
        if process.pid not in before and process.pid != attacker_pid
    ]
    assert spawned_after == []
    failures = [
        event
        for event in kernel._journal.read_all()
        if event.pid == attacker_pid and event.event_type == "SPAWN_FAILED"
    ]
    assert len(failures) == 1
    assert failures[0].payload == {"reason": "invalid_parent"}


@pytest.mark.asyncio
async def test_child_cannot_gain_capabilities_from_spawn_payload(kernel) -> None:
    child_capability = Capability(
        resource_pattern="device:model/mock",
        operations={"invoke"},
    )
    program = _OneShotProgram(
        "restricted-parent",
        lambda caller: SpawnRequest(
            pid=caller,
            parent_pid=caller,
            program_id="restricted-child",
            capabilities=["full", "resource:admin/*:acquire"],
        ),
    )
    parent_pid = await kernel.spawn(program)
    parent_caps = kernel._capability_service.get_capability_set(parent_pid)
    assert parent_caps is not None
    parent_caps.capabilities = [child_capability]
    kernel._capability_service._upsert_capability_set(parent_caps)
    parent = kernel._process_service.get_process(parent_pid)
    assert parent is not None

    await kernel._run_process_step(parent)

    children = [
        process
        for process in kernel._process_service.list_all()
        if process.parent_pid == parent_pid
    ]
    assert len(children) == 1
    child_caps = kernel._capability_service.get_capability_set(children[0].pid)
    assert child_caps is not None
    assert {
        (cap.resource_pattern, frozenset(cap.operations)) for cap in child_caps.capabilities
    } == {("device:model/mock", frozenset({"invoke"}))}
    assert kernel._capability_service.verify_child_subset(parent_pid, children[0].pid)
    assert not kernel._capability_service.check(
        children[0].pid,
        "resource:admin/secret",
        "acquire",
    )


@pytest.mark.asyncio
async def test_child_inherits_parent_namespace_and_resource_group(kernel) -> None:
    program = _OneShotProgram(
        "isolated-parent",
        lambda caller: SpawnRequest(
            pid=caller,
            parent_pid=caller,
            program_id="isolated-child",
            namespace_id="ns-admin",
            resource_group_id="rg-admin",
        ),
    )
    parent_pid = await kernel.spawn(program, namespace_id="ns-parent")
    parent = kernel._process_service.get_process(parent_pid)
    assert parent is not None
    kernel._storage.execute(
        "UPDATE processes_projection SET resource_group_id = ? WHERE pid = ?",
        ("rg-parent", parent_pid),
    )
    parent = kernel._process_service.get_process(parent_pid)
    assert parent is not None

    await kernel._run_process_step(parent)

    children = [
        process
        for process in kernel._process_service.list_all()
        if process.parent_pid == parent_pid
    ]
    assert len(children) == 1
    assert children[0].namespace_id == "ns-parent"
    assert children[0].resource_group_id == "rg-parent"


@pytest.mark.asyncio
async def test_normal_process_request_keeps_working(kernel) -> None:
    program = _OneShotProgram(
        "normal-submit",
        lambda caller: SubmitActionRequest(
            pid=caller,
            device_type="model/mock",
            operation="generate",
        ),
    )

    pid = await _spawn_and_run(kernel, program)

    actions = kernel._action_service.list_by_pid(pid)
    assert len(actions) == 1
    assert actions[0].state == ActionState.ADMITTED
    pcb = kernel._process_service.get_process(pid)
    assert pcb is not None
    assert pcb.state == ProcessState.BLOCKED


@pytest.mark.asyncio
async def test_non_kernel_request_is_rejected_fail_closed(kernel) -> None:
    class _MalformedProgram:
        program_id = "malformed-request"

        async def step(
            self,
            state: dict[str, Any],
            event: KernelEvent | None,
        ) -> ProgramStepResult:
            del event
            return ProgramStepResult(new_state=state, request={"pid": "victim"})

    program = _MalformedProgram()
    pid = await kernel.spawn(program)
    lease = kernel._lease_service.atomic_acquire(
        pid,
        [{"resource_id": "resource:R1", "mode": "exclusive"}],
    )[0]
    pcb = kernel._process_service.get_process(pid)
    assert pcb is not None

    await kernel._run_process_step(pcb)

    pcb = kernel._process_service.get_process(pid)
    assert pcb is not None
    assert pcb.state == ProcessState.FAILED
    assert kernel._lease_service.get_lease(lease.lease_id) is None
    rejected = [
        event
        for event in kernel._journal.read_all()
        if event.pid == pid and event.event_type == "PROGRAM_REQUEST_REJECTED"
    ]
    assert len(rejected) == 1
    assert rejected[0].payload["reason"] == "invalid_request_type"
