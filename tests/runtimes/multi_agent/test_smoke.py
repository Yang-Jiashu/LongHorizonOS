"""Smoke test for the provider / wiring layer."""


def test_world_fixture_registers_and_schedules(world):
    from lhos.runtimes.multi_agent import (
        AgentDescriptor,
        AgentRegistry,
        ClaimState,
        create_scheduler,
    )

    reg = AgentRegistry()
    pid = world.proc.list_all().__len__()  # baseline 0
    assert pid == 0

    # Spawn a real kernel process and register it as an agent.
    kernel_pid = world.kernel._process_service.spawn("agent").pid
    assert world.proc.get(kernel_pid) is not None

    reg.register(
        AgentDescriptor(
            agent_id="a1",
            process_id=kernel_pid,
            supported_task_kinds=("*",),
            specializations=("python",),
        )
    )

    sch = create_scheduler(
        reg,
        vpg=world.vpg,
        process_provider=world.proc,
        lease_provider=world.lease,
        capability_provider=world.cap,
    )

    # Empty frontier => idle schedule.
    gid = world.vpg_rt.create_graph(owner_pid="agent-1").graph_id
    res = sch.schedule_once(gid)
    assert res.idle
    assert res.dispatched == []

    # Sanity check claim lifecycle types.
    assert ClaimState.ACTIVE.value == "active"
