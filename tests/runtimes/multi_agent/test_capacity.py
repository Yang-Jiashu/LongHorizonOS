"""max_concurrency enforcement (Section 21).

The Scheduler must never give an Agent more ACTIVE claims than its
max_concurrency allows.  Verified with the real Kernel-backed providers
from the World fixture.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, ClaimState, create_scheduler
from tests.runtimes.multi_agent.helpers import FakeVPG


def _sched(world, spec):
    reg = AgentRegistry()
    pid = world.kernel._process_service.spawn("a").pid
    reg.register(AgentDescriptor(agent_id="a", process_id=pid, **spec))
    return create_scheduler(
        reg,
        vpg=world.vpg,
        process_provider=world.proc,
        lease_provider=world.lease,
        capability_provider=world.cap,
    )


def test_max_concurrency_1_limits_one_claim(world):
    sch = _sched(world, {"supported_task_kinds": ("*",), "specializations": ("python",),
                           "max_concurrency": 1})
    gid = world.vpg_rt.create_graph(owner_pid="kernel").graph_id
    from tests.runtimes.multi_agent.test_claim_exclusivity import _make_task_patch
    world.vpg_rt.submit_patch(_make_task_patch(world, gid, "t0", "code_review", ("python",)))
    world.vpg_rt.submit_patch(_make_task_patch(world, gid, "t1", "code_review", ("python",)))
    res = sch.schedule_until_idle(gid, max_dispatches=50)
    total_dispatched = sum(len(r.dispatched) for r in res)
    # Agent can only own 1 ACTIVE claim at a time; since there is no VPG
    # completion, the second task stays pending.
    assert total_dispatched == 1
    active = [c for c in sch.claims if c.state == ClaimState.ACTIVE]
    assert len(active) == 1


def test_max_concurrency_zero_always_ineligible():
    from tests.runtimes.multi_agent.helpers import fake_scheduler
    vpg = FakeVPG()
    sch = fake_scheduler({"a": {"max_concurrency": 0, "supported_task_kinds": ("*",),
                                  "specializations": ("python",)}},
                         fake_vpg=vpg)
    vpg.add_ready_task("t1", required_specializations=("python",))
    res = sch.schedule_once(vpg.graph_id)
    assert res.dispatched == []
    assert "no eligible agent" in res.skipped[0][1]


def test_underutilized_agent_gets_more_claims(world):
    """When one agent is capacity-bound but another is free, work spills over."""
    reg = AgentRegistry()
    p1 = world.kernel._process_service.spawn("a1").pid
    p2 = world.kernel._process_service.spawn("a2").pid
    reg.register(AgentDescriptor(agent_id="a1", process_id=p1,
                                 supported_task_kinds=("*",),
                                 specializations=("python",),
                                 max_concurrency=1))
    reg.register(AgentDescriptor(agent_id="a2", process_id=p2,
                                 supported_task_kinds=("*",),
                                 specializations=("python",),
                                 max_concurrency=5))
    sch = create_scheduler(reg, vpg=world.vpg, process_provider=world.proc,
                           lease_provider=world.lease, capability_provider=world.cap)
    gid = world.vpg_rt.create_graph(owner_pid="kernel").graph_id
    from tests.runtimes.multi_agent.test_claim_exclusivity import _make_task_patch
    for i in range(4):
        world.vpg_rt.submit_patch(_make_task_patch(world, gid, f"t{i}", "code_review", ("python",)))
    # Patch scheduler's projection hack: re-register the same vpg adapter.
    # Schedule to completion — a1 gets 1 claim, a2 gets the rest.
    # Because tasks never become VERIFIED in this test, only ACTIVE claims
    # accumulate; we assert a2 picks up the overflow.
    sch.schedule_until_idle(gid, max_dispatches=50)
    claims_by_agent: dict[str, int] = {}
    for c in sch.claims:
        if c.state == ClaimState.ACTIVE:
            claims_by_agent[c.agent_id] = claims_by_agent.get(c.agent_id, 0) + 1
    assert claims_by_agent.get("a1", 0) == 1
    # a2 must have taken at least the second task.
    assert claims_by_agent.get("a2", 0) >= 1


def test_active_count_by_agent_helper():
    from lhos.runtimes.multi_agent.models import ClaimState, TaskClaim
    from lhos.runtimes.multi_agent.projections import active_claim_count_by_agent

    claims = [
        TaskClaim(graph_id="g", graph_version=1, task_id="t1", agent_id="a1",
                  process_id="p1", lease_resource="r", state=ClaimState.ACTIVE),
        TaskClaim(graph_id="g", graph_version=1, task_id="t2", agent_id="a1",
                  process_id="p1", lease_resource="r", state=ClaimState.ACTIVE),
        TaskClaim(graph_id="g", graph_version=1, task_id="t3", agent_id="a1",
                  process_id="p1", lease_resource="r", state=ClaimState.COMPLETED),
        TaskClaim(graph_id="g", graph_version=1, task_id="t4", agent_id="a2",
                  process_id="p2", lease_resource="r", state=ClaimState.ACTIVE),
    ]
    counts = active_claim_count_by_agent(claims)
    assert counts == {"a1": 2, "a2": 1}
