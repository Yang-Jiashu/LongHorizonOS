"""D2-I4: each Task gets at most one ACTIVE claim."""
from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent import AgentDescriptor, AgentRegistry, create_scheduler


def _world_sched(world, agent_specs):
    """Register agents on fresh kernel processes; return scheduler."""
    reg = AgentRegistry()
    for aid, specs in agent_specs.items():
        pid = world.kernel._process_service.spawn(aid).pid
        reg.register(
            AgentDescriptor(
                agent_id=aid,
                process_id=pid,
                supported_task_kinds=("*",),
                specializations=specs.get("specs", ()),
                max_concurrency=specs.get("max_concurrency", 1),
            )
        )
    return create_scheduler(
        reg,
        vpg=world.vpg,
        process_provider=world.proc,
        lease_provider=world.lease,
        capability_provider=world.cap,
    )


def test_exactly_one_active_claim_per_task(world):
    sch = _world_sched(
        world,
        {
            "a1": {"specs": ("python",), "max_concurrency": 5},
            "a2": {"specs": ("python",), "max_concurrency": 5},
            "a3": {"specs": ("python",), "max_concurrency": 5},
        },
    )
    gid = world.vpg_rt.create_graph(owner_pid="kernel").graph_id
    world.vpg_rt.submit_patch(
        _make_task_patch(world, gid, "t1", "code_review", ("python",))
    )
    res = sch.schedule_once(gid)
    assert len(res.dispatched) == 1
    active_for_t1 = [
        c for c in sch.claims if c.task_id == "t1"
    ]
    assert len(active_for_t1) == 1


def test_schedule_until_idle_never_creates_duplicate_active(world):
    sch = _world_sched(
        world,
        {"a1": {"specs": ("python",), "max_concurrency": 10}},
    )
    gid = world.vpg_rt.create_graph(owner_pid="kernel").graph_id
    for i in range(5):
        world.vpg_rt.submit_patch(
            _make_task_patch(world, gid, f"t{i}", "code_review", ("python",))
        )
    sch.schedule_until_idle(gid, max_dispatches=50)
    by_task: dict[str, int] = {}
    for c in sch.claims:
        by_task[c.task_id] = by_task.get(c.task_id, 0) + 1
    # At most one claim per task, ever — even after leasing.
    assert all(v == 1 for v in by_task.values())


def _make_task_patch(world, gid, tid, kind, sched_specs):
    from lhos.runtimes.verified_progress.patches import (
        AddNodeOp, GraphPatchProposal,
    )
    return GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=world.vpg_rt.get_graph(gid).current_version,
        author_pid="kernel",
        idempotency_key=f"add-{tid}",
        operations=(
            AddNodeOp(
                node_id=tid,
                graph_id=gid,
                node_type="task",
                created_by_pid="kernel",
                metadata={
                    "scheduler": {
                        "task_kind": kind,
                        "required_specializations": list(sched_specs),
                    }
                },
            ),
        ),
    )
