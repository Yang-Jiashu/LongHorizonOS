"""Scheduler Projection rebuild — byte-identical across repeated rebuilds."""

from __future__ import annotations

from lhos.runtimes.multi_agent.models import (
    AgentDescriptor,
    AttemptState,
    ClaimState,
    ScheduledExecutionAttempt,
    TaskClaim,
)
from lhos.runtimes.multi_agent.projections import SchedulerProjection
from lhos.runtimes.multi_agent.recovery import (
    projection_fingerprint,
    rebuild_projection,
)


def _build_projection():
    agents = [
        AgentDescriptor(agent_id="a1", process_id="p1", max_concurrency=3),
        AgentDescriptor(agent_id="a2", process_id="p2", max_concurrency=1),
    ]
    claims = [
        TaskClaim(
            claim_id="c1", graph_id="g", graph_version=1, task_id="t1",
            agent_id="a1", process_id="p1", lease_resource="r1",
            state=ClaimState.ACTIVE, lease_id="lease-1",
        ),
        TaskClaim(
            claim_id="c2", graph_id="g", graph_version=1, task_id="t2",
            agent_id="a2", process_id="p2", lease_resource="r2",
            state=ClaimState.COMPLETED, lease_id=None,
        ),
    ]
    attempts = [
        ScheduledExecutionAttempt(
            attempt_id="att-1", task_id="t1", claim_id="c1",
            agent_id="a1", process_id="p1", state=AttemptState.RUNNING,
        ),
    ]
    proj = SchedulerProjection()
    proj.rebuild(agents, claims, attempts)
    return proj


def test_projection_rebuild_deterministic_fingerprint():
    """Rebuilding the SAME inputs must yield the SAME fingerprint three
    times (the Projection Rebuild Audit, Section 45)."""
    fp1 = projection_fingerprint(_build_projection())
    fp2 = projection_fingerprint(_build_projection())
    fp3 = projection_fingerprint(_build_projection())
    assert fp1 == fp2 == fp3
    assert len(fp1) == 64  # sha256 hexdigest


def test_different_inputs_yield_different_fingerprint():
    proj_a = _build_projection()
    agents_b = [
        AgentDescriptor(agent_id="a3", process_id="p3", max_concurrency=3),
    ]
    proj_b = rebuild_projection(agents_b, [], [],
                                lease_is_live=lambda l: True,
                                process_is_alive=lambda p: True)
    assert projection_fingerprint(proj_a) != projection_fingerprint(proj_b)


def test_rebuild_projection_from_authoritative_inputs():
    proj = _build_projection()
    assert set(proj.agents) == {"a1", "a2"}
    assert set(proj.claims) == {"c1", "c2"}
    assert set(proj.attempts) == {"att-1"}
    # Active counts: a1 has 1 ACTIVE, a2 has 0.
    assert proj.loads["a1"].active_claims == 1
    assert proj.loads["a2"].active_claims == 0
    assert proj.loads["a1"].max_concurrency == 3
