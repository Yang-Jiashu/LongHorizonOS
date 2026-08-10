"""Scheduler Reconciliation (Section 27)."""

from __future__ import annotations

from datetime import UTC

import pytest

from lhos.runtimes.multi_agent.errors import LeaseReleaseFailed
from lhos.runtimes.multi_agent.models import ClaimState, TaskClaim
from lhos.runtimes.multi_agent.reconciliation import (
    detect_invariants_violations,
    reconcile,
)


def _claim(**kw):
    defaults = dict(
        claim_id="c",
        graph_id="g",
        graph_version=1,
        task_id="t",
        agent_id="a",
        process_id="p",
        lease_resource="vpg://g/task/t/claim",
        state=ClaimState.ACTIVE,
        lease_id="lease-1",
    )
    defaults.update(kw)
    return TaskClaim(**defaults)


def _lease(lease_id, live=True, expires_in_secs=1800):
    from datetime import datetime, timedelta

    class _L:
        lease_id: str = ""
        expires_at: object = None
        _live: bool = True

    l = _L()
    l.lease_id = lease_id
    l.expires_at = datetime.now(UTC) + timedelta(seconds=expires_in_secs)
    l._live = live
    return l


def test_reconcile_no_issues_when_everything_consistent():
    live = _lease("lease-1")
    claims = [_claim(lease_id="lease-1")]

    def lease_is_live(lid):
        return lid == "lease-1"

    def process_is_alive(pid):
        return True

    def vpg_verified(graph_id, task_id):
        return False

    def vpg_stale(graph_id, task_id):
        return False

    def lease_lookup(claim):
        return live if claim.lease_id == "lease-1" else None

    res = reconcile(
        claims,
        [],
        lease_is_live=lease_is_live,
        process_is_alive=process_is_alive,
        vpg_task_verified=vpg_verified,
        vpg_task_stale=vpg_stale,
        lease_lookup=lease_lookup,
        release_lease=lambda lid: True,
    )
    assert res.claims_marked_lost == 0
    assert res.claims_completed == 0
    assert res.issues == []


def test_reconcile_process_dead_marks_claim_lost():
    claims = [_claim(lease_id="lease-1")]

    def lease_is_live(lid):
        return True

    def process_is_alive(pid):
        return False

    def lease_lookup(claim):
        return _lease("lease-1")

    res = reconcile(
        claims,
        [],
        lease_is_live=lease_is_live,
        process_is_alive=process_is_alive,
        vpg_task_verified=lambda graph_id, task_id: False,
        vpg_task_stale=lambda graph_id, task_id: False,
        lease_lookup=lease_lookup,
        release_lease=lambda lid: True,
        clock_now=_now,
    )
    assert res.claims_marked_lost == 1
    assert claims[0].state == ClaimState.LOST


def test_reconcile_vanished_lease_marks_lost_and_releases():
    claims = [_claim(lease_id="lease-1")]

    res = reconcile(
        claims,
        [],
        lease_is_live=lambda lid: False,
        process_is_alive=lambda pid: True,
        vpg_task_verified=lambda graph_id, task_id: False,
        vpg_task_stale=lambda graph_id, task_id: False,
        lease_lookup=lambda c: None,
        release_lease=lambda lid: True,
        clock_now=_now,
    )
    assert res.claims_marked_lost == 1
    assert res.orphan_leases_released >= 1
    assert claims[0].state == ClaimState.LOST


def test_reconcile_task_verified_completes_claim():
    claims = [_claim(lease_id="lease-1")]

    res = reconcile(
        claims,
        [],
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: True,
        vpg_task_verified=lambda graph_id, task_id: True,
        vpg_task_stale=lambda graph_id, task_id: False,
        lease_lookup=lambda c: _lease("lease-1"),
        release_lease=lambda lid: True,
        clock_now=_now,
    )
    assert res.claims_completed == 1
    assert claims[0].state == ClaimState.COMPLETED


def test_reconcile_release_exception_preserves_active_claim():
    claims = [_claim(lease_id="lease-1")]

    def bad_release(lease_id):
        raise RuntimeError("lease service unavailable")

    with pytest.raises(LeaseReleaseFailed):
        reconcile(
            claims,
            [],
            lease_is_live=lambda lease: True,
            process_is_alive=lambda pid: True,
            vpg_task_verified=lambda graph_id, task_id: True,
            vpg_task_stale=lambda graph_id, task_id: False,
            lease_lookup=lambda claim: _lease("lease-1"),
            release_lease=bad_release,
            clock_now=_now,
        )
    assert claims[0].state == ClaimState.ACTIVE


def test_reconcile_orphan_proposed_claim_no_kernel_lease():
    """A lingering PROPOSED claim with no Kernel lease is reconciled to LOST."""
    claims = [_claim(state=ClaimState.PROPOSED, lease_id=None)]
    res = reconcile(
        claims,
        [],
        lease_is_live=lambda lid: False,
        process_is_alive=lambda pid: True,
        vpg_task_verified=lambda graph_id, task_id: False,
        vpg_task_stale=lambda graph_id, task_id: False,
        lease_lookup=lambda c: None,
        release_lease=lambda lid: True,
        clock_now=_now,
    )
    assert res.claims_marked_lost == 1


def test_detect_invariants_violations_d2_i4_multiple_active_per_task():
    claims = [
        _claim(claim_id="c1"),
        _claim(claim_id="c2"),
    ]
    violations = detect_invariants_violations(
        claims,
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: True,
    )
    assert any("D2-I4" in v for v in violations)


def test_detect_invariants_violations_d2_i5_no_lease_id():
    claims = [_claim(lease_id=None)]
    violations = detect_invariants_violations(
        claims,
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: True,
    )
    assert any("D2-I5" in v and "no lease" in v for v in violations)


def test_detect_invariants_violations_d2_i7_dead_process():
    claims = [_claim(lease_id="lease-1")]
    violations = detect_invariants_violations(
        claims,
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: False,
    )
    assert any("D2-I7" in v for v in violations)


def test_session_reconcile_does_not_crash_with_active_claim(world):
    """Section 32 regression: SchedulerSession.reconcile() is the public entry
    and must not crash when there is at least one ACTIVE claim.  Before the
    fix it raised ``TypeError`` because `MultiAgentScheduler._vpg_task_verified`
    is a 2-arg (graph_id, task_id) helper but `reconciliation.reconcile`
    invokes the `vpg_task_verified(task_id)` callback with a single arg.

    Uses the real Kernel-backed ``world`` fixture so the claim's lease is
    persisted and found back by reconcile (the FakeVPG/_NullLease in
    ``fake_scheduler`` deliberately does not track leases)."""
    from lhos.runtimes.verified_progress.patches import (
        AddNodeOp,
        GraphPatchProposal,
    )
    from tests.runtimes.multi_agent.helpers import scheduler_with_agents

    pid = world.kernel._process_service.spawn("a").pid
    sch = scheduler_with_agents(
        world,
        {"a": {"supported_task_kinds": ("*",), "specializations": ("python",)}},
    )
    gid = world.vpg_rt.create_graph(owner_pid=pid).graph_id
    v = world.vpg_rt.get_graph(gid).current_version
    patch = GraphPatchProposal(
        graph_id=gid,
        expected_graph_version=v,
        author_pid=pid,
        idempotency_key="add-t1",
        operations=(
            AddNodeOp(
                node_id="t1",
                graph_id=gid,
                node_type="task",
                created_by_pid=pid,
                task_kind="code_review",
                metadata={
                    "scheduler": {
                        "task_kind": "code_review",
                        "required_specializations": ["python"],
                        "required_tools": [],
                    }
                },
            ),
        ),
    )
    world.vpg_rt.submit_patch(patch)
    sch.schedule_once(gid)
    active = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.ACTIVE]
    assert len(active) == 1, "precondition: one ACTIVE claim exists"

    # This must NOT raise TypeError (the original failure mode).
    res = sch.reconcile()
    # After reconcile on a healthy ACTIVE claim: nothing lost, nothing completed.
    post_active = [c for c in sch.claims if c.task_id == "t1" and c.state == ClaimState.ACTIVE]
    assert len(post_active) == 1
    assert res.claims_marked_lost == 0
    assert res.claims_completed == 0


def _now():
    from datetime import datetime

    return datetime.now(UTC)
