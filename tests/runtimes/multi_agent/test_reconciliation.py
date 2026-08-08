"""Scheduler Reconciliation (Section 27)."""

from __future__ import annotations

from datetime import UTC

from lhos.runtimes.multi_agent.models import ClaimState, TaskClaim
from lhos.runtimes.multi_agent.reconciliation import (
    detect_invariants_violations,
    reconcile,
)


def _claim(**kw):
    defaults = dict(
        claim_id="c", graph_id="g", graph_version=1, task_id="t",
        agent_id="a", process_id="p", lease_resource="vpg://g/task/t/claim",
        state=ClaimState.ACTIVE, lease_id="lease-1",
    )
    defaults.update(kw)
    return TaskClaim(**defaults)


def _lease(lease_id, live=True, expires_in_secs=1800):
    from datetime import datetime, timedelta

    class _L:
        pass

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

    def vpg_verified(tid):
        return False

    def vpg_stale(tid):
        return False

    def lease_lookup(claim):
        return live if claim.lease_id == "lease-1" else None

    res = reconcile(claims, [],
                    lease_is_live=lease_is_live,
                    process_is_alive=process_is_alive,
                    vpg_task_verified=vpg_verified,
                    vpg_task_stale=vpg_stale,
                    lease_lookup=lease_lookup,
                    release_lease=lambda lid: True)
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

    res = reconcile(claims, [],
                    lease_is_live=lease_is_live,
                    process_is_alive=process_is_alive,
                    vpg_task_verified=lambda tid: False,
                    vpg_task_stale=lambda tid: False,
                    lease_lookup=lease_lookup,
                    release_lease=lambda lid: True,
                    clock_now=_now)
    assert res.claims_marked_lost == 1
    assert claims[0].state == ClaimState.LOST


def test_reconcile_vanished_lease_marks_lost_and_releases():
    claims = [_claim(lease_id="lease-1")]

    res = reconcile(claims, [],
                    lease_is_live=lambda lid: False,
                    process_is_alive=lambda pid: True,
                    vpg_task_verified=lambda tid: False,
                    vpg_task_stale=lambda tid: False,
                    lease_lookup=lambda c: None,
                    release_lease=lambda lid: True,
                    clock_now=_now)
    assert res.claims_marked_lost == 1
    assert res.orphan_leases_released >= 1
    assert claims[0].state == ClaimState.LOST


def test_reconcile_task_verified_completes_claim():
    claims = [_claim(lease_id="lease-1")]

    res = reconcile(claims, [],
                    lease_is_live=lambda lid: True,
                    process_is_alive=lambda pid: True,
                    vpg_task_verified=lambda tid: True,
                    vpg_task_stale=lambda tid: False,
                    lease_lookup=lambda c: _lease("lease-1"),
                    release_lease=lambda lid: True,
                    clock_now=_now)
    assert res.claims_completed == 1
    assert claims[0].state == ClaimState.COMPLETED


def test_reconcile_orphan_proposed_claim_no_kernel_lease():
    """A lingering PROPOSED claim with no Kernel lease is reconciled to LOST."""
    claims = [_claim(state=ClaimState.PROPOSED, lease_id=None)]
    res = reconcile(claims, [],
                    lease_is_live=lambda lid: False,
                    process_is_alive=lambda pid: True,
                    vpg_task_verified=lambda tid: False,
                    vpg_task_stale=lambda tid: False,
                    lease_lookup=lambda c: None,
                    release_lease=lambda lid: True,
                    clock_now=_now)
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


def _now():
    from datetime import datetime
    return datetime.now(UTC)
