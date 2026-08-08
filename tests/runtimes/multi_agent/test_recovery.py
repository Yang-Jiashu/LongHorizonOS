"""Scheduler recovery — restart-time finalize + projection rebuild."""

from __future__ import annotations

from lhos.runtimes.multi_agent.models import ClaimState, TaskClaim
from lhos.runtimes.multi_agent.recovery import finalize_after_restart


def _claim(**kw):
    defaults = dict(
        claim_id="c", graph_id="g", graph_version=1, task_id="t",
        agent_id="a", process_id="p", lease_resource="vpg://g/task/t/claim",
        state=ClaimState.ACTIVE, lease_id="lease-1",
    )
    defaults.update(kw)
    return TaskClaim(**defaults)


def test_finalize_after_restart_marks_lost_claims_with_dead_process():
    claims = [_claim(claim_id="c1", lease_id="lease-1")]
    tally = finalize_after_restart(
        claims,
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: False,
        release_lease=lambda lid: True,
    )
    assert tally["claims_marked_lost"] == 1
    assert claims[0].state == ClaimState.LOST
    assert "process_dead" in claims[0].reason


def test_finalize_after_restart_releases_orphan_lease_when_expired():
    claims = [_claim(claim_id="c1", lease_id="lease-1")]
    tally = finalize_after_restart(
        claims,
        lease_is_live=lambda lid: False,
        process_is_alive=lambda pid: True,
        release_lease=lambda lid: True,
    )
    assert tally["claims_marked_lost"] == 1
    assert tally["orphan_leases_released"] == 1


def test_finalize_keeps_healthy_active_claim_intact():
    claims = [_claim(claim_id="c1", lease_id="lease-1")]
    tally = finalize_after_restart(
        claims,
        lease_is_live=lambda lid: True,
        process_is_alive=lambda pid: True,
        release_lease=lambda lid: True,
    )
    assert tally["claims_marked_lost"] == 0
    assert claims[0].state == ClaimState.ACTIVE


def test_finalize_skips_already_terminal_claims():
    for s in [ClaimState.COMPLETED, ClaimState.LOST, ClaimState.RELEASED, ClaimState.REJECTED]:
        claims = [_claim(claim_id="c1", lease_id=None, state=s)]
        # Reset to active to ensure we start from deterministic state
        claims[0].state = s
        tally = finalize_after_restart(
            claims,
            lease_is_live=lambda lid: False,
            process_is_alive=lambda pid: False,
            release_lease=lambda lid: True,
        )
        assert tally["claims_marked_lost"] == 0
        assert claims[0].state == s


def test_finalize_release_exception_does_not_propagate():
    claims = [_claim(claim_id="c1", lease_id="lease-1")]

    def bad_release(lid):
        raise RuntimeError("lease service down")

    tally = finalize_after_restart(
        claims,
        lease_is_live=lambda lid: False,
        process_is_alive=lambda pid: True,
        release_lease=bad_release,
    )
    # Even when release throws, the claim should still be marked LOST.
    assert tally["claims_marked_lost"] == 1
    assert claims[0].state == ClaimState.LOST
