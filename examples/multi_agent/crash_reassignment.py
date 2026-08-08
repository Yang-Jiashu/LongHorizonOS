"""Demo 3 — Crash Reassignment.

Contract (Section 26):
    1. Agent A1 is dispatched task T1 and holds an ACTIVE claim.
    2. A1 dies (its Kernel process is gone / its lease is revoked).
    3. The next reconcile() marks A1's claim LOST.
    4. After the claim is LOST, T1 is READY again in the VPG; a following
       schedule_once picks it up for agent A2.

Demonstrates death -> reconcile -> reschedule loop.  Real-Kernel SIGKILL
cases live under tests/runtimes/multi_agent/test_sigkill_reassignment.py.
"""

from __future__ import annotations

import sys

from lhos.runtimes.multi_agent.models import ClaimState
from lhos.runtimes.multi_agent.reconciliation import reconcile
from tests.runtimes.multi_agent.helpers import FakeVPG, _DeadProc, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "a1": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
            },
            "a2": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
            },
        },
        fake_vpg=vpg,
    )

    vpg.add_ready_task("t1", required_specializations=("python",))
    r1 = sch.schedule_once(vpg.graph_id)
    assert len(r1.dispatched) == 1, r1.dispatched
    first_agent = r1.dispatched[0]["agent_id"]
    second_agent = "a2" if first_agent == "a1" else "a1"
    print(f"Initial dispatch -> {first_agent} owns t1")

    active_claim = sch.active_claim_for_task("t1")
    assert active_claim is not None
    assert active_claim.state == ClaimState.ACTIVE

    # Simulate A1 death: its lease is revoked, the process dies.  Sync the
    # scheduler's process provider liveness view so subsequent eligibility
    # checks reject the dead agent.
    dead_pid = active_claim.process_id
    sch._s._process = _DeadProc({dead_pid})
    all_claims = list(sch.claims)

    def process_is_alive(pid: str) -> bool:
        return pid != dead_pid

    rec = reconcile(
        all_claims, [],
        lease_is_live=lambda lid: False,
        process_is_alive=process_is_alive,
        vpg_task_verified=lambda tid: False,
        vpg_task_stale=lambda tid: False,
        lease_lookup=lambda c: None,
        release_lease=lambda lid: True,
    )
    print(f"Reconciliation result: {rec.claims_marked_lost} claim(s) LOST")
    assert rec.claims_marked_lost == 1, rec.claims_marked_lost
    assert active_claim.state == ClaimState.LOST

    # Bump graph version (as VPG would) and re-run the scheduler.  The
    # surviving agent takes over.
    vpg.bump_version()
    vpg.add_ready_task("t1", required_specializations=("python",),
                       version=vpg.current_version)
    r2 = sch.schedule_once(vpg.graph_id)
    assert len(r2.dispatched) == 1, r2.dispatched
    assert r2.dispatched[0]["agent_id"] == second_agent, r2.dispatched
    print(f"Reassigned to {second_agent} after A1 crashed")

    print("\nPASSED -- crash reassignment: LOST -> reschedule -> new owner")
    return 0


if __name__ == "__main__":
    sys.exit(main())
