"""Demo 6 — Semantic / Operational Separation.

Key contract (Section 22/24):
    SUCCEEDED_OPERATIONALLY != VERIFIED_SEMANTICALLY.

    - Operational success: the Agent's side-effect action committed locally.
      Recorded via AttemptManager.mark_operationally_succeeded.
    - Semantic verification: the VPG derived the Task to validity=VERIFIED from
      evidence.  Scheduler derives claim COMPLETED only via observe_vpg.

Demo drives both transitions explicitly:
    1. Schedule a task for agent A.
    2. Mark attempt operationally-succeeded — VPG still UNVERIFIED.
    3. Assert: claim is still ACTIVE.
    4. Mark attempt semantically-verified + VPG sees VERIFIED.
    5. Assert: observe_vpg completes the claim.

Attempts promoted RUNNING -> VERIFIED_SEMANTICALLY without the OPERATIONAL
milestone are flagged in the audit trail (error field) but still reach
VERIFIED.
"""

from __future__ import annotations

import sys

from lhos.runtimes.multi_agent.models import AttemptState, ClaimState
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "a1": {
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

    claim = sch.active_claim_for_task("t1")
    assert claim is not None and claim.state == ClaimState.ACTIVE

    # Use the Scheduler-owned Attempt. A detached AttemptManager record would
    # not be authoritative for claim completion or durable replay.
    att = sch.attempt_for_claim(claim.claim_id)
    assert att is not None
    assert sch.mark_execution_started(claim.claim_id) is att

    # Step 1: Operation only — VPG still UNVERIFIED.
    assert sch.mark_execution_operationally_succeeded(claim.claim_id) is att
    assert att.state == AttemptState.SUCCEEDED_OPERATIONALLY
    assert vpg.validities["t1"] == "unverified"
    tally = sch.observe_vpg(vpg.graph_id)
    assert tally["claims_completed"] == 0, tally
    assert claim.state == ClaimState.ACTIVE
    print("Operational success recorded; claim remains ACTIVE (VPG unverified)")

    # Step 2: Semantic verification is derived by the Scheduler after VPG
    # validity changes; callers do not mutate a detached Attempt directly.
    vpg.set_validity("t1", "verified")
    tally = sch.observe_vpg(vpg.graph_id)
    assert tally["claims_completed"] == 1, tally
    assert att.state == AttemptState.VERIFIED_SEMANTICALLY
    assert claim.state == ClaimState.COMPLETED
    print("Semantic verification derived -> claim COMPLETED via observe_vpg")

    print("\nPASSED -- operational success != semantic verification enforced")
    return 0


if __name__ == "__main__":
    sys.exit(main())
