"""Demo 4 — No Eligible Agent.

A task requires specialization "rust".  Every registered agent only has
"python".  The scheduler must skip the task and record the eligibility
reasons in ScheduleResult.skipped — proving Section 11's "WHY NOT ELIGIBLE"
audit trail.
"""

from __future__ import annotations

import sys

from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "py1": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
            },
            "py2": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
            },
        },
        fake_vpg=vpg,
    )

    vpg.add_ready_task("rust_task", required_specializations=("rust",))
    res = sch.schedule_once(vpg.graph_id)
    assert res.dispatched == [], res.dispatched
    assert len(res.skipped) == 1
    task_id, reason = res.skipped[0]
    assert task_id == "rust_task"
    print(f"Skipped {task_id}: {reason}")

    # The skip reason references eligibility being rejected.
    assert "no eligible" in reason.lower() or "rust" in reason.lower(), reason
    assert sch.claims == [], "no claims should have been created"

    print("\nPASSED -- no eligible agent leaves the task pending and auditable")
    return 0


if __name__ == "__main__":
    sys.exit(main())
