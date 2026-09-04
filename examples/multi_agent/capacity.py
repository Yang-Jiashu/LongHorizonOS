"""Demo 5 — Capacity.

A single agent with max_concurrency=1.  Five tasks are simultaneously READY.
The scheduler dispatches exactly one claim; the remaining four are skipped.

Demonstrates the D2-I5 projection-bounded concurrency guard — the agent is
never given more work than it can hold at once.
"""

from __future__ import annotations

import sys

from lhos.runtimes.multi_agent.models import ClaimState
from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "sole": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 1,
            },
        },
        fake_vpg=vpg,
    )

    for i in range(5):
        vpg.add_ready_task(f"t{i}", required_specializations=("python",))
    results = sch.schedule_until_idle(vpg.graph_id, max_dispatches=50)
    total = sum(len(r.dispatched) for r in results)
    active = [c for c in sch.claims if c.state == ClaimState.ACTIVE]
    print(f"Dispatched {total} task(s); ACTIVE claims: {len(active)}")

    assert total == 1, f"max_concurrency=1 must cap at 1 dispatch, got {total}"
    assert len(active) == 1, len(active)

    print(f"  sole agent claim: {active[0].task_id}")
    print("\nPASSED -- capacity bounded the agent to max_concurrency=1")
    return 0


if __name__ == "__main__":
    sys.exit(main())
