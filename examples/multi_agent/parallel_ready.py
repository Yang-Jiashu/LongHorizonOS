"""Demo 2 — Parallel Ready.

Four independent ready tasks; two agents each with max_concurrency=2.
With all tasks simultaneously READY and enough concurrency headroom, the
scheduler fills both agents to capacity in a single pass.

Demonstrates Section 21 (max_concurrency) and the deterministic tie-break
policy when multiple agents are equally eligible.
"""

from __future__ import annotations

import sys

from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "a1": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
                "cost_weight": 100,
            },
            "a2": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
                "cost_weight": 200,
            },
        },
        fake_vpg=vpg,
    )

    for i in range(4):
        vpg.add_ready_task(f"t{i}", required_specializations=("python",))

    results = sch.schedule_until_idle(vpg.graph_id, max_dispatches=50)
    total = sum(len(r.dispatched) for r in results)
    print(f"Dispatched {total} tasks across {len(results)} pass(es)")
    by_agent: dict[str, int] = {}
    for c in sch.claims:
        by_agent[c.agent_id] = by_agent.get(c.agent_id, 0) + 1
    for agent, n in sorted(by_agent.items()):
        print(f"  {agent}: {n} claim(s)")

    # With 2 agents x 2 concurrency and 4 tasks, all 4 dispatched and
    # neither agent exceeds its cap.
    assert total == 4, f"expected 4 dispatched, got {total}"
    assert by_agent["a1"] <= 2
    assert by_agent["a2"] <= 2
    # Best-fit: cheaper agent (a1) gets the tie-break for the first two
    # tasks; a2 picks up the rest once a1 is full.
    assert by_agent["a1"] == 2, by_agent

    print("\nPASSED -- parallel ready filled both agents to capacity")
    return 0


if __name__ == "__main__":
    sys.exit(main())
