"""Demo 1 — Specialized Pipeline.

Three agents with non-overlapping specializations; the scheduler routes each
ready task to the ONLY agent that satisfies its required_specializations.
Demonstrates Section 11 (10-predicate Eligibility) + Section 13 (Deterministic
Best-Fit Matching) when hard eligibility dominates.

Topology (VPG-ready tasks, independent):
   python_task , review_task , deploy_task

Each task tags its required_specializations in the "scheduler" metadata.
Only one agent matches each — routing is forced, not scored.
"""

from __future__ import annotations

import sys

from tests.runtimes.multi_agent.helpers import FakeVPG, fake_scheduler


def main() -> int:
    vpg = FakeVPG()
    sch = fake_scheduler(
        {
            "py-agent": {
                "supported_task_kinds": ("*",),
                "specializations": ("python",),
                "max_concurrency": 2,
            },
            "review-agent": {
                "supported_task_kinds": ("*",),
                "specializations": ("code-review",),
                "max_concurrency": 2,
            },
            "deploy-agent": {
                "supported_task_kinds": ("*",),
                "specializations": ("deploy",),
                "max_concurrency": 2,
            },
        },
        fake_vpg=vpg,
    )

    vpg.add_ready_task("python_task", required_specializations=("python",))
    vpg.add_ready_task("review_task", required_specializations=("code-review",))
    vpg.add_ready_task("deploy_task", required_specializations=("deploy",))

    results = sch.schedule_until_idle(vpg.graph_id, max_dispatches=50)
    total = sum(len(r.dispatched) for r in results)
    print(f"Dispatched {total} tasks across {len(results)} pass(es)")
    by_agent: dict[str, list[str]] = {}
    for c in sch.claims:
        by_agent.setdefault(c.agent_id, []).append(c.task_id)
    for agent, tasks in sorted(by_agent.items()):
        print(f"  {agent}: {tasks}")

    # Each specialist must have received its natural task.
    assert by_agent.get("py-agent") == ["python_task"], by_agent
    assert by_agent.get("review-agent") == ["review_task"], by_agent
    assert by_agent.get("deploy-agent") == ["deploy_task"], by_agent
    assert total == 3

    print("\nPASSED -- specialized pipeline routed each task to the matching agent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
