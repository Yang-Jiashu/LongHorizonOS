"""LongHorizonOS quickstart: authority-backed observation repair.

This is the preferred v0.x repair path:

1. Execute and verify a Goal.
2. Register the exact changed bytes with ``AgentOS.observe_artifact``.
3. Pass the returned graph-bound ``ObservationToken`` to ``AgentOS.repair``.
4. Re-run only the affected frontier with fresh Evidence.

Run from anywhere with ``lhos`` installed:

    python observation_repair.py
"""

from __future__ import annotations

import json

from lhos.sdk import Agent, AgentOS, Goal, scripted_executor


def main() -> dict[str, object]:
    with AgentOS(":memory:") as os_:
        os_.add_agent(Agent("worker", specializations=("python",)))

        goal = Goal("Safe observation repair")
        build = goal.task(
            "Build",
            agent="worker",
            verify=scripted_executor(
                artifact_id="source.py",
                version=1,
                content="print('v1')\n",
            ),
        )
        goal.task(
            "Review",
            agent="worker",
            depends_on=(build,),
            verify=scripted_executor(
                artifact_id="review.txt",
                version=1,
                content="reviewed",
            ),
        )
        goal.task(
            "Independent",
            agent="worker",
            verify=scripted_executor(
                artifact_id="notes.txt",
                version=1,
                content="unchanged",
            ),
        )

        initial = os_.run(goal, max_dispatches=8)
        if initial.goal_state != "closed":
            raise RuntimeError(f"initial Goal did not close: {initial.as_dict()}")

        # The token is issued only after the exact bytes and their SHA-256 hash
        # are registered by the FactsProvider. It is also bound to this graph.
        token = os_.observe_artifact(
            goal,
            "source.py",
            2,
            b"print('v2')\n",
        )
        repair = os_.repair(goal, observation=token)

        # Supply fresh exact-version Evidence for the affected source task.
        build.verify = scripted_executor(
            artifact_id="source.py",
            version=2,
            content="print('v2')\n",
        )
        restored = os_.run(goal, max_dispatches=8)
        if restored.goal_state != "closed":
            raise RuntimeError(f"repaired Goal did not re-close: {restored.as_dict()}")

        result: dict[str, object] = {
            "schema_version": "observation-repair-demo-v1",
            "token": {
                "artifact_id": token.artifact_id,
                "version": token.version,
                "content_hash": token.content_hash,
                "graph_id": token.graph_id,
            },
            "initial_closed": initial.goal_state == "closed",
            "affected": sorted(repair.affected),
            "preserved": sorted(repair.preserved),
            "repair_frontier": sorted(repair.frontier),
            "final_closed": restored.goal_state == "closed",
        }
        print(json.dumps(result, sort_keys=True))
        return result


if __name__ == "__main__":
    main()
