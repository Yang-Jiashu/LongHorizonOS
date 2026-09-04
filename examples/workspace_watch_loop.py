"""Bounded caller-owned workspace observation loop.

This example is intentionally deterministic and local.  It demonstrates one
epoch, a file mutation, and two subsequent watcher-driven epochs; it does not
start a daemon or claim to be a universal world watcher.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import (
    Agent,
    AgentOS,
    Goal,
    WorkspaceWatchLoop,
    scripted_executor,
)


async def run() -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="lhos-watch-loop-") as folder:
        workspace = WorkspaceTool(Path(folder) / "workspace")
        workspace.write("input.txt", b"v1")
        runtime = AgentOS(":memory:")
        try:
            runtime.add_agent(Agent("worker", specializations=("python",)))
            goal = Goal("workspace-watch-loop-example")
            consumer = goal.task(
                "consumer",
                agent="worker",
                inputs=("workspace://input.txt",),
                verify=scripted_executor(
                    artifact_id="consumer.out",
                    version=1,
                    content="consumer-verified",
                ),
            )
            goal.task(
                "dependent",
                agent="worker",
                depends_on=(consumer,),
                verify=scripted_executor(
                    artifact_id="dependent.out",
                    version=1,
                    content="dependent-verified",
                ),
            )
            goal.compile(runtime)
            watcher = runtime.workspace_watcher(
                goal,
                workspace,
                ("workspace://input.txt",),
                task_ids_by_resource={"workspace://input.txt": ("consumer",)},
            )
            watcher.initialize()
            supervisor = runtime.event_supervisor(
                goal,
                watcher=watcher,
                max_epochs=3,
                max_concurrency=1,
                max_dispatches_per_epoch=1,
                poll_workspace=True,
                persist_epoch=False,
            )

            # First bounded epoch executes the initial consumer.
            first = await supervisor.step()
            workspace.write("input.txt", b"v2")

            # The next two epochs observe/reconcile the changed input and
            # execute only bounded repair work.
            loop = WorkspaceWatchLoop(supervisor, poll_interval=0)
            remainder = await loop.run(max_steps=2)
            return {
                "first_status": first.status.value,
                "loop_stop_reason": remainder.stop_reason.value,
                "polls_completed": remainder.polls_completed,
                "goal_state": supervisor.snapshot.goal_state,
                "scope": {
                    "caller_owned": True,
                    "explicit_file_set": True,
                    "daemon": False,
                    "single_process": True,
                },
            }
        finally:
            runtime.close()


def main() -> int:
    print(json.dumps(asyncio.run(run()), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
