"""Bounded online-supervisor demonstration.

This module intentionally uses only the public SDK.  It is a small,
deterministic vertical slice for presentations and smoke tests:

``start -> observe -> execute bounded epochs -> observe -> CLOSED``.

The demonstration uses a controlled local executor and scripted verifiers; it
does not call an LLM, start a daemon, or claim physical resource telemetry.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from lhos.sdk import (
    Agent,
    AgentOS,
    EventDrivenSupervisor,
    Goal,
    SupervisorState,
    SupervisorStepStatus,
    scripted_executor,
)


@dataclass
class OnlineSupervisorSemantics:
    """Stable semantic projection returned by :func:`run_online_supervisor`."""

    schema_version: str = "online-supervisor-demo.v1"
    goal_id: str = "online-supervisor-demo"
    bounded: bool = True
    uses_real_sdk: bool = True
    uses_controlled_executor: bool = True
    uses_llm: bool = False
    daemon_started: bool = False
    start_state: str = ""
    observation_status: str = ""
    execution_statuses: list[str] = field(default_factory=list)
    graph_versions: list[int] = field(default_factory=list)
    dispatched_task_ids: list[str] = field(default_factory=list)
    verified_task_ids: list[str] = field(default_factory=list)
    final_state: str = ""
    final_closed: bool = False
    epochs_attempted: int = 0
    steps: int = 0
    stop_reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "goal_id": self.goal_id,
            "bounded": self.bounded,
            "uses_real_sdk": self.uses_real_sdk,
            "uses_controlled_executor": self.uses_controlled_executor,
            "uses_llm": self.uses_llm,
            "daemon_started": self.daemon_started,
            "start_state": self.start_state,
            "observation_status": self.observation_status,
            "execution_statuses": list(self.execution_statuses),
            "graph_versions": list(self.graph_versions),
            "dispatched_task_ids": list(self.dispatched_task_ids),
            "verified_task_ids": list(self.verified_task_ids),
            "final_state": self.final_state,
            "final_closed": self.final_closed,
            "epochs_attempted": self.epochs_attempted,
            "steps": self.steps,
            "stop_reason": self.stop_reason,
        }


class OnlineSupervisorDemoAssertionError(RuntimeError):
    """Raised when the demo fails to reach the expected semantic closure."""


def _fail(message: str) -> None:
    raise OnlineSupervisorDemoAssertionError(
        f"online-supervisor demo semantic assertion failed: {message}"
    )


def _controlled_executor(task_id: str) -> dict[str, Any]:
    """Deterministic stand-in for a Harness/Agent invocation."""

    return {"task_id": str(task_id), "executor": "controlled", "ok": True}


async def _run_async(*, max_epochs: int = 6) -> tuple[AgentOS, OnlineSupervisorSemantics]:
    if isinstance(max_epochs, bool) or not isinstance(max_epochs, int) or max_epochs < 1:
        raise ValueError("max_epochs must be a positive integer")

    agent_os = AgentOS(":memory:")
    agent_os.add_agent(
        Agent(
            "demo-worker",
            executor=_controlled_executor,
            specializations=("python",),
        )
    )

    goal = Goal("online-supervisor-demo")
    prepare = goal.task(
        "prepare",
        agent="demo-worker",
        verify=scripted_executor(artifact_id="prepare.out", version=1),
    )
    validate = goal.task(
        "validate",
        agent="demo-worker",
        depends_on=(prepare,),
        verify=scripted_executor(artifact_id="validate.out", version=1),
    )
    goal.task(
        "publish",
        agent="demo-worker",
        depends_on=(validate,),
        verify=scripted_executor(artifact_id="publish.out", version=1),
    )
    goal.compile(agent_os)

    supervisor = EventDrivenSupervisor(
        agent_os,
        goal,
        max_epochs=max_epochs,
        max_concurrency=1,
        max_dispatches_per_epoch=1,
    )
    started = supervisor.start()
    semantics = OnlineSupervisorSemantics(start_state=started.state.value)
    if started.state is not SupervisorState.RUNNING:
        _fail(f"expected RUNNING after start, got {started.state.value}")

    # Explicit observation demonstrates the caller-owned event boundary.  It
    # does no execution and is intentionally separate from the first epoch.
    observed = await supervisor.step(execute=False)
    semantics.observation_status = observed.status.value
    semantics.graph_versions.append(observed.graph_version)
    if observed.status is not SupervisorStepStatus.OBSERVED:
        _fail(f"expected OBSERVED step, got {observed.status.value}")

    while not supervisor.terminal:
        result = await supervisor.step()
        semantics.execution_statuses.append(result.status.value)
        semantics.graph_versions.append(result.graph_version)
        semantics.steps += 1
        execution = result.execution_result
        if execution is not None:
            verified = getattr(execution, "verified", ()) or ()
            semantics.verified_task_ids = sorted({str(item) for item in verified})
            online = getattr(execution, "meta", {}).get("online_epoch", {})
            dispatched = online.get("actual_dispatched_task_ids", ()) or ()
            semantics.dispatched_task_ids.extend(str(item) for item in dispatched)
        if result.status in {
            SupervisorStepStatus.CLOSED,
            SupervisorStepStatus.FAILED_CLOSED,
            SupervisorStepStatus.STOPPED,
            SupervisorStepStatus.BUDGET_EXHAUSTED,
        }:
            break

    final = supervisor.snapshot
    semantics.final_state = final.state.value
    semantics.final_closed = final.state is SupervisorState.CLOSED and final.goal_state == "closed"
    semantics.epochs_attempted = final.epochs_attempted
    semantics.stop_reason = final.stop_reason
    semantics.dispatched_task_ids = list(dict.fromkeys(semantics.dispatched_task_ids))
    if not semantics.final_closed:
        _fail(f"goal did not close; final supervisor state={final.state.value}")
    expected = ["prepare", "validate", "publish"]
    if semantics.dispatched_task_ids != expected:
        _fail(
            "expected one bounded dispatch per epoch in dependency order; "
            f"got {semantics.dispatched_task_ids!r}"
        )
    if semantics.verified_task_ids != sorted(expected):
        _fail(f"expected all tasks verified, got {semantics.verified_task_ids!r}")
    return agent_os, semantics


def run_online_supervisor(*, max_epochs: int = 6) -> tuple[AgentOS, OnlineSupervisorSemantics]:
    """Run the deterministic bounded supervisor demo.

    The returned ``AgentOS`` is intentionally exposed so embedding callers can
    inspect the authoritative runtime state before closing it.
    """

    return asyncio.run(_run_async(max_epochs=max_epochs))


__all__ = [
    "OnlineSupervisorDemoAssertionError",
    "OnlineSupervisorSemantics",
    "run_online_supervisor",
]
