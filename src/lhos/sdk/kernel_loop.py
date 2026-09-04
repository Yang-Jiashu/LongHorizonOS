"""Self-driving kernel loop: drive one Goal to a VERIFIED close.

Everything needed for online scheduling already exists in this SDK -- the
frontier/graph-utility ranking, conflict-aware batching, context-residency
matching, the semantic-interrupt policy, and the workspace watcher.  What was
missing is *agency*: :class:`~lhos.sdk.event_supervisor.EventDrivenSupervisor`
is deliberately caller-owned and takes one step per call, and
``AgentOS.run(adaptive=...)`` defaults to ``False``.  So the mechanisms of an OS
were present while the loop that exercises them was not.

This module supplies that loop and nothing else.  It is additive: no existing
default changes, and every authority stays where it was -- Scheduler eligibility,
Claim/Lease fencing, independent verification, and the serialized Evidence
commit are untouched.

Two properties are kept deliberately:

*Bounded, never a daemon.*  Both the epoch count and the idle-poll count are
finite, so the loop always terminates with an explicit ``stop_reason``.  The
existing supervisor refuses to turn a transient no-work observation into a
daemon; this loop keeps that refusal but adds a *bounded* idle wait, because for
a long-horizon task the interesting events (a changed requirement, API, or
artifact) arrive from outside and a scheduler that quits on the first idle tick
can never observe them.

*Watching is on when it is possible.*  If a watcher is supplied, polling and
interrupt delivery are enabled by default rather than opt-in.  An OS that only
notices change when asked is not managing anything.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Final

from .errors import ConfigurationError
from .event_supervisor import (
    EventDrivenSupervisor,
    SupervisorStepResult,
    SupervisorStepStatus,
)

KERNEL_LOOP_SCHEMA_VERSION: Final[str] = "kernel-loop.v1"

_TERMINAL_STATUSES: Final[frozenset[SupervisorStepStatus]] = frozenset(
    {
        SupervisorStepStatus.CLOSED,
        SupervisorStepStatus.STOPPED,
        SupervisorStepStatus.FAILED_CLOSED,
        SupervisorStepStatus.EVENT_REJECTED,
        SupervisorStepStatus.BUDGET_EXHAUSTED,
    }
)


@dataclass(frozen=True)
class KernelRunResult:
    """Transcript of one self-driven run.

    ``stop_reason`` is always populated: a caller must be able to tell a
    verified close apart from an exhausted budget or a starved frontier without
    re-deriving it from the step list.
    """

    schema_version: str
    goal_id: str
    goal_closed: bool
    stop_reason: str
    epochs_executed: int
    idle_polls_used: int
    dispatched_task_ids: tuple[str, ...]
    verified_task_ids: tuple[str, ...]
    dispatch_orders: tuple[tuple[str, ...], ...]
    locality_matched_task_ids: tuple[str, ...]
    parallelism_decisions: tuple[Any, ...]
    steps: tuple[SupervisorStepResult, ...]

    @property
    def progressed(self) -> bool:
        return bool(self.dispatched_task_ids) or bool(self.verified_task_ids)


def _online_epoch_meta(step: SupervisorStepResult) -> dict[str, Any]:
    result = step.execution_result
    meta = getattr(result, "meta", None)
    if not isinstance(meta, dict):
        return {}
    online = meta.get("online_epoch")
    return online if isinstance(online, dict) else {}


def _ids(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(str(item).strip() for item in value if str(item).strip())


async def drive_goal_to_closure(
    agent_os: Any,
    goal: Any,
    *,
    max_epochs: int = 256,
    max_concurrency: int = 1,
    max_dispatches_per_epoch: int | None = None,
    max_parallelism: int | None = None,
    resource_aware: bool = False,
    conflict_graph: Any | None = None,
    watcher: Any | None = None,
    idle_polls: int = 0,
    idle_poll_seconds: float = 0.0,
    persist_epoch: bool = True,
    adaptive_parallelism: bool = False,
) -> KernelRunResult:
    """Repeatedly observe, plan, dispatch and re-evaluate until the Goal closes.

    ``idle_polls`` bounds how many times the loop may find no admissible work
    and still continue, re-polling the watcher for external change instead of
    giving up.  It defaults to ``0``, which reproduces the existing
    quit-on-idle behaviour exactly; raise it only when a watcher can actually
    deliver new work, otherwise the loop just sleeps to no purpose.

    ``adaptive_parallelism`` lets the loop *choose* the degree for each epoch
    from observed conflict, resource and contention state instead of holding the
    caller's ``max_parallelism`` constant for the whole run.  The caller's value
    stays a ceiling; the decision only ever lands at or below it.  It is opt-in
    because it changes how many agents a run will occupy.
    """

    if not isinstance(max_epochs, int) or isinstance(max_epochs, bool) or max_epochs < 1:
        raise ConfigurationError("max_epochs must be an integer >= 1")
    if not isinstance(adaptive_parallelism, bool):
        raise ConfigurationError("adaptive_parallelism must be a boolean")
    if not isinstance(idle_polls, int) or isinstance(idle_polls, bool) or idle_polls < 0:
        raise ConfigurationError("idle_polls must be an integer >= 0")
    if isinstance(idle_poll_seconds, bool) or not isinstance(idle_poll_seconds, (int, float)):
        raise ConfigurationError("idle_poll_seconds must be a number")
    if idle_poll_seconds < 0:
        raise ConfigurationError("idle_poll_seconds must be >= 0")
    if idle_polls and watcher is None and idle_poll_seconds == 0:
        raise ConfigurationError(
            "idle_polls without a watcher or a poll interval cannot observe new work; "
            "supply a watcher, set idle_poll_seconds, or leave idle_polls=0"
        )

    watching = watcher is not None
    # Compile on entry if needed.  Requiring the caller to compile first would
    # keep a step of the ceremony this loop exists to remove, and it is what
    # ``AgentOS.run`` already does.
    goal_id = str(getattr(goal, "goal_id", goal)).strip()
    if goal_id and agent_os._gid_for(goal_id) is None and callable(getattr(goal, "compile", None)):
        goal.compile(agent_os)

    supervisor = EventDrivenSupervisor(
        agent_os,
        goal,
        watcher=watcher,
        max_epochs=max_epochs,
        max_concurrency=max_concurrency,
        max_dispatches_per_epoch=max_dispatches_per_epoch,
        max_parallelism=max_parallelism,
        resource_aware=resource_aware,
        conflict_graph=conflict_graph,
        persist_epoch=persist_epoch,
        poll_workspace=watching,
        route_workspace=watching,
        deliver_interrupts=watching,
    )
    supervisor.start()

    steps: list[SupervisorStepResult] = []
    dispatched: list[str] = []
    verified: list[str] = []
    dispatch_orders: list[tuple[str, ...]] = []
    locality: list[str] = []
    idle_used = 0
    stop_reason = ""
    parallelism_decisions: list[Any] = []

    try:
        while len(steps) < max_epochs:
            if adaptive_parallelism:
                decision = _decide_degree(
                    agent_os,
                    goal,
                    conflict_graph=conflict_graph,
                    epoch_id=len(steps),
                    ceiling=max_parallelism,
                )
                if decision is not None:
                    parallelism_decisions.append(decision)
                    # A chosen degree of 0 means "no stable admissible work".
                    # Fall back to serial rather than to the ceiling: admitting a
                    # wide batch on that observation is exactly the premature
                    # parallelism this decision exists to prevent.
                    supervisor.set_max_parallelism(max(1, int(decision.chosen_degree)))
            step = await supervisor.step()
            steps.append(step)

            meta = _online_epoch_meta(step)
            dispatched.extend(_ids(meta.get("actual_dispatched_task_ids")))
            locality.extend(_ids(meta.get("locality_matched_task_ids")))
            order = _ids(meta.get("dispatch_order_applied"))
            if order:
                dispatch_orders.append(order)
            verified.extend(_ids(getattr(step.execution_result, "verified", ())))

            if step.goal_closed:
                stop_reason = "goal_closed"
                break
            if step.status in _TERMINAL_STATUSES:
                stop_reason = str(step.status.value)
                break
            if step.status is SupervisorStepStatus.NO_WORK:
                if idle_used >= idle_polls:
                    stop_reason = "no_admissible_work"
                    break
                idle_used += 1
                if idle_poll_seconds:
                    await asyncio.sleep(idle_poll_seconds)
        else:
            stop_reason = "max_epochs"
    finally:
        if not supervisor.terminal:
            supervisor.stop(reason=stop_reason or "kernel_loop_stop")

    closed = bool(steps) and steps[-1].goal_closed
    return KernelRunResult(
        schema_version=KERNEL_LOOP_SCHEMA_VERSION,
        goal_id=str(getattr(goal, "goal_id", goal)),
        goal_closed=closed,
        stop_reason=stop_reason or "no_steps",
        epochs_executed=len(steps),
        idle_polls_used=idle_used,
        dispatched_task_ids=tuple(dispatched),
        verified_task_ids=tuple(dict.fromkeys(verified)),
        dispatch_orders=tuple(dispatch_orders),
        locality_matched_task_ids=tuple(locality),
        parallelism_decisions=tuple(parallelism_decisions),
        steps=tuple(steps),
    )


def _decide_degree(
    agent_os: Any,
    goal: Any,
    *,
    conflict_graph: Any | None,
    epoch_id: int,
    ceiling: int | None,
) -> Any:
    """Choose this epoch's parallelism from observed state, or ``None``.

    Returns ``None`` when the observation itself is unavailable: holding the
    previous degree is safer than reacting to a state we could not read.
    """

    from .parallelism_policy import decide_parallelism

    try:
        state = agent_os.runtime_state(goal)
        graph = agent_os._coerce_adaptive_conflict_graph(goal, conflict_graph)
    except Exception:
        return None
    return decide_parallelism(
        state,
        graph,
        epoch_id=epoch_id,
        max_parallelism=max(1, int(ceiling or 1)),
    )


__all__ = [
    "KERNEL_LOOP_SCHEMA_VERSION",
    "KernelRunResult",
    "drive_goal_to_closure",
]
