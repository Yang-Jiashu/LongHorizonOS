"""Durable, journal-only integration for semantic-interrupt proposals."""

from __future__ import annotations

import pytest

from lhos.runtimes.multi_agent.durable_state import SchedulerStateStore
from lhos.runtimes.multi_agent.events import SchedulerEventType, record_event
from lhos.sdk import (
    AgentOS,
    ConfigurationError,
    Goal,
    SemanticInterrupt,
    SemanticInterruptKind,
)


def _compiled_goal(os_: AgentOS) -> Goal:
    goal = Goal("interrupt-persistence-goal")
    goal.task("task-a", agent="")
    os_._compile_goal(goal)
    return goal


def _interrupt(graph_id: str, graph_version: int) -> SemanticInterrupt:
    return SemanticInterrupt(
        interrupt_id="interrupt-1",
        graph_id=graph_id,
        graph_version=graph_version,
        kind=SemanticInterruptKind.TASK_VERIFIED,
        reason="observed verification",
        affected_task_ids=("task-a",),
    )


def test_persisted_interrupt_proposal_is_durable_and_idempotent(tmp_path) -> None:
    db = tmp_path / "interrupts.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        version = os_._vpg_surface.current_graph_version(gid)
        interrupt = _interrupt(gid, version)

        before = (
            tuple(claim.model_dump_json() for claim in os_.scheduler.claims),
            tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts),
        )
        epoch = os_.plan_interrupts(goal, [interrupt], epoch_id=2, persist=True)
        assert epoch.decisions[0].action.value == "defer"
        proposals = [
            event
            for event in os_.scheduler.events
            if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED
        ]
        assert len(proposals) == 1
        assert proposals[0].graph_id == gid
        assert proposals[0].decision_hash == epoch.decision_hash
        assert proposals[0].metadata["decisions"][0]["action"] == "defer"
        assert (
            tuple(claim.model_dump_json() for claim in os_.scheduler.claims),
            tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts),
        ) == before

        # The deterministic event id makes a retry an idempotent journal write.
        os_.plan_interrupts(goal, [interrupt], epoch_id=2, persist=True)
        assert (
            len(
                [
                    event
                    for event in os_.scheduler.events
                    if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED
                ]
            )
            == 1
        )
    finally:
        os_.close()

    store = SchedulerStateStore(db)
    try:
        state = store.load()
        persisted = [
            event
            for event in state.events
            if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED
        ]
        assert len(persisted) == 1
        assert persisted[0].decision_hash == epoch.decision_hash
    finally:
        store.close()


def test_read_only_agentos_cannot_persist_interrupt_proposal(tmp_path) -> None:
    db = tmp_path / "interrupts-readonly.sqlite"
    writer = AgentOS(str(db))
    try:
        goal = _compiled_goal(writer)
        gid = writer._gid_for(goal.goal_id)
        assert gid is not None
        version = writer._vpg_surface.current_graph_version(gid)
        interrupt = _interrupt(gid, version)
        # Persist the graph/manifest through the normal durable DB, then
        # exercise the explicit read-only guard on the same live object.
        writer._read_only = True
        with pytest.raises(ConfigurationError, match="cannot persist"):
            writer.plan_interrupts(goal, [interrupt], persist=True)
    finally:
        writer.close()


def test_scheduler_rejects_conflicting_external_event_identity(tmp_path) -> None:
    db = tmp_path / "interrupt-conflict.sqlite"
    os_ = AgentOS(str(db))
    try:
        goal = _compiled_goal(os_)
        gid = os_._gid_for(goal.goal_id)
        assert gid is not None
        version = os_._vpg_surface.current_graph_version(gid)
        os_.plan_interrupts(
            goal,
            [_interrupt(gid, version)],
            epoch_id=3,
            persist=True,
        )
        proposal = next(
            event
            for event in os_.scheduler.events
            if event.event_type is SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED
        )
        conflict = proposal.model_copy(update={"decision_hash": "f" * 64})
        with pytest.raises(ValueError, match="conflicting scheduler event id"):
            os_.scheduler.record_event(conflict)
        assert sum(event.event_id == proposal.event_id for event in os_.scheduler.events) == 1
    finally:
        os_.close()


def test_external_scheduler_audit_hook_rejects_lifecycle_event(tmp_path) -> None:
    db = tmp_path / "interrupt-event-boundary.sqlite"
    os_ = AgentOS(str(db))
    try:
        with pytest.raises(ValueError, match="only accepts"):
            os_.scheduler.record_event(
                record_event(
                    SchedulerEventType.CLAIM_COMPLETED,
                    event_id="forged-claim-completed",
                    graph_id="graph",
                    task_id="task",
                )
            )
        assert not any(event.event_id == "forged-claim-completed" for event in os_.scheduler.events)
    finally:
        os_.close()
