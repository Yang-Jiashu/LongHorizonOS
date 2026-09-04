"""Opt-in adaptive AgentOS execution tests.

These tests intentionally verify only the bounded integration contract:
the policy selects an advisory task batch, while Scheduler/Kernel still own
claims, leases, eligibility, and resource admission.  The default execution
path must remain unchanged.
"""

from __future__ import annotations

import asyncio

from lhos.runtimes.multi_agent.events import SchedulerEventType
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    ConflictGraph,
    Goal,
    TaskAccessSet,
    VerificationOutcome,
)


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


def test_sync_adaptive_unknown_access_is_serial_and_replans_each_epoch() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-sync-unknown")
        goal.task("first", agent="worker", verify=lambda: _pass("first"))
        goal.task("second", agent="worker", verify=lambda: _pass("second"))

        result = os_.run(
            goal,
            max_dispatches=2,
            max_steps=4,
            adaptive=True,
        )

        assert result.goal_state == "closed"
        assert result.verified == ["first", "second"]
        assert result.meta["adaptive"] is True
        assert result.meta["adaptive_policy"] == "conflict-aware"
        epochs = result.meta["adaptive_epochs"]
        assert len(epochs) == 2
        assert [tuple(epoch["selected_task_ids"]) for epoch in epochs] == [
            ("first",),
            ("second",),
        ]
        assert [tuple(epoch["actual_dispatched_task_ids"]) for epoch in epochs] == [
            ("first",),
            ("second",),
        ]
        assert all(len(epoch["selected_task_ids"]) == 1 for epoch in epochs)
        assert all(epoch["fallback_dispatched_task_ids"] == () for epoch in epochs)
        audit_events = [
            event
            for event in os_.scheduler.events
            if event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
        ]
        assert len(audit_events) == len(epochs)
        for event, epoch in zip(audit_events, epochs, strict=True):
            assert event.decision_hash == epoch["decision_hash"]
            assert event.metadata["schema_version"] == "scheduling-epoch.v1"
            assert event.metadata["policy_id"] == "conflict-aware-parallelism.v1"
            assert tuple(event.metadata["selected_task_ids"]) == tuple(epoch["selected_task_ids"])
        # Journal-only policy records must not leave operational ownership
        # behind after the run has committed its work.
        assert all(
            getattr(claim.state, "value", claim.state) not in {"active", "acquiring"}
            for claim in os_.scheduler.claims
        )
    finally:
        os_.close()


def test_sync_adaptive_respects_explicit_parallelism_bound() -> None:
    """Synchronous adaptive execution can now propose an independent batch."""

    executed: list[str] = []

    def verify(task_id: str) -> VerificationOutcome:
        executed.append(task_id)
        return _pass(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-sync-parallel")
        goal.task(
            "left",
            agent="worker",
            inputs=("workspace://left",),
            outputs=("workspace://left",),
            verify=lambda: verify("left"),
        )
        goal.task(
            "right",
            agent="worker",
            inputs=("workspace://right",),
            outputs=("workspace://right",),
            verify=lambda: verify("right"),
        )
        graph = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(task_id="left", write_set=("workspace://left",)),
                TaskAccessSet(task_id="right", write_set=("workspace://right",)),
            ]
        )

        result = os_.run(
            goal,
            max_dispatches=2,
            max_steps=2,
            adaptive=True,
            conflict_graph=graph,
            max_parallelism=2,
        )

        assert result.goal_state == "closed"
        assert set(result.verified) == {"left", "right"}
        assert set(executed) == {"left", "right"}
        epochs = result.meta["adaptive_epochs"]
        assert epochs[0]["selected_task_ids"] == ("left", "right")
        assert epochs[0]["actual_dispatched_task_ids"] == ("left", "right")
    finally:
        os_.close()


def test_sync_adaptive_rejects_invalid_parallelism() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-sync-parallel-validation")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))
        import pytest

        with pytest.raises(ConfigurationError, match="max_parallelism must be >= 1"):
            os_.run(goal, adaptive=True, max_parallelism=0)
    finally:
        os_.close()


def test_adaptive_run_can_disable_durable_epoch_audits() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-no-audit")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=2,
            adaptive=True,
            persist_adaptive_epochs=False,
        )

        assert result.goal_state == "closed"
        assert result.meta["adaptive_epochs"]
        assert not any(
            event.event_type is SchedulerEventType.SCHEDULING_EPOCH_PLANNED
            for event in os_.scheduler.events
        )
    finally:
        os_.close()


def test_adaptive_epoch_records_explicit_compute_routing_advisory_only() -> None:
    """Routing metadata is audited without changing Scheduler selection."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-routing-audit")
        goal.task(
            "critical",
            agent="worker",
            metadata={
                "compute_routing": {
                    "criticality": 10,
                    "downstream_fanout": 8,
                    "failure_blast_radius": 9,
                    "input_stability": 0.8,
                    "context_budget_tokens": 2_000,
                }
            },
            verify=lambda: _pass("critical"),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=2,
            adaptive=True,
        )

        assert result.goal_state == "closed"
        epoch = result.meta["adaptive_epochs"][0]
        assert epoch["selected_task_ids"] == ("critical",)
        routing = epoch["compute_routing"]["critical"]
        assert routing["status"] == "ok"
        assert routing["task_id"] == "critical"
        assert routing["model_tier"] == "strong"
        assert routing["verification_strength"] == "strong"
        assert routing["context_budget_tokens"] == 2_000
        assert len(routing["decision_hash"]) == 64
        # The advisory must not claim or route a different Agent.
        assert os_.scheduler.active_claim_for_task("critical", os_._goal_gid[goal.goal_id]) is None
    finally:
        os_.close()


def test_adaptive_routing_metadata_is_fail_soft_and_bounded() -> None:
    """Malformed opt-in metadata is recorded, not allowed to break a run."""

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-routing-invalid")
        goal.task(
            "task",
            agent="worker",
            metadata={
                "compute_routing": {
                    "criticality": "not-an-integer",
                    "untrusted_prompt": "x" * 10000,
                }
            },
            verify=lambda: _pass("task"),
        )

        result = os_.run(goal, max_dispatches=1, max_steps=2, adaptive=True)

        assert result.goal_state == "closed"
        routing = result.meta["adaptive_epochs"][0]["compute_routing"]["task"]
        assert routing["status"] == "invalid"
        assert "ValidationError" in routing["reason"]
        assert len(routing["reason"]) <= 240
        assert "untrusted_prompt" not in str(routing)
    finally:
        os_.close()


async def test_async_adaptive_explicit_conflict_graph_limits_batch() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)
        await asyncio.sleep(0)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
                max_concurrency=2,
            )
        )
        goal = Goal("adaptive-async-conflict")
        goal.task("left", agent="worker", verify=lambda: _pass("left"))
        goal.task("right", agent="worker", verify=lambda: _pass("right"))
        goal.task("independent", agent="worker", verify=lambda: _pass("independent"))
        conflict_graph = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(task_id="left", write_set=("workspace://shared",)),
                TaskAccessSet(task_id="right", write_set=("workspace://shared",)),
                TaskAccessSet(task_id="independent", write_set=("workspace://other",)),
            ]
        )

        result = await os_.run_async(
            goal,
            max_dispatches=3,
            max_steps=4,
            max_concurrency=2,
            adaptive=True,
            conflict_graph=conflict_graph,
        )

        assert result.goal_state == "closed"
        assert set(result.verified) == {"left", "right", "independent"}
        assert set(executed) == {"left", "right", "independent"}
        assert result.meta["adaptive"] is True
        assert result.meta["adaptive_policy"] == "conflict-aware"
        epochs = result.meta["adaptive_epochs"]
        assert epochs
        # The first epoch may choose either lexical conflict member plus the
        # independent task, but it must never select both conflicting writers.
        first_selected = set(epochs[0]["selected_task_ids"])
        first_dispatched = set(epochs[0]["actual_dispatched_task_ids"])
        assert not {"left", "right"} <= first_selected
        assert len(first_selected) <= 2
        assert first_dispatched == first_selected
        assert epochs[0]["fallback_dispatched_task_ids"] == ()
    finally:
        os_.close()


def test_default_sync_run_meta_remains_unchanged() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-default-sync")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        result = os_.run(goal, max_dispatches=1)

        assert result.goal_state == "closed"
        assert "adaptive" not in result.meta
        assert "adaptive_epochs" not in result.meta
    finally:
        os_.close()


def test_sync_adaptive_falls_back_when_selected_task_has_no_eligible_agent() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-sync-fallback")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("a-ineligible"),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: _pass("b-eligible"),
        )

        result = os_.run(
            goal,
            max_dispatches=1,
            max_steps=2,
            adaptive=True,
        )

        assert result.verified == ["b-eligible"]
        assert result.task_states["a-ineligible"] == "unverified"
        epochs = result.meta["adaptive_epochs"]
        assert epochs[0]["selected_task_ids"] == ("a-ineligible",)
        assert epochs[0]["fallback_attempted"] is True
        assert epochs[0]["fallback_parallelism"] == 1
        assert epochs[0]["actual_dispatched_task_ids"] == ("b-eligible",)
        assert epochs[0]["fallback_dispatched_task_ids"] == ("b-eligible",)
    finally:
        os_.close()


async def test_default_async_run_meta_remains_unchanged() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("adaptive-default-async")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        assert result.goal_state == "closed"
        assert result.meta == {
            "execution_mode": "async",
            "dispatched": 1,
            "max_concurrency": 1,
        }
    finally:
        os_.close()


async def test_async_adaptive_falls_back_when_selected_task_has_no_eligible_agent() -> None:
    os_ = AgentOS(":memory:")
    try:

        async def execute(_task_id: str) -> None:
            await asyncio.sleep(0)

        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("adaptive-async-fallback")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("a-ineligible"),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: _pass("b-eligible"),
        )

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=2,
            max_concurrency=1,
            adaptive=True,
        )

        assert result.verified == ["b-eligible"]
        assert result.task_states["a-ineligible"] == "unverified"
        epochs = result.meta["adaptive_epochs"]
        assert epochs[0]["selected_task_ids"] == ("a-ineligible",)
        assert epochs[0]["fallback_attempted"] is True
        assert epochs[0]["fallback_parallelism"] == 1
        assert epochs[0]["actual_dispatched_task_ids"] == ("b-eligible",)
        assert epochs[0]["fallback_dispatched_task_ids"] == ("b-eligible",)
    finally:
        os_.close()


async def test_async_adaptive_audits_partial_scheduler_admission_skips() -> None:
    """Policy-selected tasks retain authoritative partial-admission reasons."""

    async def execute(_task_id: str) -> None:
        await asyncio.sleep(0)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
                max_concurrency=2,
            )
        )
        goal = Goal("adaptive-async-partial-admission")
        goal.task(
            "a-ineligible",
            agent="worker",
            required_specializations=("gpu",),
            verify=lambda: _pass("a-ineligible"),
            inputs=("workspace://a",),
            outputs=("workspace://a.out",),
        )
        goal.task(
            "b-eligible",
            agent="worker",
            required_specializations=("python",),
            verify=lambda: _pass("b-eligible"),
            inputs=("workspace://b",),
            outputs=("workspace://b.out",),
        )
        conflict_graph = ConflictGraph.from_access_sets(
            [
                TaskAccessSet(
                    task_id="a-ineligible",
                    read_set=("workspace://a",),
                    write_set=("workspace://a.out",),
                ),
                TaskAccessSet(
                    task_id="b-eligible",
                    read_set=("workspace://b",),
                    write_set=("workspace://b.out",),
                ),
            ]
        )

        result = await os_.run_async(
            goal,
            max_dispatches=2,
            max_steps=1,
            max_concurrency=2,
            adaptive=True,
            conflict_graph=conflict_graph,
        )

        assert result.verified == ["b-eligible"]
        epoch = result.meta["adaptive_epochs"][0]
        assert set(epoch["selected_task_ids"]) == {"a-ineligible", "b-eligible"}
        assert epoch["actual_dispatched_task_ids"] == ("b-eligible",)
        assert epoch["fallback_attempted"] is False
        skipped = tuple(epoch["scheduler_skipped"])
        assert any(
            task_id == "a-ineligible" and "no eligible agent" in reason
            for task_id, reason in skipped
        )
        assert epoch["scheduler_skipped_count"] == len(skipped)
        assert epoch["scheduler_skipped_truncated"] is False
    finally:
        os_.close()
