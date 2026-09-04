"""Read-only GlobalRuntimeState projection tests."""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from lhos.runtimes.multi_agent import (
    AgentSnapshot,
    AttemptState,
    ClaimState,
    ComputationCost,
    ContextIdentity,
    ResourceBinding,
    ResourceReservation,
    ResourceVector,
    TaskClaim,
)
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    GlobalRuntimeState,
    Goal,
    RuntimeStateView,
    build_runtime_state_view,
)


def _active_runtime(*, with_context: bool = False) -> tuple[AgentOS, Goal, str]:
    os_ = AgentOS(":memory:")
    os_.add_agent(
        Agent(
            "worker",
            resource_capacity={
                "cpu_millis": 4_000,
                "ram_bytes": 16_000,
                "gpu_count": 1,
                "vram_bytes": 8_000,
                "model_slots": {"reasoner": 2},
            },
        )
    )
    goal = Goal("runtime-state-goal")
    goal.task(
        "implement",
        agent="worker",
        resources={
            "cpu_millis": 1_000,
            "ram_bytes": 4_000,
            "gpu_count": 1,
            "vram_bytes": 2_000,
            "model_slots": {"reasoner": 1},
        },
    )
    graph_id = os_._compile_goal(goal)
    scheduled = os_.scheduler.schedule_once(graph_id, max_claims=1)
    assert [item["task_id"] for item in scheduled.dispatched] == ["implement"]
    if with_context:
        claim = os_.scheduler.claims[0]
        assert os_.scheduler.bind_attempt_context_snapshot(
            claim.claim_id,
            snapshot_id="context-snapshot-1",
            manifest_id="manifest-1",
            manifest_hash="1" * 64,
            working_set_hash="2" * 64,
            materialized_hash="3" * 64,
        )
        assert os_.scheduler.bind_attempt_provenance(claim.claim_id, "4" * 64)
    return os_, goal, graph_id


def _state_fingerprint(os_: AgentOS, graph_id: str) -> dict[str, object]:
    manager = os_.scheduler.resource_manager
    return {
        "graph_version": os_.vpg.get_graph(graph_id).current_version,
        "graph_events": tuple(
            event.model_dump_json() for event in os_.vpg.store.get_events(graph_id)
        ),
        "claims": tuple(claim.model_dump_json() for claim in os_.scheduler.claims),
        "attempts": tuple(attempt.model_dump_json() for attempt in os_.scheduler.attempts),
        "scheduler_events": tuple(event.model_dump_json() for event in os_.scheduler.events),
        "reservations": tuple(
            reservation.model_dump_json() for reservation in manager.list_active()
        ),
    }


def test_uncompiled_goal_fails_without_mutating_runtime() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("not-compiled")
        before_goals = dict(os_._goals)
        before_mappings = dict(os_._goal_gid)
        before_processes = tuple(os_.kernel._process_service.list_all())

        with pytest.raises(ConfigurationError, match="not compiled"):
            build_runtime_state_view(os_, goal)

        assert os_._goals == before_goals
        assert os_._goal_gid == before_mappings
        assert tuple(os_.kernel._process_service.list_all()) == before_processes
    finally:
        os_.close()


def test_recent_events_are_bounded_deterministic_and_payload_redacted() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("event-tail-goal")
        previous = None
        for index in range(24):
            previous = goal.task(
                f"task-{index:02d}",
                agent="worker",
                depends_on=() if previous is None else (previous,),
            )
        graph_id = os_._compile_goal(goal)

        graph = os_.vpg.get_graph(graph_id)

        state = build_runtime_state_view(os_, goal)
        assert 0 < len(state.recent_events) <= 32
        raw_tail = os_.vpg.store.get_recent_events(
            graph_id,
            through_version=graph.current_version,
            limit=32,
        )
        assert tuple(item.event_id for item in state.recent_events) == tuple(
            event.event_id for event in raw_tail
        )
        serialized = state.model_dump_json()
        assert all(
            str(event.payload).strip("{}") not in serialized for event in raw_tail if event.payload
        )
        assert all(len(item.payload_hash) == 64 for item in state.recent_events)
    finally:
        os_.close()


def test_projection_is_deterministic_frozen_and_side_effect_free() -> None:
    os_, goal, graph_id = _active_runtime()
    try:
        before = _state_fingerprint(os_, graph_id)

        first = build_runtime_state_view(os_, goal)
        second = build_runtime_state_view(os_, goal.goal_id)

        assert isinstance(first, GlobalRuntimeState)
        assert isinstance(first, RuntimeStateView)
        assert first == second
        assert first.as_dict() == second.as_dict()
        assert _state_fingerprint(os_, graph_id) == before
        with pytest.raises(ValidationError):
            first.progress.graph_version = 999
        with pytest.raises(AttributeError):
            first.progress.ready_frontier.append("other")  # type: ignore[attr-defined]
    finally:
        os_.close()


def test_agent_os_runtime_state_is_read_only_thin_wrapper() -> None:
    os_, goal, graph_id = _active_runtime()
    try:
        before = _state_fingerprint(os_, graph_id)
        projected = os_.runtime_state(goal)
        assert isinstance(projected, GlobalRuntimeState)
        assert projected.graph_id == graph_id
        assert _state_fingerprint(os_, graph_id) == before
    finally:
        os_.close()


def test_projection_reports_progress_and_explicitly_unavailable_attempt_fields() -> None:
    os_, goal, graph_id = _active_runtime()
    try:
        state = build_runtime_state_view(os_, goal)

        assert state.graph_id == graph_id
        assert state.progress.graph_version == os_.vpg.get_graph(graph_id).current_version
        assert state.progress.ready_frontier == ("implement",)
        assert state.progress.repair_ready_frontier == ()
        assert state.progress.unverified_task_ids == ("implement",)
        assert not state.progress.goal_closed

        assert state.agent_cognition.available
        attempt = state.agent_cognition.current_attempts[0]
        assert attempt.task_id == "implement"
        assert attempt.agent_id == "worker"
        assert attempt.attempt_state == "dispatched"
        assert attempt.semantic_epoch == os_.scheduler.attempts[0].semantic_epoch
        assert {item.name for item in attempt.unavailable} == {
            "context_snapshot_id",
            "cost",
            "progress",
            "provenance_digest",
            "read_set",
            "write_set",
        }
        assert not state.context.available
        assert state.context.reason == ("no current Scheduler attempt has a bound ContextSnapshot")
    finally:
        os_.close()


def test_projection_reports_context_snapshot_cognition_and_logical_resources() -> None:
    os_, goal, _ = _active_runtime(with_context=True)
    try:
        raw_attempt = os_.scheduler.attempts[0]
        identity = ContextIdentity.from_attempt(raw_attempt)
        assert identity is not None
        snapshot = AgentSnapshot(
            agent_id=raw_attempt.agent_id,
            process_id=raw_attempt.process_id,
            task_id=raw_attempt.task_id,
            claim_id=raw_attempt.claim_id,
            attempt_id=raw_attempt.attempt_id,
            graph_id=raw_attempt.graph_id,
            graph_version=raw_attempt.graph_version,
            semantic_epoch=raw_attempt.semantic_epoch,
            context_identity=identity,
            read_set=(
                ResourceBinding(
                    operation="read",
                    resource_uri="vpg://requirements",
                    artifact_id="requirements",
                    version=8,
                    content_hash="5" * 64,
                    source="context_vm",
                ),
            ),
            write_set=(
                ResourceBinding(
                    operation="write",
                    resource_uri="workspace://payment.py",
                    source="tool_runtime",
                ),
            ),
            progress=0.72,
            cost=ComputationCost(
                input_tokens=32_000,
                output_tokens=1_500,
                tool_calls=3,
                elapsed_ms=420_000,
            ),
            started_at=raw_attempt.started_at,
            captured_at=raw_attempt.started_at + timedelta(seconds=1),
            state=AttemptState.DISPATCHED,
        )
        assert os_.scheduler.bind_agent_snapshot(raw_attempt.claim_id, snapshot)

        state = build_runtime_state_view(os_, goal)

        cognition = state.agent_cognition.current_attempts[0]
        assert cognition.provenance_digest == "4" * 64
        assert cognition.context_snapshot_id == "context-snapshot-1"
        assert cognition.progress == pytest.approx(0.72)
        assert cognition.cost is not None
        assert cognition.cost.input_tokens == 32_000
        assert cognition.read_set[0].artifact_id == "requirements"
        assert cognition.read_set[0].version == 8
        assert cognition.write_set[0].resource_uri == "workspace://payment.py"
        assert cognition.unavailable == ()

        assert state.context.available
        binding = state.context.bindings[0]
        assert binding.snapshot_id == "context-snapshot-1"
        assert binding.manifest_id == "manifest-1"
        assert binding.summary is None
        assert binding.unavailable[0].name == "summary"

        assert state.resources.available
        assert state.resources.scope == "scheduler_logical_resources"
        pool = state.resources.pools[0]
        assert pool.pool_id == "worker"
        assert pool.capacity is not None
        assert pool.available is not None
        assert pool.capacity.cpu_millis == 4_000
        assert pool.reserved.cpu_millis == 1_000
        assert pool.available.cpu_millis == 3_000
        assert pool.available.ram_bytes == 12_000
        assert pool.available.gpu_count == 0
        assert pool.available.vram_bytes == 6_000
        assert [(slot.name, slot.quantity) for slot in pool.available.model_slots] == [
            ("reasoner", 1)
        ]
        assert pool.active_claim_ids == (cognition.claim_id,)
        assert state.resources.active_claims[0].reservation_id in pool.reservation_ids
    finally:
        os_.close()


def test_read_only_reopen_marks_nonrestored_runtime_layers_unavailable(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    manifest = tmp_path / "run.json"
    os_ = AgentOS(str(db))
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("persisted-goal")
        goal.task("task", agent="worker")
        graph_id = os_._compile_goal(goal)
        os_.save_run(str(manifest))
    finally:
        os_.close()

    reopened = AgentOS.open_run(str(manifest))
    try:
        state = build_runtime_state_view(reopened, "persisted-goal")

        assert state.graph_id == graph_id
        assert state.progress.ready_frontier == ("task",)
        assert not state.agent_cognition.available
        assert state.agent_cognition.reason == (
            "scheduler durable state is not loaded by read-only AgentOS"
        )
        assert not state.context.available
        assert state.context.reason == "Context VM is not wired"
        assert not state.resources.available
        assert state.resources.reason == (
            "scheduler durable state is not loaded by read-only AgentOS"
        )
    finally:
        reopened.close()


def test_projection_retries_when_graph_advances_during_snapshot_read(monkeypatch) -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("racing-goal")
        goal.task("task", agent="")
        graph_id = os_._compile_goal(goal)
        original_get_graph = os_.vpg.get_graph
        calls = 0

        def get_graph_with_one_race(candidate_graph_id: str):
            nonlocal calls
            calls += 1
            graph = original_get_graph(candidate_graph_id)
            if calls == 2:
                return graph.model_copy(update={"current_version": graph.current_version + 1})
            return graph

        monkeypatch.setattr(os_.vpg, "get_graph", get_graph_with_one_race)

        state = build_runtime_state_view(os_, goal)

        assert calls == 4
        assert state.graph_id == graph_id
        assert state.progress.graph_version == original_get_graph(graph_id).current_version
    finally:
        os_.close()


def test_resources_are_explicitly_global_across_goals() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                max_concurrency=2,
                resource_capacity={"cpu_millis": 2_000},
            )
        )
        first = Goal("first-goal")
        first.task(
            "first-task",
            agent="worker",
            resources={"cpu_millis": 1_000},
        )
        second = Goal("second-goal")
        second.task(
            "second-task",
            agent="worker",
            resources={"cpu_millis": 500},
        )
        first_graph = os_._compile_goal(first)
        second_graph = os_._compile_goal(second)
        assert os_.scheduler.schedule_once(first_graph, max_claims=1).dispatched
        assert os_.scheduler.schedule_once(second_graph, max_claims=1).dispatched

        state = build_runtime_state_view(os_, first)

        assert state.resources.scope_id == "global"
        assert {claim.task_id for claim in state.resources.active_claims} == {
            "first-task",
            "second-task",
        }
        pool = next(pool for pool in state.resources.pools if pool.pool_id == "worker")
        assert pool.reserved.cpu_millis == 1_500
        assert pool.available is not None
        assert pool.available.cpu_millis == 500
        assert set(pool.active_claim_ids) == {
            claim.claim_id for claim in state.resources.active_claims
        }
        assert [attempt.task_id for attempt in state.agent_cognition.current_attempts] == [
            "first-task"
        ]
    finally:
        os_.close()


def test_resource_overcommit_and_context_lookup_failure_are_explicit() -> None:
    os_, goal, _ = _active_runtime(with_context=True)
    try:

        class BrokenContextLookup:
            @staticmethod
            def get_snapshot(_snapshot_id: str):
                raise RuntimeError("lookup unavailable")

        os_._context_service = BrokenContextLookup()
        manager = os_.scheduler.resource_manager
        with manager._lock:
            manager._capacities["worker"] = ResourceVector(cpu_millis=500)

        state = build_runtime_state_view(os_, goal)

        assert state.context.available
        assert state.context.bindings[0].summary is None
        assert state.context.bindings[0].unavailable[0].reason == (
            "Context VM snapshot lookup failed: RuntimeError"
        )
        assert not state.resources.available
        assert state.resources.reason == (
            "one or more Scheduler resource pools are inconsistent or unavailable"
        )
        pool = next(pool for pool in state.resources.pools if pool.pool_id == "worker")
        assert pool.available is None
        assert pool.unavailable[0].name == "available"
        assert "cpu_millis=500" in pool.unavailable[0].reason
    finally:
        os_.close()


def test_goal_mapping_must_point_to_a_graph_that_owns_the_goal() -> None:
    os_ = AgentOS(":memory:")
    try:
        first = Goal("first-goal")
        first.task("first-task", agent="")
        second = Goal("second-goal")
        second.task("second-task", agent="")
        first_graph = os_._compile_goal(first)
        second_graph = os_._compile_goal(second)
        before_first = os_.vpg.get_graph(first_graph).current_version
        before_second = os_.vpg.get_graph(second_graph).current_version
        os_._goal_gid[first.goal_id] = second_graph

        with pytest.raises(ConfigurationError, match="not owned by graph"):
            build_runtime_state_view(os_, first)

        assert os_.vpg.get_graph(first_graph).current_version == before_first
        assert os_.vpg.get_graph(second_graph).current_version == before_second
    finally:
        os_.close()


def test_claim_without_attempt_preserves_claim_graph_version() -> None:
    os_ = AgentOS(":memory:")
    try:
        agent = os_.add_agent(Agent("worker"))
        goal = Goal("claim-only-goal")
        goal.task("task", agent="worker")
        graph_id = os_._compile_goal(goal)
        graph_version = os_.vpg.get_graph(graph_id).current_version
        claim = TaskClaim(
            graph_id=graph_id,
            graph_version=graph_version,
            task_id="task",
            agent_id="worker",
            process_id=agent.process_id or "",
            lease_resource=f"vpg://{graph_id}/task/task/claim",
            state=ClaimState.PROPOSED,
        )
        core = os_.scheduler._s
        with core._schedule_lock:
            core._claims.append(claim)

        state = build_runtime_state_view(os_, goal)

        cognition = state.agent_cognition.current_attempts[0]
        assert cognition.graph_version == graph_version
        assert cognition.attempt_id is None
        assert "graph_version" not in {item.name for item in cognition.unavailable}
        assert {
            "attempt_id",
            "attempt_state",
            "semantic_epoch",
            "provenance_digest",
            "context_snapshot_id",
            "read_set",
            "write_set",
            "progress",
            "cost",
        } == {item.name for item in cognition.unavailable}
    finally:
        os_.close()


def test_missing_resource_manager_and_pending_restore_are_unavailable() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("resource-state-goal")
        goal.task("task", agent="")
        os_._compile_goal(goal)
        core = os_.scheduler._s
        manager = core._resource_manager
        core._resource_manager = None
        try:
            missing = build_runtime_state_view(os_, goal)
        finally:
            core._resource_manager = manager

        assert not missing.resources.available
        assert missing.resources.reason == "Scheduler exposes no logical resource manager"

        pending = ResourceReservation(
            reservation_id="pending-reservation",
            pool_id="worker-not-registered",
            owner_id="pending-claim",
            resources=ResourceVector(cpu_millis=500),
        )
        with core._schedule_lock:
            core._pending_durable_reservations = [pending]

        restoring = build_runtime_state_view(os_, goal)

        assert not restoring.resources.available
        assert restoring.resources.reason == (
            "durable resource reservations are pending capacity restoration"
        )
        pool = next(
            pool for pool in restoring.resources.pools if pool.pool_id == "worker-not-registered"
        )
        assert pool.pending_reservation_ids == ("pending-reservation",)
        assert pool.available is None
    finally:
        os_.close()


def test_partial_context_identity_is_not_published_as_a_binding() -> None:
    os_, goal, _ = _active_runtime()
    try:
        claim = os_.scheduler.claims[0]
        core = os_.scheduler._s
        with core._schedule_lock:
            attempt = core.get_attempt_for_claim(claim.claim_id)
            assert attempt is not None
            attempt.context_snapshot_id = "partial-context"

        state = build_runtime_state_view(os_, goal)

        assert not state.context.available
        assert state.context.reason == (
            "one or more current ContextSnapshot bindings are incomplete"
        )
        assert state.context.bindings == ()
        unavailable = state.context.unavailable[0]
        assert unavailable.name.endswith(".context_identity")
        assert "manifest_id" in unavailable.reason
        assert "materialized_hash" in unavailable.reason
    finally:
        os_.close()
