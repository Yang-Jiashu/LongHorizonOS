"""Public SDK resource configuration and persistence tests."""

from __future__ import annotations

import json

import pytest

from lhos.runtimes.multi_agent import ResourceVector
from lhos.sdk import Agent, AgentOS, ConfigurationError, Goal, Task


def test_agent_resource_capacity_reaches_registered_descriptor() -> None:
    capacity = {
        "cpu_millis": 4_000,
        "ram_bytes": 16_000,
        "gpu_count": 1,
        "vram_bytes": 8_000,
        "model_slots": {"review-model": 2},
    }
    os_ = AgentOS(":memory:")
    try:
        agent = os_.add_agent(Agent("worker", resource_capacity=capacity))

        descriptor = os_._registry.get("worker")
        assert descriptor is not None
        assert agent.resource_capacity == ResourceVector(**capacity)
        assert descriptor.resource_capacity == agent.resource_capacity
    finally:
        os_.close()


def test_task_resources_compile_into_scheduler_metadata() -> None:
    resources = ResourceVector(
        cpu_millis=750,
        ram_bytes=2_000,
        model_slots={"reasoner": 1},
    )
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("G")
        task = goal.task("T", agent="worker", resources=resources)

        graph_id = os_._compile_goal(goal)
        nodes, _ = os_.vpg.snapshot_projection(graph_id)

        assert task.resources == resources
        assert nodes["T"].metadata["scheduler"]["resources"] == resources.model_dump(mode="json")
    finally:
        os_.close()


def test_direct_task_accepts_resource_dict() -> None:
    task = Task("T", resources={"gpu_count": 2, "model_slots": {"vision": 1}})

    assert task.resources == ResourceVector(
        gpu_count=2,
        model_slots={"vision": 1},
    )


def test_legacy_scheduler_metadata_resources_are_preserved_and_validated() -> None:
    task = Task(
        "T",
        metadata={
            "scheduler": {
                "resources": {
                    "cpu_millis": 250,
                    "model_slots": {"legacy": 1},
                }
            }
        },
    )

    assert task.resources == ResourceVector(
        cpu_millis=250,
        model_slots={"legacy": 1},
    )


@pytest.mark.parametrize(
    ("factory", "value"),
    [
        (lambda value: Agent("worker", resource_capacity=value), {"cpu_millis": -1}),
        (lambda value: Agent("worker", resource_capacity=value), {"unknown": 1}),
        (lambda value: Task("T", resources=value), {"gpu_count": "1"}),
        (lambda value: Goal("G").task("T", resources=value), ["not", "a", "vector"]),
        (lambda value: Task("T", resources=value), {"model_slots": {"": 1}}),
    ],
)
def test_invalid_resource_configuration_fails_fast(factory, value) -> None:
    with pytest.raises(ConfigurationError):
        factory(value)


def test_scheduler_state_is_ephemeral_for_memory_and_durable_for_file(tmp_path) -> None:
    memory_runtime = AgentOS(":memory:")
    try:
        assert memory_runtime.scheduler._s._state_store is None
    finally:
        memory_runtime.close()

    db = tmp_path / "durable.sqlite"
    file_runtime = AgentOS(str(db))
    try:
        store = file_runtime.scheduler._s._state_store
        assert store is not None
        assert store.path == str(db)
    finally:
        file_runtime.close()


def test_manifest_round_trip_preserves_agent_and_task_resources(tmp_path) -> None:
    db = tmp_path / "state.sqlite"
    manifest = tmp_path / "run.json"
    round_trip_manifest = tmp_path / "run-round-trip.json"
    capacity = ResourceVector(cpu_millis=2_000, ram_bytes=8_000)
    request = ResourceVector(
        cpu_millis=500,
        ram_bytes=1_000,
        model_slots={"coder": 1},
    )

    os_ = AgentOS(str(db))
    try:
        os_.add_agent(Agent("worker", resource_capacity=capacity))
        goal = Goal("G")
        goal.task("T", agent="worker", resources=request)
        os_._compile_goal(goal)
        os_.save_run(str(manifest))
    finally:
        os_.close()

    saved = json.loads(manifest.read_text(encoding="utf-8"))
    assert saved["agents"][0]["resource_capacity"] == capacity.model_dump(mode="json")
    assert saved["goals"]["G"]["tasks"][0]["resources"] == request.model_dump(mode="json")

    reopened = AgentOS.open_run(str(manifest))
    try:
        assert len(reopened._registry) == 0
        assert reopened._agents["worker"].process_id is None
        assert reopened._agents["worker"].resource_capacity == capacity
        assert reopened._goals["G"].tasks[0].resources == request

        reopened.save_run(str(round_trip_manifest))
    finally:
        reopened.close()

    round_trip = json.loads(round_trip_manifest.read_text(encoding="utf-8"))
    assert round_trip["agents"][0]["resource_capacity"] == capacity.model_dump(mode="json")
    assert round_trip["goals"]["G"]["tasks"][0]["resources"] == request.model_dump(mode="json")
