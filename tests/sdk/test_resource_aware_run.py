"""Bounded SDK integration tests for the opt-in resource-aware run path."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from lhos.sdk import Agent, AgentOS, ConfigurationError, ConflictGraph, Goal, TaskAccessSet
from lhos.sdk.os import _bounded_resource_aware_epoch_audit
from lhos.sdk.verification import VerificationOutcome


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=f"{artifact_id}-v1",
    )


def _graph() -> ConflictGraph:
    return ConflictGraph.from_access_sets(
        [
            TaskAccessSet(task_id="left", write_set=("workspace://left",)),
            TaskAccessSet(task_id="right", write_set=("workspace://right",)),
        ]
    )


def test_sync_resource_aware_run_uses_task_vectors_as_advisory_filter() -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=2,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("resource-aware-sync")
        goal.task(
            "left",
            agent="worker",
            resources={"cpu_millis": 500},
            verify=lambda: _pass("left"),
        )
        goal.task(
            "right",
            agent="worker",
            resources={"cpu_millis": 500},
            verify=lambda: _pass("right"),
        )

        result = runtime.run(
            goal,
            max_dispatches=2,
            max_steps=2,
            adaptive=True,
            resource_aware=True,
            conflict_graph=_graph(),
            max_parallelism=2,
        )

        assert result.goal_state == "closed"
        assert set(result.verified) == {"left", "right"}
        assert result.meta["resource_aware"] is True
        assert result.meta["adaptive_policy"] == "resource-aware-conflict"
        assert result.meta["adaptive_epochs"]
        first_epoch = result.meta["adaptive_epochs"][0]
        assert set(first_epoch["selected_task_ids"]) == {
            "left",
            "right",
        }
        audit = first_epoch["resource_audit"]
        assert audit["schema_version"] == "resource-aware-run-audit.v1"
        assert audit["source_schema_version"] == "resource-aware-parallelism.v1"
        assert audit["assignment_count"] == 2
        assert {(item["task_id"], item["pool_id"]) for item in audit["assignments"]} == {
            ("left", "worker"),
            ("right", "worker"),
        }
        assert {
            (item["task_id"], item["action"], item["reason"]) for item in audit["decisions"]
        } == {
            ("left", "run", "independent"),
            ("right", "run", "independent"),
        }
        assert audit["pool_ids"] == ("worker",)
        assert audit["safe_under_declared_resources"] is True
        assert audit["safe_under_constraints"] is True
        assert audit["unavailable"] == ()
        assert audit["truncated"] is False
    finally:
        runtime.close()


@pytest.mark.asyncio
async def test_async_resource_aware_run_respects_logical_capacity() -> None:
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)
        await asyncio.sleep(0)

    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
                max_concurrency=2,
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("resource-aware-async")
        goal.task(
            "left",
            agent="worker",
            resources={"cpu_millis": 700},
            verify=lambda: _pass("left"),
        )
        goal.task(
            "right",
            agent="worker",
            resources={"cpu_millis": 700},
            verify=lambda: _pass("right"),
        )

        result = await runtime.run_async(
            goal,
            max_dispatches=2,
            max_steps=3,
            max_concurrency=2,
            adaptive=True,
            resource_aware=True,
            conflict_graph=_graph(),
            max_parallelism=2,
        )

        assert result.goal_state == "closed"
        assert set(executed) == {"left", "right"}
        assert result.meta["resource_aware"] is True
        assert result.meta["adaptive_policy"] == "resource-aware-conflict"
        # 700 + 700 exceeds the declared logical pool, so the policy must
        # select at most one task per epoch; the second task is replanned.
        assert all(len(epoch["selected_task_ids"]) <= 1 for epoch in result.meta["adaptive_epochs"])
        first_audit = result.meta["adaptive_epochs"][0]["resource_audit"]
        assert first_audit["schema_version"] == "resource-aware-run-audit.v1"
        assert first_audit["assignment_count"] == 1
        assert first_audit["assignments"][0]["task_id"] == "left"
        assert first_audit["assignments"][0]["pool_id"] == "worker"
        decisions = {item["task_id"]: item for item in first_audit["decisions"]}
        assert decisions["left"]["action"] == "run"
        assert decisions["left"]["reason"] == "independent"
        assert decisions["right"]["action"] == "defer"
        assert decisions["right"]["reason"] == "insufficient_resources"
        assert "worker:cpu_millis=400" in decisions["right"]["blockers"]
        assert first_audit["pool_ids"] == ("worker",)
        assert first_audit["safe_under_declared_resources"] is True
        assert first_audit["safe_under_constraints"] is True
        assert first_audit["unavailable"] == ()
        assert first_audit["truncated"] is False
    finally:
        runtime.close()


@pytest.mark.parametrize("runner", ["sync", "async"])
def test_resource_aware_requires_adaptive(runner: str) -> None:
    runtime = AgentOS(":memory:")
    try:
        runtime.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal(f"resource-aware-validation-{runner}")
        goal.task("task", agent="worker", verify=lambda: _pass("task"))
        if runner == "sync":
            with pytest.raises(ConfigurationError, match="resource_aware requires"):
                runtime.run(goal, resource_aware=True)
        else:
            with pytest.raises(ConfigurationError, match="resource_aware requires"):
                import asyncio as _asyncio

                _asyncio.run(runtime.run_async(goal, resource_aware=True))
    finally:
        runtime.close()


def test_resource_aware_epoch_run_audit_is_bounded_and_payload_free() -> None:
    long_identifier = "x" * 500
    vector = SimpleNamespace(
        cpu_millis=1,
        ram_bytes=2,
        gpu_count=3,
        vram_bytes=4,
        model_slots=tuple(
            SimpleNamespace(
                name=f"slot-{index:03d}-{long_identifier}",
                quantity=index + 1,
            )
            for index in range(9)
        ),
    )
    assignments = tuple(
        SimpleNamespace(
            task_id=f"assignment-{index:03d}-{long_identifier}",
            pool_id=f"assignment-pool-{index:03d}-{long_identifier}",
            resources=vector,
            prompt="DO_NOT_COPY" * 1_000,
        )
        for index in range(33)
    )
    decisions = tuple(
        SimpleNamespace(
            task_id=f"decision-{index:03d}-{long_identifier}",
            action=SimpleNamespace(value="defer"),
            reason="r" * 500,
            pool_id=f"decision-pool-{index:03d}-{long_identifier}",
            blockers=tuple(
                f"blocker-{blocker_index:03d}-{'b' * 500}" for blocker_index in range(9)
            ),
            context="DO_NOT_COPY" * 1_000,
        )
        for index in range(65)
    )
    unavailable = tuple(
        SimpleNamespace(
            name=f"unavailable-{index:03d}-{long_identifier}",
            reason="u" * 500,
            payload="DO_NOT_COPY" * 1_000,
        )
        for index in range(33)
    )
    epoch = SimpleNamespace(
        schema_version="resource-aware-parallelism.v1",
        policy_id="resource-aware-conflict-greedy.v1",
        conflict_graph_hash="a" * 64,
        parallelism_hint=33,
        safe_under_declared_resources=True,
        safe_under_constraints=False,
        assignments=assignments,
        decisions=decisions,
        unavailable=unavailable,
        context_manifest="DO_NOT_COPY" * 1_000,
    )

    audit = _bounded_resource_aware_epoch_audit(epoch)

    assert audit["assignment_count"] == 33
    assert len(audit["assignments"]) == 32
    assert audit["assignments_truncated"] is True
    assert audit["decision_count"] == 65
    assert len(audit["decisions"]) == 64
    assert audit["decisions_truncated"] is True
    assert audit["unavailable_count"] == 33
    assert len(audit["unavailable"]) == 32
    assert audit["unavailable_truncated"] is True
    assert audit["pool_id_count"] == 98
    assert len(audit["pool_ids"]) == 32
    assert audit["pool_ids_truncated"] is True
    assert audit["truncated"] is True

    first_assignment = audit["assignments"][0]
    assert len(first_assignment["task_id"]) == 160
    assert len(first_assignment["pool_id"]) == 160
    resources = first_assignment["resources"]
    assert resources["model_slot_count"] == 9
    assert len(resources["model_slots"]) == 4
    assert resources["model_slots_truncated"] is True

    first_decision = audit["decisions"][0]
    assert len(first_decision["reason"]) == 240
    assert first_decision["blocker_count"] == 9
    assert len(first_decision["blockers"]) == 4
    assert first_decision["blockers_truncated"] is True
    assert all(len(item) <= 240 for item in first_decision["blockers"])
    assert all(len(item["reason"]) <= 240 for item in audit["unavailable"])

    encoded = json.dumps(audit, ensure_ascii=True, sort_keys=True)
    assert len(encoded) < 200_000
    assert "DO_NOT_COPY" not in encoded
