"""Regression tests for the SDK execution/verification closure."""

from __future__ import annotations

import gc
import warnings

import pytest

from lhos.runtimes.multi_agent.lease_adapter import claim_resource_uri
from lhos.sdk import (
    Agent,
    AgentOS,
    ConfigurationError,
    Goal,
    VerificationError,
    VerificationOutcome,
)


def _pass(artifact_id: str, version: int = 1) -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=version,
        content="ok",
    )


def _evidence_ids(
    graph_id: str,
    task_id: str,
    version: int,
    attempt_number: int = 0,
) -> tuple[str, str, str, str]:
    suffix = "" if attempt_number == 0 else f"-a{attempt_number}"
    identity = f"{graph_id}-{task_id}-{version}{suffix}"
    return (
        f"AR-{identity}",
        f"V-{identity}",
        f"E-{identity}",
        f"sdk-act-{identity}",
    )


def test_dispatch_budget_limits_claims_and_next_run_finishes_tail():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                specializations=("python",),
                max_concurrency=20,
            )
        )
        goal = Goal("G")
        for index in range(10):
            goal.task(
                f"T{index}",
                agent="worker",
                verify=lambda index=index: _pass(f"a{index}"),
            )

        first = os_.run(goal, max_dispatches=8)
        assert len(first.verified) == 8
        assert len(os_.scheduler.claims) == 8
        assert not any(claim.state.value == "active" for claim in os_.scheduler.claims)

        second = os_.run(goal, max_dispatches=8)
        assert set(second.verified) == {f"T{index}" for index in range(10)}
        assert len(os_.scheduler.claims) == 10
        assert not any(claim.state.value == "active" for claim in os_.scheduler.claims)
    finally:
        os_.close()


def test_agent_executor_runs_before_independent_verifier():
    calls: list[tuple[str, str]] = []

    def execute(task_id: str) -> None:
        calls.append(("execute", task_id))

    def verify() -> VerificationOutcome:
        calls.append(("verify", "T"))
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)

        result = os_.run(goal, max_dispatches=1)

        assert calls == [("execute", "T"), ("verify", "T")]
        assert result.verified == ["T"]
    finally:
        os_.close()


def test_executor_outcome_is_legacy_verifier_when_task_verify_missing():
    calls: list[str] = []

    def execute(task_id: str) -> VerificationOutcome:
        calls.append(task_id)
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker")

        result = os_.run(goal, max_dispatches=1)

        assert calls == ["T"]
        assert result.verified == ["T"]
    finally:
        os_.close()


def test_attempt_is_running_before_executor_and_success_becomes_verified():
    observed_states: list[str] = []
    os_ = AgentOS(":memory:")

    def execute(_task_id: str) -> None:
        observed_states.append(os_.scheduler.attempts[-1].state.value)

    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        os_.run(goal, max_dispatches=1)

        assert observed_states == ["running"]
        assert os_.scheduler.attempts[-1].state.value == "verified_semantically"
        assert any(event.event_type.value == "execution_started" for event in os_.scheduler.events)
    finally:
        os_.close()


def test_executor_failure_releases_exact_claim_and_kernel_lease():
    def execute(_task_id: str) -> None:
        raise RuntimeError("boom")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=execute,
                specializations=("python",),
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        result = os_.run(goal, max_dispatches=1)
        gid = os_._gid_for("G")

        assert result.task_states["T"] == "unverified"
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.attempts[-1].state.value == "failed"
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_evidence_attachment_failure_releases_claim_and_kernel_lease(monkeypatch):
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        def fail_attach(*_args, **_kwargs) -> None:
            raise RuntimeError("patch failed")

        monkeypatch.setattr(os_, "_attach_evidence", fail_attach)

        with pytest.raises(VerificationError, match="failed to attach Evidence"):
            os_.run(goal, max_dispatches=1)
        gid = os_._gid_for("G")

        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.attempts[-1].state.value == "failed"
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_evidence_nodes_commit_atomically_in_one_graph_version():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))
        gid = os_._compile_goal(goal)
        before = os_.vpg.get_graph(gid).current_version

        result = os_.run(goal, max_dispatches=1)
        after = os_.vpg.get_graph(gid).current_version

        assert result.verified == ["T"]
        assert after == before + 1
        nodes, _ = os_.vpg.snapshot_projection(gid)
        artref_id, verification_id, evidence_id, _ = _evidence_ids(gid, "T", 1)
        assert {artref_id, verification_id, evidence_id} <= set(nodes)
    finally:
        os_.close()


def test_same_task_id_is_isolated_across_goal_graphs_and_projection_rebuild():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        first_goal = Goal("G1")
        first_goal.task("T", agent="worker", verify=lambda: _pass("first"))
        second_goal = Goal("G2")
        second_goal.task("T", agent="worker", verify=lambda: _pass("second"))

        assert os_.run(first_goal, max_dispatches=1).verified == ["T"]
        first_gid = os_._gid_for("G1")
        assert first_gid is not None
        assert os_.run(second_goal, max_dispatches=1).verified == ["T"]
        second_gid = os_._gid_for("G2")
        assert second_gid is not None

        first_ids = _evidence_ids(first_gid, "T", 1)
        second_ids = _evidence_ids(second_gid, "T", 1)
        assert set(first_ids).isdisjoint(second_ids)
        assert os_._facts.get_action(first_ids[-1]) is not None
        assert os_._facts.get_action(second_ids[-1]) is not None

        first_nodes, _ = os_.vpg.snapshot_projection(first_gid)
        second_nodes, _ = os_.vpg.snapshot_projection(second_gid)
        assert {"G1", "T", *first_ids[:3]} <= set(first_nodes)
        assert {"G2", "T", *second_ids[:3]} <= set(second_nodes)
        assert os_.result(first_gid).verified == ["T"]
        assert os_.result(second_gid).verified == ["T"]

        # Rebuilding one graph must delete/replay only that graph's rows even
        # though both projections intentionally contain a task named ``T``.
        os_.vpg.rebuild_projection(first_gid)
        assert os_.result(first_gid).verified == ["T"]
        assert os_.result(second_gid).verified == ["T"]
        assert os_.vpg.inspect_node(second_gid, "T") is not None
    finally:
        os_.close()


def test_stale_verifier_outcome_cannot_be_promoted_to_latest_artifact_version():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task(
            "T",
            agent="worker",
            verify=lambda: VerificationOutcome(
                passed=True,
                artifact_id="artifact",
                version=1,
                content="verified-v1",
            ),
        )

        initial = os_.run(goal, max_dispatches=1)
        assert initial.goal_state == "closed"

        os_._facts.add_version("artifact", 2, "unverified-v2")
        os_.repair(goal, artifact_id="artifact", new_artifact_version=2)
        repaired = os_.run(goal, max_dispatches=1)

        gid = os_._gid_for("G")
        nodes, _ = os_.vpg.snapshot_projection(gid)
        evidence_versions = {
            binding.version
            for node in nodes.values()
            if getattr(node, "node_type", "") == "evidence"
            for binding in node.artifact_bindings
            if binding.artifact_id == "artifact"
        }
        assert repaired.task_states["T"] == "stale"
        assert repaired.goal_state == "open"
        assert evidence_versions == {1}
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.attempts[-1].state.value == "failed"
        assert "stale_verifier_outcome" in os_.scheduler.attempts[-1].error
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_repair_uses_exact_registered_version_and_rejects_rollback():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))
        os_.run(goal, max_dispatches=1)
        os_._facts.add_version("artifact", 2, "v2")

        os_.repair(goal, artifact_id="artifact", new_artifact_version=2)
        assert os_._facts.versions()["artifact"] == [1, 2]

        with pytest.raises(ConfigurationError, match="stale version 1"):
            os_.repair(goal, artifact_id="artifact", new_artifact_version=1)
        assert os_._facts.versions()["artifact"] == [1, 2]

        os_.repair(goal, artifact_id="artifact", new_artifact_version=4)
        assert os_._facts.versions()["artifact"] == [1, 2, 4]
    finally:
        os_.close()


def test_repair_cause_versions_follow_predecessor_across_multiple_repairs():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        task = goal.task(
            "T",
            agent="worker",
            verify=lambda: _pass("artifact", version=1),
        )
        os_.run(goal, max_dispatches=1)

        os_._facts.add_version("artifact", 2, "v2")
        first = os_.repair(goal, artifact_id="artifact", new_artifact_version=2)
        assert [(d["old_version"], d["new_version"]) for d in first.cause_details] == [(1, 2)]
        assert first.affected == ["T"]
        assert first.frontier == ["T"]

        task.verify = lambda: _pass("artifact", version=2)
        assert os_.run(goal, max_dispatches=1).goal_state == "closed"

        os_._facts.add_version("artifact", 3, "v3")
        second = os_.repair(goal, artifact_id="artifact", new_artifact_version=3)
        assert [(d["old_version"], d["new_version"]) for d in second.cause_details] == [(2, 3)]
        assert second.affected == ["T"]
        assert second.frontier == ["T"]

        task.verify = lambda: _pass("artifact", version=3)
        assert os_.run(goal, max_dispatches=1).goal_state == "closed"
    finally:
        os_.close()


def test_repair_unknown_artifact_fails_without_fact_or_graph_side_effect():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("real", version=1))
        os_.run(goal, max_dispatches=1)
        gid = os_._gid_for("G")
        before_versions = os_._facts.versions()
        before_graph_version = os_.vpg.get_graph(gid).current_version
        before_nodes, _ = os_.vpg.snapshot_projection(gid)

        with pytest.raises(ConfigurationError, match="unknown or unreferenced"):
            os_.repair(goal, artifact_id="typo", new_artifact_version=2)

        after_nodes, _ = os_.vpg.snapshot_projection(gid)
        assert os_._facts.versions() == before_versions
        assert os_.vpg.get_graph(gid).current_version == before_graph_version
        assert set(after_nodes) == set(before_nodes)
    finally:
        os_.close()


def test_async_executor_function_is_accepted_for_run_async():
    async def execute(_task_id: str) -> None:
        return None

    agent = Agent("worker", executor=execute)

    assert agent.executor is execute
    assert agent.executor_is_async is True


def test_async_callable_object_is_accepted_for_run_async():
    class AsyncExecutor:
        async def __call__(self, _task_id: str) -> None:
            return None

    executor = AsyncExecutor()
    agent = Agent("worker", executor=executor)

    assert agent.executor is executor
    assert agent.executor_is_async is True


def test_sync_wrapper_returning_coroutine_is_rejected_without_warning():
    calls: list[str] = []

    async def async_execute(task_id: str) -> None:
        calls.append(task_id)

    def execute(task_id: str):
        return async_execute(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(ConfigurationError, match="run_async"):
                os_.run(goal, max_dispatches=1)
            gc.collect()

        gid = os_._gid_for("G")
        assert calls == []
        assert os_.result(gid).task_states["T"] == "unverified"
        assert not any("was never awaited" in str(item.message) for item in caught)
        assert os_.scheduler.claims[-1].state.value == "released"
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_claim_lost_after_fact_write_cannot_attach_stale_evidence(monkeypatch):
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))
        gid = os_._compile_goal(goal)
        original_add_version = os_._facts.add_version

        def add_version_then_lose_claim(
            artifact_id: str,
            version: int,
            content: str,
        ) -> None:
            original_add_version(artifact_id, version, content)
            os_.scheduler.release_task(gid, "T", reason="injected_race")

        monkeypatch.setattr(os_._facts, "add_version", add_version_then_lose_claim)

        result = os_.run(goal, max_dispatches=1)

        nodes, _ = os_.vpg.snapshot_projection(gid)
        assert result.task_states["T"] == "unverified"
        artref_id, verification_id, evidence_id, action_id = _evidence_ids(gid, "T", 1)
        assert not {artref_id, verification_id, evidence_id} & set(nodes)
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_._facts.get_action(action_id) is None
    finally:
        os_.close()


def test_claim_lost_after_action_commit_cannot_attach_stale_evidence(monkeypatch):
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))
        gid = os_._compile_goal(goal)
        original_commit_action = os_._facts.commit_action

        def commit_action_then_lose_claim(action_id: str, *, pid: str, exit_code: int = 0):
            committed = original_commit_action(action_id, pid=pid, exit_code=exit_code)
            os_.scheduler.release_task(gid, "T", reason="injected_race")
            return committed

        monkeypatch.setattr(os_._facts, "commit_action", commit_action_then_lose_claim)

        result = os_.run(goal, max_dispatches=1)

        nodes, _ = os_.vpg.snapshot_projection(gid)
        assert result.task_states["T"] == "unverified"
        artref_id, verification_id, evidence_id, _ = _evidence_ids(gid, "T", 1)
        assert not {artref_id, verification_id, evidence_id} & set(nodes)
        assert os_.scheduler.claims[-1].state.value == "released"
    finally:
        os_.close()
