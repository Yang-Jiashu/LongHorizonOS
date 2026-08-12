"""Public AgentOS.run_async concurrency and semantic-closure tests."""

from __future__ import annotations

import asyncio
import threading

import pytest

from lhos.runtimes.multi_agent import AttemptState
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
        content=f"{artifact_id}-v{version}",
    )


class _OverlapExecutor:
    def __init__(self, expected_overlap: int) -> None:
        self.expected_overlap = expected_overlap
        self.entered: asyncio.Queue[str] = asyncio.Queue()
        self.release = asyncio.Event()
        self.active = 0
        self.peak = 0

    async def __call__(self, task_id: str) -> None:
        self.active += 1
        self.peak = max(self.peak, self.active)
        await self.entered.put(task_id)
        try:
            await self.release.wait()
        finally:
            self.active -= 1

    async def wait_for_overlap(self) -> set[str]:
        return {
            await asyncio.wait_for(self.entered.get(), timeout=1)
            for _ in range(self.expected_overlap)
        }


async def test_run_async_executes_independent_tasks_with_real_overlap_and_closes_goal():
    executor = _OverlapExecutor(expected_overlap=2)
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=executor,
                specializations=("python",),
                max_concurrency=2,
            )
        )
        goal = Goal("G")
        goal.task("T1", agent="worker", verify=lambda: _pass("one"))
        goal.task("T2", agent="worker", verify=lambda: _pass("two"))

        running = asyncio.create_task(os_.run_async(goal, max_dispatches=2, max_concurrency=2))
        assert await executor.wait_for_overlap() == {"T1", "T2"}
        assert executor.peak == 2
        executor.release.set()

        result = await asyncio.wait_for(running, timeout=2)
        assert result.goal_state == "closed"
        assert set(result.verified) == {"T1", "T2"}
        assert result.meta == {
            "execution_mode": "async",
            "dispatched": 2,
            "max_concurrency": 2,
        }
        assert all(
            attempt.state == AttemptState.VERIFIED_SEMANTICALLY
            for attempt in os_.scheduler.attempts
        )
    finally:
        os_.close()


async def test_run_async_enforces_agent_capacity_and_runs_next_wave():
    executor = _OverlapExecutor(expected_overlap=1)
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "serial-worker",
                executor=executor,
                specializations=("python",),
                max_concurrency=1,
            )
        )
        goal = Goal("G")
        goal.task("T1", agent="serial-worker", verify=lambda: _pass("one"))
        goal.task("T2", agent="serial-worker", verify=lambda: _pass("two"))

        running = asyncio.create_task(os_.run_async(goal, max_dispatches=2, max_concurrency=2))
        first = await asyncio.wait_for(executor.entered.get(), timeout=1)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert executor.entered.empty()
        executor.release.set()
        second = await asyncio.wait_for(executor.entered.get(), timeout=1)

        result = await asyncio.wait_for(running, timeout=2)
        assert {first, second} == {"T1", "T2"}
        assert executor.peak == 1
        assert result.goal_state == "closed"
    finally:
        os_.close()


async def test_run_async_offloads_sync_executor_from_event_loop_thread():
    event_loop_thread = threading.get_ident()
    executor_threads: list[int] = []

    def execute(_task_id: str) -> None:
        executor_threads.append(threading.get_ident())

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        assert result.goal_state == "closed"
        assert len(executor_threads) == 1
        assert executor_threads[0] != event_loop_thread
    finally:
        os_.close()


async def test_run_async_awaits_coroutine_returned_by_sync_executor_wrapper():
    calls: list[str] = []

    async def execute_async(task_id: str) -> None:
        calls.append(task_id)

    def execute(task_id: str):
        return execute_async(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        assert calls == ["T"]
        assert result.goal_state == "closed"
    finally:
        os_.close()


async def test_run_async_schedules_dependency_only_after_upstream_verification():
    execution_order: list[str] = []

    async def execute(task_id: str) -> None:
        execution_order.append(task_id)

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
        goal = Goal("G")
        upstream = goal.task(
            "upstream",
            agent="worker",
            verify=lambda: _pass("upstream-artifact"),
        )

        def verify_downstream() -> VerificationOutcome:
            gid = os_._gid_for("G")
            assert gid is not None
            assert os_.result(gid).task_states["upstream"] == "verified"
            return _pass("downstream-artifact")

        goal.task(
            "downstream",
            agent="worker",
            depends_on=(upstream,),
            verify=verify_downstream,
        )

        result = await os_.run_async(
            goal,
            max_dispatches=2,
            max_steps=2,
            max_concurrency=2,
        )

        assert execution_order == ["upstream", "downstream"]
        assert result.goal_state == "closed"
        assert set(result.verified) == {"upstream", "downstream"}
    finally:
        os_.close()


async def test_resource_blocked_ready_task_does_not_hide_later_runnable_task():
    executed: list[str] = []

    async def execute(task_id: str) -> None:
        executed.append(task_id)

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "cpu-only",
                executor=execute,
                specializations=("python",),
                resource_capacity={"cpu_millis": 1_000},
            )
        )
        goal = Goal("G")
        goal.task(
            "A-needs-gpu",
            agent="cpu-only",
            verify=lambda: _pass("gpu-artifact"),
            resources={"gpu_count": 1},
        )
        goal.task(
            "B-runnable",
            agent="cpu-only",
            verify=lambda: _pass("cpu-artifact"),
        )

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
        )

        assert executed == ["B-runnable"]
        assert result.task_states == {
            "A-needs-gpu": "unverified",
            "B-runnable": "verified",
        }
    finally:
        os_.close()


async def test_operational_failure_skips_verifier_and_releases_claim_and_lease():
    verifier_called = False

    async def fail(_task_id: str) -> None:
        raise RuntimeError("executor boom")

    def verify() -> VerificationOutcome:
        nonlocal verifier_called
        verifier_called = True
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=fail, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)

        result = await os_.run_async(
            goal,
            max_dispatches=1,
            max_steps=1,
            max_concurrency=1,
        )
        gid = os_._gid_for("G")
        assert gid is not None
        assert verifier_called is False
        assert result.task_states["T"] == "unverified"
        assert result.failures and "RuntimeError: executor boom" in result.failures[0]
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.attempts[-1].state == AttemptState.FAILED
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


async def test_evidence_failure_double_cleanup_does_not_release_reassigned_claim(
    monkeypatch,
):
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda _task_id: None,
                specializations=("python",),
            )
        )
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"), max_attempts=2)
        gid = os_._compile_goal(goal)

        def fail_commit(*_args, **_kwargs) -> bool:
            raise RuntimeError("commit boom")

        monkeypatch.setattr(os_, "_commit_verified_outcome", fail_commit)
        original_release = os_.scheduler.release_task
        release_calls = 0
        replacement_claim_ids: list[str] = []

        def release_and_reassign(
            graph_id: str,
            task_id: str,
            *,
            reason: str = "execution_failed",
            retry: bool = True,
        ) -> None:
            nonlocal release_calls
            release_calls += 1
            original_release(
                graph_id,
                task_id,
                reason=reason,
                retry=retry,
            )
            if release_calls == 1:
                res = os_.scheduler.run_pass(graph_id, max_claims=1)
                replacement_claim_ids.extend(item["claim_id"] for item in res.dispatched)

        monkeypatch.setattr(os_.scheduler, "release_task", release_and_reassign)

        with pytest.raises(VerificationError, match="failed to attach Evidence"):
            await os_.run_async(
                goal,
                max_dispatches=1,
                max_steps=1,
                max_concurrency=1,
            )

        assert release_calls == 1
        assert len(replacement_claim_ids) == 1
        replacement = os_.scheduler.active_claim_for_task("T", gid)
        assert replacement is not None
        assert replacement.claim_id == replacement_claim_ids[0]
    finally:
        os_.close()


async def test_verifier_observes_operational_success_before_semantic_commit():
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "worker",
                executor=lambda _task_id: None,
                specializations=("python",),
            )
        )
        goal = Goal("G")

        def verify() -> VerificationOutcome:
            [attempt] = os_.scheduler.attempts
            assert attempt.state == AttemptState.SUCCEEDED_OPERATIONALLY
            gid = os_._gid_for("G")
            assert gid is not None
            assert os_.result(gid).task_states["T"] == "unverified"
            return _pass("artifact")

        goal.task("T", agent="worker", verify=verify)
        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        assert result.goal_state == "closed"
        assert os_.scheduler.attempts[-1].state == AttemptState.VERIFIED_SEMANTICALLY
    finally:
        os_.close()


async def test_cancelling_run_async_releases_active_claims():
    entered = asyncio.Event()
    hold = asyncio.Event()

    async def execute(_task_id: str) -> None:
        entered.set()
        await hold.wait()

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))
        running = asyncio.create_task(os_.run_async(goal, max_dispatches=1, max_concurrency=1))
        await asyncio.wait_for(entered.wait(), timeout=1)

        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        gid = os_._gid_for("G")
        assert gid is not None
        assert os_.scheduler.active_claim_for_task("T", gid) is None
        assert os_.scheduler.claims[-1].state.value == "released"
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_sync_run_rejects_selected_async_executor_and_releases_claim():
    async def execute(_task_id: str) -> None:
        return None

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=lambda: _pass("artifact"))

        with pytest.raises(ConfigurationError, match="run_async"):
            os_.run(goal, max_dispatches=1)

        gid = os_._gid_for("G")
        assert gid is not None
        assert os_.scheduler.active_claim_for_task("T", gid) is None
    finally:
        os_.close()


def test_sync_run_async_executor_releases_every_claim_in_same_dispatch_batch():
    executed: list[str] = []

    async def execute_async(_task_id: str) -> None:
        executed.append("async")

    def execute_sync(_task_id: str) -> None:
        executed.append("sync")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "async-worker",
                executor=execute_async,
                specializations=("async-kind",),
            )
        )
        os_.add_agent(
            Agent(
                "sync-worker",
                executor=execute_sync,
                specializations=("sync-kind",),
            )
        )
        goal = Goal("G")
        goal.task(
            "A-async",
            agent="async-worker",
            required_specializations=("async-kind",),
            verify=lambda: _pass("async-artifact"),
        )
        goal.task(
            "B-sync",
            agent="sync-worker",
            required_specializations=("sync-kind",),
            verify=lambda: _pass("sync-artifact"),
        )

        with pytest.raises(ConfigurationError, match="run_async"):
            os_.run(goal, max_dispatches=2)

        gid = os_._gid_for("G")
        assert gid is not None
        assert executed == []
        assert all(claim.state.value == "released" for claim in os_.scheduler.claims)
        assert os_.scheduler.active_claim_for_task("A-async", gid) is None
        assert os_.scheduler.active_claim_for_task("B-sync", gid) is None
    finally:
        os_.close()


def test_sync_run_awaitable_wrapper_releases_unexecuted_batch_tail():
    executed: list[str] = []

    async def execute_async(_task_id: str) -> None:
        executed.append("wrapped")

    def wrapped(task_id: str):
        return execute_async(task_id)

    def execute_sync(_task_id: str) -> None:
        executed.append("sync")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(
            Agent(
                "wrapped-worker",
                executor=wrapped,
                specializations=("wrapped-kind",),
            )
        )
        os_.add_agent(
            Agent(
                "sync-worker",
                executor=execute_sync,
                specializations=("sync-kind",),
            )
        )
        goal = Goal("G")
        goal.task(
            "A-wrapped",
            agent="wrapped-worker",
            required_specializations=("wrapped-kind",),
            verify=lambda: _pass("wrapped-artifact"),
        )
        goal.task(
            "B-sync",
            agent="sync-worker",
            required_specializations=("sync-kind",),
            verify=lambda: _pass("sync-artifact"),
        )

        with pytest.raises(ConfigurationError, match="run_async"):
            os_.run(goal, max_dispatches=2)

        gid = os_._gid_for("G")
        assert gid is not None
        assert executed == []
        assert all(claim.state.value == "released" for claim in os_.scheduler.claims)
        assert os_.scheduler.active_claim_for_task("A-wrapped", gid) is None
        assert os_.scheduler.active_claim_for_task("B-sync", gid) is None
    finally:
        os_.close()


async def test_run_async_accepts_async_task_verifier_and_closes_goal():
    async def verify() -> VerificationOutcome:
        await asyncio.sleep(0)
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        assert result.goal_state == "closed"
        assert result.verified == ["T"]
        assert os_.scheduler.active_claim_for_task("T", os_._gid_for("G")) is None
    finally:
        os_.close()


async def test_run_async_async_verifier_failure_releases_claim_and_stays_unverified():
    async def execute(_task_id: str) -> None:
        return None

    async def verify() -> VerificationOutcome:
        await asyncio.sleep(0)
        raise RuntimeError("async verifier boom")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)

        result = await os_.run_async(goal, max_dispatches=1, max_concurrency=1)

        gid = os_._gid_for("G")
        assert gid is not None
        assert result.goal_state == "open"
        assert result.task_states["T"] == "unverified"
        assert any("verifier_failed:RuntimeError" in item for item in result.failures)
        assert os_.scheduler.active_claim_for_task("T", gid) is None
        assert os_.scheduler.claims[-1].state.value == "released"
    finally:
        os_.close()


async def test_run_async_cancelling_async_verifier_releases_claim_and_lease():
    verifier_started = asyncio.Event()
    verifier_release = asyncio.Event()

    async def execute(_task_id: str) -> None:
        return None

    async def verify() -> VerificationOutcome:
        verifier_started.set()
        await verifier_release.wait()
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", executor=execute, specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)
        running = asyncio.create_task(os_.run_async(goal, max_dispatches=1, max_concurrency=1))
        await asyncio.wait_for(verifier_started.wait(), timeout=1)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running

        gid = os_._gid_for("G")
        assert gid is not None
        assert os_.scheduler.active_claim_for_task("T", gid) is None
        assert os_.scheduler.claims[-1].state.value == "released"
        assert (
            os_.kernel._lease_service.list_active_leases_for_resource(claim_resource_uri(gid, "T"))
            == []
        )
    finally:
        os_.close()


def test_sync_run_rejects_async_task_verifier_and_releases_claim():
    async def verify() -> VerificationOutcome:
        return _pass("artifact")

    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker", specializations=("python",)))
        goal = Goal("G")
        goal.task("T", agent="worker", verify=verify)

        with pytest.raises(ConfigurationError, match=r"Task\.verify.*run_async"):
            os_.run(goal, max_dispatches=1)

        gid = os_._gid_for("G")
        assert gid is not None
        assert os_.scheduler.active_claim_for_task("T", gid) is None
        assert os_.scheduler.claims[-1].state.value == "released"
    finally:
        os_.close()
