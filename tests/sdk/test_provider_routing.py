"""Focused tests for the opt-in provider-backed execution adapter."""

from __future__ import annotations

import asyncio

import pytest

from lhos.sdk import (
    Agent,
    AgentOS,
    ComputeProviderRegistry,
    ConfigurationError,
    Goal,
    VerificationOutcome,
)


def _pass(artifact_id: str, content: str = "ok") -> VerificationOutcome:
    return VerificationOutcome(
        passed=True,
        artifact_id=artifact_id,
        version=1,
        content=content,
    )


class _ModelProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def execute(self, task_id: str, context: object, base_executor: object) -> VerificationOutcome:
        self.calls.append((task_id, context))
        return _pass("provider-model", "model-output")


class _VerifierProvider:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, object, object]] = []

    def verify(
        self,
        task_id: str,
        context: object,
        executor_outcome: object,
        base_verifier: object,
    ) -> VerificationOutcome:
        self.calls.append((task_id, context, executor_outcome, base_verifier))
        assert isinstance(executor_outcome, VerificationOutcome)
        return executor_outcome


def _routed_metadata() -> dict[str, object]:
    return {
        "compute_routing": {
            "criticality": 1,
            "provider_routing": {
                "enabled": True,
                "model": "cheap",
                "verifier": "light",
            },
        }
    }


def test_sync_opt_in_route_runs_after_claim_and_uses_provider_hooks() -> None:
    model = _ModelProvider()
    verifier = _VerifierProvider()
    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", model)
        .register_verifier("light", verifier)
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        base_calls: list[str] = []

        def base_executor(task_id: str) -> VerificationOutcome:
            base_calls.append(task_id)
            return _pass("base")

        os_.add_agent(
            Agent(
                "worker",
                executor=base_executor,
            )
        )
        goal = Goal("provider-sync")
        goal.task(
            "task",
            agent="worker",
            metadata=_routed_metadata(),
            verify=lambda: _pass("base-verifier"),
        )

        result = os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)

        assert result.goal_state == "closed"
        assert [item[0] for item in model.calls] == ["task"]
        assert [item[0] for item in verifier.calls] == ["task"]
        assert base_calls == []
        # The task was still claimed and semantically committed through the
        # ordinary Scheduler/VPG path.
        assert result.verified == ["task"]
    finally:
        os_.close()


@pytest.mark.asyncio
async def test_async_opt_in_route_supports_async_provider_hooks() -> None:
    calls: list[str] = []

    class AsyncModel:
        async def execute(self, task_id: str, context: object, base_executor: object) -> object:
            calls.append(f"model:{task_id}")
            await asyncio.sleep(0)
            return _pass("async-model")

    class AsyncVerifier:
        async def verify(
            self,
            task_id: str,
            context: object,
            executor_outcome: object,
            base_verifier: object,
        ) -> object:
            calls.append(f"verifier:{task_id}")
            await asyncio.sleep(0)
            return executor_outcome

    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", AsyncModel())
        .register_verifier("light", AsyncVerifier())
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:

        async def base_executor(_task_id: str) -> object:
            calls.append("base")
            return _pass("base")

        os_.add_agent(Agent("worker", executor=base_executor))
        goal = Goal("provider-async")
        goal.task("task", agent="worker", metadata=_routed_metadata())

        result = await os_.run_async(
            goal,
            adaptive=True,
            max_dispatches=1,
            max_steps=2,
            max_concurrency=1,
        )

        assert result.goal_state == "closed"
        assert calls == ["model:task", "verifier:task"]
    finally:
        os_.close()


def test_enabled_route_without_registry_fails_closed() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("provider-missing")
        goal.task(
            "task",
            agent="worker",
            metadata=_routed_metadata(),
            verify=lambda: _pass("base"),
        )

        with pytest.raises(ConfigurationError, match="no provider_registry"):
            os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)
    finally:
        os_.close()


def test_unopted_task_keeps_base_executor_and_verifier() -> None:
    model = _ModelProvider()
    verifier = _VerifierProvider()
    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", model)
        .register_verifier("light", verifier)
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        base_calls: list[str] = []

        def base_executor(task_id: str) -> VerificationOutcome:
            base_calls.append(task_id)
            return _pass("base")

        os_.add_agent(
            Agent(
                "worker",
                executor=base_executor,
            )
        )
        goal = Goal("provider-default")
        goal.task(
            "task",
            agent="worker",
            verify=lambda: _pass("base-verifier"),
        )

        result = os_.run(goal, max_dispatches=1, max_steps=2)

        assert result.goal_state == "closed"
        assert base_calls == ["task"]
        assert model.calls == []
        assert verifier.calls == []
    finally:
        os_.close()


def test_provider_executor_exception_releases_exact_claim() -> None:
    class FailingModel:
        def execute(self, task_id: str, context: object, base_executor: object) -> object:
            raise RuntimeError("provider down")

    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", FailingModel())
        .register_verifier("light", _VerifierProvider())
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("provider-executor-failure")
        goal.task(
            "task",
            agent="worker",
            metadata=_routed_metadata(),
            verify=lambda: _pass("base"),
        )
        with pytest.raises(RuntimeError, match="provider down"):
            os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)
        claim = os_.scheduler.claims[-1]
        attempt = os_.scheduler.attempts[-1]
        assert claim.state.value == "released"
        assert claim.reason == "provider_executor_failed:RuntimeError"
        assert attempt.state.value == "failed"
        assert os_.result(os_._goal_gid[goal.goal_id]).verified == []
    finally:
        os_.close()


def test_provider_verifier_failure_does_not_publish_evidence() -> None:
    class FailingVerifier:
        def verify(
            self,
            task_id: str,
            context: object,
            executor_outcome: object,
            base_verifier: object,
        ) -> object:
            raise RuntimeError("verifier down")

    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", _ModelProvider())
        .register_verifier("light", FailingVerifier())
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("provider-verifier-failure")
        goal.task("task", agent="worker", metadata=_routed_metadata())
        result = os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)
        assert result.verified == []
        assert result.task_states["task"] == "unverified"
        assert os_.scheduler.attempts[-1].state.value == "failed"
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.claims[-1].reason == "verifier_failed:RuntimeError"
    finally:
        os_.close()


def test_context_adapter_receives_exact_context_and_preserves_falsey_view() -> None:
    model = _ModelProvider()
    verifier = _VerifierProvider()
    seen: list[tuple[str, object]] = []

    class Adapter:
        def adapt(self, task_id: str, context: object) -> object:
            seen.append((task_id, context))
            return _FalseyContext(context)

    class _FalseyContext:
        def __init__(self, original: object) -> None:
            self.original = original

        def __bool__(self) -> bool:
            return False

    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", model)
        .register_verifier("light", verifier)
        .register_context_adapter("minimal", Adapter())
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("provider-context-adapter")
        metadata = _routed_metadata()
        metadata["compute_routing"]["provider_routing"]["context_adapter"] = "minimal"  # type: ignore[index]
        goal.task("task", agent="worker", metadata=metadata)
        result = os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)
        assert result.goal_state == "closed"
        assert seen and seen[0][0] == "task"
        assert isinstance(model.calls[0][1], _FalseyContext)
        assert isinstance(verifier.calls[0][1], _FalseyContext)
        assert verifier.calls[0][1] is model.calls[0][1]
    finally:
        os_.close()


def test_context_adapter_exception_releases_exact_claim() -> None:
    class FailingAdapter:
        def adapt(self, task_id: str, context: object) -> object:
            raise RuntimeError("context adapter down")

    registry = (
        ComputeProviderRegistry()
        .register_model("cheap", _ModelProvider())
        .register_verifier("light", _VerifierProvider())
        .register_context_adapter("broken", FailingAdapter())
    )
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("provider-context-failure")
        metadata = _routed_metadata()
        metadata["compute_routing"]["provider_routing"]["context_adapter"] = "broken"  # type: ignore[index]
        goal.task("task", agent="worker", metadata=metadata)
        with pytest.raises(RuntimeError, match="context adapter down"):
            os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)
        assert os_.scheduler.claims[-1].state.value == "released"
        assert os_.scheduler.claims[-1].reason == "provider_context_failed:RuntimeError"
    finally:
        os_.close()
