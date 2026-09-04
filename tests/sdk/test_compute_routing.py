"""Focused tests for the bounded Stage-8 compute-routing primitive."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from lhos.sdk import (
    Agent,
    AgentCognitionState,
    AgentOS,
    AgentReuseAction,
    CandidateTaskMetadata,
    ComputeRoutingDecision,
    ComputeRoutingPolicy,
    ConfigurationError,
    GlobalRuntimeState,
    Goal,
    ModelTier,
    ProgressSemanticState,
    RegisteredAgentMetadata,
    ResourceRuntimeState,
    RoutingBinding,
    RoutingContextManifest,
    VerificationStrength,
    plan_compute_routing,
)


def _state(
    *,
    task_id: str = "refund",
    ready: tuple[str, ...] = ("refund",),
    critical_path: tuple[str, ...] = (),
    unlock: tuple[dict[str, object], ...] = (),
    closed: bool = False,
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=12,
            projection_hash="p" * 64,
            graph_closed=closed,
            goal_closed=closed,
            ready_frontier=ready,
            repair_ready_frontier=(),
            verified_task_ids=(),
            stale_task_ids=(),
            invalid_task_ids=(),
            unverified_task_ids=ready,
            critical_path=critical_path,
            downstream_unlock_values=tuple(unlock),
            parallel_frontier=ready,
        ),
        agent_cognition=AgentCognitionState(available=True),
        context={"available": False, "reason": "not bound"},
        resources=ResourceRuntimeState(available=True),
    )


def _binding(
    uri: str,
    *,
    artifact_id: str = "artifact",
    version: int = 1,
    content_hash: str = "a" * 64,
    known: bool = True,
    stale: bool = False,
    token_cost: int = 100,
) -> RoutingBinding:
    return RoutingBinding(
        operation="read",
        resource_uri=uri,
        artifact_id=artifact_id,
        version=version,
        content_hash=content_hash,
        known=known,
        stale=stale,
        token_cost=token_cost,
    )


def _warm_agent(
    bindings: tuple[RoutingBinding, ...],
    *,
    agent_id: str = "coder-7",
    **kwargs: object,
) -> RegisteredAgentMetadata:
    return RegisteredAgentMetadata(
        agent_id=agent_id,
        context_snapshot_id="ctx-1",
        read_set=bindings,
        reconstruction_token_cost=900,
        **kwargs,
    )


def test_exact_version_overlap_selects_reuse_with_auditable_score() -> None:
    bindings = (_binding("artifact://api"), _binding("artifact://requirements", version=8))
    state = _state()
    candidate = CandidateTaskMetadata(task_id="refund", required_bindings=bindings)

    decision = ComputeRoutingPolicy().route(state, candidate, [_warm_agent(bindings)])

    assert decision.action is AgentReuseAction.REUSE_AGENT
    assert decision.selected_agent_id == "coder-7"
    assert decision.locality_score == 1.0
    assert decision.exact_overlap_count == 2
    assert decision.required_binding_count == 2
    assert "locality_threshold_met" in decision.reasons
    assert len(decision.decision_hash) == 64


def test_version_or_hash_mismatch_does_not_receive_locality_credit() -> None:
    required = (_binding("artifact://api", version=2, content_hash="b" * 64),)
    old = (_binding("artifact://api", version=1, content_hash="a" * 64),)

    decision = plan_compute_routing(
        _state(),
        CandidateTaskMetadata(task_id="refund", required_bindings=required),
        [_warm_agent(old)],
    )

    assert decision.action is AgentReuseAction.FRESH_AGENT
    assert decision.locality_score == 0.0
    assert decision.exact_overlap_count == 0
    assert "exact_context_overlap_below_safe_threshold" in decision.reasons


def test_unknown_fresh_context_cost_does_not_fabricate_savings() -> None:
    binding = _binding("artifact://api")
    decision = ComputeRoutingPolicy().route(
        _state(),
        CandidateTaskMetadata(task_id="refund", required_bindings=(binding,)),
        [_warm_agent((binding,))],
    )

    assert decision.action is AgentReuseAction.REUSE_AGENT
    assert decision.locality_score == 1.0
    assert decision.fresh_context_token_cost is None
    assert decision.estimated_context_token_savings is None
    assert "fresh_context_cost_unavailable" in decision.reasons


def test_known_fresh_context_cost_blocks_reuse_when_reconstruction_is_worse() -> None:
    binding = _binding("artifact://api")
    decision = ComputeRoutingPolicy().route(
        _state(),
        CandidateTaskMetadata(
            task_id="refund",
            required_bindings=(binding,),
            estimated_context_tokens=500,
        ),
        [_warm_agent((binding,))],
    )

    assert decision.action is AgentReuseAction.FRESH_AGENT
    assert decision.fresh_context_token_cost == 500
    assert decision.reconstruction_token_cost == 900
    assert decision.estimated_context_token_savings == 0
    assert "reconstruction_cost_not_better_than_fresh" in decision.reasons


def test_unknown_or_stale_bindings_fail_closed() -> None:
    required = (
        _binding("artifact://api", known=False),
        _binding("artifact://requirements", stale=True),
    )
    decision = ComputeRoutingPolicy().route(
        _state(),
        CandidateTaskMetadata(task_id="refund", required_bindings=required),
        [_warm_agent(required)],
    )

    assert decision.action is AgentReuseAction.FRESH_AGENT
    assert decision.locality_score == 0.0
    assert decision.unknown_binding_count >= 1
    assert decision.stale_binding_count >= 1
    assert any(item.name == "candidate.required_bindings" for item in decision.unavailable)


def test_context_manifest_is_explicit_input_and_budget_is_bounded() -> None:
    manifest = RoutingContextManifest(
        manifest_id="manifest-1",
        token_budget=50_000,
        bindings=(_binding("artifact://api"),),
    )
    decision = ComputeRoutingPolicy(max_context_budget_tokens=10_000).route(
        _state(),
        CandidateTaskMetadata(task_id="refund", context_manifest=manifest),
        [_warm_agent(manifest.bindings)],
    )

    assert decision.context_budget_tokens == 10_000
    assert "context_budget_from_manifest" in decision.reasons
    assert "context_budget_clamped_to_policy_maximum" in decision.reasons
    assert decision.action is AgentReuseAction.REUSE_AGENT


def test_risk_signals_route_strong_model_and_verifier() -> None:
    state = _state(critical_path=("refund",), unlock=({"task_id": "refund", "unlock_value": 30},))
    candidate = CandidateTaskMetadata(
        task_id="refund",
        criticality=10,
        downstream_fanout=10,
        failure_blast_radius=10,
    )
    decision = ComputeRoutingPolicy().route(state, candidate, [_warm_agent(())])

    assert decision.model_tier is ModelTier.STRONG
    assert decision.verification_strength is VerificationStrength.STRONG
    assert decision.criticality == 10
    assert decision.downstream_fanout == 10


def test_missing_hidden_context_never_claims_reuse() -> None:
    binding = _binding("artifact://api")
    decision = ComputeRoutingPolicy().route(
        _state(),
        CandidateTaskMetadata(
            task_id="refund",
            required_bindings=(binding,),
            hidden_context=True,
        ),
        [_warm_agent((binding,))],
    )

    assert decision.action is AgentReuseAction.FRESH_AGENT
    assert decision.locality_score == 0.0
    assert any(item.name == "candidate.context" for item in decision.unavailable)


def test_not_ready_or_closed_task_is_ineligible_without_mutating_state() -> None:
    state = _state(ready=(), closed=True)
    before = state.model_dump_json()
    decision = ComputeRoutingPolicy().route(
        state,
        CandidateTaskMetadata(task_id="refund"),
        [_warm_agent(())],
    )

    assert not decision.eligible
    assert "goal_or_graph_closed" in decision.reasons
    assert state.model_dump_json() == before


def test_decision_is_immutable_and_byte_stable() -> None:
    binding = _binding("artifact://api")
    candidate = CandidateTaskMetadata(task_id="refund", required_bindings=(binding,))
    agent = _warm_agent((binding,))
    first = plan_compute_routing(_state(), candidate, [agent])
    second = plan_compute_routing(_state(), candidate, [agent])

    assert first == second
    assert first.model_dump_json() == second.model_dump_json()
    with pytest.raises(ValidationError):
        first.action = AgentReuseAction.FRESH_AGENT  # type: ignore[misc]
    with pytest.raises(ValidationError):
        ComputeRoutingDecision.model_validate({**first.model_dump(), "extra": True})


def test_agent_os_compute_routing_facade_is_read_only_and_explicit() -> None:
    os_ = AgentOS(":memory:")
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("routing-facade-goal")
        goal.task("refund", agent="worker")
        os_._compile_goal(goal)
        candidate = CandidateTaskMetadata(
            task_id="refund",
            required_bindings=(_binding("artifact://api"),),
            context_budget_tokens=2_000,
        )
        registered = [
            _warm_agent(
                (_binding("artifact://api"),),
                agent_id="worker",
            )
        ]
        before = (
            os_.vpg.get_graph(os_._goal_gid[goal.goal_id]).current_version,
            tuple(os_.scheduler.claims),
            tuple(os_.scheduler.attempts),
        )
        decision = os_.plan_compute_routing(goal, candidate, registered)
        assert decision.task_id == "refund"
        assert decision.action is AgentReuseAction.REUSE_AGENT
        assert decision.context_budget_tokens == 2_000
        assert (
            os_.vpg.get_graph(os_._goal_gid[goal.goal_id]).current_version,
            tuple(os_.scheduler.claims),
            tuple(os_.scheduler.attempts),
        ) == before
    finally:
        os_.close()


def test_agent_os_compute_routing_facade_rejects_uncompiled_goal_without_mutation() -> None:
    os_ = AgentOS(":memory:")
    try:
        goal = Goal("not-compiled-routing")
        before = (dict(os_._goals), dict(os_._goal_gid), tuple(os_.scheduler.claims))
        with pytest.raises(ConfigurationError, match="not compiled"):
            os_.plan_compute_routing(
                goal,
                CandidateTaskMetadata(task_id="refund"),
            )
        assert (dict(os_._goals), dict(os_._goal_gid), tuple(os_.scheduler.claims)) == before
    finally:
        os_.close()
