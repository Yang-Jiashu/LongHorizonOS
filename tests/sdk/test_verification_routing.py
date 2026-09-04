"""Graph-derived verification-routing tests.

Verification effort is a fan-out floor: it scales with the blast radius of a
false VERIFIED (how many downstream tasks structurally depend on a node),
derived from the graph's own ``DownstreamUnlock`` signal plus critical-path
membership.  Declared risk composes on top and may only raise the floor, never
lower it.  These tests pin that behaviour and prove it reaches execution
through the existing opt-in provider seam.
"""

from __future__ import annotations

from lhos.sdk import (
    Agent,
    AgentCognitionState,
    AgentOS,
    CandidateTaskMetadata,
    ComputeProviderRegistry,
    GlobalRuntimeState,
    Goal,
    ProgressSemanticState,
    ResourceRuntimeState,
    VerificationOutcome,
    VerificationStrength,
    plan_compute_routing,
)


def _state(
    *,
    ready: tuple[str, ...] = ("H",),
    critical_path: tuple[str, ...] = (),
    unlock: tuple[dict[str, object], ...] = (),
    closed: bool = False,
) -> GlobalRuntimeState:
    return GlobalRuntimeState(
        goal_id="goal",
        graph_id="graph",
        progress=ProgressSemanticState(
            graph_id="graph",
            graph_version=7,
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


def _pass(artifact_id: str) -> VerificationOutcome:
    return VerificationOutcome(passed=True, artifact_id=artifact_id, version=1, content="ok")


class _Model:
    def execute(self, task_id: str, context: object, base_executor: object) -> VerificationOutcome:
        del context, base_executor
        return _pass(task_id)


class _Verifier:
    def __init__(self, label: str) -> None:
        self.label = label
        self.calls: list[str] = []

    def verify(
        self,
        task_id: str,
        context: object,
        executor_outcome: object,
        base_verifier: object,
    ) -> VerificationOutcome:
        del context, base_verifier
        self.calls.append(task_id)
        assert isinstance(executor_outcome, VerificationOutcome)
        return executor_outcome


def _fanout_registry() -> tuple[_Model, dict[str, _Verifier], ComputeProviderRegistry]:
    model = _Model()
    verifiers = {
        "light": _Verifier("light"),
        "standard": _Verifier("standard"),
        "strong": _Verifier("strong"),
    }
    registry = ComputeProviderRegistry().register_model("m", model)
    for key, verifier in verifiers.items():
        registry.register_verifier(key, verifier)
    return model, verifiers, registry


def _provider_metadata() -> dict[str, object]:
    # Deliberately omit an explicit ``verifier`` so the verifier key is derived
    # from the policy decision's ``verification_strength`` (the seam under test).
    return {"compute_routing": {"provider_routing": {"enabled": True, "model": "m"}}}


def test_high_fanout_node_is_strong_while_equivalent_leaf_is_light() -> None:
    unlock = (
        {"task_id": "H", "unlock_value": 12},
        {"task_id": "L", "unlock_value": 0},
    )
    hub = plan_compute_routing(
        _state(ready=("H",), unlock=unlock),
        CandidateTaskMetadata(task_id="H"),
    )
    leaf = plan_compute_routing(
        _state(ready=("L",), unlock=unlock),
        CandidateTaskMetadata(task_id="L"),
    )

    assert hub.verification_strength is VerificationStrength.STRONG
    assert hub.verification_fanout == 12
    assert hub.verification_on_critical_path is False
    assert "verification_floor_fanout=12" in hub.reasons

    # Same graph, same absence of declared risk: only the observed structure
    # differs, and a genuine leaf (present with fan-out 0) drops to LIGHT.
    assert leaf.verification_strength is VerificationStrength.LIGHT
    assert leaf.verification_fanout == 0


def test_declared_risk_escalates_but_never_deescalates() -> None:
    # (a) declared risk raises a graph LIGHT leaf all the way to STRONG.
    escalated = plan_compute_routing(
        _state(ready=("L",), unlock=({"task_id": "L", "unlock_value": 0},)),
        CandidateTaskMetadata(
            task_id="L",
            criticality=10,
            downstream_fanout=10,
            failure_blast_radius=10,
        ),
    )
    assert escalated.verification_strength is VerificationStrength.STRONG
    assert escalated.verification_fanout == 0
    assert "verification_strength_raised_above_graph_floor" in escalated.reasons

    # (b) a caller declaring zero risk AND requesting LIGHT cannot pull a
    # high-fan-out node below the STRONG the graph justifies.
    protected = plan_compute_routing(
        _state(ready=("H",), unlock=({"task_id": "H", "unlock_value": 12},)),
        CandidateTaskMetadata(
            task_id="H",
            criticality=0,
            downstream_fanout=0,
            failure_blast_radius=0,
            input_stability=1.0,
            requested_verification_strength=VerificationStrength.LIGHT,
        ),
    )
    assert protected.verification_strength is VerificationStrength.STRONG
    assert protected.verification_fanout == 12
    assert "verification_strength_at_graph_floor" in protected.reasons


def test_critical_path_membership_raises_a_zero_fanout_node_to_standard() -> None:
    decision = plan_compute_routing(
        _state(
            ready=("H",),
            critical_path=("H",),
            unlock=({"task_id": "H", "unlock_value": 0},),
        ),
        CandidateTaskMetadata(task_id="H"),
    )
    assert decision.verification_strength is VerificationStrength.STANDARD
    assert decision.verification_on_critical_path is True
    assert "verification_floor_on_critical_path" in decision.reasons


def test_unobservable_fanout_fails_closed_to_standard_not_leaf() -> None:
    # ``H`` is missing from the fan-out projection: structure was not observed.
    decision = plan_compute_routing(
        _state(ready=("H",), unlock=()),
        CandidateTaskMetadata(task_id="H"),
    )
    assert decision.verification_strength is VerificationStrength.STANDARD
    assert decision.verification_fanout is None
    assert "verification_floor_fanout_unobservable" in decision.reasons
    assert any(item.name == "graph.downstream_fanout" for item in decision.unavailable)


def test_verification_floor_is_deterministic_across_insertion_orders() -> None:
    forward = (
        {"task_id": "H", "unlock_value": 12},
        {"task_id": "a", "unlock_value": 1},
        {"task_id": "b", "unlock_value": 0},
    )
    reversed_order = tuple(reversed(forward))
    first = plan_compute_routing(
        _state(ready=("H",), unlock=forward),
        CandidateTaskMetadata(task_id="H"),
    )
    second = plan_compute_routing(
        _state(ready=("H",), unlock=reversed_order),
        CandidateTaskMetadata(task_id="H"),
    )
    assert (
        first.verification_strength is second.verification_strength is VerificationStrength.STRONG
    )
    assert first.verification_fanout == second.verification_fanout == 12
    # No wall-clock, no RNG: the decision hash is a stable function of inputs.
    assert first.decision_hash == second.decision_hash


def test_provider_route_selects_verifier_key_from_graph_fanout() -> None:
    _model, _verifiers, registry = _fanout_registry()
    metadata = _provider_metadata()

    hub_route = registry.route_task(
        _state(ready=("H",), unlock=({"task_id": "H", "unlock_value": 12},)),
        "H",
        metadata,
    )
    leaf_route = registry.route_task(
        _state(ready=("L",), unlock=({"task_id": "L", "unlock_value": 0},)),
        "L",
        metadata,
    )

    assert hub_route is not None
    assert hub_route.decision.verification_strength is VerificationStrength.STRONG
    assert hub_route.verifier_key == "strong"

    assert leaf_route is not None
    assert leaf_route.decision.verification_strength is VerificationStrength.LIGHT
    assert leaf_route.verifier_key == "light"


def test_strong_verifier_hook_actually_runs_for_high_fanout_node() -> None:
    _model, verifiers, registry = _fanout_registry()
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = Goal("high-fanout")
        hub = goal.task(
            "H",
            agent="worker",
            metadata=_provider_metadata(),
            verify=lambda: _pass("H"),
        )
        # Ten consumers that depend only on ``H`` give it a fan-out of 10 at the
        # moment it is routed: a false VERIFIED here would rework ten tasks.
        for index in range(10):
            goal.task(
                f"c{index}",
                agent="worker",
                depends_on=(hub,),
                verify=lambda index=index: _pass(f"c{index}"),
            )

        result = os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)

        # The graph-derived STRONG verifier ran; the weaker hooks did not.
        assert verifiers["strong"].calls == ["H"]
        assert verifiers["standard"].calls == []
        assert verifiers["light"].calls == []
        # Execution still went through the ordinary claim/Evidence path.
        assert "H" in result.verified
    finally:
        os_.close()
