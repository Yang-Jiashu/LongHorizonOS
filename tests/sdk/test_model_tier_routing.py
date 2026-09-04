"""Graph-derived model-tier routing tests.

The model tier (cheap / standard / strong) is a *floor* derived from observed
graph structure -- downstream fan-out (blast radius of a wrong output),
critical-path membership, and observed rework (a task being redone because its
prior Evidence went STALE/INVALID).  Declared preference and an explicit caller
override compose on top and may only RAISE the tier, never lower the floor the
structure justifies.  These tests pin that behaviour and prove it reaches
execution through the existing opt-in provider seam.

The fake providers below carry arbitrary per-tier cost weights (cheap < standard
< strong).  Those weights are deterministic bookkeeping used to prove the
routing *mechanism* -- which provider hook runs and the relative spend it
implies.  They are NOT a claim about any real model's quality, price, or
economics; no network, model SDK, GPU, or wall-clock service is involved.
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
    ModelTier,
    ProgressSemanticState,
    ResourceRuntimeState,
    VerificationOutcome,
    plan_compute_routing,
)

# Arbitrary deterministic cost weights, strictly ordered by tier.  They exist
# only to make relative spend observable; they are not real model prices.
_TIER_COST: dict[str, int] = {"cheap": 1, "standard": 5, "strong": 25}


def _state(
    *,
    ready: tuple[str, ...] = ("H",),
    critical_path: tuple[str, ...] = (),
    unlock: tuple[dict[str, object], ...] = (),
    stale: tuple[str, ...] = (),
    invalid: tuple[str, ...] = (),
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
            stale_task_ids=stale,
            invalid_task_ids=invalid,
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


class _CostingModel:
    """A deterministic fake model with a declared cost profile and behaviour.

    Each tier behaves differently: it stamps its own tier onto the artifact
    content and charges its own cost weight.  ``calls`` records every task the
    hook actually ran for, so a test can assert the routing invoked *this* hook
    rather than merely emitting a label.
    """

    def __init__(self, tier: str) -> None:
        self.tier = tier
        self.cost = _TIER_COST[tier]
        self.calls: list[str] = []
        self.total_cost = 0

    def execute(
        self,
        task_id: str,
        context: object,
        base_executor: object,
    ) -> VerificationOutcome:
        del context, base_executor
        self.calls.append(task_id)
        self.total_cost += self.cost
        return VerificationOutcome(
            passed=True,
            artifact_id=task_id,
            version=1,
            content=f"{task_id}:{self.tier}",
        )


class _Verifier:
    def __init__(self) -> None:
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


def _tiered_registry() -> tuple[dict[str, _CostingModel], _Verifier, ComputeProviderRegistry]:
    models = {tier: _CostingModel(tier) for tier in ("cheap", "standard", "strong")}
    verifier = _Verifier()
    registry = ComputeProviderRegistry()
    for tier, model in models.items():
        registry.register_model(tier, model)
    registry.register_verifier("v", verifier)
    return models, verifier, registry


def _derived_metadata() -> dict[str, object]:
    # Deliberately omit an explicit ``model`` so the model key is derived from
    # the policy decision's ``model_tier`` (the seam under test).  A fixed
    # verifier key isolates the comparison to model-tier cost.
    return {"compute_routing": {"provider_routing": {"enabled": True, "verifier": "v"}}}


def _forced_strong_metadata() -> dict[str, object]:
    return {
        "compute_routing": {
            "provider_routing": {"enabled": True, "model": "strong", "verifier": "v"}
        }
    }


def _hub_and_leaves_goal(os_: AgentOS, *, leaves: int = 10) -> Goal:
    goal = Goal("model-tier-fanout")
    hub = goal.task("H", agent="worker", metadata=_derived_metadata(), verify=lambda: _pass("H"))
    # Every leaf depends only on the hub, so at routing time the hub has a
    # fan-out equal to the leaf count while each leaf is a genuine fan-out-0 leaf.
    for index in range(leaves):
        goal.task(
            f"c{index}",
            agent="worker",
            depends_on=(hub,),
            metadata=_derived_metadata(),
            verify=lambda index=index: _pass(f"c{index}"),
        )
    return goal


def test_high_fanout_node_is_strong_while_equivalent_leaf_is_cheap() -> None:
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

    assert hub.model_tier is ModelTier.STRONG
    assert hub.model_tier_fanout == 12
    assert hub.model_tier_on_critical_path is False
    assert hub.model_tier_rework_observed is False
    assert "model_tier_floor_fanout=12" in hub.reasons

    # Same graph, same absence of declared risk: only observed structure
    # differs, and a genuine leaf (present with fan-out 0) drops to CHEAP.
    assert leaf.model_tier is ModelTier.CHEAP
    assert leaf.model_tier_fanout == 0
    assert "model_tier_at_graph_floor" in leaf.reasons


def test_declared_preference_escalates_but_never_deescalates() -> None:
    # (a) declared risk raises a graph CHEAP leaf all the way to STRONG.
    escalated = plan_compute_routing(
        _state(ready=("L",), unlock=({"task_id": "L", "unlock_value": 0},)),
        CandidateTaskMetadata(
            task_id="L",
            criticality=10,
            downstream_fanout=10,
            failure_blast_radius=10,
        ),
    )
    assert escalated.model_tier is ModelTier.STRONG
    assert escalated.model_tier_fanout == 0
    assert "model_tier_raised_above_graph_floor" in escalated.reasons

    # (b) a caller declaring zero risk AND explicitly requesting CHEAP cannot
    # pull a high-fan-out node below the STRONG the graph justifies.
    protected = plan_compute_routing(
        _state(ready=("H",), unlock=({"task_id": "H", "unlock_value": 12},)),
        CandidateTaskMetadata(
            task_id="H",
            criticality=0,
            downstream_fanout=0,
            failure_blast_radius=0,
            input_stability=1.0,
            requested_model_tier=ModelTier.CHEAP,
        ),
    )
    assert protected.model_tier is ModelTier.STRONG
    assert protected.model_tier_fanout == 12
    assert "model_tier_at_graph_floor" in protected.reasons


def test_critical_path_membership_raises_a_zero_fanout_node_to_standard() -> None:
    decision = plan_compute_routing(
        _state(
            ready=("H",),
            critical_path=("H",),
            unlock=({"task_id": "H", "unlock_value": 0},),
        ),
        CandidateTaskMetadata(task_id="H"),
    )
    assert decision.model_tier is ModelTier.STANDARD
    assert decision.model_tier_on_critical_path is True
    assert "model_tier_floor_on_critical_path" in decision.reasons


def test_observed_rework_raises_a_zero_fanout_leaf_to_standard() -> None:
    # A fan-out-0 leaf that is being redone because its prior Evidence went
    # STALE is observed rework, not a declared opinion: re-spending the cheapest
    # model on already-thrown-away work compounds waste, so the floor is the
    # safe middle tier.
    decision = plan_compute_routing(
        _state(
            ready=("L",),
            unlock=({"task_id": "L", "unlock_value": 0},),
            stale=("L",),
        ),
        CandidateTaskMetadata(task_id="L"),
    )
    assert decision.model_tier is ModelTier.STANDARD
    assert decision.model_tier_rework_observed is True
    assert "model_tier_floor_rework_observed" in decision.reasons


def test_unobservable_fanout_fails_closed_to_standard_not_cheap() -> None:
    # ``H`` is missing from the fan-out projection: structure was not observed.
    # Fail-closed must NOT spend the cheapest model where structure is unknown.
    decision = plan_compute_routing(
        _state(ready=("H",), unlock=()),
        CandidateTaskMetadata(task_id="H"),
    )
    assert decision.model_tier is ModelTier.STANDARD
    assert decision.model_tier_fanout is None
    assert "model_tier_floor_fanout_unobservable" in decision.reasons
    assert any(item.name == "graph.downstream_fanout" for item in decision.unavailable)


def test_model_tier_floor_is_deterministic_across_insertion_orders() -> None:
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
    assert first.model_tier is second.model_tier is ModelTier.STRONG
    assert first.model_tier_fanout == second.model_tier_fanout == 12
    # No wall-clock, no RNG: the decision hash is a stable function of inputs.
    assert first.decision_hash == second.decision_hash


def test_provider_route_selects_model_key_from_graph_fanout() -> None:
    _models, _verifier, registry = _tiered_registry()
    metadata = _derived_metadata()

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
    assert hub_route.decision.model_tier is ModelTier.STRONG
    assert hub_route.model_key == "strong"

    assert leaf_route is not None
    assert leaf_route.decision.model_tier is ModelTier.CHEAP
    assert leaf_route.model_key == "cheap"


def test_strong_model_hook_actually_runs_for_high_fanout_node() -> None:
    models, verifier, registry = _tiered_registry()
    os_ = AgentOS(":memory:", provider_registry=registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = _hub_and_leaves_goal(os_, leaves=10)

        result = os_.run(goal, adaptive=True, max_dispatches=1, max_steps=2)

        # The graph-derived STRONG model ran for the hub; the cheaper model hooks
        # did not.  This is the routing changing execution, not just a label.
        assert models["strong"].calls == ["H"]
        assert models["standard"].calls == []
        assert models["cheap"].calls == []
        assert verifier.calls == ["H"]
        # Execution still went through the ordinary claim/Evidence path.
        assert "H" in result.verified
    finally:
        os_.close()


def test_graph_derived_routing_costs_less_than_route_everything_strong() -> None:
    """Total model spend over one goal is lower than routing everything strong.

    This establishes only that the graph-derived floor spends the strong model
    where observed consequence is high (the hub) and cheaper models elsewhere,
    so summed cost is strictly below a route-everything-strong policy on the
    *same* compiled graph.  The per-tier costs are deterministic bookkeeping; it
    does NOT establish anything about real model quality, price, or that a cheap
    model would actually succeed on the leaf tasks.
    """

    leaves = 10
    # ── graph-derived routing: model key derived per task from the decision ──
    derived_models, _dv, derived_registry = _tiered_registry()
    os_ = AgentOS(":memory:", provider_registry=derived_registry)
    try:
        os_.add_agent(Agent("worker"))
        goal = _hub_and_leaves_goal(os_, leaves=leaves)
        os_._compile_goal(goal)
        state = os_.runtime_state(goal)
        task_ids = ["H", *(f"c{index}" for index in range(leaves))]

        derived_keys: dict[str, str] = {}
        for task_id in task_ids:
            route = derived_registry.route_task(state, task_id, _derived_metadata())
            assert route is not None
            # Invoke the exact hook os.py would invoke for this attempt.
            derived_registry.execute_sync(route, task_id, None, None)
            assert route.model_key is not None
            derived_keys[task_id] = route.model_key
    finally:
        os_.close()

    # ── route-everything-strong on the same graph ──
    strong_models, _sv, strong_registry = _tiered_registry()
    os2 = AgentOS(":memory:", provider_registry=strong_registry)
    try:
        os2.add_agent(Agent("worker"))
        goal2 = _hub_and_leaves_goal(os2, leaves=leaves)
        os2._compile_goal(goal2)
        state2 = os2.runtime_state(goal2)
        task_ids = ["H", *(f"c{index}" for index in range(leaves))]
        for task_id in task_ids:
            route = strong_registry.route_task(state2, task_id, _forced_strong_metadata())
            assert route is not None
            strong_registry.execute_sync(route, task_id, None, None)
    finally:
        os2.close()

    # The hub routed strong; the critical-path leaf ``c0`` routed standard; the
    # remaining fan-out-0 leaves routed cheap.  All hooks actually ran.
    assert derived_keys["H"] == "strong"
    assert derived_keys["c0"] == "standard"
    assert {derived_keys[f"c{index}"] for index in range(1, leaves)} == {"cheap"}
    assert derived_models["strong"].calls == ["H"]
    assert derived_models["standard"].calls == ["c0"]
    assert derived_models["cheap"].calls == [f"c{index}" for index in range(1, leaves)]

    # Route-everything-strong invoked the strong hook for every task.
    assert strong_models["strong"].calls == ["H", *(f"c{index}" for index in range(leaves))]

    derived_cost = sum(model.total_cost for model in derived_models.values())
    strong_cost = sum(model.total_cost for model in strong_models.values())
    assert derived_cost == 25 + 5 + (leaves - 1) * 1  # hub strong + c0 std + 9 cheap
    assert strong_cost == (leaves + 1) * 25
    assert derived_cost < strong_cost
