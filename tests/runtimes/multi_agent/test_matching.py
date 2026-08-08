"""Deterministic matching (D2-I11, Section 13)."""
from __future__ import annotations

from lhos.runtimes.multi_agent import AgentDescriptor
from lhos.runtimes.multi_agent.matching import (
    match_deterministic_best_fit_v1,
)


def _agent(aid, cost=100, specs=(), tools=(), kinds=("*",)):
    return AgentDescriptor(
        agent_id=aid,
        process_id=f"pid-{aid}",
        specializations=specs,
        supported_tools=tools,
        supported_task_kinds=kinds,
        cost_weight=cost,
    )


def test_prefers_cheaper_agent_when_scores_equal_otherwise():
    lo = _agent("a-low", cost=50)
    hi = _agent("a-high", cost=200)
    d = match_deterministic_best_fit_v1(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0, eligible_agents=[hi, lo],
        active_claims_by_agent={}, preferred_specializations=(),
    )
    assert d.selected_agent_id == "a-low"


def test_prefers_lower_load_when_cost_tie():
    a1 = _agent("a1", cost=100)
    a2 = _agent("a2", cost=100)
    d = match_deterministic_best_fit_v1(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0, eligible_agents=[a1, a2],
        active_claims_by_agent={"a1": 2, "a2": 0},
        preferred_specializations=(),
    )
    assert d.selected_agent_id == "a2"


def test_agent_id_ascending_tiebreak():
    b = _agent("b", cost=100)
    a = _agent("a", cost=100)
    c = _agent("c", cost=100)
    d = match_deterministic_best_fit_v1(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0, eligible_agents=[b, c, a],
        active_claims_by_agent={},
        preferred_specializations=(),
    )
    assert d.selected_agent_id == "a"
    assert {c.agent_id for c in d.candidates} == {"a", "b", "c"}


def test_preferred_specialization_bonuses_score():
    a1 = _agent("a1", specs=("python",))
    a2 = _agent("a2", specs=())
    d = match_deterministic_best_fit_v1(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0, eligible_agents=[a2, a1],
        active_claims_by_agent={},
        preferred_specializations=("python", "rust"),
    )
    assert d.selected_agent_id == "a1"


def test_decision_hash_deterministic():
    a1 = _agent("a1")
    a2 = _agent("a2")
    args = dict(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0, eligible_agents=[a2, a1],
        active_claims_by_agent={}, preferred_specializations=(),
    )
    h1 = match_deterministic_best_fit_v1(**args).decision_hash
    h2 = match_deterministic_best_fit_v1(**args).decision_hash
    assert h1 == h2
    assert len(h1) == 64


def test_candidates_vector_deterministic_across_insertion_order():
    a1 = _agent("a1")
    a2 = _agent("a2")
    a3 = _agent("a3")
    args = dict(
        graph_id="g1", graph_version=1, task_id="t1",
        task_priority=0,
        active_claims_by_agent={}, preferred_specializations=(),
    )
    d1 = match_deterministic_best_fit_v1(**dict(args, eligible_agents=[a3, a1, a2]))
    d2 = match_deterministic_best_fit_v1(**dict(args, eligible_agents=[a1, a2, a3]))
    assert [c.agent_id for c in d1.candidates] == [c.agent_id for c in d2.candidates]
