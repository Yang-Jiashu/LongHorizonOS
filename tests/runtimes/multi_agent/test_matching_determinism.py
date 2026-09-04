"""Matching Determinism Audit (Section 47).

`deterministic_best_fit_v1` MUST produce the same decision_hash regardless
of agent insertion order or hash seed.
"""

from __future__ import annotations

import copy

from lhos.runtimes.multi_agent.matching import match_deterministic_best_fit_v1
from lhos.runtimes.multi_agent.models import AgentDescriptor


def _agent(aid, cost, specs=("python",)):
    return AgentDescriptor(
        agent_id=aid,
        process_id=f"pid-{aid}",
        supported_task_kinds=("*",),
        specializations=specs,
        cost_weight=cost,
    )


def test_same_pool_same_hash_regardless_of_order():
    pool_a = [_agent("a1", 100), _agent("a2", 200), _agent("a3", 300)]
    pool_b = [_agent("a3", 300), _agent("a1", 100), _agent("a2", 200)]
    d1 = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=pool_a,
        active_claims_by_agent={},
        preferred_specializations=("python",),
    )
    d2 = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=pool_b,
        active_claims_by_agent={},
        preferred_specializations=("python",),
    )
    assert d1.decision_hash == d2.decision_hash
    assert d1.selected_agent_id == d2.selected_agent_id


def test_candidates_vector_order_deterministic():
    """Candidates tuple in the decision is sorted deterministically."""
    pool = [_agent("z", 100), _agent("a", 100), _agent("m", 100)]
    d = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=pool,
        active_claims_by_agent={},
    )
    ids = tuple(c.agent_id for c in d.candidates)
    # All equal-cost, equal-load, no preferred specs: sort by score DESC,
    # then load, then cost, then agent_id ASC. The FIRST candidate is the
    # winner; with equal scores the smallest agent_id wins -> 'a'.
    assert ids[0] == d.selected_agent_id == "a"


def test_repeat_runs_identical_hash():
    pool = [_agent("x", 100), _agent("y", 150), _agent("z", 100)]
    hashes = set()
    for _ in range(10):
        d = match_deterministic_best_fit_v1(
            graph_id="g",
            graph_version=2,
            task_id="t",
            task_priority=1,
            eligible_agents=copy.deepcopy(pool),
            active_claims_by_agent={"x": 0, "y": 0, "z": 0},
            preferred_specializations=("python", "typing"),
        )
        hashes.add(d.decision_hash)
    assert len(hashes) == 1


def test_cheaper_agent_preferred_on_tie():
    pool = [_agent("expensive", 200), _agent("cheap", 50)]
    d = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=pool,
        active_claims_by_agent={},
    )
    assert d.selected_agent_id == "cheap"


def test_decision_hash_changes_with_inputs():
    base_pool = [_agent("a", 100), _agent("b", 200)]
    d_base = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=base_pool,
        active_claims_by_agent={},
    )
    d_priority = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=5,
        eligible_agents=[_agent("a", 100), _agent("b", 200)],
        active_claims_by_agent={},
    )
    assert d_base.decision_hash != d_priority.decision_hash
