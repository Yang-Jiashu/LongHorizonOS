"""Mutation-test style checks that the Scheduler's core safety invariants
hold even when implementation logic is deliberately weakened.

These aren't real mutation-analysis (which mutates source under the hood);
rather they assert the *observations* that a mutation SHOULD cause a test
failure for.  If a test here is weakened, a regression has slipped in.

Each check covers one or more of D2-01..D2-20 from the spec's audit table.
"""

from __future__ import annotations

import copy

from lhos.runtimes.multi_agent.eligibility import evaluate_eligibility
from lhos.runtimes.multi_agent.matching import match_deterministic_best_fit_v1
from lhos.runtimes.multi_agent.models import (
    AgentDescriptor,
    ClaimState,
    TaskClaim,
)
from lhos.runtimes.multi_agent.projections import active_claim_count_by_agent
from lhos.runtimes.multi_agent.reconciliation import detect_invariants_violations


# ── helpers ──────────────────────────────────────────────────────────────
def _agent(**kw):
    defaults = dict(
        agent_id="a",
        process_id="p",
        supported_task_kinds=("*",),
        specializations=(),
        max_concurrency=1,
        cost_weight=100,
        enabled=True,
    )
    defaults.update(kw)
    return AgentDescriptor(**defaults)


# ── mutations ────────────────────────────────────────────────────────────
def test_m01_disabled_agent_always_ineligible():
    a = _agent(enabled=False)
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert "agent disabled" in r.reasons


def test_m02_nonexistent_process_rejected():
    a = _agent()
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state=None,
        process_exists=False,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("does not exist" in x for x in r.reasons)


def test_m03_terminal_process_rejected():
    a = _agent()
    for term in ("exited", "failed"):
        r = evaluate_eligibility(
            a,
            "t",
            "g",
            1,
            task_kind="x",
            required_specializations=(),
            required_tools=(),
            required_capabilities=(),
            readiness_version=1,
            active_claims_for_agent=0,
            process_state=term,
            process_exists=True,
            capability_checker=None,
        )
        assert not r.eligible
        assert any("terminal" in x for x in r.reasons)


def test_m04_capacity_exhausted_rejected():
    a = _agent(max_concurrency=1)
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=1,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("capacity" in x.lower() or "exhausted" in x.lower() for x in r.reasons)


def test_m05_missing_specialization_rejected():
    a = _agent(specializations=("python",))
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=("python", "rust"),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("specialization" in x for x in r.reasons)


def test_m06_missing_tool_rejected():
    a = _agent(supported_tools=("bash",))
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=(),
        required_tools=("bash", "git"),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("tool" in x for x in r.reasons)


def test_m07_missing_kernel_capability_rejected():
    class _Cap:
        def check(self, pid, r, o):
            return False

    a = _agent()
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="x",
        required_specializations=(),
        required_tools=(),
        required_capabilities=("device:tool/mock:invoke",),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=_Cap(),
    )
    assert not r.eligible
    assert any("kernel capability" in x.lower() or "missing kernel" in x.lower() for x in r.reasons)


def test_m08_task_kind_mismatch_rejected():
    a = _agent(supported_task_kinds=("code_review", "test"))
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="deploy",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("task_kind" in x for x in r.reasons)


def test_m09_wildcard_accepts_any_kind():
    a = _agent(supported_task_kinds=("*",))
    r = evaluate_eligibility(
        a,
        "t",
        "g",
        1,
        task_kind="deploy",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="ready",
        process_exists=True,
        capability_checker=None,
    )
    assert r.eligible


def test_m10_matching_deterministic():
    pool = [_agent(agent_id="a", cost_weight=100), _agent(agent_id="b", cost_weight=50)]
    d = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=copy.deepcopy(pool),
        active_claims_by_agent={},
    )
    assert d.selected_agent_id == "b"


def test_m11_matching_insertion_order_invariance():
    one = [_agent(agent_id="a"), _agent(agent_id="b"), _agent(agent_id="c")]
    two = [_agent(agent_id="c"), _agent(agent_id="a"), _agent(agent_id="b")]
    d1 = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=copy.deepcopy(one),
        active_claims_by_agent={},
    )
    d2 = match_deterministic_best_fit_v1(
        graph_id="g",
        graph_version=1,
        task_id="t",
        task_priority=0,
        eligible_agents=copy.deepcopy(two),
        active_claims_by_agent={},
    )
    assert d1.decision_hash == d2.decision_hash


def test_m12_i4_detect_multi_active_per_task():
    claims = [
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t",
            agent_id="a1",
            process_id="p1",
            lease_resource="r",
            state=ClaimState.ACTIVE,
        ),
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t",
            agent_id="a2",
            process_id="p2",
            lease_resource="r",
            state=ClaimState.ACTIVE,
        ),
    ]
    v = detect_invariants_violations(
        claims, lease_is_live=lambda lid: True, process_is_alive=lambda pid: True
    )
    assert any("D2-I4" in x for x in v)


def test_m13_i5_active_claim_requires_lease_id():
    claims = [
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.ACTIVE,
            lease_id=None,
        ),
    ]
    v = detect_invariants_violations(
        claims, lease_is_live=lambda lid: True, process_is_alive=lambda pid: True
    )
    assert any("D2-I5" in x for x in v)


def test_m14_i5_active_claim_requires_live_lease():
    claims = [
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.ACTIVE,
            lease_id="dead",
        ),
    ]
    v = detect_invariants_violations(
        claims, lease_is_live=lambda lid: False, process_is_alive=lambda pid: True
    )
    assert any("D2-I5" in x for x in v)


def test_m15_i7_dead_process_is_violation():
    claims = [
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.ACTIVE,
            lease_id="l",
        ),
    ]
    v = detect_invariants_violations(
        claims, lease_is_live=lambda lid: True, process_is_alive=lambda pid: False
    )
    assert any("D2-I7" in x for x in v)


def test_m16_active_claim_count_helper():
    claims = [
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t1",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.ACTIVE,
        ),
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t2",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.ACTIVE,
        ),
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t3",
            agent_id="a",
            process_id="p",
            lease_resource="r",
            state=ClaimState.COMPLETED,
        ),
        TaskClaim(
            graph_id="g",
            graph_version=1,
            task_id="t4",
            agent_id="b",
            process_id="q",
            lease_resource="r",
            state=ClaimState.ACTIVE,
        ),
    ]
    counts = active_claim_count_by_agent(claims)
    assert counts["a"] == 2
    assert counts["b"] == 1


def _active_only_counts():
    return {"a": 2}


def test_m17_claim_state_enum_complete():
    """Every ClaimState value referenced in the codebase exists."""
    values = {e.value for e in ClaimState}
    for s in ("proposed", "acquiring", "active", "released", "lost", "completed", "rejected"):
        assert s in values


def test_m20_drei_plus_states_are_terminal():
    from lhos.runtimes.multi_agent.models import TERMINAL_CLAIM_STATES

    for s in (ClaimState.COMPLETED, ClaimState.LOST, ClaimState.RELEASED, ClaimState.REJECTED):
        assert s in TERMINAL_CLAIM_STATES
    # PROPOSED / ACQUIRING / ACTIVE are NOT terminal.
    for s in (ClaimState.PROPOSED, ClaimState.ACQUIRING, ClaimState.ACTIVE):
        assert s not in TERMINAL_CLAIM_STATES
