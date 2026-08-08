"""D2-I10: Eligibility must be deterministic and auditable."""

from __future__ import annotations

from lhos.runtimes.multi_agent import AgentDescriptor
from lhos.runtimes.multi_agent.eligibility import evaluate_eligibility


def _agent(aid, pid, **kw):
    return AgentDescriptor(agent_id=aid, process_id=pid, **kw)


def test_eligible_python_code_review():
    agent = _agent(
        "a1",
        "p1",
        specializations=("python", "review"),
        supported_task_kinds=("code_review",),
        supported_tools=("shell", "git"),
    )
    r = evaluate_eligibility(
        agent,
        task_id="t1",
        graph_id="g1",
        graph_version=5,
        task_kind="code_review",
        required_specializations=("python",),
        required_tools=("shell",),
        required_capabilities=(),
        readiness_version=5,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert r.eligible
    assert r.reasons == ()


def test_rejects_disabled_agent_when_process_live():
    agent = _agent("a1", "p1", enabled=False)
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("disabled" in reason for reason in r.reasons)


def test_rejects_missing_process():
    agent = _agent("a1", "dead-pid")
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
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
    assert any("does not exist" in reason for reason in r.reasons)


def test_rejects_exited_process_due_to_liveness():
    agent = _agent("a1", "p1")
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="exited",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("terminal" in reason for reason in r.reasons)


def test_rejects_capacity_exhaustion():
    agent = _agent("a1", "p1", max_concurrency=1)
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=1,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("capacity" in reason for reason in r.reasons)


def test_max_concurrency_zero_always_ineligible():
    agent = _agent("a1", "p1", max_concurrency=0)
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible


def test_rejects_missing_specialization():
    agent = _agent("a1", "p1", specializations=("go",))
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=("python",),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible
    assert any("specializations" in reason for reason in r.reasons)


def test_rejects_missing_tool():
    agent = _agent("a1", "p1", supported_tools=("git",))
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=("shell",),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible


def test_kind_wildcard_matches_anything():
    agent = _agent(
        "a1",
        "p1",
        supported_task_kinds=("*",),
    )
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="some_random_kind",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert r.eligible


def test_required_task_kind_must_be_supported():
    agent = _agent(
        "a1",
        "p1",
        supported_task_kinds=("coding",),
    )
    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="code_review",
        required_specializations=(),
        required_tools=(),
        required_capabilities=(),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=None,
    )
    assert not r.eligible


def test_rejects_missing_kernel_capability():
    agent = _agent(
        "a1",
        "p1",
        supported_task_kinds=("*",),
    )

    class _CapCheck:
        def check(self, pid, resource, operation):
            return False

    r = evaluate_eligibility(
        agent,
        "t1",
        "g1",
        1,
        task_kind="",
        required_specializations=(),
        required_tools=(),
        required_capabilities=("device:tool/mock:invoke",),
        readiness_version=1,
        active_claims_for_agent=0,
        process_state="running",
        process_exists=True,
        capability_checker=_CapCheck(),
    )
    assert not r.eligible
    assert any("capabilities" in reason for reason in r.reasons)


def test_determinism_across_reason_order():
    """Deterministic reasons ordering across runs (D2-I10)."""
    agent = _agent("a1", "p1")
    args = dict(
        task_id="t1",
        graph_id="g1",
        graph_version=1,
        task_kind="coding",
        required_specializations=("python", "rust"),
        required_tools=("shell",),
        required_capabilities=("device:tool/mock:invoke",),
        readiness_version=1,
        active_claims_for_agent=5,
        process_state="exited",
        process_exists=True,
        capability_checker=None,
    )
    r1 = evaluate_eligibility(agent, **args)
    r2 = evaluate_eligibility(agent, **args)
    assert r1.reasons == r2.reasons
