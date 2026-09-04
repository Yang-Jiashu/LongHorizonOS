"""Execution Attempt lifecycle — operational success != semantic verification.

Verifies the AttemptManager state machine (Section 22): the boundary
between SUCCEEDED_OPERATIONALLY and VERIFIED_SEMANTICALLY is enforced.
"""

from __future__ import annotations

from lhos.runtimes.multi_agent.attempts import AttemptManager
from lhos.runtimes.multi_agent.models import AttemptState


def test_full_attempt_lifecycle():
    mgr = AttemptManager()
    a = mgr.start_attempt(
        attempt_id="att-1",
        task_id="t1",
        claim_id="c1",
        agent_id="a1",
        process_id="p1",
    )
    assert a.state == AttemptState.DISPATCHED
    mgr.mark_running(a)
    assert a.state == AttemptState.RUNNING
    mgr.mark_operationally_succeeded(a)
    assert a.state == AttemptState.SUCCEEDED_OPERATIONALLY
    mgr.mark_semantically_verified(a)
    assert a.state == AttemptState.VERIFIED_SEMANTICALLY


def test_operational_semantic_boundary_is_fail_closed():
    """Promoting from RUNNING cannot bypass the operational milestone."""
    mgr = AttemptManager()
    a = mgr.start_attempt(
        attempt_id="att-1", task_id="t1", claim_id="c1", agent_id="a1", process_id="p1"
    )
    mgr.mark_running(a)
    # Attempt to verify semantically without operational success.
    assert not mgr.mark_semantically_verified(a)
    assert a.state == AttemptState.RUNNING
    assert a.error is None


def test_semantic_verification_replay_is_idempotent_and_terminal_states_stay_closed():
    mgr = AttemptManager()
    failed = mgr.start_attempt(
        attempt_id="att-failed",
        task_id="t1",
        claim_id="c-failed",
        agent_id="a1",
        process_id="p1",
    )
    mgr.mark_failed(failed, error="executor failed")
    assert not mgr.mark_semantically_verified(failed)
    assert failed.state == AttemptState.FAILED

    succeeded = mgr.start_attempt(
        attempt_id="att-succeeded",
        task_id="t1",
        claim_id="c-succeeded",
        agent_id="a1",
        process_id="p1",
    )
    mgr.mark_running(succeeded)
    mgr.mark_operationally_succeeded(succeeded)
    assert mgr.mark_semantically_verified(succeeded)
    assert mgr.mark_semantically_verified(succeeded)
    assert succeeded.state == AttemptState.VERIFIED_SEMANTICALLY


def test_crashed_attempt_records_error_and_end():
    mgr = AttemptManager()
    a = mgr.start_attempt(
        attempt_id="att-1", task_id="t1", claim_id="c1", agent_id="a1", process_id="p1"
    )
    mgr.mark_running(a)
    mgr.mark_crashed(a, error="sigkill")
    assert a.state == AttemptState.CRASHED
    assert a.error == "sigkill"
    assert a.ended_at is not None


def test_failed_attempt_records_end():
    mgr = AttemptManager()
    a = mgr.start_attempt(
        attempt_id="att-1", task_id="t1", claim_id="c1", agent_id="a1", process_id="p1"
    )
    mgr.mark_failed(a, error="nonzero exit")
    assert a.state == AttemptState.FAILED
    assert a.ended_at is not None


def test_attempts_for_task_filter_and_latest():
    mgr = AttemptManager()
    mgr.start_attempt(
        attempt_id="att-1", task_id="t1", claim_id="c1", agent_id="a1", process_id="p1"
    )
    mgr.start_attempt(
        attempt_id="att-2", task_id="t1", claim_id="c2", agent_id="a2", process_id="p2"
    )
    mgr.start_attempt(
        attempt_id="att-3", task_id="t2", claim_id="c3", agent_id="a1", process_id="p1"
    )
    t1 = mgr.attempts_for_task("t1")
    assert len(t1) == 2
    latest = mgr.latest_attempt_for_task("t1")
    assert latest is not None
    assert latest.attempt_id == "att-2"
    assert mgr.count_attempts_for_task("t1") == 2
    assert mgr.count_attempts_for_task("t2") == 1
    assert mgr.count_attempts_for_task("missing") == 0
    assert mgr.latest_attempt_for_task("missing") is None


def test_get_by_id_and_all():
    mgr = AttemptManager()
    a = mgr.start_attempt(
        attempt_id="att-x", task_id="t", claim_id="c", agent_id="a", process_id="p"
    )
    assert mgr.get("att-x") is a
    assert mgr.get("nope") is None
    assert mgr.all_attempts() == [a]
