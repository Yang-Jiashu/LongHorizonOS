"""Context Hot-Potato unit tests (P0 prototype)."""
from __future__ import annotations

import time

import pytest

from lhos.sdk.context_hot_potato import (
    ContextDelta,
    ContextHotPotato,
    ContextMigrationTicket,
)


def test_delta_token_estimate() -> None:
    delta = ContextDelta(
        new_messages=({"role": "user", "content": "hello world"},),
        new_tool_calls=({"name": "read_file", "args": {"path": "/tmp/x"}},),
        scratch_pad="thinking about the problem",
    )
    assert delta.token_estimate > 0
    assert not delta.is_empty()


def test_empty_delta() -> None:
    delta = ContextDelta()
    assert delta.is_empty()
    assert delta.token_estimate == 0


def test_create_and_apply_ticket() -> None:
    hp = ContextHotPotato()
    delta = ContextDelta(
        new_messages=({"role": "assistant", "content": "I found the bug"},),
        scratch_pad="bug is in module x",
    )
    ticket = hp.create_ticket(
        source_claim_id="claim-abc",
        target_task_id="task-2",
        graph_id="g1",
        graph_version=3,
        l0_refs=("hash-goal",),
        l1_refs=("hash-evidence-1",),
        l2_delta=delta,
    )
    assert ticket.ticket_id.startswith("hpt-")
    assert hp.tickets_created == 1

    merged = hp.apply_ticket(ticket)
    assert merged["migrated_from"] == "claim-abc"
    assert len(merged["messages"]) == 1
    assert merged["scratch_pad"] == "bug is in module x"
    assert "hash-goal" in merged["l0_refs"]
    assert hp.tickets_applied == 1


def test_ticket_expiry() -> None:
    ticket = ContextMigrationTicket(
        source_claim_id="c1", target_task_id="t1",
        graph_id="g1", graph_version=1, ttl_seconds=0.01,
    )
    time.sleep(0.02)
    assert ticket.is_expired()

    hp = ContextHotPotato()
    with pytest.raises(ValueError, match="expired"):
        hp.apply_ticket(ticket)


def test_estimate_savings() -> None:
    hp = ContextHotPotato()
    delta = ContextDelta(scratch_pad="x" * 400)  # ~100 tokens
    ticket = hp.create_ticket(
        source_claim_id="c1", target_task_id="t1",
        graph_id="g1", graph_version=1, l2_delta=delta,
    )
    cold = 5000
    savings = hp.estimate_savings(ticket, cold)
    assert savings == cold - delta.token_estimate
    assert savings > 0


def test_ticket_fingerprint_deterministic() -> None:
    t1 = ContextMigrationTicket(
        source_claim_id="c1", target_task_id="t1",
        graph_id="g1", graph_version=1,
        l0_refs=("a",), l1_refs=("b",),
        l2_delta=ContextDelta(scratch_pad="test"),
    )
    t2 = ContextMigrationTicket(
        source_claim_id="c1", target_task_id="t1",
        graph_id="g1", graph_version=1,
        l0_refs=("a",), l1_refs=("b",),
        l2_delta=ContextDelta(scratch_pad="test"),
    )
    # ticket_id differs but fingerprint is content-based
    assert t1.ticket_id != t2.ticket_id
    assert t1.fingerprint() == t2.fingerprint()


def test_apply_merges_with_existing_context() -> None:
    hp = ContextHotPotato()
    delta = ContextDelta(
        new_messages=({"role": "assistant", "content": "continuation"},),
    )
    ticket = hp.create_ticket(
        source_claim_id="c1", target_task_id="t1",
        graph_id="g1", graph_version=1, l2_delta=delta,
    )
    existing = {
        "messages": [{"role": "user", "content": "original"}],
        "scratch_pad": "old notes ",
    }
    merged = hp.apply_ticket(ticket, current_context=existing)
    assert len(merged["messages"]) == 2
    assert merged["messages"][0]["content"] == "original"
    assert merged["messages"][1]["content"] == "continuation"
    assert merged["scratch_pad"] == "old notes "
