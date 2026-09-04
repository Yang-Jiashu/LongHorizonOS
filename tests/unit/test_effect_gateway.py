"""Secure ActionGateway boundary tests for ExecutionContext."""

from __future__ import annotations

import pytest

from lhos.provenance import (
    EffectBoundaryError,
    EffectReceipt,
    EffectRequest,
    EffectSubmissionError,
    ExecutionContext,
)


class _Gateway:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.requests: list[EffectRequest] = []
        self.result = result
        self.error = error

    def submit(self, request: EffectRequest):
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.result or {
            "effect_id": request.effect_id,
            "status": "completed",
            "action_id": f"action-{request.effect_id}",
            "idempotency_key": (
                None if request.declaration is None else request.declaration.idempotency_key
            ),
        }


def test_secure_context_rejects_undeclared_effect_before_gateway() -> None:
    gateway = _Gateway()
    context = ExecutionContext(
        "g",
        task_id="t",
        attempt_id="a",
        secure_mode=True,
        action_gateway=gateway,
    )

    with pytest.raises(EffectBoundaryError, match="not declared"):
        context.submit_effect("e1", "send")
    assert gateway.requests == []
    assert context.effect_receipts == ()


def test_secure_context_requires_idempotency_for_non_pure_effect() -> None:
    gateway = _Gateway()
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect("e1", side_effect_class="irreversible", operation="send")

    with pytest.raises(EffectBoundaryError, match="idempotency_key"):
        context.submit_effect("e1", "send")
    assert gateway.requests == []


def test_secure_gateway_receives_identity_and_records_completed_write() -> None:
    gateway = _Gateway()
    context = ExecutionContext(
        "graph-1",
        task_id="task-1",
        attempt_id="attempt-1",
        semantic_epoch=7,
        secure_mode=True,
        action_gateway=gateway,
    )
    context.claim_id = "claim-1"
    context.declare_effect(
        "e1",
        side_effect_class="idempotent",
        operation="send",
        resource_uri="sink://mail",
        idempotency_key="idem-1",
    )

    receipt = context.submit_effect("e1", "send", arguments={"to": "x"})

    assert isinstance(receipt, EffectReceipt)
    assert receipt.status == "completed"
    assert len(gateway.requests) == 1
    request = gateway.requests[0]
    assert request.graph_id == "graph-1"
    assert request.task_id == "task-1"
    assert request.claim_id == "claim-1"
    assert request.attempt_id == "attempt-1"
    assert request.semantic_epoch == 7
    assert request.arguments == {"to": "x"}
    writes = [event for event in context.events if event.resource_uri == "sink://mail"]
    assert len(writes) == 1
    assert writes[0].known is True
    assert writes[0].action_id == "action-e1"


def test_gateway_exception_is_uncertain_and_never_silently_completed() -> None:
    gateway = _Gateway(error=RuntimeError("sink timeout"))
    context = ExecutionContext(
        "g",
        task_id="t",
        secure_mode=True,
        action_gateway=gateway,
    )
    context.declare_effect(
        "e1",
        side_effect_class="irreversible",
        operation="charge",
        idempotency_key="charge-1",
        resource_uri="sink://payments",
    )

    with pytest.raises(EffectSubmissionError, match="submission failed"):
        context.submit_effect("e1", "charge")

    assert len(context.effect_receipts) == 0
    events = [event for event in context.events if event.resource_uri == "sink://payments"]
    assert len(events) == 1
    assert events[0].known is False
    assert events[0].metadata["status"] == "uncertain"


def test_mismatched_gateway_receipt_fails_closed() -> None:
    gateway = _Gateway(
        result={
            "effect_id": "other",
            "status": "completed",
            "action_id": "a-other",
        }
    )
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect(
        "e1",
        side_effect_class="idempotent",
        operation="write",
        idempotency_key="idem-1",
    )

    with pytest.raises(EffectSubmissionError, match="does not match"):
        context.submit_effect("e1", "write")
    assert context.effect_receipts == ()
    assert len(context.events) == 1
    event = context.events[0]
    assert event.op.value == "write"
    assert event.resource_uri == "effect:e1"
    assert event.known is False
    assert event.action_id is None
    assert event.metadata["status"] == "uncertain"
    assert event.metadata["reason"] == "untrusted_gateway_receipt"


def test_malformed_gateway_receipt_records_uncertain_write() -> None:
    gateway = _Gateway(result={"effect_id": "e1"})
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect(
        "e1",
        side_effect_class="idempotent",
        operation="write",
        resource_uri="sink://external",
        idempotency_key="idem-1",
    )

    with pytest.raises(EffectSubmissionError, match="malformed receipt"):
        context.submit_effect("e1", "write")

    assert context.effect_receipts == ()
    assert len(context.events) == 1
    event = context.events[0]
    assert event.resource_uri == "sink://external"
    assert event.known is False
    assert event.idempotency_key == "idem-1"
    assert event.metadata["status"] == "uncertain"
    assert event.metadata["reason"] == "untrusted_gateway_receipt"


def test_mismatched_receipt_idempotency_records_uncertain_write() -> None:
    gateway = _Gateway(
        result={
            "effect_id": "e1",
            "status": "completed",
            "action_id": "a-e1",
            "idempotency_key": "wrong-key",
        }
    )
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect(
        "e1",
        side_effect_class="idempotent",
        operation="write",
        resource_uri="sink://external",
        idempotency_key="idem-1",
    )

    with pytest.raises(EffectSubmissionError, match="idempotency key does not match"):
        context.submit_effect(
            "e1",
            "write",
            status="caller-cannot-override",
            reason="caller-cannot-override",
        )

    assert context.effect_receipts == ()
    assert len(context.events) == 1
    event = context.events[0]
    assert event.known is False
    assert event.action_id is None
    assert event.metadata["status"] == "uncertain"
    assert event.metadata["reason"] == "untrusted_gateway_receipt"
    assert "does not match" in event.metadata["error"]


@pytest.mark.parametrize("status", ["uncertain", "rejected"])
def test_secure_non_completed_receipt_blocks_semantic_progress(status: str) -> None:
    gateway = _Gateway(
        result={
            "effect_id": "e1",
            "status": status,
            "action_id": "a-e1",
            "idempotency_key": "idem-1",
        }
    )
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect(
        "e1",
        side_effect_class="idempotent",
        operation="write",
        idempotency_key="idem-1",
        resource_uri="sink://external",
    )

    with pytest.raises(EffectSubmissionError, match=status):
        context.submit_effect("e1", "write")

    assert context.effect_receipts[0].status == status
    event = context.events[-1]
    assert event.known is False
    assert event.metadata["status"] == status


def test_secure_non_pure_receipt_must_bind_idempotency_key() -> None:
    gateway = _Gateway(
        result={
            "effect_id": "e1",
            "status": "completed",
            "action_id": "a-e1",
        }
    )
    context = ExecutionContext("g", task_id="t", secure_mode=True, action_gateway=gateway)
    context.declare_effect(
        "e1",
        side_effect_class="irreversible",
        operation="charge",
        idempotency_key="charge-1",
    )

    with pytest.raises(EffectSubmissionError, match="idempotency_key"):
        context.submit_effect("e1", "charge")
    assert context.effect_receipts == ()


def test_submit_tool_declares_and_submits_one_effect() -> None:
    gateway = _Gateway()
    context = ExecutionContext("g", task_id="t", action_gateway=gateway)

    receipt = context.submit_tool(
        "filesystem.write",
        arguments={"path": "out.txt"},
        side_effect_class="idempotent",
        idempotency_key="write-1",
    )

    assert receipt.status == "completed"
    assert context.effect_declarations[0].effect_id == "tool:filesystem.write"
    assert gateway.requests[0].declaration is not None
    assert gateway.requests[0].declaration.idempotency_key == "write-1"
