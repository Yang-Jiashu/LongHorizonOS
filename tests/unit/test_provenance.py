"""Focused tests for the standalone v0.2 provenance primitives."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from lhos.provenance import (
    CoveragePolicy,
    ExecutionContext,
    InMemoryProvenanceStore,
    JSONLProvenanceStore,
    ProvenanceCoverageError,
    ProvenanceEvent,
    ProvenanceOperation,
    ProvenanceStoreCorruption,
    assess_coverage,
    evaluate_coverage,
)

FIXED_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _event(**overrides: object) -> ProvenanceEvent:
    values: dict[str, object] = {
        "graph_id": "g",
        "task_id": "t",
        "attempt_id": "a",
        "semantic_epoch": 2,
        "op": ProvenanceOperation.READ,
        "resource_uri": "workspace://input.txt",
        "observed_at": FIXED_TIME,
    }
    values.update(overrides)
    return ProvenanceEvent(**values)


def test_event_fingerprint_and_id_are_deterministic() -> None:
    first = _event()
    second = _event()
    assert first.fingerprint() == second.fingerprint()
    assert first.deterministic_id(1) == second.deterministic_id(1)

    committed_a = first.bind_chain(sequence=1, previous_hash="0" * 64)
    committed_b = second.bind_chain(sequence=1, previous_hash="0" * 64)
    assert committed_a == committed_b
    assert committed_a.event_hash == committed_a.compute_hash("0" * 64)


def test_memory_store_hash_chain_idempotency_and_replay() -> None:
    store = InMemoryProvenanceStore()
    first = store.append(_event(idempotency_key="read:1"))
    replayed = store.append(_event(idempotency_key="read:1"))
    second = store.append(_event(resource_uri="workspace://other", idempotency_key="read:2"))

    assert replayed == first
    assert second.sequence == 2
    assert store.replay() == (first, second)


def test_jsonl_store_replays_after_reopen_and_fails_closed_on_tamper(tmp_path) -> None:
    path = tmp_path / "provenance.jsonl"
    store = JSONLProvenanceStore(path)
    first = store.append(_event(idempotency_key="one"))
    store.append(_event(resource_uri="workspace://b", idempotency_key="two"))
    store.close()

    reopened = JSONLProvenanceStore(path)
    assert reopened.replay()[0] == first
    reopened.close()

    rows = path.read_text(encoding="utf-8").splitlines()
    payload = json.loads(rows[0])
    payload["resource_uri"] = "workspace://tampered"
    path.write_text("\n".join([json.dumps(payload), rows[1]]) + "\n", encoding="utf-8")
    with pytest.raises(ProvenanceStoreCorruption):
        JSONLProvenanceStore(path)


def test_coverage_complete_and_strict_admission() -> None:
    event = _event()
    report = assess_coverage(
        ["workspace://input.txt"],
        [event],
        graph_id="g",
        task_id="t",
        required_operations=["read"],
    )
    assert report.status == "COMPLETE"
    assert report.missing_inputs == ()
    assert report.report_hash
    decision = evaluate_coverage(report, CoveragePolicy.STRICT)
    assert decision.allowed


def test_coverage_unknown_is_denied_in_strict_but_audited_in_audit() -> None:
    store = InMemoryProvenanceStore()
    with ExecutionContext("g", task_id="t", store=store) as context:
        context.observe_unknown(resource_hint="env://hidden")
    report = assess_coverage(["workspace://input.txt"], store.replay())
    assert report.status == "UNKNOWN"
    audit = evaluate_coverage(report, CoveragePolicy.AUDIT)
    assert audit.allowed
    assert audit.warnings
    strict = evaluate_coverage(report, CoveragePolicy.STRICT)
    assert not strict.allowed
    with pytest.raises(ProvenanceCoverageError):
        from lhos.provenance import enforce_coverage

        enforce_coverage(report, CoveragePolicy.STRICT)


def test_legacy_and_audit_allow_partial_coverage() -> None:
    report = assess_coverage(["workspace://declared"], [_event()])
    assert report.status == "PARTIAL"
    legacy = evaluate_coverage(report, "legacy")
    audit = evaluate_coverage(report, "audit")
    assert legacy.allowed and audit.allowed
    assert legacy.warnings and audit.warnings


def test_context_helpers_record_typed_operations() -> None:
    context = ExecutionContext("g", task_id="t")
    context.read("file://a")
    context.write("file://b")
    context.record_tool("grep", action_id="act-1")
    context.record_network("https://example.test", content_hash="h")
    context.record_model("model-x", prompt_hash="p", schema_hash="s")
    ops = [event.op for event in context.events]
    assert ops == [
        ProvenanceOperation.READ,
        ProvenanceOperation.WRITE,
        ProvenanceOperation.TOOL,
        ProvenanceOperation.NETWORK,
        ProvenanceOperation.MODEL,
    ]
