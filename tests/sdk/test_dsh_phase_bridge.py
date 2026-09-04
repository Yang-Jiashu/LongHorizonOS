"""Tests for the DeepSeek Harness rc.8 phase telemetry bridge."""

from __future__ import annotations

import json

import pytest

from scripts.lhtb_dsh_phase_bridge import (
    BRIDGE_SCHEMA_VERSION,
    BridgeBinding,
    BridgeValidationError,
    build_phase_bridge_js,
    parse_phase_bridge_ndjson,
    summarize_phase_bridge,
)


@pytest.fixture
def binding() -> BridgeBinding:
    return BridgeBinding(
        claim_id="claim-a",
        attempt_id="attempt-a",
        semantic_epoch=3,
        lease_fencing_token=8,
    )


def _line(binding: BridgeBinding, bridge_seq: int, source_seq: int, *, event_type: str) -> str:
    return json.dumps(
        {
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "record_type": "session_event",
            "bridge_seq": bridge_seq,
            "invocation_id": "invocation-a",
            "binding": binding.as_dict(),
            "session_id": "session-a",
            "source_seq": source_seq,
            "event_type": event_type,
            "turn": 1,
            "step": 1,
            "usage": {
                "output_tokens": 4,
                "model_calls": 1,
                "tool_calls": 1 if event_type == "tool/call" else 0,
            },
            "status": None,
            "tool_name": "bash" if event_type == "tool/call" else None,
            "tool_name_sha256": None,
            "compaction_id": None,
            "durability": None,
        }
    )


def test_parse_valid_records_and_summarize(binding: BridgeBinding) -> None:
    records = parse_phase_bridge_ndjson(
        "\n".join(
            [
                _line(binding, 0, 10, event_type="turn/start"),
                _line(binding, 1, 12, event_type="tool/call"),
            ]
        ),
        expected_binding=binding,
        expected_invocation_id="invocation-a",
    )

    assert len(records) == 2
    summary = summarize_phase_bridge(records)
    assert summary["tool_call_count"] == 1
    assert summary["source_seq_first"] == 10
    assert summary["source_seq_last"] == 12
    assert summary["usage_totals"]["output_tokens"] == 8


def test_bridge_and_durable_sequences_are_fail_closed(binding: BridgeBinding) -> None:
    with pytest.raises(BridgeValidationError, match="bridge sequence discontinuity"):
        parse_phase_bridge_ndjson(
            "\n".join(
                [
                    _line(binding, 0, 10, event_type="turn/start"),
                    _line(binding, 2, 11, event_type="turn/end"),
                ]
            )
        )

    with pytest.raises(BridgeValidationError, match="source sequence regressed"):
        parse_phase_bridge_ndjson(
            "\n".join(
                [
                    _line(binding, 0, 10, event_type="turn/start"),
                    _line(binding, 1, 10, event_type="turn/end"),
                ]
            )
        )


def test_binding_and_invocation_mismatch_are_rejected(binding: BridgeBinding) -> None:
    other = BridgeBinding("claim-b", "attempt-b", 3, 8)
    with pytest.raises(BridgeValidationError, match="binding"):
        parse_phase_bridge_ndjson(
            _line(other, 0, 1, event_type="turn/start"),
            expected_binding=binding,
        )
    with pytest.raises(BridgeValidationError, match="invocation"):
        parse_phase_bridge_ndjson(
            _line(binding, 0, 1, event_type="turn/start"),
            expected_invocation_id="different-invocation",
        )


def test_forbidden_payload_fields_cannot_cross_boundary(binding: BridgeBinding) -> None:
    value = json.loads(_line(binding, 0, 1, event_type="turn/start"))
    value["content"] = "secret model output"
    with pytest.raises(BridgeValidationError, match="forbidden field"):
        parse_phase_bridge_ndjson(json.dumps(value))


def test_js_bridge_is_agent_scoped_and_does_not_serialize_payloads(
    binding: BridgeBinding,
) -> None:
    source = build_phase_bridge_js(
        binding,
        "invocation-a",
        session_id="session-a",
        tool_name_allowlist=("bash",),
        output="stdout",
    )

    assert 'agent.ctx.on("session/event"' in source
    assert 'agent.ctx.on("session/flush"' in source
    assert 'agent.ctx.on("agent/status"' in source
    assert 'tool_name_sha256' in source
    assert 'TOOL_ALLOWLIST.has(name)' in source
    assert "JSON.stringify(event)" not in source
    assert "data.content" not in source
    assert "data.arguments" not in source
    assert "data.message" not in source
    assert "process.env" not in source


def test_file_output_requires_explicit_path(binding: BridgeBinding) -> None:
    with pytest.raises(ValueError, match="output_path"):
        build_phase_bridge_js(binding, "invocation-a", output="file")

