"""DeepSeek Harness durable-trace parser tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from lhos.integrations.harness import (
    DeepSeekTraceSummary,
    HarnessExecutionBinding,
    HarnessFailureClass,
    HarnessPhaseKind,
    HarnessUsage,
    ManagedProcessResult,
    classify_deepseek_failure,
    parse_deepseek_sessions,
)


def _binding() -> HarnessExecutionBinding:
    return HarnessExecutionBinding(
        graph_id="graph-1",
        graph_version=4,
        semantic_epoch=2,
        task_id="task-1",
        agent_id="agent-1",
        claim_id="claim-1",
        attempt_id="attempt-1",
    )


def _write_session(root: Path, events: list[dict[str, Any] | str]) -> Path:
    session = root / "sessions" / "project" / "session-1"
    session.mkdir(parents=True)
    path = session / "session.jsonl"
    path.write_text(
        "\n".join(
            json.dumps(event, separators=(",", ":")) if isinstance(event, dict) else event
            for event in events
        ),
        encoding="utf-8",
    )
    return path


def _process(
    *,
    exit_code: int = 1,
    stdout: str = "",
    stderr: str = "",
    timed_out: bool = False,
    terminated_by: str | None = None,
) -> ManagedProcessResult:
    return ManagedProcessResult(
        command=("dsh",),
        pid=123,
        exit_code=exit_code,
        elapsed_ms=10,
        stdout_tail=stdout,
        stderr_tail=stderr,
        timed_out=timed_out,
        terminated_by=terminated_by,
        hard_killed=False,
    )


def test_parser_sequences_and_deduplicates_usage_and_tool_replay(tmp_path: Path) -> None:
    usage = {
        "inputTokens": 10,
        "outputTokens": 3,
        "reasoningTokens": 2,
        "cacheReadTokens": 20,
    }
    _write_session(
        tmp_path,
        [
            {"type": "session", "id": "session-1"},
            {"type": "turn/start", "seq": 0, "time": 1, "data": {"turn": 1}},
            {
                "type": "assistant/chunk",
                "seq": 1,
                "time": 2,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "chunk": {"type": "usage", "usage": usage},
                },
            },
            {
                "type": "assistant/message",
                "seq": 2,
                "time": 3,
                "data": {"turn": 1, "step": 1, "usage": usage},
            },
            # A replayed committed message is the same logical provider call.
            {
                "type": "assistant/message",
                "seq": 3,
                "time": 4,
                "data": {"turn": 1, "step": 1, "usage": usage},
            },
            {
                "type": "tool/call",
                "seq": 4,
                "time": 5,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "callId": "call-1",
                    "name": "read",
                    "arguments": '{"path":"src/example.py"}',
                },
            },
            # The durable log may repeat a call row during replay/recovery.
            {
                "type": "tool/call",
                "seq": 5,
                "time": 6,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "callId": "call-1",
                    "name": "read",
                    "arguments": '{"path":"src/example.py"}',
                },
            },
        ],
    )

    trace = parse_deepseek_sessions(
        tmp_path / "sessions",
        workspace=tmp_path,
        binding=_binding(),
        phase_seq_start=1,
    )

    assert trace.usage.model_calls == 1
    assert trace.usage.tool_calls == 1
    assert trace.usage.uncached_input_tokens == 10
    assert trace.usage.output_tokens == 3
    assert trace.usage.reasoning_tokens == 2
    assert trace.usage.cache_read_tokens == 20
    assert len(trace.tool_calls) == 1
    assert [event.phase_seq for event in trace.events] == list(range(1, 1 + len(trace.events)))
    assert [event.phase for event in trace.events] == [
        HarnessPhaseKind.READY,
        HarnessPhaseKind.MODEL_CALL,
        HarnessPhaseKind.TOOL_CALL,
    ]
    assert trace.events[-1].usage_cumulative == trace.usage


def test_torn_tail_keeps_partial_chunk_usage_and_marks_provenance_unknown(
    tmp_path: Path,
) -> None:
    _write_session(
        tmp_path,
        [
            {"type": "session", "id": "session-1"},
            {"type": "turn/start", "seq": 0, "time": "bad", "data": {"turn": "bad"}},
            {
                "type": "assistant/chunk",
                "seq": 1,
                "time": 2,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "chunk": {
                        "type": "usage",
                        "usage": {
                            "inputTokens": "7",
                            "outputTokens": 2,
                            "cacheWriteTokens": 5,
                        },
                    },
                },
            },
            {
                "type": "llm/retry",
                "seq": 2,
                "time": 3,
                "data": {"turn": 1, "step": 1, "delayMs": "not-a-number"},
            },
            '{"type":"assistant/message"',
        ],
    )

    trace = parse_deepseek_sessions(
        tmp_path / "sessions",
        workspace=tmp_path,
        binding=_binding(),
        phase_seq_start=4,
    )

    assert trace.usage.model_calls == 1
    assert trace.usage.uncached_input_tokens == 7
    assert trace.usage.output_tokens == 2
    assert trace.usage.cache_write_tokens == 5
    assert trace.retries == 1
    assert trace.retry_delay_ms == 0
    assert trace.unknown_io is True
    assert [event.phase_seq for event in trace.events] == [4, 5]
    assert trace.events[-1].phase is HarnessPhaseKind.MODEL_CALL
    assert trace.events[-1].usage_cumulative == trace.usage


def test_retry_generation_preserves_failed_and_successful_provider_usage(
    tmp_path: Path,
) -> None:
    _write_session(
        tmp_path,
        [
            {"type": "session", "id": "session-1"},
            {
                "type": "assistant/chunk",
                "seq": 1,
                "time": 1,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "chunk": {
                        "type": "usage",
                        "usage": {"inputTokens": 5, "outputTokens": 1},
                    },
                },
            },
            {
                "type": "llm/retry",
                "seq": 2,
                "time": 2,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "retryId": "retry-1",
                    "retry": 1,
                    "delayMs": 50,
                },
            },
            {
                "type": "llm/retry",
                "seq": 3,
                "time": 3,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "retryId": "retry-1",
                    "retry": 1,
                    "delayMs": 50,
                },
            },
            {
                "type": "llm/retry-started",
                "seq": 4,
                "time": 4,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "retryId": "retry-1",
                    "retry": 1,
                },
            },
            {
                "type": "assistant/message",
                "seq": 5,
                "time": 5,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "usage": {"inputTokens": 7, "outputTokens": 2},
                },
            },
        ],
    )

    trace = parse_deepseek_sessions(
        tmp_path / "sessions",
        workspace=tmp_path,
        binding=_binding(),
    )

    assert trace.usage.uncached_input_tokens == 12
    assert trace.usage.output_tokens == 3
    assert trace.usage.model_calls == 2
    assert trace.retries == 1
    assert trace.retry_delay_ms == 50


def test_retry_without_failed_usage_still_counts_the_provider_call(
    tmp_path: Path,
) -> None:
    _write_session(
        tmp_path,
        [
            {"type": "session", "id": "session-1"},
            {
                "type": "llm/retry",
                "seq": 1,
                "time": 1,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "retryId": "retry-1",
                    "retry": 1,
                    "delayMs": 10,
                },
            },
            {
                "type": "llm/retry-started",
                "seq": 2,
                "time": 2,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "retryId": "retry-1",
                    "retry": 1,
                },
            },
            {
                "type": "assistant/message",
                "seq": 3,
                "time": 3,
                "data": {
                    "turn": 1,
                    "step": 1,
                    "usage": {"inputTokens": 7, "outputTokens": 2},
                },
            },
        ],
    )

    trace = parse_deepseek_sessions(
        tmp_path / "sessions",
        workspace=tmp_path,
        binding=_binding(),
    )

    assert trace.usage.model_calls == 2
    assert trace.usage.total_token_units == 9


def test_typed_tool_with_unusable_path_fails_closed(tmp_path: Path) -> None:
    outside = (tmp_path.parent / "outside.py").resolve()
    _write_session(
        tmp_path,
        [
            {"type": "session", "id": "session-1"},
            {
                "type": "tool/call",
                "seq": 1,
                "time": 1,
                "data": {
                    "callId": "call-1",
                    "name": "read",
                    "arguments": json.dumps({"path": str(outside)}),
                },
            },
        ],
    )

    trace = parse_deepseek_sessions(
        tmp_path / "sessions",
        workspace=tmp_path,
        binding=_binding(),
    )

    assert trace.read_set == ()
    assert trace.unknown_io is True
    assert trace.tool_calls[0].unknown_io is True


def test_successful_stdout_that_mentions_rate_limits_is_not_a_failure() -> None:
    trace = DeepSeekTraceSummary(
        usage=HarnessUsage(model_calls=1),
        turn_end_reasons=({"kind": "completed"},),
    )

    failure = classify_deepseek_failure(
        _process(exit_code=0, stdout="Explain HTTP 429 rate limit handling"),
        trace,
    )

    assert failure is None


@pytest.mark.parametrize(
    ("process", "trace", "expected", "retryable"),
    [
        (
            _process(stderr="451 content_policy censorship_blocked"),
            DeepSeekTraceSummary(),
            HarnessFailureClass.CONTENT_POLICY,
            False,
        ),
        (
            _process(stderr="429 rate limit retry-after: 2.5"),
            DeepSeekTraceSummary(),
            HarnessFailureClass.RATE_LIMIT,
            True,
        ),
        (
            _process(stderr="503 provider unavailable"),
            DeepSeekTraceSummary(),
            HarnessFailureClass.PROVIDER_5XX,
            True,
        ),
        (
            _process(stderr="400 invalid request"),
            DeepSeekTraceSummary(),
            HarnessFailureClass.INVALID_REQUEST,
            False,
        ),
        (
            _process(stderr="network connection reset"),
            DeepSeekTraceSummary(),
            HarnessFailureClass.NETWORK_TRANSIENT,
            True,
        ),
        (
            _process(),
            DeepSeekTraceSummary(turn_end_reasons=({"kind": "blocked"},)),
            HarnessFailureClass.INVALID_REQUEST,
            False,
        ),
        (
            _process(),
            DeepSeekTraceSummary(turn_end_reasons=({"kind": "cancelled"},)),
            HarnessFailureClass.CANCELLED,
            False,
        ),
        (
            _process(),
            DeepSeekTraceSummary(turn_end_reasons=({"kind": "max-tokens"},)),
            HarnessFailureClass.INVALID_REQUEST,
            False,
        ),
    ],
)
def test_failure_classification_matrix(
    process: ManagedProcessResult,
    trace: DeepSeekTraceSummary,
    expected: HarnessFailureClass,
    retryable: bool,
) -> None:
    failure = classify_deepseek_failure(process, trace)

    assert failure is not None
    assert failure.failure_class is expected
    assert failure.retryable is retryable
    if expected is HarnessFailureClass.RATE_LIMIT:
        assert failure.retry_after_seconds == 2.5
