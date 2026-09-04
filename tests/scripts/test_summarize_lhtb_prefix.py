from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from scripts.summarize_lhtb_prefix import (
    NA,
    summarize,
    summarize_observability,
)

ANCHOR = "2026-01-01T00:00:00+00:00"


def _time(seconds: int) -> str:
    value = datetime.fromisoformat(ANCHOR) + timedelta(seconds=seconds)
    return value.isoformat()


def _observability() -> dict:
    return {
        "schema_version": "lhos-lhtb-dsh-harbor-agent.v1",
        "started_at": ANCHOR,
        "updated_at": _time(360),
        "session_id": "session-fixture",
        "session_reused": True,
        "invocation_count": 3,
        "resume_count": 2,
        "invocations": [
            {
                "invocation": 1,
                "started_at": _time(0),
                "finished_at": _time(100),
                "elapsed_ms": 100_000,
                "resume": False,
                "status": "max_tokens_checkpoint",
                "session_generation": 0,
            },
            {
                "invocation": 2,
                "started_at": _time(120),
                "finished_at": _time(240),
                "elapsed_ms": 120_000,
                "resume": True,
                "resume_session_id": "session-fixture",
                "status": "completed",
                "session_generation": 0,
            },
            {
                "invocation": 3,
                "started_at": _time(300),
                "elapsed_ms": 180_000,
                "resume": True,
                "resume_session_id": "session-fixture",
                "status": "running",
                "session_generation": 1,
            },
        ],
        "semantic_decisions": [
            {
                "action": "resume",
                "decided_at": _time(120),
                "session_id": "session-fixture",
                "session_generation": 0,
                "usage_delta": {
                    "model_calls": 2,
                    "tool_calls": 3,
                    "total_token_units": 100,
                },
            },
            {
                "action": "resume",
                "decided_at": _time(240),
                "session_id": "session-fixture",
                "session_generation": 1,
                "usage_delta": {
                    "model_calls": 4,
                    "tool_calls": 5,
                    "total_token_units": 200,
                },
            },
        ],
        # This is intentionally much larger than any prefix delta.  It must
        # not be copied into an early cutoff.
        "usage": {
            "model_calls": 999,
            "tool_calls": 999,
            "total_token_units": 999_999,
        },
        "event_count": 100,
    }


def test_prefix_clips_active_time_and_keeps_resume_state_before_cutoff() -> None:
    rows = summarize_observability(
        _observability(),
        cutoffs=(90, 150, 250, 330),
        reward_checkpoints=[
            (datetime.fromisoformat(_time(210)), 0.4),
        ],
        final_reward=0.9,
    )

    first, second, third, fourth = rows
    assert first["active_elapsed_ms"] == 90_000.0
    assert first["invocation_count"] == 1
    assert first["last_invocation_status"] == "running"
    assert first["resume_count"] == 0
    assert first["usage"]["model_calls"] == NA
    assert first["reward"] == NA

    # At 150s the first resume has started, but the second decision has not.
    assert second["active_elapsed_ms"] == 130_000.0
    assert second["invocation_count"] == 2
    assert second["resume_count"] == 1
    assert second["session_id"] == "session-fixture"
    assert second["usage"]["model_calls"] == 2
    assert second["usage"]["tool_calls"] == 3
    assert second["usage_observation"] == "partial_prefix_delta"
    assert second["reward"] == NA
    assert second["reward_observation"] == "final_only_unusable"

    assert third["active_elapsed_ms"] == 220_000.0
    assert third["invocation_count"] == 2
    assert third["resume_count"] == 2
    assert third["semantic_decision_count"] == 2
    assert third["usage"]["model_calls"] == 6
    assert third["reward"] == 0.4
    assert third["reward_observation"] == "checkpoint"

    # The in-flight third invocation is clipped at 330s: 100 + 120 + 30.
    assert fourth["active_elapsed_ms"] == 250_000.0
    assert fourth["invocation_count"] == 3
    assert fourth["last_invocation_status"] == "running"
    assert fourth["session_generation_count"] == 2
    assert fourth["usage"]["model_calls"] == 6


def test_final_only_usage_and_reward_never_become_prefix_measurements() -> None:
    observability = {
        "started_at": ANCHOR,
        "updated_at": _time(20),
        "invocations": [
            {
                "started_at": _time(0),
                "finished_at": _time(20),
                "elapsed_ms": 20_000,
                "resume": False,
                "status": "completed",
            }
        ],
        "usage": {"model_calls": 12, "total_token_units": 1_200},
    }
    rows = summarize_observability(observability, cutoffs=(10, 30), final_reward=0.8)
    assert rows[0]["usage"]["model_calls"] == NA
    assert rows[0]["usage_observation"] == "final_cumulative_only"
    assert rows[0]["reward"] == NA
    assert rows[0]["reward_observation"] == "final_only_unusable"
    assert rows[1]["usage"]["model_calls"] == NA
    assert rows[1]["reward"] == NA


def test_summary_discovers_run_records_without_mutating_them(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    runs = run_root / "runs" / "task-a"
    runs.mkdir(parents=True)
    observability_path = run_root / "observability.json"
    observability_path.write_text(
        json.dumps(_observability()),
        encoding="utf-8",
    )
    record = {
        "task_name": "task-a",
        "metrics": {
            "observability": str(observability_path),
            "reward": 0.9,
        },
    }
    record_path = runs / "lhos_resume.json"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    (runs / "dsh_fresh.json").write_text(json.dumps(record), encoding="utf-8")
    (run_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-lhtb-dsh-controlled-pair.v3",
                "official_protocol": False,
                "official_score": False,
                "controlled_pair_experiment": {"harness_constant": True},
            }
        ),
        encoding="utf-8",
    )

    result = summarize(run_root, cutoffs=(150,))
    assert result["schema_version"] == "lhos-lhtb-prefix-summary.v1"
    assert result["task_count"] == 1
    assert result["record_count"] == 2
    assert {item["arm"] for item in result["records"]} == {"dsh_fresh", "lhos_resume"}
    assert all(item["prefixes"][0]["reward"] == NA for item in result["records"])


def test_raw_session_events_are_capped_at_observability_update(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    runs = run_root / "runs" / "task-a"
    runs.mkdir(parents=True)
    agent_root = run_root / "trial" / "agent"
    session = (
        agent_root
        / "dsh-runs"
        / "invocation-0001"
        / "dsh-home"
        / "sessions"
        / "--app--"
        / "session-fixture"
        / "session.jsonl"
    )
    session.parent.mkdir(parents=True)
    anchor_ms = int(datetime.fromisoformat(ANCHOR).timestamp() * 1000)
    events = [
        {
            "type": "assistant/message",
            "time": anchor_ms + 10_000,
            "data": {
                "message": {"source": {"kind": "model"}},
                "usage": {
                    "inputTokens": 10,
                    "outputTokens": 20,
                    "cacheReadTokens": 40,
                },
            },
        },
        {
            "type": "tool/call",
            "time": anchor_ms + 11_000,
            "data": {"name": "shell"},
        },
        # This event was appended after the final observability update.  It
        # must not be counted even when the requested cutoff is later.
        {
            "type": "assistant/message",
            "time": anchor_ms + 200_000,
            "data": {
                "message": {"source": {"kind": "model"}},
                "usage": {
                    "inputTokens": 100,
                    "outputTokens": 200,
                    "cacheReadTokens": 400,
                },
            },
        },
    ]
    session.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n",
        encoding="utf-8",
    )
    observability = {
        "arm": "baseline",
        "updated_at": _time(20),
        "invocation_count": 1,
        "invocations": [
            {
                "started_at": ANCHOR,
                "finished_at": _time(20),
                "elapsed_ms": 20_000,
                "resume": False,
                "status": "completed",
            }
        ],
        "usage": {
            "cache_write_tokens": 0,
            "reasoning_tokens": 0,
            "model_calls": 1,
            "tool_calls": 1,
            "total_token_units": 70,
        },
    }
    observability_path = agent_root / "dsh-observability.json"
    observability_path.write_text(json.dumps(observability), encoding="utf-8")
    (runs / "dsh_fresh.json").write_text(
        json.dumps({"metrics": {"observability": str(observability_path)}}),
        encoding="utf-8",
    )

    result = summarize(run_root, cutoffs=(300,))
    prefix = result["records"][0]["prefixes"][0]
    assert prefix["usage_observation"] == "raw_session_events_deduplicated"
    assert prefix["usage"]["model_calls"] == 1
    assert prefix["usage"]["tool_calls"] == 1
    assert prefix["usage"]["uncached_input_tokens"] == 10
    assert prefix["usage"]["output_tokens"] == 20
    assert prefix["usage"]["cache_read_tokens"] == 40
    assert prefix["usage"]["total_token_units"] == 70


def test_raw_session_usage_matches_dsh_deduplication_and_ignores_stale_rows(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "run"
    runs = run_root / "runs" / "task-a"
    runs.mkdir(parents=True)
    agent_root = run_root / "trial" / "agent"
    session = (
        agent_root
        / "dsh-runs"
        / "invocation-0001"
        / "dsh-home"
        / "sessions"
        / "--app--"
        / "session-fixture"
        / "session.jsonl"
    )
    session.parent.mkdir(parents=True)
    anchor_ms = int(datetime.fromisoformat(ANCHOR).timestamp() * 1000)
    events = [
        {"type": "session", "id": "session-fixture"},
        {
            "type": "assistant/message",
            "seq": 1,
            "time": anchor_ms - 1_000,
            "data": {
                "turn": 0,
                "step": 0,
                "usage": {"inputTokens": 999, "outputTokens": 999},
            },
        },
        {
            "type": "assistant/chunk",
            "seq": 2,
            "time": anchor_ms + 5_000,
            "data": {
                "turn": 1,
                "step": 1,
                "chunk": {
                    "type": "usage",
                    "usage": {"inputTokens": 1, "outputTokens": 2},
                },
            },
        },
        {
            "type": "assistant/message",
            "seq": 3,
            "time": anchor_ms + 6_000,
            "data": {
                "turn": 1,
                "step": 1,
                "usage": {
                    "inputTokens": 10,
                    "outputTokens": 20,
                    "cacheReadTokens": 40,
                    "reasoningTokens": 5,
                },
            },
        },
        # Replayed final and tool rows are one logical call each.
        {
            "type": "assistant/message",
            "seq": 4,
            "time": anchor_ms + 7_000,
            "data": {
                "turn": 1,
                "step": 1,
                "usage": {
                    "inputTokens": 10,
                    "outputTokens": 20,
                    "cacheReadTokens": 40,
                    "reasoningTokens": 5,
                },
            },
        },
        {
            "type": "tool/call",
            "seq": 5,
            "time": anchor_ms + 8_000,
            "data": {"turn": 1, "step": 1, "callId": "call-a"},
        },
        {
            "type": "tool/call",
            "seq": 6,
            "time": anchor_ms + 9_000,
            "data": {"turn": 1, "step": 1, "callId": "call-a"},
        },
        # A retry boundary proves a failed provider call even without usage.
        {
            "type": "llm/retry",
            "seq": 7,
            "time": anchor_ms + 10_000,
            "data": {"turn": 1, "step": 2, "retryId": "retry-a", "retry": 0},
        },
    ]
    session.write_text(
        "\n".join(json.dumps(event) for event in events) + "\n{torn",
        encoding="utf-8",
    )
    observability = {
        "arm": "baseline",
        "updated_at": _time(20),
        "invocations": [
            {
                "started_at": ANCHOR,
                "finished_at": _time(20),
                "status": "completed",
            }
        ],
    }
    observability_path = agent_root / "dsh-observability.json"
    observability_path.write_text(json.dumps(observability), encoding="utf-8")
    (runs / "dsh_fresh.json").write_text(
        json.dumps({"metrics": {"observability": str(observability_path)}}),
        encoding="utf-8",
    )

    result = summarize(run_root, cutoffs=(15,))
    prefix = result["records"][0]["prefixes"][0]
    assert prefix["usage_observation"] == (
        "partial_raw_session_events_deduplicated_stale_rows_ignored"
    )
    assert prefix["usage"]["model_calls"] == 2
    assert prefix["usage"]["tool_calls"] == 1
    assert prefix["usage"]["uncached_input_tokens"] == 10
    assert prefix["usage"]["reasoning_tokens"] == 5
    # HarnessUsage reports reasoning separately; it is not in total_token_units.
    assert prefix["usage"]["total_token_units"] == 70


def test_invalid_raw_session_falls_back_to_semantic_usage(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    runs = run_root / "runs" / "task-a"
    runs.mkdir(parents=True)
    agent_root = run_root / "trial" / "agent"
    session = (
        agent_root
        / "dsh-runs"
        / "invocation-0001"
        / "dsh-home"
        / "sessions"
        / "--app--"
        / "session-fixture"
        / "session.jsonl"
    )
    session.parent.mkdir(parents=True)
    session.write_text(
        '{"type":"session","id":"session-fixture"}\n{torn', encoding="utf-8"
    )
    observability = {
        "arm": "baseline",
        "updated_at": _time(20),
        "invocations": [
            {"started_at": ANCHOR, "finished_at": _time(20), "status": "completed"}
        ],
        "semantic_decisions": [
            {
                "decided_at": _time(10),
                "usage_delta": {"model_calls": 2, "tool_calls": 3},
            }
        ],
    }
    observability_path = agent_root / "dsh-observability.json"
    observability_path.write_text(json.dumps(observability), encoding="utf-8")
    (runs / "dsh_fresh.json").write_text(
        json.dumps({"metrics": {"observability": str(observability_path)}}),
        encoding="utf-8",
    )

    prefix = summarize(run_root, cutoffs=(15,))["records"][0]["prefixes"][0]
    assert prefix["usage_observation"] == "partial_prefix_delta"
    assert prefix["usage"]["model_calls"] == 2
    assert prefix["usage"]["tool_calls"] == 3


def test_summary_marks_incomplete_pairs_and_resolves_run_relative_observability(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "run"
    runs = run_root / "runs" / "task-a"
    runs.mkdir(parents=True)
    (run_root / "observability.json").write_text(
        json.dumps(_observability()), encoding="utf-8"
    )
    (runs / "dsh_fresh.json").write_text(
        json.dumps({"metrics": {"observability": "observability.json"}}),
        encoding="utf-8",
    )
    (run_root / "manifest.json").write_text(
        json.dumps(
            {
                "controlled_pair_experiment": {
                    "harness_constant": {"credential": "must-not-escape"}
                }
            }
        ),
        encoding="utf-8",
    )

    result = summarize(run_root, cutoffs=(15,))
    assert result["official_score"] is False
    assert result["leaderboard_comparable"] is False
    assert result["pair_complete"] is False
    assert result["complete_pair_count"] == 0
    assert result["incomplete_pair_task_names"] == ["task-a"]
    assert result["harness_constant"] == NA
    assert result["records"][0]["prefixes"][0]["invocation_count"] == 1
    assert "must-not-escape" not in json.dumps(result)
