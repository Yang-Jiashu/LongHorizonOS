from __future__ import annotations

import json
from pathlib import Path

from scripts.lhtb_semantic_policy_v3_candidate import (
    CandidateGuard,
    CandidateObservation,
    evaluate_results_root,
    parse_session_jsonl,
)


def _observation(
    phase: int,
    *,
    cache: int,
    events: int,
    max_tokens: bool = False,
    completed: bool = False,
    writes: int = 0,
    artifact_signal: str = "unknown",
) -> CandidateObservation:
    return CandidateObservation(
        phase_index=phase,
        session_id="session-test",
        event_count=events,
        cache_read_tokens=cache,
        model_calls=1,
        write_set_size=writes,
        artifact_signal=artifact_signal,
        max_tokens_checkpoint=max_tokens,
        turn_reason=(
            "max-tokens" if max_tokens else "completed" if completed else ""
        ),
        completed=completed,
    )


def test_candidate_replays_dicom_and_climate_guards_without_early_alp_audio_restart() -> None:
    guard = CandidateGuard()

    dicom_first = _observation(
        1,
        cache=91_712,
        events=3_245,
        max_tokens=True,
    )
    dicom_second = _observation(
        2,
        cache=38_336,
        events=3_439,
        max_tokens=True,
    )
    dicom = guard.decide((dicom_first,), dicom_second)
    assert dicom.changed is True
    assert dicom.action == "restart_compacted"
    assert set(dicom.reasons) == {
        "consecutive_max_tokens",
        "cumulative_session_cache",
        "two_phase_no_artifact_or_event_progress",
    }

    climate_history = (
        _observation(1, cache=31_168, events=2_753, max_tokens=True),
        _observation(2, cache=51_712, events=6_524, max_tokens=True),
    )
    climate = guard.decide(
        climate_history,
        _observation(3, cache=56_896, events=9_492, max_tokens=True),
    )
    assert climate.action == "restart_compacted"
    assert "cumulative_session_cache" in climate.reasons

    alp = guard.decide(
        (),
        _observation(
            1,
            cache=25_280,
            events=112,
            completed=True,
            artifact_signal="no",
        ),
    )
    assert alp.action == "resume"
    assert alp.diagnostics == ("completed_without_verification",)

    audio_first = _observation(
        1,
        cache=1_757_696,
        events=7_617,
        completed=True,
        writes=1,
        artifact_signal="yes",
    )
    audio = guard.decide(
        (audio_first,),
        _observation(
            2,
            cache=7_232,
            events=10_650,
            max_tokens=True,
            artifact_signal="no",
        ),
    )
    assert audio.action == "resume"
    assert audio.changed is False


def test_candidate_parser_recovers_turn_reasons_usage_and_write_signals(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.jsonl"
    rows = [
        {"type": "session", "id": "session-test", "cwd": "/app"},
        {
            "type": "assistant/message",
            "seq": 1,
            "data": {
                "turn": 1,
                "usage": {
                    "cacheReadTokens": 7_232,
                    "inputTokens": 3,
                    "outputTokens": 2,
                },
            },
        },
        {
            "type": "tool/call",
            "seq": 2,
            "data": {
                "turn": 1,
                "name": "edit",
                "arguments": '{"path":"/app/src/a.py"}',
            },
        },
        {
            "type": "turn/end",
            "seq": 3,
            "data": {"turn": 1, "reason": {"kind": "max-tokens"}},
        },
    ]
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    trace = parse_session_jsonl(path)

    assert trace.session_id == "session-test"
    assert trace.turns[1].cache_read_tokens == 7_232
    assert trace.turns[1].typed_writes == 1
    assert trace.turns[1].artifact_signal == "yes"
    assert trace.turns[1].max_tokens is True


def test_terminal_completed_unverified_is_reported_as_continue_repair(
    tmp_path: Path,
) -> None:
    results = tmp_path / "results"
    run_dir = results / "runs" / "dicom-radiology-audit"
    run_dir.mkdir(parents=True)
    agent = tmp_path / "jobs" / "agent"
    agent.mkdir(parents=True)
    observability = agent / "dsh-observability.json"
    semantic = agent / "dsh-semantic-control.json"

    observability.write_text(
        json.dumps(
            {
                "session_id": "session-test",
                "session_generation": 0,
                "event_count": 200,
                "usage": {
                    "cache_read_tokens": 200_000,
                    "model_calls": 4,
                },
                "invocations": [
                    {
                        "invocation": 1,
                        "status": "completed",
                        "session_generation": 0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    semantic.write_text(
        json.dumps(
            {
                "decisions": [
                    {
                        "phase_index": 1,
                        "session_id": "session-test",
                        "session_generation": 0,
                        "action": "resume",
                        "event_count": 200,
                        "write_set_size": 1,
                        "usage_delta": {
                            "cache_read_tokens": 200_000,
                            "model_calls": 4,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "lhos_resume.json").write_text(
        json.dumps(
            {
                "arm": "lhos_resume",
                "task_name": "dicom-radiology-audit",
                "status": "completed",
                "metrics": {
                    "observability": str(observability),
                    "verified": False,
                    "resolved": False,
                    "result_eligible": True,
                    "mechanism_eligible": True,
                },
            }
        ),
        encoding="utf-8",
    )

    report = evaluate_results_root(results)
    case = report["cases"][0]

    assert case["decisions"][0]["action"] == "resume"
    assert case["decisions"][0]["diagnostics"] == [
        "completed_without_verification"
    ]
    assert case["terminal_guard"]["action"] == "continue_repair"
    assert case["terminal_guard"]["diagnostics"] == [
        "completed_without_verification"
    ]

