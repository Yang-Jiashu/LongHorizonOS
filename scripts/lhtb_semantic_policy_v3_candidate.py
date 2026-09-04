#!/usr/bin/env python3
"""Offline candidate policy for the next DeepSeek Harness/LHOS guard.

This module is intentionally *not* imported by the benchmark runner.  It is a
counterfactual evaluator for already-produced LHTB traces.  The evaluator
answers a narrow question:

    "At a verifier-rejection boundary, would a stricter LHOS policy have
    restarted a compacted session instead of resuming the old conversation?"

The candidate has three restart triggers plus one correctness safeguard:

* two consecutive ``max-tokens`` checkpoints in one session;
* cumulative cache-read tokens in one session/generation over a budget;
* two phases without a typed artifact write *and* without event progress;
* a completed turn is never treated as success until the verifier passes; it
  continues verifier-guided repair without forcing compaction by itself.

The implementation only uses standard-library types so importing it cannot
change the production adapter or the current benchmark.  It can consume the
durable ``dsh-semantic-control.json``, ``dsh-observability.json`` and, when
available, the DSH ``session.jsonl`` file.  Missing/corrupt telemetry fails
closed and is reported in the output rather than raising during a suite scan.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Any

DEFAULT_RESULTS_ROOT = Path(
    r"D:\LHTB-results\lhtb-full46-primary-natural-fixed900-20260822-r3"
)

_WRITE_COMMANDS = frozenset({"create", "str_replace", "insert", "replace", "delete"})
_READ_COMMANDS = frozenset({"view", "grep"})
_READ_TOOL_NAMES = frozenset({"read", "grep", "glob", "cat", "head", "tail"})
_WRITE_TOOL_NAMES = frozenset({"edit", "write", "rm", "remove", "delete"})
_UNKNOWN_TOOL_NAMES = frozenset({"bash", "pwsh", "shell", "terminal", "exec"})
_OBVIOUS_SHELL_WRITE = re.compile(
    r"(?:"
    r"(?:^|[\s;&|])(?:tee|touch|mkdir|rm|mv|cp|install|sed\s+-i|perl\s+-i)\b|"
    r"(?:^|[\s])(?:cat|printf|echo)\b[^;&|]*>>?|"
    r"(?:>|>>)"
    r")",
    re.IGNORECASE,
)


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return default


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _safe_rglob(root: Path, pattern: str) -> list[Path]:
    try:
        return sorted(path for path in root.rglob(pattern) if path.is_file())
    except (OSError, RuntimeError):
        # A partially downloaded Harbor generation can contain a dangling
        # directory.  That must not invalidate the rest of a suite report.
        return []


@dataclass(frozen=True)
class CandidateGuardConfig:
    """Thresholds for the offline candidate.

    ``cumulative_cache_read_tokens`` is intentionally configurable.  A value
    of 2M is conservative for a full task; use 1.5M in a sensitivity run.
    ``no_progress_event_ratio`` compares the event delta of the latest phase
    with the preceding phase.  The default (0.10) recognizes a sharp progress
    collapse while avoiding a restart merely because the cursor advanced.
    """

    max_consecutive_max_tokens: int = 2
    cumulative_cache_read_tokens: int = 128_000
    cumulative_cache_requires_max_tokens: int = 2
    no_progress_phases: int = 2
    no_progress_event_ratio: float = 0.10
    max_restarts: int = 2
    force_completed_without_verification: bool = True

    def __post_init__(self) -> None:
        if self.max_consecutive_max_tokens < 1:
            raise ValueError("max_consecutive_max_tokens must be positive")
        if self.cumulative_cache_read_tokens < 1:
            raise ValueError("cumulative_cache_read_tokens must be positive")
        if self.no_progress_phases < 2:
            raise ValueError("no_progress_phases must be at least two")
        if self.cumulative_cache_requires_max_tokens < 1:
            raise ValueError("cumulative_cache_requires_max_tokens must be positive")
        if not 0.0 <= self.no_progress_event_ratio <= 1.0:
            raise ValueError("no_progress_event_ratio must be between zero and one")
        if self.max_restarts < 0:
            raise ValueError("max_restarts must be non-negative")


@dataclass(frozen=True)
class TurnTelemetry:
    """Telemetry reconstructed from one DSH turn in a JSONL session."""

    turn: int
    event_count: int = 0
    reason: str = ""
    cache_read_tokens: int = 0
    model_calls: int = 0
    typed_writes: int = 0
    unknown_io: bool = False

    @property
    def completed(self) -> bool:
        return self.reason == "completed"

    @property
    def max_tokens(self) -> bool:
        return self.reason == "max-tokens"

    @property
    def artifact_signal(self) -> str:
        """``yes``, ``no`` or ``unknown`` for artifact progress."""

        if self.typed_writes > 0:
            return "yes"
        if self.unknown_io:
            return "unknown"
        return "no"


@dataclass(frozen=True)
class CandidateObservation:
    """One verifier boundary consumed by :class:`CandidateGuard`."""

    phase_index: int
    session_id: str
    generation: int = 0
    event_count: int = 0
    cache_read_tokens: int = 0
    model_calls: int = 0
    write_set_size: int = 0
    artifact_signal: str = "unknown"
    turn_reason: str = ""
    max_tokens_checkpoint: bool = False
    verifier_passed: bool = False
    # ``completed`` is deliberately separate from ``verifier_passed``:
    # Harness turn completion is not evidence of task correctness.
    completed: bool = False
    source: str = "semantic-control"

    @property
    def effective_max_tokens(self) -> bool:
        return bool(self.max_tokens_checkpoint or self.turn_reason == "max-tokens")

    @property
    def completed_without_verification(self) -> bool:
        return bool(self.completed and not self.verifier_passed)


@dataclass(frozen=True)
class CandidateDecision:
    """A deterministic candidate action and the guards that caused it."""

    phase_index: int
    actual_action: str
    action: str
    action_changed: bool
    changed: bool
    reasons: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()
    consecutive_max_tokens: int = 0
    cumulative_cache_read_tokens: int = 0
    no_progress_window: bool = False
    event_progress_ratio: float | None = None
    completed_without_verification: bool = False
    restart_count: int = 0
    session_id: str = ""
    generation: int = 0

    def as_json(self) -> dict[str, Any]:
        value = asdict(self)
        value["reasons"] = list(self.reasons)
        value["diagnostics"] = list(self.diagnostics)
        return value


@dataclass
class SessionTrace:
    """Parsed session-level telemetry used to enrich semantic records."""

    session_id: str = ""
    turns: dict[int, TurnTelemetry] = field(default_factory=dict)
    path: str = ""
    error: str | None = None


def _tool_write_classification(name: Any, arguments: Any) -> tuple[bool, bool]:
    """Return ``(typed_write, unknown_io)`` for one DSH tool call."""

    normalized = str(name or "").strip().lower()
    payload: Mapping[str, Any]
    if isinstance(arguments, str):
        try:
            decoded = json.loads(arguments)
        except (TypeError, ValueError):
            decoded = {}
        payload = decoded if isinstance(decoded, Mapping) else {}
    elif isinstance(arguments, Mapping):
        payload = arguments
    else:
        payload = {}

    command = str(payload.get("command", "") or "").strip().lower()
    if normalized == "str_replace_editor":
        if command in _WRITE_COMMANDS:
            return True, False
        if command in _READ_COMMANDS:
            return False, False
        return False, True
    if normalized in _WRITE_TOOL_NAMES:
        return True, False
    if normalized in _READ_TOOL_NAMES:
        return False, False
    if normalized in _UNKNOWN_TOOL_NAMES:
        shell = str(payload.get("command", "") or "")
        # An unknown shell command is not proof of a write.  We only mark it
        # typed progress when a conservative write token is visible; otherwise
        # preserve ``unknown`` so the no-progress guard fails closed.
        return bool(_OBVIOUS_SHELL_WRITE.search(shell)), True
    # Unknown third-party tools fail closed for artifact reasoning.
    return False, True


def parse_session_jsonl(path: Path) -> SessionTrace:
    """Parse turn boundaries, usage and conservative write signals."""

    turns: dict[int, dict[str, Any]] = defaultdict(
        lambda: {
            "event_count": 0,
            "reason": "",
            "cache_read_tokens": 0,
            "model_calls": 0,
            "typed_writes": 0,
            "unknown_io": False,
        }
    )
    session_id = ""
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if not isinstance(event, Mapping):
                    continue
                if event.get("type") == "session":
                    session_id = str(event.get("id", "") or "")
                    continue
                event_type = str(event.get("type", "") or "")
                data = event.get("data")
                if not isinstance(data, Mapping):
                    data = {}
                turn = _int(data.get("turn"), default=0)
                if turn <= 0:
                    continue
                row = turns[turn]
                row["event_count"] = max(row["event_count"], _int(event.get("seq"), 0) + 1)
                if event_type == "turn/end":
                    reason = data.get("reason")
                    if isinstance(reason, Mapping):
                        row["reason"] = str(reason.get("kind", "") or "")
                    else:
                        row["reason"] = str(reason or "")
                elif event_type == "assistant/message":
                    usage = data.get("usage")
                    if isinstance(usage, Mapping):
                        row["cache_read_tokens"] += _int(usage.get("cacheReadTokens"))
                        row["model_calls"] += 1
                elif event_type == "tool/call":
                    typed, unknown = _tool_write_classification(
                        data.get("name"),
                        data.get("arguments"),
                    )
                    row["typed_writes"] += int(typed)
                    row["unknown_io"] = bool(row["unknown_io"] or unknown)
    except (OSError, UnicodeError) as exc:
        return SessionTrace(path=str(path), error=f"{type(exc).__name__}: {exc}")

    return SessionTrace(
        session_id=session_id,
        turns={
            turn: TurnTelemetry(turn=turn, **values)
            for turn, values in sorted(turns.items())
        },
        path=str(path),
    )


def _find_session_traces(observability_path: Path) -> dict[str, SessionTrace]:
    """Find all downloaded DSH sessions below an agent log directory."""

    traces: dict[str, SessionTrace] = {}
    agent_root = observability_path.parent
    for path in _safe_rglob(agent_root, "*.jsonl"):
        trace = parse_session_jsonl(path)
        if trace.session_id:
            traces[trace.session_id] = trace
    return traces


def _turn_for(
    traces: Mapping[str, SessionTrace],
    *,
    session_id: str,
    phase_index: int,
) -> TurnTelemetry | None:
    trace = traces.get(session_id)
    if trace is None:
        return None
    return trace.turns.get(phase_index)


def _invocation_fallbacks(observability: Mapping[str, Any]) -> dict[int, TurnTelemetry]:
    """Project Harbor invocation status into turn-like telemetry.

    This fallback intentionally contains no guessed usage or event cursor.
    It only recovers the terminal reason (max-tokens/completed), which is
    enough to evaluate the max-token and completed-without-verification
    guards when Docker did not download a generation's JSONL.
    """

    result: dict[int, TurnTelemetry] = {}
    invocations = observability.get("invocations")
    if not isinstance(invocations, list):
        return result
    for raw in invocations:
        if not isinstance(raw, Mapping):
            continue
        turn = _int(raw.get("invocation"), default=0)
        if turn <= 0:
            continue
        status = str(raw.get("status", "") or "")
        reason = {
            "max_tokens_checkpoint": "max-tokens",
            "completed": "completed",
            "cancelled": "cancelled",
        }.get(status, status)
        result[turn] = TurnTelemetry(
            turn=turn,
            reason=reason,
            unknown_io=True,
        )
    return result


def _observation_from_record(
    record: Mapping[str, Any],
    *,
    turn: TurnTelemetry | None,
    invocation: TurnTelemetry | None = None,
    verifier_passed: bool,
) -> CandidateObservation:
    usage = record.get("usage_delta")
    if not isinstance(usage, Mapping):
        usage = {}
    phase_index = _int(record.get("phase_index"), default=0)
    session_id = str(record.get("session_id", "") or "")
    # A session JSONL is the strongest source.  Harbor observability is a
    # deliberately smaller fallback because compacted-generation logs may not
    # be downloadable after a Docker run.
    telemetry = turn or invocation
    reason = telemetry.reason if telemetry is not None else ""
    completed = reason == "completed"
    max_tokens = bool(
        (telemetry.max_tokens if telemetry is not None else False)
        or record.get("reason_code") == "max_tokens_checkpoint"
    )
    return CandidateObservation(
        phase_index=phase_index,
        session_id=session_id,
        generation=_int(record.get("session_generation"), default=0),
        event_count=_int(
            turn.event_count if turn is not None else record.get("event_count"),
            default=0,
        ),
        cache_read_tokens=_int(
            telemetry.cache_read_tokens
            if telemetry is not None and telemetry.cache_read_tokens
            else usage.get("cache_read_tokens"),
            default=0,
        ),
        model_calls=_int(
            telemetry.model_calls
            if telemetry is not None and telemetry.model_calls
            else usage.get("model_calls"),
            default=0,
        ),
        write_set_size=_int(record.get("write_set_size"), default=0),
        artifact_signal=turn.artifact_signal if turn is not None else (
            "yes" if _int(record.get("write_set_size"), 0) > 0 else "unknown"
        ),
        turn_reason=reason,
        max_tokens_checkpoint=max_tokens,
        verifier_passed=verifier_passed,
        completed=completed,
        source="semantic-control",
    )


def _same_generation(
    observations: Sequence[CandidateObservation],
    current: CandidateObservation,
) -> tuple[CandidateObservation, ...]:
    return tuple(
        item
        for item in observations
        if item.session_id == current.session_id
        and item.generation == current.generation
        and item.phase_index <= current.phase_index
    )


def _event_delta(
    previous: CandidateObservation,
    current: CandidateObservation,
) -> int | None:
    if (
        previous.session_id != current.session_id
        or previous.generation != current.generation
        or current.event_count < previous.event_count
    ):
        return None
    return current.event_count - previous.event_count


def _event_deltas(
    observations: Sequence[CandidateObservation],
) -> tuple[int, ...]:
    """Return per-phase event deltas with a zero-cursor first phase."""

    if not observations:
        return ()
    result = [max(0, observations[0].event_count)]
    for previous, current in pairwise(observations):
        delta = _event_delta(previous, current)
        result.append(0 if delta is None else delta)
    return tuple(result)


class CandidateGuard:
    """Pure candidate policy; no provider or Harness imports."""

    def __init__(self, config: CandidateGuardConfig | None = None) -> None:
        self.config = config or CandidateGuardConfig()

    def decide(
        self,
        history: Sequence[CandidateObservation],
        current: CandidateObservation,
        *,
        actual_action: str = "resume",
    ) -> CandidateDecision:
        timeline = tuple(
            sorted(
                (
                    *history,
                    current,
                ),
                key=lambda item: item.phase_index,
            )
        )
        same = _same_generation(timeline, current)
        # A verifier success is the only condition that suppresses a
        # destructive restart.  ``completed`` is intentionally not success.
        if current.verifier_passed:
            return CandidateDecision(
                phase_index=current.phase_index,
                actual_action=actual_action,
                action="resume",
                action_changed=actual_action != "resume",
                changed=actual_action == "resume",
                session_id=current.session_id,
                generation=current.generation,
            )

        reasons: list[str] = []
        streak = 0
        for item in reversed(same):
            if not item.effective_max_tokens:
                break
            streak += 1
        if streak >= self.config.max_consecutive_max_tokens:
            reasons.append("consecutive_max_tokens")

        cumulative_cache = sum(item.cache_read_tokens for item in same)
        if (
            cumulative_cache >= self.config.cumulative_cache_read_tokens
            and streak >= self.config.cumulative_cache_requires_max_tokens
        ):
            reasons.append("cumulative_session_cache")

        no_progress = False
        event_progress_ratio: float | None = None
        if len(same) >= self.config.no_progress_phases:
            window = same[-self.config.no_progress_phases :]
            artifact_free = all(
                item.artifact_signal != "yes" and item.write_set_size == 0
                for item in window
            )
            all_deltas = _event_deltas(same)
            first_window_index = len(same) - len(window)
            window_deltas = all_deltas[first_window_index:]
            event_stalled = True
            for previous_delta, delta in pairwise(window_deltas):
                event_progress_ratio = delta / max(1, previous_delta)
                if event_progress_ratio > self.config.no_progress_event_ratio:
                    event_stalled = False
                    break
            no_progress = artifact_free and event_stalled
            if no_progress:
                reasons.append("two_phase_no_artifact_or_event_progress")

        # A naturally completed turn that fails verification is a diagnostic
        # warning, not by itself a reason to destroy useful cognitive locality.
        # ALP/audio traces demonstrate why: both made productive writes before
        # the verifier asked for a continuation.  The warning is still exposed
        # in the decision so callers cannot mistake ``completed`` for VERIFIED.
        completed_without_verification = (
            self.config.force_completed_without_verification
            and current.completed_without_verification
        )
        diagnostics = (
            ("completed_without_verification",)
            if completed_without_verification
            else ()
        )

        restart_count = sum(
            left.session_id != right.session_id
            or left.generation != right.generation
            for left, right in pairwise(timeline)
        )
        if reasons and restart_count < self.config.max_restarts:
            action = "restart_compacted"
        elif reasons:
            action = "resume"
            reasons.append("restart_limit_reached")
        else:
            action = "resume"

        return CandidateDecision(
            phase_index=current.phase_index,
            actual_action=actual_action,
            action=action,
            action_changed=action != actual_action,
            # ``changed`` is the headline counterfactual requested by the
            # report: only a resume -> restart transition is counted.  A
            # missing trace must not turn an already-recorded restart into a
            # false "speedup" or "regression" signal.
            changed=actual_action == "resume" and action == "restart_compacted",
            reasons=tuple(reasons),
            diagnostics=diagnostics,
            consecutive_max_tokens=streak,
            cumulative_cache_read_tokens=cumulative_cache,
            no_progress_window=no_progress,
            event_progress_ratio=event_progress_ratio,
            completed_without_verification=completed_without_verification,
            restart_count=restart_count,
            session_id=current.session_id,
            generation=current.generation,
        )


def _resolve_observability(run: Mapping[str, Any], results_root: Path) -> Path | None:
    metrics = run.get("metrics")
    if not isinstance(metrics, Mapping):
        return None
    raw = metrics.get("observability")
    if not raw:
        return None
    candidate = Path(str(raw))
    if candidate.is_file():
        return candidate
    relative = results_root / candidate
    if relative.is_file():
        return relative
    return None


def evaluate_run(
    run_path: Path,
    *,
    results_root: Path,
    guard: CandidateGuard,
) -> dict[str, Any]:
    run = _read_json(run_path)
    task = str(run.get("task_name") or run_path.parent.name)
    metrics = run.get("metrics") if isinstance(run.get("metrics"), Mapping) else {}
    observability_path = _resolve_observability(run, results_root)
    observability = _read_json(observability_path) if observability_path else {}
    semantic_path = (
        observability_path.parent / "dsh-semantic-control.json"
        if observability_path
        else None
    )
    semantic = _read_json(semantic_path) if semantic_path else {}
    records = semantic.get("decisions")
    if not isinstance(records, list):
        records = []
    traces = _find_session_traces(observability_path) if observability_path else {}
    invocation_fallbacks = _invocation_fallbacks(observability)
    verified = bool(metrics.get("verified") or metrics.get("resolved"))

    observations: list[CandidateObservation] = []
    decisions: list[dict[str, Any]] = []
    for raw_record in records:
        if not isinstance(raw_record, Mapping):
            continue
        session_id = str(raw_record.get("session_id", "") or "")
        phase_index = _int(raw_record.get("phase_index"), default=0)
        turn = _turn_for(traces, session_id=session_id, phase_index=phase_index)
        invocation = invocation_fallbacks.get(phase_index)
        observation = _observation_from_record(
            raw_record,
            turn=turn,
            invocation=invocation,
            verifier_passed=verified,
        )
        actual = str(raw_record.get("action", "resume") or "resume")
        result = guard.decide(
            tuple(observations),
            observation,
            actual_action=actual,
        )
        observations.append(observation)
        decisions.append(
            {
                **result.as_json(),
                "event_delta": (
                    _event_delta(observations[-2], observation)
                    if len(observations) >= 2
                    else None
                ),
                "artifact_signal": observation.artifact_signal,
                "turn_reason": observation.turn_reason,
                "source_record_reason": str(raw_record.get("reason_code", "") or ""),
            }
        )

    # There is often no semantic decision after the final completed turn:
    # Harbor verifies it and closes the run.  Emit a separate terminal guard
    # so reports do not accidentally equate "completed" with "verified".
    terminal: dict[str, Any] | None = None
    invocations = observability.get("invocations")
    if isinstance(invocations, list) and invocations:
        last = invocations[-1]
        if isinstance(last, Mapping):
            status = str(last.get("status", "") or "")
            if status == "completed" and not verified:
                session_id = str(observability.get("session_id", "") or "")
                generation = _int(
                    last.get("session_generation", observability.get("session_generation")),
                    default=0,
                )
                turn_index = _int(last.get("invocation"), default=len(invocations))
                # Use a synthetic record because no verifier-boundary decision
                # was persisted after a terminal turn.
                terminal_observation = CandidateObservation(
                    phase_index=turn_index,
                    session_id=session_id or "unknown-session",
                    generation=generation,
                    event_count=_int(observability.get("event_count"), default=0),
                    cache_read_tokens=_int(
                        (observability.get("usage") or {}).get("cache_read_tokens")
                        if isinstance(observability.get("usage"), Mapping)
                        else 0
                    ),
                    model_calls=_int(
                        (observability.get("usage") or {}).get("model_calls")
                        if isinstance(observability.get("usage"), Mapping)
                        else 0
                    ),
                    artifact_signal="unknown",
                    turn_reason="completed",
                    completed=True,
                    verifier_passed=False,
                    source="observability-terminal",
                )
                terminal_result = guard.decide(
                    tuple(observations),
                    terminal_observation,
                    actual_action="completed",
                )
                terminal = terminal_result.as_json()
                terminal.update(
                    {
                        # The run may not silently stop at Harness completion.
                        # It must re-enter verifier-guided repair; this is not
                        # necessarily a compacted restart.
                        "action": "continue_repair",
                        "action_changed": True,
                        "artifact_signal": terminal_observation.artifact_signal,
                        "turn_reason": terminal_observation.turn_reason,
                        "event_delta": (
                            _event_delta(observations[-1], terminal_observation)
                            if observations
                            else None
                        ),
                    }
                )

    return {
        "task_name": task,
        "run_path": str(run_path),
        "arm": run.get("arm"),
        "status": run.get("status"),
        "result_eligible": bool(metrics.get("result_eligible")),
        "mechanism_eligible": bool(metrics.get("mechanism_eligible")),
        "reward": metrics.get("reward"),
        "observability_path": str(observability_path) if observability_path else None,
        "semantic_control_path": str(semantic_path) if semantic_path else None,
        "session_trace_count": len(traces),
        "decisions": decisions,
        "terminal_guard": terminal,
        "parse_warnings": (
            ["missing_observability"]
            if observability_path is None
            else []
        ),
    }


def evaluate_results_root(
    results_root: Path,
    *,
    guard: CandidateGuard | None = None,
) -> dict[str, Any]:
    """Evaluate every ``runs/*/lhos_resume.json`` under ``results_root``."""

    policy = guard or CandidateGuard()
    cases: list[dict[str, Any]] = []
    runs_root = results_root / "runs"
    try:
        run_paths = sorted(runs_root.glob("*/lhos_resume.json"))
    except OSError:
        run_paths = []
    for run_path in run_paths:
        cases.append(
            evaluate_run(
                run_path,
                results_root=results_root,
                guard=policy,
            )
        )

    changed = [
        item
        for case in cases
        for item in case["decisions"]
        if item.get("changed")
    ]
    terminal_count = sum(case["terminal_guard"] is not None for case in cases)
    reason_counts: Counter[str] = Counter(
        reason
        for case in cases
        for item in case["decisions"]
        for reason in item.get("reasons", [])
        if reason != "restart_limit_reached"
    )
    return {
        "schema_version": "lhos-semantic-policy-v3-candidate.v1",
        "results_root": str(results_root),
        "config": asdict(policy.config),
        "summary": {
            "cases": len(cases),
            "cases_with_decisions": sum(bool(case["decisions"]) for case in cases),
            "decision_count": sum(len(case["decisions"]) for case in cases),
            "resume_to_restart_count": len(changed),
            "terminal_completed_unverified_count": terminal_count,
            "trigger_counts": dict(sorted(reason_counts.items())),
        },
        "cases": cases,
    }


def _markdown(report: Mapping[str, Any]) -> str:
    config = report.get("config", {})
    summary = report.get("summary", {})
    lines = [
        "# LHOS Semantic Policy v3 Candidate (offline counterfactual)",
        "",
        "This report does not alter or re-run a benchmark.  It replays durable "
        "LHOS/DSH telemetry and marks boundaries where the candidate guard would "
        "replace `resume` with `restart_compacted`.",
        "",
        f"- Results root: `{report.get('results_root', '')}`",
        f"- Candidate config: `{json.dumps(config, ensure_ascii=True, sort_keys=True)}`",
        f"- Cases: **{summary.get('cases', 0)}**; decisions: "
        f"**{summary.get('decision_count', 0)}**; resume→restart: "
        f"**{summary.get('resume_to_restart_count', 0)}**",
        f"- Terminal completed-but-unverified cases: "
        f"**{summary.get('terminal_completed_unverified_count', 0)}**",
        "",
        "| Task | Phase | Actual | Candidate | Reasons | Max-token streak | Cumulative cache | Artifact signal | Event Δ | Changed |",
        "|---|---:|---|---|---|---:|---:|---|---:|---:|",
    ]
    for case in report.get("cases", []):
        for item in case.get("decisions", []):
            reasons = ", ".join(
                (*item.get("reasons", []), *item.get("diagnostics", []))
            ) or "—"
            event_delta = "—" if item.get("event_delta") is None else str(item["event_delta"])
            lines.append(
                "| {task} | {phase} | `{actual}` | `{candidate}` | {reasons} | "
                "{streak} | {cache:,} | {artifact} | {delta} | {changed} |".format(
                    task=case.get("task_name", ""),
                    phase=item.get("phase_index", ""),
                    actual=item.get("actual_action", ""),
                    candidate=item.get("action", ""),
                    reasons=reasons,
                    streak=item.get("consecutive_max_tokens", 0),
                    cache=item.get("cumulative_cache_read_tokens", 0),
                    artifact=item.get("artifact_signal", "unknown"),
                    delta=event_delta,
                    changed="yes" if item.get("changed") else "no",
                )
            )
        terminal = case.get("terminal_guard")
        if terminal:
            reasons = ", ".join(
                (*terminal.get("reasons", []), *terminal.get("diagnostics", []))
            ) or "—"
            lines.append(
                "| {task} | terminal | `completed` | `{candidate}` | {reasons} | "
                "{streak} | {cache:,} | {artifact} | {delta} | {changed} |".format(
                    task=case.get("task_name", ""),
                    candidate=terminal.get("action", ""),
                    reasons=reasons,
                    streak=terminal.get("consecutive_max_tokens", 0),
                    cache=terminal.get("cumulative_cache_read_tokens", 0),
                    artifact=terminal.get("artifact_signal", "unknown"),
                    delta=(
                        "—"
                        if terminal.get("event_delta") is None
                        else terminal.get("event_delta")
                    ),
                    changed="yes" if terminal.get("changed") else "no",
                )
            )
    return "\n".join(lines) + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-root",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="LHTB result root containing runs/*/lhos_resume.json",
    )
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument(
        "--cache-threshold",
        type=int,
        default=CandidateGuardConfig.cumulative_cache_read_tokens,
        help="cumulative session cache-read threshold (default: 128000)",
    )
    parser.add_argument(
        "--max-consecutive-max-tokens",
        type=int,
        default=CandidateGuardConfig.max_consecutive_max_tokens,
    )
    parser.add_argument(
        "--no-progress-event-ratio",
        type=float,
        default=CandidateGuardConfig.no_progress_event_ratio,
    )
    args = parser.parse_args(argv)
    config = CandidateGuardConfig(
        max_consecutive_max_tokens=args.max_consecutive_max_tokens,
        cumulative_cache_read_tokens=args.cache_threshold,
        no_progress_event_ratio=args.no_progress_event_ratio,
    )
    report = evaluate_results_root(args.results_root, guard=CandidateGuard(config))
    text = json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True)
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(text + "\n", encoding="utf-8")
    if args.markdown_out:
        args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_out.write_text(_markdown(report), encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
