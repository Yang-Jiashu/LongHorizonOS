"""Summarize LHTB arm records at fixed, official-style time cutoffs.

This module is deliberately post-hoc and read-only.  It consumes the pair
runner's ``runs/<task>/<arm>.json`` records and the observability/checkpoint
artifacts they reference; it never starts Harbor, changes a harness, or uses
the final cumulative reward/usage as an earlier observation.

The official leaderboard cutoffs are the reference task budgets (in seconds):
3600, 5400, 10800, 14400, 18000, 21600, and 28800.  A custom run can still be
described with these cutoffs, but its protocol metadata remains visible in the
output and is not promoted to an official score.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "lhos-lhtb-prefix-summary.v1"
NA = "NA"
OFFICIAL_CUTOFFS_SECONDS = (3600, 5400, 10800, 14400, 18000, 21600, 28800)
OFFICIAL_REFERENCE = {
    "task_count": 46,
    "agent_timeout_seconds": 5400,
    "n_attempts": 1,
    "timeout_multiplier": 1.0,
    "environment_delete": True,
}
ARMS = ("dsh_fresh", "lhos_resume")
USAGE_KEYS = (
    "cache_read_tokens",
    "cache_write_tokens",
    "uncached_input_tokens",
    "output_tokens",
    "reasoning_tokens",
    "total_token_units",
    "model_calls",
    "tool_calls",
)
_USAGE_ALIASES = {
    "cache_read_tokens": ("cache_read_tokens", "cache_tokens", "cached_tokens"),
    "cache_write_tokens": ("cache_write_tokens",),
    "uncached_input_tokens": (
        "uncached_input_tokens",
        "input_tokens",
        "prompt_tokens",
    ),
    "output_tokens": ("output_tokens", "completion_tokens"),
    "reasoning_tokens": ("reasoning_tokens",),
    "total_token_units": ("total_token_units", "token_units", "total_tokens"),
    "model_calls": ("model_calls", "llm_calls"),
    "tool_calls": ("tool_calls",),
}


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _integer(value: Any) -> int | None:
    parsed = _number(value)
    if parsed is None:
        return None
    return int(parsed)


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _cutoffs(values: Iterable[int | float]) -> tuple[int, ...]:
    parsed = sorted({int(value) for value in values})
    if not parsed or any(value <= 0 for value in parsed):
        raise ValueError("cutoffs must contain positive seconds")
    return tuple(parsed)


def _invocation_times(invocation: Mapping[str, Any]) -> tuple[datetime | None, datetime | None]:
    started = _parse_time(invocation.get("started_at"))
    finished = _parse_time(invocation.get("finished_at"))
    if finished is None and started is not None:
        elapsed = _number(invocation.get("elapsed_ms"))
        if elapsed is not None and elapsed >= 0:
            finished = started + timedelta(milliseconds=elapsed)
    return started, finished


def _sorted_invocations(observability: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = observability.get("invocations")
    if not isinstance(raw, list):
        return []
    values = [item for item in raw if isinstance(item, dict)]
    # Preserve invocation order when timestamps are absent; timestamped items
    # are sorted chronologically so a malformed ordinal cannot leak a final
    # status into an earlier prefix.
    def sort_key(pair: tuple[int, dict[str, Any]]) -> tuple[bool, datetime, int]:
        started, _finished = _invocation_times(pair[1])
        return (
            started is None,
            started or datetime.max.replace(tzinfo=UTC),
            pair[0],
        )

    ordered = sorted(enumerate(values), key=sort_key)
    return [item for _, item in ordered]


def _usage_values(payload: Mapping[str, Any]) -> dict[str, float]:
    values: dict[str, float] = {}
    for canonical, aliases in _USAGE_ALIASES.items():
        for alias in aliases:
            parsed = _number(payload.get(alias))
            if parsed is not None:
                values[canonical] = parsed
                break
    return values


def _usage_event(
    payload: Mapping[str, Any],
    timestamp: datetime | None,
    *,
    kind: str,
) -> tuple[datetime | None, str, dict[str, float]] | None:
    values = _usage_values(payload)
    if not values:
        return None
    return timestamp, kind, values


def _usage_events(observability: Mapping[str, Any]) -> tuple[list[tuple[datetime, str, dict[str, float]]], bool]:
    """Return timestamped usage deltas/snapshots and whether final usage exists."""

    events: list[tuple[datetime, str, dict[str, float]]] = []
    decisions = observability.get("semantic_decisions")
    if isinstance(decisions, list):
        for decision in decisions:
            if not isinstance(decision, dict):
                continue
            timestamp = _parse_time(decision.get("decided_at"))
            delta = decision.get("usage_delta")
            if isinstance(delta, dict) and timestamp is not None:
                values = _usage_values(delta)
                if values:
                    events.append((timestamp, "delta", values))

    # Some older/newer records persist usage directly on each invocation.  Use
    # these only when semantic decision deltas are unavailable, avoiding a
    # double count of the same provider calls.
    if not events:
        for invocation in _sorted_invocations(observability):
            started, finished = _invocation_times(invocation)
            timestamp = finished or started
            delta = invocation.get("usage_delta")
            if isinstance(delta, dict):
                event = _usage_event(delta, timestamp, kind="delta")
                if event and event[0] is not None:
                    events.append(event)  # type: ignore[arg-type]
                continue
            snapshot = invocation.get("usage_snapshot")
            if isinstance(snapshot, dict):
                event = _usage_event(snapshot, timestamp, kind="snapshot")
                if event and event[0] is not None:
                    events.append(event)  # type: ignore[arg-type]
                continue
            usage = invocation.get("usage")
            if isinstance(usage, dict):
                event = _usage_event(usage, timestamp, kind="snapshot")
                if event and event[0] is not None:
                    events.append(event)  # type: ignore[arg-type]

    events.sort(key=lambda item: item[0])
    final_usage = isinstance(observability.get("usage"), dict) and bool(
        _usage_values(observability.get("usage") or {})
    )
    return events, final_usage


def _session_paths(agent_root: Path, observability: Mapping[str, Any]) -> list[Path]:
    """Return only known DSH session locations; never recurse through profiles."""

    arm = str(observability.get("arm", ""))
    patterns = (
        (
            "dsh-home/sessions/*/*/session.jsonl",
            "dsh-generations/generation-*/dsh-home/sessions/*/*/session.jsonl",
        )
        if arm == "lhos"
        else ("dsh-runs/invocation-*/dsh-home/sessions/*/*/session.jsonl",)
    )
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(path for path in agent_root.glob(pattern) if path.is_file())
    return sorted(set(paths))


def _event_time(record: Mapping[str, Any]) -> datetime | None:
    milliseconds = _number(record.get("time"))
    if milliseconds is None or milliseconds < 0:
        return None
    try:
        return datetime.fromtimestamp(milliseconds / 1000.0, tz=UTC)
    except (OSError, OverflowError, ValueError):
        return None


def _event_integer(value: Any, *, default: int = -1) -> int:
    parsed = _integer(value)
    return parsed if parsed is not None else default


def _raw_usage_payload(value: Any) -> dict[str, float] | None:
    """Mirror the production DSH parser's per-call token buckets."""

    if not isinstance(value, Mapping):
        return None
    fields = {
        "uncached_input_tokens": "inputTokens",
        "output_tokens": "outputTokens",
        "reasoning_tokens": "reasoningTokens",
        "cache_read_tokens": "cacheReadTokens",
        "cache_write_tokens": "cacheWriteTokens",
    }
    return {
        key: max(0.0, _number(value.get(field)) or 0.0)
        for key, field in fields.items()
    }


def _raw_session_usage(
    agent_root: Path | None,
    observability: Mapping[str, Any],
    cutoff: datetime,
) -> tuple[dict[str, int | str], str] | None:
    """Rebuild prefix usage from timestamped DSH session events.

    DSH's top-level ``usage`` is a final cumulative value.  Session events are
    timestamped, so they provide the exact available model/tool activity up to
    a cutoff.  ``updated_at`` is a hard cap: session files can be written after
    the observability artifact was finalized.
    """

    if agent_root is None or not agent_root.is_dir():
        return None
    paths = _session_paths(agent_root, observability)
    if not paths:
        return None
    observation_cap = min(
        cutoff,
        _parse_time(observability.get("updated_at")) or cutoff,
    )
    starts = [_invocation_times(item)[0] for item in _sorted_invocations(observability)]
    anchor = min((item for item in starts if item is not None), default=None)
    anchor = anchor or _parse_time(observability.get("started_at"))
    final_usage_by_call: dict[
        tuple[str, int, int, int], tuple[datetime, dict[str, float]]
    ] = {}
    chunk_usage_by_call: dict[
        tuple[str, int, int, int], tuple[datetime, dict[str, float]]
    ] = {}
    retry_generation: dict[tuple[str, int, int], int] = {}
    seen_retries: set[tuple[str, str, int]] = set()
    seen_retry_starts: set[tuple[str, str, int]] = set()
    tool_calls: set[tuple[str, str]] = set()
    relevant_event_seen = False
    malformed_rows = 0
    stale_rows = 0
    io_errors = 0
    for path in paths:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError):
            io_errors += 1
            continue
        for index, line in enumerate(lines):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                # A live writer can leave a partial trailing line.  Already
                # committed rows remain usable, but the result is a lower bound.
                malformed_rows += 1
                continue
            if not isinstance(event, dict):
                malformed_rows += 1
                continue
            if index == 0 and event.get("type") == "session":
                continue
            event_type = str(event.get("type", ""))
            if event_type not in {
                "assistant/message",
                "assistant/chunk",
                "llm/retry",
                "llm/retry-started",
                "tool/call",
            }:
                continue
            data = event.get("data")
            if not isinstance(data, dict):
                malformed_rows += 1
                continue
            timestamp = _event_time(event)
            if timestamp is None:
                malformed_rows += 1
                continue
            if anchor is not None and timestamp < anchor:
                stale_rows += 1
                continue
            relevant_event_seen = True
            if timestamp > observation_cap:
                continue

            turn = _event_integer(data.get("turn"))
            step = _event_integer(data.get("step"))
            base_key = (str(path), turn, step)
            generation = retry_generation.get(base_key, 0)
            key = (*base_key, generation)
            source_position = _event_integer(event.get("seq"), default=index)

            if event_type == "assistant/message":
                usage = _raw_usage_payload(data.get("usage"))
                if usage is not None:
                    # Replayed final messages overwrite one logical provider call.
                    final_usage_by_call[key] = (timestamp, usage)
            elif event_type == "assistant/chunk":
                chunk = data.get("chunk")
                usage = (
                    _raw_usage_payload(chunk.get("usage"))
                    if isinstance(chunk, dict) and chunk.get("type") == "usage"
                    else None
                )
                if usage is not None:
                    # A final assistant/message at or before the cutoff supersedes it.
                    chunk_usage_by_call[key] = (timestamp, usage)
            elif event_type == "llm/retry":
                retry_id = str(data.get("retryId", "") or "")
                retry_number = max(0, _event_integer(data.get("retry"), default=0))
                retry_key = (str(path), retry_id, retry_number)
                if retry_key not in seen_retries:
                    seen_retries.add(retry_key)
                    if key not in final_usage_by_call and key not in chunk_usage_by_call:
                        chunk_usage_by_call[key] = (
                            timestamp,
                            {
                                "uncached_input_tokens": 0.0,
                                "output_tokens": 0.0,
                                "reasoning_tokens": 0.0,
                                "cache_read_tokens": 0.0,
                                "cache_write_tokens": 0.0,
                            },
                        )
            elif event_type == "llm/retry-started":
                retry_id = str(data.get("retryId", "") or "")
                retry_number = max(0, _event_integer(data.get("retry"), default=0))
                retry_key = (str(path), retry_id, retry_number)
                if retry_key not in seen_retry_starts:
                    seen_retry_starts.add(retry_key)
                    retry_generation[base_key] = generation + 1
            elif event_type == "tool/call":
                call_id = str(data.get("callId", "") or "")
                if not call_id:
                    call_id = f"anonymous-{source_position}"
                tool_calls.add((str(path), call_id))

    if not relevant_event_seen:
        # Empty/all-invalid files must not turn a usable semantic delta into an
        # apparently exact zero measurement.
        return None

    usage_by_call = dict(chunk_usage_by_call)
    usage_by_call.update(final_usage_by_call)
    totals = {
        "cache_read_tokens": 0.0,
        "cache_write_tokens": 0.0,
        "uncached_input_tokens": 0.0,
        "output_tokens": 0.0,
        "reasoning_tokens": 0.0,
    }
    for _timestamp, usage in usage_by_call.values():
        for key in totals:
            totals[key] += usage[key]

    output: dict[str, int | str] = {key: NA for key in USAGE_KEYS}
    for key, value in totals.items():
        output[key] = int(value) if value.is_integer() else value
    output["model_calls"] = len(usage_by_call)
    output["tool_calls"] = len(tool_calls)

    # Match HarnessUsage.total_token_units: reasoning is reported separately,
    # while the DSH provider parser contributes no verification-token bucket.
    total = (
        totals["cache_read_tokens"]
        + totals["cache_write_tokens"]
        + totals["uncached_input_tokens"]
        + totals["output_tokens"]
    )
    output["total_token_units"] = int(total) if total.is_integer() else total
    observation = "raw_session_events_deduplicated"
    if malformed_rows or io_errors:
        observation = "partial_" + observation
    if stale_rows:
        observation += "_stale_rows_ignored"
    return output, observation


def _prefix_usage(
    observability: Mapping[str, Any],
    cutoff: datetime,
    *,
    run_end: datetime | None,
    agent_root: Path | None = None,
) -> tuple[dict[str, int | str], str]:
    raw_usage = _raw_session_usage(agent_root, observability, cutoff)
    if raw_usage is not None:
        return raw_usage
    events, final_usage = _usage_events(observability)
    eligible = [event for event in events if event[0] <= cutoff]
    if not eligible:
        return {key: NA for key in USAGE_KEYS}, (
            "final_cumulative_only" if final_usage else "missing_prefix_usage"
        )

    kind_set = {event[1] for event in eligible}
    values: dict[str, float] = {}
    if "delta" in kind_set:
        for _, kind, payload in eligible:
            if kind != "delta":
                continue
            for key, value in payload.items():
                values[key] = values.get(key, 0.0) + value
        observation = "prefix_delta"
    else:
        # A snapshot is cumulative; the latest snapshot before the cutoff is
        # the only value that can be attributed without double counting.
        for key, value in eligible[-1][2].items():
            values[key] = value
        observation = "prefix_snapshot"

    # If the run continued past the last usage checkpoint, these are an
    # explicitly observed lower bound, not an invented final total.
    if eligible[-1][0] < cutoff and (run_end is None or run_end > eligible[-1][0]):
        observation = "partial_" + observation
    output: dict[str, int | str] = {}
    for key in USAGE_KEYS:
        value = values.get(key)
        output[key] = int(value) if value is not None and value.is_integer() else value if value is not None else NA
    return output, observation


def _reward_value(record: Mapping[str, Any]) -> float | None:
    rewards = record.get("rewards")
    value: Any = rewards.get("reward") if isinstance(rewards, dict) else rewards
    if value is None:
        value = record.get("process_reward", record.get("reward"))
    return _number(value)


def _checkpoint_time(record: Mapping[str, Any], anchor: datetime) -> datetime | None:
    for key in (
        "verifier_finished_at",
        "finished_at",
        "recorded_at",
        "timestamp",
        "at",
        "created_at",
    ):
        parsed = _parse_time(record.get(key))
        if parsed is not None:
            return parsed
    for key in ("active_elapsed_ms", "elapsed_ms", "agent_elapsed_ms"):
        elapsed = _number(record.get(key))
        if elapsed is not None and elapsed >= 0:
            return anchor + timedelta(milliseconds=elapsed)
    return None


def _reward_checkpoints(record: Mapping[str, Any], anchor: datetime) -> list[tuple[datetime, float]]:
    candidates: list[Mapping[str, Any]] = []
    for container in (
        record.get("reward_checkpoints"),
        record.get("checkpoints"),
        (record.get("metrics") or {}).get("reward_checkpoints")
        if isinstance(record.get("metrics"), dict)
        else None,
    ):
        if isinstance(container, list):
            candidates.extend(item for item in container if isinstance(item, dict))
    metrics = record.get("metrics")
    trial_dir = metrics.get("trial_dir") if isinstance(metrics, dict) else None
    if trial_dir:
        trial = Path(str(trial_dir)).expanduser()
        jsonl = trial / "process_reward.jsonl"
        if jsonl.is_file():
            try:
                for line in jsonl.read_text(encoding="utf-8").splitlines():
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(item, dict):
                        candidates.append(item)
            except OSError:
                pass
        checkpoint_root = trial / "process_reward_checkpoints"
        if checkpoint_root.is_dir():
            for path in sorted(checkpoint_root.glob("*/checkpoint.json")):
                try:
                    item = _load(path)
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(item, dict):
                    candidates.append(item)
    seen: set[str] = set()
    output: list[tuple[datetime, float]] = []
    for item in candidates:
        value = _reward_value(item)
        timestamp = _checkpoint_time(item, anchor)
        if value is None or timestamp is None or timestamp < anchor:
            continue
        checkpoint_id = str(item.get("checkpoint_id") or "")
        key = checkpoint_id or f"{timestamp.isoformat()}:{value}"
        if key in seen:
            continue
        seen.add(key)
        output.append((timestamp, value))
    output.sort(key=lambda item: item[0])
    return output


def _interval_union_ms(intervals: Sequence[tuple[datetime, datetime]]) -> float:
    if not intervals:
        return 0.0
    ordered = sorted(intervals)
    merged_start, merged_end = ordered[0]
    total = 0.0
    for start, end in ordered[1:]:
        if start <= merged_end:
            if end > merged_end:
                merged_end = end
        else:
            total += max(0.0, (merged_end - merged_start).total_seconds() * 1000.0)
            merged_start, merged_end = start, end
    total += max(0.0, (merged_end - merged_start).total_seconds() * 1000.0)
    return round(total, 3)


def summarize_observability(
    observability: Mapping[str, Any],
    *,
    cutoffs: Iterable[int | float] = OFFICIAL_CUTOFFS_SECONDS,
    reward_checkpoints: Sequence[tuple[datetime, float]] = (),
    final_reward: float | None = None,
    agent_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Return one conservative prefix row per cutoff for an observability record."""

    cutoff_values = _cutoffs(cutoffs)
    invocations = _sorted_invocations(observability)
    starts = [_invocation_times(item)[0] for item in invocations]
    starts = [item for item in starts if item is not None]
    anchor = min(starts) if starts else _parse_time(observability.get("started_at"))
    if anchor is None:
        return [_empty_prefix_row(value) for value in cutoff_values]

    invocation_bounds: list[tuple[dict[str, Any], datetime, datetime | None]] = []
    for item in invocations:
        started, finished = _invocation_times(item)
        if started is not None:
            invocation_bounds.append((item, started, finished))
    observed_end = max(
        [end for _, _, end in invocation_bounds if end is not None]
        + [value for value in (_parse_time(observability.get("updated_at")),) if value is not None],
        default=None,
    )
    rows: list[dict[str, Any]] = []
    for seconds in cutoff_values:
        cutoff = anchor + timedelta(seconds=seconds)
        included = [(item, start, end) for item, start, end in invocation_bounds if start <= cutoff]
        intervals: list[tuple[datetime, datetime]] = []
        for _, start, end in included:
            clipped_end = min(end, cutoff) if end is not None else cutoff
            if clipped_end > start:
                intervals.append((start, clipped_end))
        active_ms = _interval_union_ms(intervals)
        wall_end = min(cutoff, observed_end) if observed_end is not None else cutoff
        wall_ms = max(0.0, (wall_end - anchor).total_seconds() * 1000.0)
        last_status: str | None = None
        last_status_observation = "no_invocation"
        resume_invocation_count = 0
        semantic_resume_count = 0
        session_id: str | None = None
        generation = -1
        for item, _start, end in included:
            raw_status = item.get("status")
            if end is None or end > cutoff:
                last_status = "running"
                last_status_observation = "in_progress_at_cutoff"
            else:
                last_status = str(raw_status) if raw_status is not None else "unknown"
                last_status_observation = "invocation_event"
            if item.get("resume") is True:
                resume_invocation_count += 1
            candidate_session = item.get("resume_session_id") or item.get("session_id")
            if candidate_session:
                session_id = str(candidate_session)
            candidate_generation = _integer(item.get("session_generation"))
            if candidate_generation is not None:
                generation = max(generation, candidate_generation)
        decisions = observability.get("semantic_decisions")
        decision_count = 0
        if isinstance(decisions, list):
            for decision in decisions:
                if not isinstance(decision, dict):
                    continue
                decided_at = _parse_time(decision.get("decided_at"))
                if decided_at is None or decided_at > cutoff:
                    continue
                decision_count += 1
                if decision.get("session_id"):
                    session_id = str(decision["session_id"])
                decision_generation = _integer(decision.get("session_generation"))
                if decision_generation is not None:
                    generation = max(generation, decision_generation)
                if decision.get("action") == "resume":
                    semantic_resume_count += 1
        # A semantic resume decision usually causes the following invocation,
        # so adding these counts would double count.  The larger count keeps a
        # durable decision visible when its invocation start has not yet been
        # persisted at the cutoff.
        resume_count = max(resume_invocation_count, semantic_resume_count)
        usage, usage_observation = _prefix_usage(
            observability,
            cutoff,
            run_end=observed_end,
            agent_root=agent_root,
        )
        reward = NA
        reward_observation = "missing_prefix_reward"
        eligible_rewards = [value for timestamp, value in reward_checkpoints if timestamp <= cutoff]
        if eligible_rewards:
            reward = eligible_rewards[-1]
            reward_observation = "checkpoint"
        elif final_reward is not None:
            reward_observation = "final_only_unusable"
        rows.append(
            {
                "cutoff_seconds": seconds,
                "cutoff_at": _iso(cutoff),
                "anchor_started_at": _iso(anchor),
                "active_elapsed_ms": active_ms,
                "wall_elapsed_ms": round(wall_ms, 3),
                "invocation_count": len(included),
                "invocations_started": len(included),
                "last_invocation_status": last_status or NA,
                "last_invocation_status_observation": last_status_observation,
                "usage": usage,
                "usage_observation": usage_observation,
                "resume_count": resume_count,
                "resume_invocation_count": resume_invocation_count,
                "semantic_resume_decision_count": semantic_resume_count,
                "session_id": session_id or NA,
                "session_reused": resume_count > 0 if included or decision_count else NA,
                "session_generation_count": generation + 1 if generation >= 0 else NA,
                "semantic_decision_count": decision_count,
                "reward": reward,
                "reward_observation": reward_observation,
            }
        )
    return rows


def _empty_prefix_row(seconds: int) -> dict[str, Any]:
    return {
        "cutoff_seconds": seconds,
        "cutoff_at": NA,
        "anchor_started_at": NA,
        "active_elapsed_ms": NA,
        "wall_elapsed_ms": NA,
        "invocation_count": 0,
        "invocations_started": 0,
        "last_invocation_status": NA,
        "last_invocation_status_observation": "missing_anchor",
        "usage": {key: NA for key in USAGE_KEYS},
        "usage_observation": "missing_prefix_usage",
        "resume_count": 0,
        "resume_invocation_count": 0,
        "semantic_resume_decision_count": 0,
        "session_id": NA,
        "session_reused": NA,
        "session_generation_count": NA,
        "semantic_decision_count": 0,
        "reward": NA,
        "reward_observation": "missing_prefix_reward",
    }


def _record_observability(
    record: Mapping[str, Any],
    record_path: Path,
    run_root: Path,
) -> tuple[dict[str, Any], Path] | None:
    if isinstance(record.get("invocations"), list):
        return dict(record), record_path
    metrics = record.get("metrics")
    candidates: list[Any] = []
    if isinstance(metrics, dict):
        candidates.append(metrics.get("observability"))
    candidates.append(record.get("observability"))
    for candidate in candidates:
        if not candidate:
            continue
        reference = Path(str(candidate)).expanduser()
        paths = (
            [reference]
            if reference.is_absolute()
            else [
                (record_path.parent / reference).resolve(),
                (run_root / reference).resolve(),
            ]
        )
        for path in dict.fromkeys(paths):
            if not path.is_file():
                continue
            try:
                value = _load(path)
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                return value, path
    return None


def _record_reward_inputs(
    record: Mapping[str, Any],
    obs: Mapping[str, Any],
) -> tuple[list[tuple[datetime, float]], float | None]:
    starts = [_invocation_times(item)[0] for item in _sorted_invocations(obs)]
    anchor = min((item for item in starts if item is not None), default=None)
    if anchor is None:
        anchor = _parse_time(obs.get("started_at"))
    if anchor is None:
        return [], _number((record.get("metrics") or {}).get("reward")) if isinstance(record.get("metrics"), dict) else None
    checkpoints = _reward_checkpoints(record, anchor)
    metrics = record.get("metrics")
    final_reward = _number(metrics.get("reward")) if isinstance(metrics, dict) else _number(record.get("reward"))
    return checkpoints, final_reward


def _discover_records(run_root: Path) -> list[tuple[str, str, Path, dict[str, Any]]]:
    roots = [run_root / "runs"] if (run_root / "runs").is_dir() else [run_root]
    found: list[tuple[str, str, Path, dict[str, Any]]] = []
    seen: set[Path] = set()
    for root in roots:
        for path in sorted(root.glob("*/*.json")):
            if path.name not in {"dsh_fresh.json", "lhos_resume.json"}:
                continue
            try:
                record = _load(path)
            except (OSError, json.JSONDecodeError):
                continue
            if not isinstance(record, dict) or path in seen:
                continue
            seen.add(path)
            found.append((path.parent.name, path.stem, path, record))
    return found


def _safe_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    # Keep provenance useful without copying arbitrary fields (including any
    # provider/credential metadata) into the summary artifact.
    keys = (
        "schema_version",
        "official_protocol",
        "official_score",
        "model",
        "reasoning_effort",
        "agent_timeout_seconds",
        "agent_timeout_mode",
        "time_slice_mode",
        "time_slice_seconds",
        "n_attempts",
        "timeout_multiplier",
        "environment_delete",
    )
    return {key: manifest.get(key, NA) for key in keys}


def summarize(
    run_root: Path,
    *,
    cutoffs: Iterable[int | float] = OFFICIAL_CUTOFFS_SECONDS,
) -> dict[str, Any]:
    root = run_root.expanduser().resolve()
    cutoff_values = _cutoffs(cutoffs)
    manifest: dict[str, Any] = {}
    manifest_path = root / "manifest.json"
    if manifest_path.is_file():
        try:
            loaded = _load(manifest_path)
            if isinstance(loaded, dict):
                manifest = loaded
        except (OSError, json.JSONDecodeError):
            manifest = {}
    records: list[dict[str, Any]] = []
    for task_name, arm, path, record in _discover_records(root):
        observed = _record_observability(record, path, root)
        if observed is None:
            rows = [_empty_prefix_row(value) for value in cutoff_values]
            final_reward = None
            checkpoints: list[tuple[datetime, float]] = []
        else:
            obs, observability_path = observed
            checkpoints, final_reward = _record_reward_inputs(record, obs)
            rows = summarize_observability(
                obs,
                cutoffs=cutoff_values,
                reward_checkpoints=checkpoints,
                final_reward=final_reward,
                agent_root=(
                    observability_path.parent
                    if observability_path.name == "dsh-observability.json"
                    else None
                ),
            )
        records.append(
            {
                "task_name": task_name,
                "arm": arm,
                "record": str(path),
                "observability": (
                    str((record.get("metrics") or {}).get("observability"))
                    if isinstance(record.get("metrics"), dict)
                    and (record.get("metrics") or {}).get("observability")
                    else NA
                ),
                "prefixes": rows,
            }
        )
    records.sort(key=lambda item: (str(item["task_name"]), str(item["arm"])))
    task_names = sorted({str(item["task_name"]) for item in records})
    arms_by_task = {
        task_name: sorted(
            {
                str(item["arm"])
                for item in records
                if str(item["task_name"]) == task_name
            }
        )
        for task_name in task_names
    }
    complete_task_names = [
        task_name
        for task_name, arms in arms_by_task.items()
        if arms == sorted(ARMS)
    ]
    incomplete_task_names = [
        task_name for task_name in task_names if task_name not in complete_task_names
    ]
    controlled = manifest.get("controlled_pair_experiment")
    declared_harness_constant = (
        controlled.get("harness_constant") if isinstance(controlled, dict) else None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "claim_status": "descriptive_prefix_observation",
        "official_score": False,
        "leaderboard_comparable": False,
        "run_root": str(root),
        "cutoffs_seconds": list(cutoff_values),
        "official_reference": {
            **OFFICIAL_REFERENCE,
            "cutoff_seconds": list(OFFICIAL_CUTOFFS_SECONDS),
        },
        "protocol": _safe_manifest(manifest),
        "task_count": len(task_names),
        "tasks": task_names,
        "complete_pair_count": len(complete_task_names),
        "complete_pair_task_names": complete_task_names,
        "incomplete_pair_count": len(incomplete_task_names),
        "incomplete_pair_task_names": incomplete_task_names,
        "pair_complete": not incomplete_task_names and bool(task_names),
        "arms_by_task": arms_by_task,
        "record_count": len(records),
        "records": records,
        "harness_constant": (
            declared_harness_constant
            if isinstance(declared_harness_constant, bool)
            else NA
        ),
        "analysis_note": (
            "Prefix reward is reported only from timestamped verifier checkpoints. "
            "Top-level final reward and final cumulative usage are never attributed "
            "to an earlier cutoff. This artifact is not a paired ITT or leaderboard "
            "score; final outcome scoring must validate all expected tasks and arms."
        ),
    }


def _display(value: Any) -> str:
    if value == NA or value is None:
        return NA
    if isinstance(value, float):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    return str(value)


def render_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# LHTB Prefix Summary",
        "",
        f"- Run root: `{summary.get('run_root', NA)}`",
        f"- Tasks: {summary.get('task_count', 0)}; records: {summary.get('record_count', 0)}",
        (
            f"- Complete pairs: {summary.get('complete_pair_count', 0)}; "
            f"incomplete: {summary.get('incomplete_pair_count', 0)}"
        ),
        f"- Harness constant: `{_display(summary.get('harness_constant'))}`",
        "- Claim: descriptive prefix observation; not a paired ITT or leaderboard score",
        "- Reference cutoffs: `3600, 5400, 10800, 14400, 18000, 21600, 28800s`",
        "",
        "| Task | Arm | Cutoff | Active ms | Invocations | Last status | Model calls | Tool calls | Reward |",
        "|---|---|---:|---:|---:|---|---:|---:|---:|",
    ]
    for record in summary.get("records", []):
        if not isinstance(record, dict):
            continue
        for row in record.get("prefixes", []):
            if not isinstance(row, dict):
                continue
            usage = row.get("usage") if isinstance(row.get("usage"), dict) else {}
            lines.append(
                f"| {record.get('task_name', NA)} | {record.get('arm', NA)} | "
                f"{row.get('cutoff_seconds', NA)}s | {_display(row.get('active_elapsed_ms'))} | "
                f"{row.get('invocation_count', NA)} | {row.get('last_invocation_status', NA)} | "
                f"{_display(usage.get('model_calls', NA))} | {_display(usage.get('tool_calls', NA))} | "
                f"{_display(row.get('reward'))} |"
            )
    lines.extend(["", summary.get("analysis_note", "")])
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_root", type=Path)
    parser.add_argument("--output", type=Path, required=True, help="JSON output path or directory")
    parser.add_argument(
        "--cutoffs",
        nargs="+",
        type=int,
        default=list(OFFICIAL_CUTOFFS_SECONDS),
        help="Prefix cutoffs in seconds (defaults to the official reference set).",
    )
    parser.add_argument("--markdown", action="store_true", help="Also write a Markdown rendering")
    args = parser.parse_args()
    result = summarize(args.run_root, cutoffs=args.cutoffs)
    output = args.output.expanduser()
    if output.suffix.lower() == ".json":
        output.parent.mkdir(parents=True, exist_ok=True)
        json_path = output
    else:
        output.mkdir(parents=True, exist_ok=True)
        json_path = output / "prefix-summary.json"
    json_path.write_text(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.markdown or output.suffix.lower() != ".json":
        markdown_path = json_path.with_name("PREFIX-SUMMARY.md")
        markdown_path.write_text(render_markdown(result), encoding="utf-8")
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
