"""Profile a real DSH dynamic-coding result, including adapter v3 records."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

CRITICAL_PATH = ("pricing_core", "pricing_api", "integration")
REUSED_TASKS = ("audit",)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _usage(record: dict[str, Any]) -> dict[str, int]:
    raw = record.get("usage_total")
    if not isinstance(raw, dict):
        trace = record.get("trace", {})
        raw = trace.get("usage", {}) if isinstance(trace, dict) else {}
    if not isinstance(raw, dict):
        raw = record.get("usage", {})
    if not isinstance(raw, dict):
        raw = {}
    uncached = int(raw.get("uncached_input_tokens", 0) or 0)
    cache_read = int(raw.get("cache_read_tokens", 0) or 0)
    cache_write = int(raw.get("cache_write_tokens", 0) or 0)
    output = int(raw.get("output_tokens", 0) or 0)
    total = int(raw.get("total_token_units", uncached + cache_read + cache_write + output) or 0)
    return {
        "total_token_units": total,
        "uncached_input_tokens": uncached,
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "output_tokens": output,
        "model_calls": int(raw.get("model_calls", 0) or 0),
        "tool_calls": int(raw.get("tool_calls", 0) or 0),
        "wall_time_ms": int(raw.get("wall_time_ms", 0) or 0),
    }


def _trace_profile(record: dict[str, Any], elapsed_ms: float) -> dict[str, Any]:
    trace = record.get("trace", {})
    if not isinstance(trace, dict):
        trace = {}
    tools = trace.get("tool_calls", ())
    if not isinstance(tools, list):
        tools = list(tools or ())
    tool_counts: Counter[str] = Counter()
    tool_duration_ms = 0.0
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        name = str(tool.get("name", "") or "")
        tool_counts[name] += 1
        tool_duration_ms += float(tool.get("duration_ms", 0) or 0)
    retry_count = int(trace.get("retries", 0) or 0)
    retry_delay_ms = float(trace.get("retry_delay_ms", 0) or 0)
    return {
        "tool_counts": dict(sorted(tool_counts.items())),
        "tool_duration_ms": round(tool_duration_ms, 3),
        "retry_count": retry_count,
        "retry_delay_ms": round(retry_delay_ms, 3),
        "model_context_provider_ms": round(
            max(0.0, elapsed_ms - tool_duration_ms - retry_delay_ms),
            3,
        ),
        "process_outside_session_ms": 0.0,
        "event_count": int(trace.get("event_count", 0) or 0),
        "unknown_io": bool(trace.get("unknown_io", False)),
    }


def _normalize_attempt(record: dict[str, Any], path: Path) -> dict[str, Any]:
    schema = str(record.get("schema_version", ""))
    if schema == "deepseek-harness-attempt.v1":
        binding = record.get("binding", {})
        binding = binding if isinstance(binding, dict) else {}
        process = record.get("process", {})
        process = process if isinstance(process, dict) else {}
        task_id = str(record.get("phase_id", "") or "")
        version = int(record.get("phase_version", 1) or 1)
        attempt_id = str(binding.get("attempt_id", "") or record.get("attempt_record_id", ""))
        attempt_number = int(record.get("attempt_number", 1) or 1)
        elapsed_ms = float(process.get("elapsed_ms", 0) or 0)
        failure = record.get("failure")
        failure_class = (
            str(failure.get("failure_class", ""))
            if isinstance(failure, dict)
            else str(failure or "")
        )
        normalized = {
            "task_id": task_id,
            "version": version,
            "attempt_id": attempt_id,
            "attempt_number": attempt_number,
            "elapsed_ms": elapsed_ms,
            "failure_class": failure_class,
            "record_path": str(path),
            "usage": _usage(record),
        }
        normalized["profile"] = _trace_profile(record, elapsed_ms)
        return normalized

    # Compatibility with the former dsh-dynamic-attempt.v1 view.
    normalized = {
        "task_id": str(record.get("task_id", "") or ""),
        "version": int(record.get("requirement_version", record.get("phase_version", 1)) or 1),
        "attempt_id": str(record.get("attempt_id", "") or path.stem),
        "attempt_number": int(record.get("attempt_number", 1) or 1),
        "elapsed_ms": float(record.get("elapsed_ms", 0) or 0),
        "failure_class": str(record.get("failure", "") or ""),
        "record_path": str(path),
        "usage": _usage(record),
    }
    normalized["profile"] = _trace_profile(record, normalized["elapsed_ms"])
    return normalized


def _attempts(arm: dict[str, Any]) -> list[dict[str, Any]]:
    workspace = Path(str(arm.get("workspace", "")))
    attempt_dir = workspace.parent / "attempts"
    if not attempt_dir.is_dir():
        return []
    dedup: dict[tuple[str, int, str, int], dict[str, Any]] = {}
    for path in sorted(attempt_dir.glob("*.json")):
        try:
            record = _load_json(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if str(record.get("schema_version", "")) not in {
            "deepseek-harness-attempt.v1",
            "dsh-dynamic-attempt.v1",
        }:
            continue
        normalized = _normalize_attempt(record, path)
        key = (
            normalized["task_id"],
            normalized["version"],
            normalized["attempt_id"],
            normalized["attempt_number"],
        )
        dedup[key] = normalized
    return sorted(
        dedup.values(),
        key=lambda item: (
            int(item["version"]),
            str(item["task_id"]),
            int(item["attempt_number"]),
        ),
    )


def _phase(records: list[dict[str, Any]], version: int) -> dict[str, Any]:
    selected = [item for item in records if int(item["version"]) == version]
    by_task: dict[str, dict[str, Any]] = {}
    tool_counts: Counter[str] = Counter()
    for record in selected:
        usage = record["usage"]
        profile = record["profile"]
        task_id = str(record["task_id"])
        existing = by_task.setdefault(
            task_id,
            {
                "attempts": 0,
                "elapsed_ms": 0.0,
                "model_calls": 0,
                "tool_calls": 0,
                "token_units": 0,
                "uncached_input_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "output_tokens": 0,
                "retry_count": 0,
                "retry_delay_ms": 0.0,
                "tool_duration_ms": 0.0,
                "model_context_provider_ms": 0.0,
                "unknown_io": False,
            },
        )
        existing["attempts"] += 1
        for key in (
            "elapsed_ms",
            "model_calls",
            "tool_calls",
            "token_units",
            "uncached_input_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "output_tokens",
        ):
            source_key = "total_token_units" if key == "token_units" else key
            if key == "elapsed_ms":
                existing[key] += float(record["elapsed_ms"])
            else:
                existing[key] += int(usage[source_key])
        for key in (
            "retry_count",
            "retry_delay_ms",
            "tool_duration_ms",
            "model_context_provider_ms",
        ):
            existing[key] += profile[key]
        existing["unknown_io"] = existing["unknown_io"] or profile["unknown_io"]
        tool_counts.update(profile["tool_counts"])
    return {
        "attempts": len(selected),
        "task_ids": sorted(by_task),
        "by_task": by_task,
        "elapsed_ms_sum": round(
            sum(float(item["elapsed_ms"]) for item in selected),
            3,
        ),
        "model_calls": sum(item["usage"]["model_calls"] for item in selected),
        "tool_calls": sum(item["usage"]["tool_calls"] for item in selected),
        "token_units": sum(item["usage"]["total_token_units"] for item in selected),
        "uncached_input_tokens": sum(item["usage"]["uncached_input_tokens"] for item in selected),
        "cache_read_tokens": sum(item["usage"]["cache_read_tokens"] for item in selected),
        "cache_write_tokens": sum(item["usage"]["cache_write_tokens"] for item in selected),
        "output_tokens": sum(item["usage"]["output_tokens"] for item in selected),
        "retry_count": sum(item["profile"]["retry_count"] for item in selected),
        "retry_delay_ms": round(
            sum(item["profile"]["retry_delay_ms"] for item in selected),
            3,
        ),
        "tool_duration_ms": round(
            sum(item["profile"]["tool_duration_ms"] for item in selected),
            3,
        ),
        "model_context_provider_ms": round(
            sum(item["profile"]["model_context_provider_ms"] for item in selected),
            3,
        ),
        "tool_counts": dict(sorted(tool_counts.items())),
    }


def _shared_generation(result: dict[str, Any]) -> dict[str, Any]:
    pair = result["pairs"][0]
    generation = pair.get("shared_v1_generation", {})
    usage = generation.get("usage", {}) if isinstance(generation, dict) else {}
    return {
        "snapshot_id": str(pair.get("shared_snapshot_id", "")),
        "valid": bool(generation.get("valid", False)),
        "token_units": int(usage.get("total_token_units", 0) or 0),
        "model_calls": int(usage.get("model_calls", 0) or 0),
        "tool_calls": int(usage.get("tool_calls", 0) or 0),
        "wall_ms": float(generation.get("wall_ms", 0) or 0),
    }


def build_profile(result_path: Path, *, pair_index: int = 0) -> dict[str, Any]:
    result = _load_json(result_path)
    pairs = result.get("pairs", [])
    if pair_index < 0 or pair_index >= len(pairs):
        raise ValueError(f"pair_index {pair_index} is outside result pairs")
    pair = pairs[pair_index]
    static = pair["static"]
    lhos = pair["lhos"]
    static_records = _attempts(static)
    lhos_records = _attempts(lhos)
    static_repair = _phase(static_records, 2)
    lhos_repair = _phase(lhos_records, 2)
    preserved = _phase(
        [item for item in static_records if item["task_id"] in REUSED_TASKS],
        2,
    )
    delta_keys = (
        "elapsed_ms_sum",
        "model_calls",
        "tool_calls",
        "token_units",
        "uncached_input_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "output_tokens",
        "retry_count",
        "retry_delay_ms",
        "tool_duration_ms",
        "model_context_provider_ms",
    )
    observed_delta = {
        key: round(float(static_repair[key]) - float(lhos_repair[key]), 3) for key in delta_keys
    }
    shared = _shared_generation(result)
    repair = pair.get("lhos", {}).get("repair", {})
    return {
        "schema_version": "lhos-case-profile.v2",
        "case": result.get("benchmark", "real_dsh_dynamic_coding"),
        "source_result": str(result_path),
        "pair_index": pair_index,
        "correctness": {
            "pair_valid": bool(pair.get("comparison", {}).get("pair_valid", False)),
            "static_public": bool(
                static.get("final_grade", {}).get("public", {}).get("passed", False)
            ),
            "static_hidden": bool(
                static.get("final_grade", {}).get("hidden", {}).get("passed", False)
            ),
            "lhos_public": bool(lhos.get("final_grade", {}).get("public", {}).get("passed", False)),
            "lhos_hidden": bool(lhos.get("final_grade", {}).get("hidden", {}).get("passed", False)),
            "under_invalidation": list(repair.get("under_invalidation", [])),
            "over_invalidation": list(repair.get("over_invalidation", [])),
        },
        "experiment": {
            "benchmark_version": result.get("benchmark_version"),
            "shared_initial_snapshot": bool(result.get("shared_initial_snapshot", False)),
            "controller_difference_only": bool(result.get("controller_difference_only", False)),
            "independent_initial_trajectories": bool(
                result.get("independent_initial_trajectories", True)
            ),
            "shared": shared,
        },
        "task_graph": {
            "critical_path": list(CRITICAL_PATH),
            "preserved_tasks": list(repair.get("preserved", [])),
            "affected_tasks": list(repair.get("affected", [])),
            "frontier": list(repair.get("frontier", [])),
        },
        "repair": {
            "static": static_repair,
            "lhos": lhos_repair,
            "static_minus_lhos": observed_delta,
            "preserved_lower_bound": preserved,
            "snapshot_adoption": {
                "wall_ms": float(lhos.get("snapshot_adoption_wall_ms", 0) or 0),
                "model_calls": int(lhos.get("snapshot_adoption_model_calls", 0) or 0),
            },
        },
    }


def render_markdown(profile: dict[str, Any]) -> str:
    repair = profile["repair"]
    static = repair["static"]
    lhos = repair["lhos"]
    delta = repair["static_minus_lhos"]
    lower = repair["preserved_lower_bound"]
    experiment = profile["experiment"]
    correctness = profile["correctness"]
    lines = [
        "# LongHorizonOS Dynamic Repair Profile",
        "",
        "## Experimental validity",
        "",
        f"- Shared verified v1 snapshot: `{experiment['shared_initial_snapshot']}`",
        f"- Controller-only comparison: `{experiment['controller_difference_only']}`",
        f"- Snapshot id: `{experiment['shared']['snapshot_id']}`",
        f"- Pair valid: `{correctness['pair_valid']}`",
        f"- Static/LHOS public: `{correctness['static_public']}` / `{correctness['lhos_public']}`",
        f"- Static/LHOS hidden: `{correctness['static_hidden']}` / `{correctness['lhos_hidden']}`",
        "",
        "## Repair result",
        "",
        "| Metric | Static | LHOS | Static - LHOS |",
        "|---|---:|---:|---:|",
        f"| Token units | {static['token_units']:,} | {lhos['token_units']:,} | "
        f"{delta['token_units']:+,} |",
        f"| Model calls | {static['model_calls']} | {lhos['model_calls']} | "
        f"{delta['model_calls']:+,} |",
        f"| Tool calls | {static['tool_calls']} | {lhos['tool_calls']} | "
        f"{delta['tool_calls']:+,} |",
        f"| Wall time | {static['elapsed_ms_sum'] / 1000:.3f}s | "
        f"{lhos['elapsed_ms_sum'] / 1000:.3f}s | "
        f"{delta['elapsed_ms_sum'] / 1000:+.3f}s |",
        "",
        "## Direct OS effect",
        "",
        f"- Preserved tasks: `{', '.join(profile['task_graph']['preserved_tasks']) or 'none'}`",
        f"- Affected tasks: `{', '.join(profile['task_graph']['affected_tasks'])}`",
        f"- Repair frontier: `{', '.join(profile['task_graph']['frontier'])}`",
        f"- Skipped-task lower bound: {lower['token_units']:,} token units, "
        f"{lower['model_calls']} model calls, {lower['tool_calls']} tool calls",
        f"- LHOS snapshot adoption: {repair['snapshot_adoption']['wall_ms'] / 1000:.3f}s, "
        f"{repair['snapshot_adoption']['model_calls']} model calls",
        "",
        "## Time components",
        "",
        "| Component | Static | LHOS | Static - LHOS |",
        "|---|---:|---:|---:|",
        f"| Retry delay | {static['retry_delay_ms'] / 1000:.3f}s | "
        f"{lhos['retry_delay_ms'] / 1000:.3f}s | "
        f"{(static['retry_delay_ms'] - lhos['retry_delay_ms']) / 1000:+.3f}s |",
        f"| Tool execution | {static['tool_duration_ms'] / 1000:.3f}s | "
        f"{lhos['tool_duration_ms'] / 1000:.3f}s | "
        f"{(static['tool_duration_ms'] - lhos['tool_duration_ms']) / 1000:+.3f}s |",
        f"| Model/context/provider residual | "
        f"{static['model_context_provider_ms'] / 1000:.3f}s | "
        f"{lhos['model_context_provider_ms'] / 1000:.3f}s | "
        f"{(static['model_context_provider_ms'] - lhos['model_context_provider_ms']) / 1000:+.3f}s |",
        "",
        "## Interpretation",
        "",
        "The preserved-task lower bound is directly attributable to LHOS selective "
        "invalidation. The remaining difference is the observed affected-task "
        "trajectory under the same v1 snapshot; report it as an empirical outcome, "
        "not as a guaranteed causal saving.",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--pair-index", type=int, default=0)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--markdown-out", type=Path)
    args = parser.parse_args()
    result_path = args.result.resolve()
    profile = build_profile(result_path, pair_index=args.pair_index)
    json_out = args.json_out or result_path.with_name("case-profile.json")
    markdown_out = args.markdown_out or result_path.with_name("CASE-PROFILE.zh-CN.md")
    json_out.write_text(
        json.dumps(profile, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    markdown_out.write_text(render_markdown(profile), encoding="utf-8")
    print(json.dumps({"json": str(json_out), "markdown": str(markdown_out)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
