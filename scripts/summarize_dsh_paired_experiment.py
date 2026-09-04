"""Summarize paired DeepSeek Harness controller experiments."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any


def _mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def _pct(saved: float, baseline: float) -> float | None:
    return None if baseline <= 0 else saved / baseline * 100.0


def _is_valid_pair(pair: dict[str, Any]) -> bool:
    return bool(pair.get("comparison", {}).get("pair_valid", False))


def build(result: dict[str, Any]) -> dict[str, Any]:
    pairs = [item for item in result.get("pairs", ()) if isinstance(item, dict)]
    valid = [pair for pair in pairs if _is_valid_pair(pair)]
    failed = [pair for pair in pairs if not _is_valid_pair(pair)]
    metrics = {
        "repair_tokens": ("static_repair_tokens", "lhos_repair_tokens"),
        "repair_model_calls": ("static_repair_model_calls", "lhos_repair_model_calls"),
        "repair_wall_ms": ("static_repair_wall_ms", "lhos_repair_wall_ms"),
    }
    summary: dict[str, Any] = {
        "pairs": len(pairs),
        "valid_pairs": len(valid),
        "failed_pairs": len(failed),
        "failed_pair_ids": [int(pair.get("pair", 0)) for pair in failed],
        "shared_snapshot_pairs": sum(bool(pair.get("shared_snapshot_id")) for pair in pairs),
        "same_snapshot_pairs": sum(
            bool(pair.get("shared_snapshot_id"))
            and bool(pair.get("static", {}).get("shared_snapshot_id"))
            and pair.get("shared_snapshot_id")
            == pair.get("static", {}).get("shared_snapshot_id")
            == pair.get("lhos", {}).get("shared_snapshot_id")
            for pair in pairs
        ),
        "exact_invalidation_pairs": sum(
            not pair.get("lhos", {}).get("repair", {}).get("under_invalidation")
            and not pair.get("lhos", {}).get("repair", {}).get("over_invalidation")
            for pair in valid
        ),
        "metrics": {},
    }
    for name, (static_key, lhos_key) in metrics.items():
        static = [float(pair.get("comparison", {}).get(static_key, 0) or 0) for pair in valid]
        lhos = [float(pair.get("comparison", {}).get(lhos_key, 0) or 0) for pair in valid]
        diffs = [left - right for left, right in zip(static, lhos, strict=True)]
        summary["metrics"][name] = {
            "static_total": sum(static),
            "lhos_total": sum(lhos),
            "aggregate_saving_pct": _pct(sum(diffs), sum(static)),
            "static_mean": _mean(static),
            "lhos_mean": _mean(lhos),
            "static_median": _median(static),
            "lhos_median": _median(lhos),
            "lhos_wins": sum(diff > 0 for diff in diffs),
            "ties": sum(diff == 0 for diff in diffs),
            "static_wins": sum(diff < 0 for diff in diffs),
        }
    return summary


def render(result: dict[str, Any], summary: dict[str, Any]) -> str:
    metrics = summary["metrics"]
    shared = bool(result.get("shared_initial_snapshot", False))
    lines = [
        "# DeepSeek Harness vs LongHorizonOS",
        "",
        "## Configuration",
        "",
        f"- Model: `{result.get('model', 'recorded by patch')}`",
        f"- Reasoning: `{result.get('reasoning_effort', 'recorded by patch')}`",
        f"- Provider: `{result.get('provider_route', '')}`",
        f"- Pairs: {summary['pairs']} total, {summary['valid_pairs']} valid, "
        f"{summary['failed_pairs']} failed",
        f"- Shared verified v1 snapshot: `{shared}`",
        f"- Controller-only comparison: `{result.get('controller_difference_only', False)}`",
        "",
        "## Correctness",
        "",
        f"- Valid pairs: {summary['valid_pairs']}/{summary['pairs']}",
        f"- Exact invalidation cone: "
        f"{summary['exact_invalidation_pairs']}/{summary['valid_pairs'] or 0} valid pairs",
        "",
        "| Metric | Static total | LHOS total | Aggregate saving | LHOS wins/ties/static wins |",
        "|---|---:|---:|---:|---:|",
    ]
    labels = {
        "repair_tokens": "Repair token units",
        "repair_model_calls": "Repair model calls",
        "repair_wall_ms": "Repair wall time (ms)",
    }
    for key in ("repair_tokens", "repair_model_calls", "repair_wall_ms"):
        metric = metrics[key]
        saving = metric["aggregate_saving_pct"]
        saving_text = "n/a" if saving is None else f"{saving:+.2f}%"
        lines.append(
            f"| {labels[key]} | {metric['static_total']:,.0f} | "
            f"{metric['lhos_total']:,.0f} | {saving_text} | "
            f"{metric['lhos_wins']}/{metric['ties']}/{metric['static_wins']} |"
        )
    lines.extend(
        [
            "",
            "## Distribution",
            "",
            "| Metric | Static mean | LHOS mean | Static median | LHOS median |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for key in ("repair_tokens", "repair_model_calls", "repair_wall_ms"):
        metric = metrics[key]
        lines.append(
            f"| {labels[key]} | {metric['static_mean']:,.1f} | "
            f"{metric['lhos_mean']:,.1f} | {metric['static_median']:,.1f} | "
            f"{metric['lhos_median']:,.1f} |"
        )
    if shared:
        conclusion = (
            "Because both arms fork from the same verified v1 snapshot, the repair "
            "comparison is a controller-level experiment. The strongest direct LHOS "
            "effect is the preserved-task computation that was not dispatched."
        )
    else:
        conclusion = (
            "The arms have independent initial trajectories. Treat repair token/time "
            "differences as observational engineering results, not causal scheduler "
            "speedups. Re-run with a shared verified v1 snapshot before publication."
        )
    lines.extend(["", "## Conclusion", "", conclusion, ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = json.loads(args.result.read_text(encoding="utf-8"))
    summary = build(result)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "paired-summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (args.output / "PAIRED-SUMMARY.zh-CN.md").write_text(
        render(result, summary),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
