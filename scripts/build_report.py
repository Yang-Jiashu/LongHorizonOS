"""Build a markdown benchmark report from `lhos benchmark` JSON results.

Usage:
    python scripts/build_report.py [results.json ...] [--out report.md]

With no arguments, the newest file in artifacts/benchmark_results/ is used.
The report contains: run configuration, per-preset aggregate tables (mean
over seeds), a mode-comparison table (mean over presets), and the metric
glossary (spec 24). Deterministic: depends only on the input JSON.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

METRIC_GLOSSARY: list[tuple[str, str]] = [
    ("success", "run completed with every schedulable node VERIFIED"),
    ("progress_ratio", "verified progress weight / total progress weight"),
    ("total_tokens", "input + output tokens over all executions (modeled; no real LLM)"),
    (
        "aupbc_tokens",
        "area under the Progress-Budget curve, token axis, normalized (higher = progress earned earlier/cheaper)",
    ),
    ("aupbc_time", "same, wall-clock axis (not reproducible across reruns)"),
    ("aupbc_tool_calls", "same, tool-call axis"),
    (
        "useful_work_ratio",
        "cost of final successful attempts of VERIFIED nodes / total execution cost",
    ),
    (
        "replanning_amplification",
        "nodes actually re-executed after invalidation / oracle true affected nodes (1.0 = perfect local repair)",
    ),
    ("invalidated_work_rate", "superseded-attempt cost / total execution cost"),
    ("recovery_overhead", "repeated cost after a crash / remaining estimated cost at crash time"),
    (
        "critical_path_stretch",
        "simulated execution time / oracle critical-path time (1.0 = optimal)",
    ),
    (
        "graph_maintenance_events",
        "reconciler/invalidation events processed (token cost is 0: deterministic rules, no LLM)",
    ),
]

KEY_METRICS = [
    "success",
    "progress_ratio",
    "total_tokens",
    "tool_calls",
    "aupbc_tokens",
    "useful_work_ratio",
    "replanning_amplification",
    "invalidated_work_rate",
    "recovery_overhead",
    "critical_path_stretch",
]


def _mean(values: list[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def _table(rows: list[dict], columns: list[str]) -> str:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    for row in rows:
        cells = []
        for col in columns:
            value = row.get(col, "")
            if isinstance(value, float):
                value = f"{value:.3f}"
            cells.append(str(value))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def build_report(payloads: list[dict]) -> str:
    rows = [r for p in payloads for r in p.get("results", [])]
    if not rows:
        return "# LongHorizonOS Controlled Benchmark Report\n\n(no results)\n"
    meta = payloads[0]

    lines = [
        "# LongHorizonOS Controlled Benchmark Report",
        "",
        f"- suite: `{meta.get('suite')}` size: `{meta.get('size')}` seeds: {meta.get('seeds')}",
        f"- modes: {', '.join(meta.get('modes', []))}",
        f"- presets: {len(meta.get('presets', []))} scenario types (spec 22.3); runs: {len(rows)}",
        "- same model (FakeWorker), tools, budget, verification and seed per cell (spec 25)",
        "",
        "## Aggregate by preset x mode (mean over seeds)",
        "",
    ]

    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        groups.setdefault((row["preset"], row["mode"]), []).append(row)
    table_rows = []
    for (preset, mode), members in sorted(groups.items()):
        entry = {"preset": preset, "mode": mode, "runs": len(members)}
        for key in KEY_METRICS:
            entry[key] = _mean([float(m.get(key, 0.0)) for m in members])
        table_rows.append(entry)
    lines.append(
        _table(
            table_rows,
            [
                "preset",
                "mode",
                "runs",
                "success",
                "total_tokens",
                "aupbc_tokens",
                "useful_work_ratio",
                "replanning_amplification",
                "invalidated_work_rate",
                "recovery_overhead",
                "critical_path_stretch",
            ],
        )
    )

    lines += ["", "## Aggregate by mode (mean over presets)", ""]
    by_mode: dict[str, list[dict]] = {}
    for row in rows:
        by_mode.setdefault(row["mode"], []).append(row)
    mode_rows = []
    for mode, members in sorted(by_mode.items()):
        entry = {"mode": mode, "runs": len(members)}
        for key in KEY_METRICS:
            entry[key] = _mean([float(m.get(key, 0.0)) for m in members])
        mode_rows.append(entry)
    lines.append(
        _table(
            mode_rows,
            [
                "mode",
                "runs",
                "success",
                "total_tokens",
                "aupbc_tokens",
                "useful_work_ratio",
                "replanning_amplification",
                "invalidated_work_rate",
                "recovery_overhead",
                "critical_path_stretch",
            ],
        )
    )

    lines += ["", "## Metric glossary (spec 24)", ""]
    for name, desc in METRIC_GLOSSARY:
        lines.append(f"- **{name}** — {desc}")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="build a benchmark report from results JSON")
    parser.add_argument(
        "results",
        nargs="*",
        help="results JSON files (default: newest in artifacts/benchmark_results)",
    )
    parser.add_argument("--out", default=None, help="output markdown path")
    args = parser.parse_args(argv)

    paths = [Path(p) for p in args.results]
    if not paths:
        candidates = sorted(Path("artifacts/benchmark_results").glob("controlled_*.json"))
        if not candidates:
            print(
                "no results found; run `lhos benchmark --suite controlled` first", file=sys.stderr
            )
            return 1
        paths = [candidates[-1]]
    payloads = [json.loads(p.read_text(encoding="utf-8")) for p in paths]
    report = build_report(payloads)
    out = Path(args.out) if args.out else paths[0].with_suffix(".report.md")
    out.write_text(report, encoding="utf-8")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
