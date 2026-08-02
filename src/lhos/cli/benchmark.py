"""`lhos benchmark` (spec sections 22-25).

Runs the controlled benchmark suite: every (preset, mode, seed) cell through
the same runtime, writes one JSON + CSV per invocation to
``artifacts/benchmark_results/`` and prints an aggregate table.

Examples:
    lhos benchmark --suite controlled --scheduler fifo,cost_aware --seeds 1,2,3
    lhos benchmark --suite controlled --mode transcript,full_lhos --seeds 1,2 --size small
"""

from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from lhos.benchmarks.controlled.generator import PRESETS, SIZES
from lhos.benchmarks.modes import MODES, mode_config
from lhos.benchmarks.runner import run_suite

# Stable column order for CSV export; anything missing is left blank.
COLUMNS: list[str] = [
    "task_id", "preset", "size", "seed", "mode",
    "success", "run_status", "verified_progress", "progress_ratio",
    "failed_nodes", "invalidated_nodes",
    "input_tokens", "output_tokens", "total_tokens", "model_calls", "tool_calls",
    "wall_time_seconds", "simulated_time_seconds", "model_cost_usd",
    "graph_maintenance_tokens", "verification_tokens", "graph_maintenance_events",
    "scheduler_time_seconds", "checkpoint_time_seconds",
    "aupbc_tokens", "aupbc_time", "aupbc_tool_calls",
    "useful_work_ratio", "replanning_amplification", "invalidated_work_rate",
    "recovery_overhead", "critical_path_stretch",
    "oracle_critical_path_seconds", "crashes", "restarts",
    "replanned_nodes", "re_executed_nodes", "oracle_affected_nodes",
    "run_id", "db_path",
]

_AGGREGATE_METRICS = [
    "success", "progress_ratio", "total_tokens", "tool_calls",
    "aupbc_tokens", "useful_work_ratio", "replanning_amplification",
    "invalidated_work_rate", "recovery_overhead", "critical_path_stretch",
]


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _select_modes(args) -> list[str]:  # noqa: ANN001
    requested = _parse_csv_list(getattr(args, "mode", None) or getattr(args, "modes", None))
    if requested:
        unknown = [m for m in requested if m not in MODES]
        if unknown:
            raise ValueError(f"unknown mode(s) {unknown}; choose from {MODES}")
        return requested
    scheduler_filter = set(_parse_csv_list(getattr(args, "scheduler", None)))
    if not scheduler_filter:
        return list(MODES)
    unknown = scheduler_filter - {"fifo", "cost_aware"}
    if unknown:
        raise ValueError(f"unknown scheduler(s) {sorted(unknown)}; choose fifo,cost_aware")
    return [m for m in MODES if m == "transcript" or mode_config(m).scheduler_family in scheduler_filter]


def aggregate(rows: list[dict]) -> list[dict]:
    """Mean of key metrics grouped by (preset, mode)."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        groups.setdefault((row["preset"], row["mode"]), []).append(row)
    out: list[dict] = []
    for (preset, mode), members in sorted(groups.items()):
        entry: dict = {"preset": preset, "mode": mode, "runs": len(members)}
        for key in _AGGREGATE_METRICS:
            values = [float(m.get(key, 0.0)) for m in members]
            entry[key] = round(sum(values) / len(values), 6) if values else 0.0
        out.append(entry)
    return out


def _print_table(entries: list[dict]) -> None:
    headers = ["preset", "mode", "runs", "success", "tokens", "aupbc_tok", "useful", "replan", "inval", "recover", "stretch"]
    print(" ".join(h.ljust(10) for h in headers))
    for e in entries:
        print(
            " ".join(
                [
                    e["preset"][:20].ljust(10),
                    e["mode"][:24].ljust(10),
                    str(e["runs"]).ljust(10),
                    f"{e['success']:.2f}".ljust(10),
                    f"{e['total_tokens']:.0f}".ljust(10),
                    f"{e['aupbc_tokens']:.3f}".ljust(10),
                    f"{e['useful_work_ratio']:.3f}".ljust(10),
                    f"{e['replanning_amplification']:.3f}".ljust(10),
                    f"{e['invalidated_work_rate']:.3f}".ljust(10),
                    f"{e['recovery_overhead']:.3f}".ljust(10),
                    f"{e['critical_path_stretch']:.2f}".ljust(10),
                ]
            )
        )


def cmd_benchmark(args) -> int:  # noqa: ANN001 - argparse.Namespace
    suite = args.suite
    if suite != "controlled":
        raise ValueError(f"unknown suite {suite!r}; only 'controlled' exists (spec 22)")
    size = args.size
    if size not in SIZES:
        raise ValueError(f"unknown size {size!r}; choose from {sorted(SIZES)}")
    seeds = [int(s) for s in _parse_csv_list(args.seeds)] or [1]
    presets = _parse_csv_list(args.tasks) or list(PRESETS)
    unknown = [p for p in presets if p not in PRESETS]
    if unknown:
        raise ValueError(f"unknown preset(s) {unknown}; choose from {PRESETS}")
    modes = _select_modes(args)

    work_root = args.work_root or "artifacts/benchmark_work"
    rows = run_suite(
        modes=modes,
        presets=presets,
        seeds=seeds,
        size=size,
        work_root=work_root,
        progress=True,
    )

    out_dir = Path(args.out or "artifacts/benchmark_results")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "suite": suite,
        "size": size,
        "seeds": seeds,
        "presets": presets,
        "modes": modes,
        "generated_at": stamp,
        "config": {
            "work_root": work_root,
            "note": "same model (FakeWorker), tools, budget, verification, seed per cell (spec 25)",
        },
        "results": rows,
        "aggregate": aggregate(rows),
    }
    json_path = out_dir / f"controlled_{stamp}.json"
    csv_path = out_dir / f"controlled_{stamp}.csv"
    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"\n{len(rows)} runs ({len(presets)} presets x {len(modes)} modes x {len(seeds)} seeds), size={size}")
    _print_table(payload["aggregate"])
    print(f"\nwrote {json_path}")
    print(f"wrote {csv_path}")
    return 0
