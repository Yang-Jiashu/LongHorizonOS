"""Semantic-repair comparative benchmark runner."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

from .harness import measure, measure_real_workspace

OUT_BASE = Path("artifacts/oss_productization_e5")
SIZES = [10, 25, 50, 100]
FRACTIONS = [0.1, 0.25, 0.5, 0.75, 1.0]
TOPOLOGIES = ["chain", "fan_out", "fan_in", "diamond", "mixed"]
SEEDS = [1, 2, 3]
QUICK_SIZES = [10, 25]
QUICK_FRACTIONS = [0.1, 0.5, 1.0]
QUICK_SEEDS = [1, 2]


def run_benchmark(
    quick: bool = True,
    out_dir: str | None = None,
    *,
    live_model: bool = False,
    model: str = "gpt-5.6-sol",
    live_timeout_seconds: float = 60.0,
) -> dict[str, Any]:
    """Run deterministic trials and an optional StepCode live-model probe."""
    out = Path(out_dir) if out_dir else OUT_BASE
    raw_dir = out / "raw"
    summary_dir = out / "summaries"
    table_dir = out / "tables"
    for directory in (raw_dir, summary_dir, table_dir):
        directory.mkdir(parents=True, exist_ok=True)

    sizes = QUICK_SIZES if quick else SIZES
    topologies = ["chain", "mixed"] if quick else TOPOLOGIES
    fractions = QUICK_FRACTIONS if quick else FRACTIONS
    seeds = QUICK_SEEDS if quick else SEEDS
    trials: list[dict[str, Any]] = []
    for n in sizes:
        for topology in topologies:
            for fraction in fractions:
                for seed in seeds:
                    started = time.perf_counter()
                    trial = measure(n, topology, seed, fraction)
                    trial["trial_wall_ms"] = round(
                        (time.perf_counter() - started) * 1000,
                        3,
                    )
                    trials.append(trial)

    valid = [trial for trial in trials if trial["valid_trial"]]
    invalid = [trial for trial in trials if not trial["valid_trial"]]
    raw_path = raw_dir / "trials.jsonl"
    with raw_path.open("w", encoding="utf-8") as handle:
        for trial in trials:
            handle.write(json.dumps(trial, sort_keys=True) + "\n")

    real_workspace = measure_real_workspace()
    summary: dict[str, Any] = {
        "schema_version": "0.2",
        "benchmark": "semantic-repair",
        "quick": quick,
        "total_trials": len(trials),
        "valid_trials": len(valid),
        "invalid_trials": len(invalid),
        "aggregate": _aggregate(valid),
        "real_workspace": real_workspace,
        "measurement_contract": {
            "lhos_rerun": "observed scheduler attempts after invalidation",
            "ownership_conflicts": "overlapping activated claim intervals",
            "false_verified": "affected tasks still VERIFIED immediately after invalidation",
            "checkpoint_baseline": "oracle-informed task-level downstream invalidation",
            "state_only_baseline": "completed-bit resume with no mutation reconciliation",
        },
        "raw_sha256": _sha256(raw_path),
    }

    if live_model:
        from .live_model import run_live_model_benchmark

        live_result = run_live_model_benchmark(
            model=model,
            timeout_seconds=live_timeout_seconds,
        )
        summary["live_model"] = live_result
        (summary_dir / "live_model.json").write_text(
            json.dumps(live_result, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    summary_path = summary_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_comparison_table(summary, table_dir / "comparison.md")
    return summary


def _aggregate(trials: list[dict[str, Any]]) -> dict[str, Any]:
    if not trials:
        return {}
    by_size: dict[str, Any] = {}
    for n in sorted({int(trial["n"]) for trial in trials}):
        group = [trial for trial in trials if trial["n"] == n]
        by_size[f"n{n}"] = _aggregate_group(group)
    by_size["overall"] = {
        **_aggregate_group(trials),
        "under_invalidation_total": sum(trial["under_invalidation"] for trial in trials),
        "over_invalidation_total": sum(trial["over_invalidation"] for trial in trials),
        "ownership_conflicts_total": sum(trial["ownership_conflicts"] for trial in trials),
        "false_verified_total": sum(trial["false_verified"] for trial in trials),
        "state_only_false_closure_trials": sum(
            bool(trial["state_only_false_closure"]) for trial in trials
        ),
        "final_goal_closure_failures": sum(
            not bool(trial["final_goal_closed"]) for trial in trials
        ),
        "lhos_checkpoint_equal_rerun_trials": sum(
            trial["lhos_rerun"] == trial["checkpoint_rerun"] for trial in trials
        ),
    }
    return by_size


def _aggregate_group(trials: list[dict[str, Any]]) -> dict[str, Any]:
    count = max(1, len(trials))

    def mean(key: str) -> float:
        return round(sum(float(trial[key]) for trial in trials) / count, 6)

    return {
        "trials": len(trials),
        "mean_affected_fraction": mean("affected_fraction"),
        "mean_full_restart_rerun": mean("full_restart_rerun"),
        "mean_checkpoint_rerun": mean("checkpoint_rerun"),
        "mean_lhos_rerun": mean("lhos_rerun"),
        "mean_lhos_invalidated": mean("lhos_invalidated"),
        "mean_lhos_preserved": mean("lhos_preserved"),
        "mean_preservation_ratio": mean("preservation_ratio"),
        "mean_recomputation_ratio": mean("recomputation_ratio"),
        "mean_checkpoint_recomputation_ratio": mean("checkpoint_recomputation_ratio"),
        "mean_repair_amplification_vs_affected": mean("repair_amplification_vs_affected"),
        "mean_weighted_saving_vs_full_restart": mean("weighted_saving_vs_full_restart"),
        "mean_weighted_saving_vs_checkpoint": mean("weighted_saving_vs_checkpoint"),
        "mean_invalidation_wall_ms": mean("invalidation_wall_ms"),
        "mean_reclose_wall_ms": mean("reclose_wall_ms"),
        "mean_trial_wall_ms": mean("trial_wall_ms"),
    }


def _write_comparison_table(summary: dict[str, Any], path: Path) -> None:
    overall = summary.get("aggregate", {}).get("overall", {})
    real = summary.get("real_workspace", {})
    lines = [
        "# Semantic Repair Comparison",
        "",
        "| Metric | Result |",
        "|---|---:|",
        f"| Valid deterministic trials | {summary.get('valid_trials', 0)} / {summary.get('total_trials', 0)} |",
        f"| Mean full-restart reruns | {overall.get('mean_full_restart_rerun', 0)} |",
        f"| Mean task-DAG checkpoint reruns | {overall.get('mean_checkpoint_rerun', 0)} |",
        f"| Mean LongHorizonOS observed reruns | {overall.get('mean_lhos_rerun', 0)} |",
        f"| Mean weighted saving vs full restart | {overall.get('mean_weighted_saving_vs_full_restart', 0)} |",
        f"| Mean weighted saving vs checkpoint | {overall.get('mean_weighted_saving_vs_checkpoint', 0)} |",
        f"| False VERIFIED after invalidation | {overall.get('false_verified_total', 0)} |",
        f"| Ownership interval conflicts | {overall.get('ownership_conflicts_total', 0)} |",
        f"| State-only false-closure trials | {overall.get('state_only_false_closure_trials', 0)} |",
        f"| Real-workspace repair attempts | {real.get('repair_attempts', 0)} |",
        f"| Real-workspace Goal reclosed | {real.get('final_goal_closed', False)} |",
        "",
        "The task-DAG checkpoint baseline is intentionally oracle-informed. "
        "Parity against it is an honest result for these task-level mutation workloads.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
