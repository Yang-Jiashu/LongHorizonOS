"""Prepare comparable fixed-budget DSH vs DSH+LHOS experiment conditions.

This helper only prepares manifests and configs. It never starts Harbor or
calls the model provider. Every generated condition uses the same selected
task order and the same custom DSH/Harbor harness; only the per-arm agent
budget changes. Run each prepared condition separately, then use the normal
``summarize`` command to obtain the budget-tier contrast.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import run_lhtb_software5_pair as runner

DEFAULT_BUDGETS = (600, 900, 1_800, 3_600, 5_400)
SWEEP_SCHEMA_VERSION = "lhos-lhtb-budget-sweep.v1"


def _output_name(seconds: int) -> str:
    return f"budget-{int(seconds):05d}s"


def _paired_seed_map(sweep_id: str) -> dict[str, str]:
    """Return deterministic verifier seeds shared by every budget condition."""

    digest = hashlib.sha256(f"{sweep_id}:nbody-accel-iterative".encode()).hexdigest()
    return {runner.NBODY_TASK_NAME: str(int(digest[:16], 16))}


def _prepare_namespace(
    args: argparse.Namespace,
    *,
    budget_seconds: int,
    output: Path,
    sweep_id: str,
    selected_mode: str,
    paired_verifier_seeds: dict[str, str] | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        command="prepare",
        lhtb_root=args.lhtb_root,
        tasks_root=args.tasks_root,
        runtime_root=args.runtime_root,
        patch=args.patch,
        output=output,
        all=selected_mode == "all",
        task_names=args.task_names,
        official_leaderboard_contract=False,
        official_model_yaml=None,
        local_images_only=args.local_images_only,
        agent_timeout_seconds=int(budget_seconds),
        use_official_agent_timeouts=False,
        # Keep this false: the core runner intentionally maps
        # ``time_to_verified`` to task-declared budgets. Fixed-budget sweep
        # conditions must remain global, while TTV fields are still collected
        # whenever a trial reaches verification.
        time_to_verified=False,
        time_slice_seconds=None,
        no_time_slice=True,
        worker_timeout_seconds=args.worker_timeout_seconds,
        max_concurrency=args.max_concurrency,
        resource_aware_pairs=args.resource_aware_pairs,
        pair_capacity_cpus=args.pair_capacity_cpus,
        pair_capacity_memory_mb=args.pair_capacity_memory_mb,
        paired_verifier_seeds=paired_verifier_seeds or {},
        budget_sweep_metadata={
            "schema_version": SWEEP_SCHEMA_VERSION,
            "sweep_id": sweep_id,
            "condition": "fixed_agent_timeout_seconds",
            "budget_seconds": int(budget_seconds),
            "same_task_cohort_required": True,
            "time_slice": "disabled",
            "harness_change": False,
            "paired_verifier_seed_policy": "deterministic_sweep_map",
        },
    )


def prepare_sweep(args: argparse.Namespace) -> dict[str, Any]:
    budgets = sorted({int(value) for value in args.budgets})
    if not budgets or any(value <= 0 for value in budgets):
        raise SystemExit("--budgets must contain positive seconds")
    output_root = args.output_root.resolve()
    if output_root.exists() and any(output_root.iterdir()):
        raise SystemExit(f"output root already exists and is not empty: {output_root}")
    output_root.mkdir(parents=True, exist_ok=True)
    sweep_id = args.sweep_id or output_root.name
    selected_mode = "all" if args.task_names is None else "task_names"
    paired_verifier_seeds = _paired_seed_map(sweep_id)
    jobs_root = getattr(args, "jobs_root", None)
    jobs_root = jobs_root.resolve() if jobs_root is not None else output_root / "jobs"
    entries: list[dict[str, Any]] = []
    for budget in budgets:
        output = output_root / _output_name(budget)
        namespace = _prepare_namespace(
            args,
            budget_seconds=budget,
            output=output,
            sweep_id=sweep_id,
            selected_mode=selected_mode,
            paired_verifier_seeds=paired_verifier_seeds,
        )
        manifest = runner.prepare(namespace)
        # ``run`` records operational state (arm selection, admission plan,
        # resource hydration) back into manifest.json.  Keep the prepared
        # contract immutable so a post-run summary can verify what was
        # actually prepared rather than hashing a file that is expected to
        # change during execution.
        manifest_path = output / "manifest.json"
        prepared_manifest_path = output / "manifest.prepared.json"
        prepared_manifest_path.write_bytes(manifest_path.read_bytes())
        prepared_manifest_sha256 = runner._sha256_file(prepared_manifest_path)
        prepared_config_sha256 = {
            f"{task_name}/{arm}": runner._sha256_file(Path(str(record[arm])))
            for task_name, record in manifest["configs"].items()
            for arm in runner.ARMS
        }
        uncontrolled = [
            str(task["name"])
            for task in manifest["tasks"]
            if bool(task.get("stochastic_verifier"))
            and not bool(task.get("stochastic_pair_controlled"))
        ]
        if uncontrolled and not getattr(args, "allow_uncontrolled_stochastic", False):
            raise RuntimeError(
                "budget sweep contains uncontrolled stochastic verifier tasks: "
                + ", ".join(uncontrolled)
                + "; exclude them or pass --allow-uncontrolled-stochastic"
            )
        entries.append(
            {
                "budget_seconds": budget,
                "output": str(output),
                "jobs_dir": str((jobs_root / output.name).resolve()),
                "task_names": [str(task["name"]) for task in manifest["tasks"]],
                "task_count": len(manifest["tasks"]),
                # Keep the historical key as an alias, while making the hash
                # policy explicit for consumers that need immutable input
                # provenance.
                "manifest_sha256": prepared_manifest_sha256,
                "prepared_manifest": str(prepared_manifest_path),
                "prepared_manifest_sha256": prepared_manifest_sha256,
                "prepared_config_sha256": prepared_config_sha256,
                "manifest_hash_policy": "prepared_immutable_snapshot",
                "time_slice_seconds": manifest.get("time_slice_seconds"),
                "agent_timeout_mode": manifest.get("agent_timeout_mode"),
            }
        )
    task_sets = {tuple(entry["task_names"]) for entry in entries}
    if len(task_sets) != 1:
        raise RuntimeError(
            "budget sweep task cohort changed between conditions; use a fixed "
            "task list and avoid per-condition image filtering"
        )
    jobs_dirs = [str(Path(str(entry["jobs_dir"])).resolve()).casefold() for entry in entries]
    if len(jobs_dirs) != len(set(jobs_dirs)):
        raise RuntimeError("budget sweep jobs_dir values must be unique")
    seed_hashes_by_task: dict[str, set[str | None]] = {}
    for entry in entries:
        manifest = runner._load_json(Path(str(entry["output"])) / "manifest.json")
        for task in manifest.get("tasks", []):
            name = str(task.get("name"))
            seed_hashes_by_task.setdefault(name, set()).add(
                task.get("paired_verifier_seed_sha256")
            )
    inconsistent_seeds = [
        name for name, values in seed_hashes_by_task.items() if len(values) > 1
    ]
    if inconsistent_seeds:
        raise RuntimeError(
            "paired verifier seed changed across budget conditions: "
            + ", ".join(sorted(inconsistent_seeds))
        )
    sweep = {
        "schema_version": SWEEP_SCHEMA_VERSION,
        "sweep_id": sweep_id,
        "repo": str(runner.REPO_ROOT),
        "lhtb_root": str(args.lhtb_root.resolve()),
        "tasks_root": str(args.tasks_root.resolve()),
        "model": runner.MODEL_LABEL,
        "agent": runner.AGENT_IMPORT_PATH,
        "harness_constant": True,
        "control_arm": "dsh_fresh",
        "treatment_arm": "lhos_resume",
        "budget_conditions": entries,
        "default_budget_seconds": list(DEFAULT_BUDGETS),
        "paired_verifier_seed_policy": "deterministic per sweep/task map",
        "jobs_dir_policy": "one fresh jobs_dir per budget condition and replicate",
        "analysis": {
            "primary": "task-level paired ITT reward contrast within budget",
            "secondary": "observed reward and continuation mechanism cohorts",
            "zero_impute_missing_reward": True,
            "one_shot_mechanism_na": True,
        },
    }
    path = output_root / "sweep.json"
    path.write_text(json.dumps(sweep, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return sweep


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lhtb-root", type=Path, required=True)
    parser.add_argument("--tasks-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, default=Path(r"D:\LHTB-dsh-runtime"))
    parser.add_argument("--patch", type=Path, default=runner.REPO_ROOT / "benchmarks" / "real_dsh_dynamic_coding" / "stepfun-3.7-pi-ai.cordis.patch.yml")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--jobs-root",
        type=Path,
        help="Root for unique Harbor jobs directories (defaults to <output-root>/jobs).",
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--all", action="store_true", help="Use all discovered tasks.")
    selection.add_argument("--task-names", nargs="+", help="Fixed ordered task cohort.")
    parser.set_defaults(all=True)
    parser.add_argument("--budgets", nargs="+", type=int, default=list(DEFAULT_BUDGETS))
    parser.add_argument("--sweep-id")
    parser.add_argument("--local-images-only", action="store_true")
    parser.add_argument("--worker-timeout-seconds", type=int, default=runner.DEFAULT_WORKER_TIMEOUT_SECONDS)
    parser.add_argument("--max-concurrency", type=int, default=runner.DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--resource-aware-pairs", action="store_true")
    parser.add_argument("--pair-capacity-cpus", type=float)
    parser.add_argument("--pair-capacity-memory-mb", type=int)
    parser.add_argument(
        "--allow-uncontrolled-stochastic",
        action="store_true",
        help="Keep stochastic verifier tasks in the descriptive output; they are excluded from controlled mechanism claims.",
    )
    return parser


def main() -> int:
    sweep = prepare_sweep(_parser().parse_args())
    print(json.dumps(sweep, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
