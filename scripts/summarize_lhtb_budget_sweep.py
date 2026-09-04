"""Combine completed fixed-budget LHTB pair results into one budget curve."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "lhos-lhtb-budget-sweep-summary.v2"
_ARMS = ("dsh_fresh", "lhos_resume")
_TASK_IDENTITY_FIELDS = (
    "name",
    "task_name",
    "task_toml_sha256",
    "task_content_sha256",
    "docker_image",
    "local_image_ids",
    "verifier_docker_image",
    "verifier_environment_mode",
    "official_agent_timeout_seconds",
    "continue_until_timeout",
    "stochastic_verifier",
    "stochastic_pair_controlled",
    "paired_verifier_seed_sha256",
    "configured_time_slice_seconds",
    "time_slice_policy",
    "continuation_boundary_mode",
)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _task_identity(manifest: dict[str, Any], output: Path) -> list[dict[str, Any]]:
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks:
        raise RuntimeError(f"{output}: manifest has no task provenance")
    identity: list[dict[str, Any]] = []
    for task in tasks:
        if not isinstance(task, dict):
            raise RuntimeError(f"{output}: manifest task provenance is invalid")
        identity.append({key: task.get(key) for key in _TASK_IDENTITY_FIELDS})
    return identity


def _config_identity(manifest: dict[str, Any], output: Path) -> str:
    """Hash paired YAML after removing the expected arm/budget differences."""

    configs = manifest.get("configs")
    if not isinstance(configs, dict) or not configs:
        raise RuntimeError(f"{output}: manifest has no paired config provenance")
    normalized: list[dict[str, Any]] = []
    for task_name in sorted(configs):
        record = configs[task_name]
        if not isinstance(record, dict):
            raise RuntimeError(f"{output}: invalid config record for {task_name}")
        for arm in _ARMS:
            raw_path = record.get(arm)
            if not raw_path:
                raise RuntimeError(f"{output}: missing {task_name}/{arm} config")
            path = Path(str(raw_path)).resolve()
            if not path.is_file():
                raise RuntimeError(f"{output}: missing config file {path}")
            try:
                payload = yaml.safe_load(path.read_text(encoding="utf-8"))
            except yaml.YAMLError as exc:
                raise RuntimeError(f"{output}: invalid YAML {path}") from exc
            if not isinstance(payload, dict):
                raise RuntimeError(f"{output}: config is not a mapping: {path}")
            payload = copy.deepcopy(payload)
            payload["job_name"] = "__JOB_NAME__"
            agents = payload.get("agents")
            if not isinstance(agents, list) or len(agents) != 1 or not isinstance(agents[0], dict):
                raise RuntimeError(f"{output}: config has invalid agent list: {path}")
            agents[0]["override_timeout_sec"] = "__BUDGET__"
            kwargs = agents[0].get("kwargs")
            if not isinstance(kwargs, dict):
                raise RuntimeError(f"{output}: config has invalid agent kwargs: {path}")
            kwargs["arm"] = "__ARM__"
            normalized.append({"task_name": str(task_name), "arm": arm, "payload": payload})
    return _stable_hash(normalized)


def _manifest_identity(manifest: dict[str, Any], output: Path) -> dict[str, Any]:
    task_identity = _task_identity(manifest, output)
    controlled = manifest.get("controlled_pair_experiment")
    if not isinstance(controlled, dict) or controlled.get("harness_constant") is not True:
        raise RuntimeError(f"{output}: controlled-pair harness provenance is missing")
    budget_condition = manifest.get("budget_condition")
    if not isinstance(budget_condition, dict):
        raise RuntimeError(f"{output}: budget condition provenance is missing")
    return {
        "model": manifest.get("model"),
        "reasoning_effort": manifest.get("reasoning_effort"),
        "lhtb_source_commit": manifest.get("lhtb_source_commit"),
        "agent": manifest.get("agent"),
        "runtime": manifest.get("runtime"),
        "harbor": manifest.get("harbor"),
        "controlled_pair_experiment": controlled,
        "task_identity": task_identity,
        "config_identity_sha256": _config_identity(manifest, output),
        "task_content": manifest.get("task_content"),
        "time_slice_mode": manifest.get("time_slice_mode"),
        "time_slice_seconds": manifest.get("time_slice_seconds"),
        "time_slice_policy": manifest.get("time_slice_policy"),
        "semantic_context_control": manifest.get("semantic_context_control"),
        "n_attempts": manifest.get("n_attempts"),
        "timeout_multiplier": manifest.get("timeout_multiplier"),
        "environment_delete": manifest.get("environment_delete"),
        "budget_source": budget_condition.get("source"),
    }


def _prepared_manifest_provenance(
    entry: dict[str, Any],
    output: Path,
    manifest: dict[str, Any],
) -> tuple[str, str | None]:
    """Validate the immutable prepare snapshot and final manifest identity.

    A pair run records admission and arm state in ``manifest.json``.  New
    budget sweeps therefore hash ``manifest.prepared.json``; older sweeps
    only have a hash of the final manifest and retain the legacy check.
    """

    prepared_ref = entry.get("prepared_manifest")
    prepared_sha = entry.get("prepared_manifest_sha256")
    if prepared_ref or prepared_sha:
        if not prepared_ref:
            prepared_ref = str(output / "manifest.prepared.json")
        if not prepared_sha:
            # The alias is emitted by the v1 preparer for compatibility with
            # consumers that only know the historical field name.
            prepared_sha = entry.get("manifest_sha256")
        prepared_path = Path(str(prepared_ref)).expanduser()
        if not prepared_path.is_absolute():
            prepared_path = output / prepared_path
        prepared_path = prepared_path.resolve()
        if not prepared_sha:
            raise RuntimeError(f"{output}: prepared manifest hash is missing")
        if not prepared_path.is_file():
            raise RuntimeError(f"{output}: immutable prepared manifest is missing")
        actual_prepared_sha = _sha256_file(prepared_path)
        if actual_prepared_sha != str(prepared_sha):
            raise RuntimeError(
                f"{output}: prepared_manifest_sha256 does not match immutable snapshot"
            )
        prepared_manifest = _load(prepared_path)
        if prepared_manifest.get("schema_version") != "lhos-lhtb-dsh-controlled-pair.v3":
            raise RuntimeError(f"{output}: unsupported prepared manifest schema")
        prepared_identity = _manifest_identity(prepared_manifest, prepared_path)
        final_identity = _manifest_identity(manifest, output)
        if prepared_identity != final_identity:
            raise RuntimeError(
                f"{output}: final manifest identity differs from prepared snapshot"
            )
        return actual_prepared_sha, str(prepared_path)

    expected_manifest_sha = entry.get("manifest_sha256")
    if not expected_manifest_sha:
        raise RuntimeError(f"{output}: sweep entry is missing manifest_sha256")
    actual_manifest_sha = _sha256_file(output / "manifest.json")
    if actual_manifest_sha != str(expected_manifest_sha):
        raise RuntimeError(f"{output}: manifest_sha256 does not match manifest.json")
    return actual_manifest_sha, None


def _validate_prepared_config_hashes(
    entry: dict[str, Any],
    manifest: dict[str, Any],
    output: Path,
) -> None:
    expected = entry.get("prepared_config_sha256")
    if expected is None:
        return
    if not isinstance(expected, dict) or not expected:
        raise RuntimeError(f"{output}: prepared config hashes are invalid")
    configs = manifest.get("configs") or {}
    observed: dict[str, str] = {}
    for task_name, record in configs.items():
        if not isinstance(record, dict):
            raise RuntimeError(f"{output}: invalid config record for {task_name}")
        for arm in _ARMS:
            path = Path(str(record.get(arm, ""))).resolve()
            if not path.is_file():
                raise RuntimeError(f"{output}: missing config file {path}")
            observed[f"{task_name}/{arm}"] = _sha256_file(path)
    if observed != {str(key): str(value) for key, value in expected.items()}:
        raise RuntimeError(f"{output}: paired config files changed after preparation")


def _validate_jobs_dir_result_paths(
    result: dict[str, Any],
    jobs_dir: str | None,
    output: Path,
) -> None:
    """Fail closed when a recorded Harbor result escapes its jobs directory."""

    if not jobs_dir:
        return
    jobs_root = Path(str(jobs_dir)).expanduser()
    if not jobs_root.is_absolute():
        jobs_root = output / jobs_root
    jobs_root = jobs_root.resolve()
    pairs = result.get("pairs")
    if not isinstance(pairs, list):
        return
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        task_name = str(pair.get("task_name", "<unknown>"))
        for arm in _ARMS:
            arm_result = pair.get(arm)
            if not isinstance(arm_result, dict):
                continue
            raw_job_result = arm_result.get("job_result")
            if raw_job_result is None:
                # Failed or censored records may not have produced a Harbor
                # result file; they remain part of the assignment-level ITT.
                continue
            job_result_path = Path(str(raw_job_result)).expanduser()
            if not job_result_path.is_absolute():
                job_result_path = (output / job_result_path).resolve()
            else:
                job_result_path = job_result_path.resolve()
            try:
                job_result_path.relative_to(jobs_root)
            except ValueError as exc:
                raise RuntimeError(
                    f"{output}: {task_name}/{arm} job_result escapes jobs_dir: "
                    f"{job_result_path} (jobs_dir={jobs_root})"
                ) from exc


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return str(value)


def summarize(sweep_root: Path) -> dict[str, Any]:
    root = sweep_root.resolve()
    sweep = _load(root / "sweep.json")
    if sweep.get("schema_version") != "lhos-lhtb-budget-sweep.v1":
        raise RuntimeError("unsupported budget sweep schema")
    if sweep.get("harness_constant") is not True:
        raise RuntimeError("sweep does not declare a constant harness")
    entries = sweep.get("budget_conditions")
    if not isinstance(entries, list) or not entries:
        raise RuntimeError("sweep.json has no budget conditions")

    conditions: list[dict[str, Any]] = []
    common_identity: dict[str, Any] | None = None
    common_tasks: tuple[str, ...] | None = None
    seen_budgets: set[int] = set()
    seen_jobs_dirs: set[str] = set()
    for entry in entries:
        output = Path(str(entry["output"])).resolve()
        budget = int(entry["budget_seconds"])
        if budget in seen_budgets:
            raise RuntimeError(f"duplicate budget condition: {budget}")
        seen_budgets.add(budget)
        manifest_path = output / "manifest.json"
        if not manifest_path.is_file():
            raise RuntimeError(f"{output}: manifest.json is missing")
        manifest = _load(manifest_path)
        prepared_manifest_sha, prepared_manifest_path = _prepared_manifest_provenance(
            entry,
            output,
            manifest,
        )
        _validate_prepared_config_hashes(entry, manifest, output)
        actual_manifest_sha = _sha256_file(manifest_path)
        if manifest.get("schema_version") != "lhos-lhtb-dsh-controlled-pair.v3":
            raise RuntimeError(f"{output}: unsupported controlled-pair manifest schema")
        sweep_metadata = manifest.get("budget_sweep") or {}
        if sweep_metadata.get("sweep_id") != sweep.get("sweep_id"):
            raise RuntimeError(f"{output}: manifest belongs to a different sweep")
        if sweep_metadata.get("budget_seconds") != budget:
            raise RuntimeError(f"{output}: manifest sweep metadata budget differs")
        manifest_budget = (manifest.get("budget_condition") or {}).get(
            "configured_agent_timeout_seconds"
        )
        if float(manifest_budget) != float(budget):
            raise RuntimeError(f"{output}: manifest budget does not match sweep entry")
        if manifest.get("agent_timeout_mode") != "global":
            raise RuntimeError(f"{output}: fixed sweep requires global agent timeout mode")
        if manifest.get("time_slice_seconds") is not None or manifest.get("time_slice_mode") != "disabled":
            raise RuntimeError(f"{output}: fixed sweep requires disabled time slicing")
        if (manifest.get("budget_condition") or {}).get("source") != "fixed_configured_agent_timeout":
            raise RuntimeError(f"{output}: task-declared timeout condition cannot enter fixed sweep")
        jobs_dir = entry.get("jobs_dir")
        if jobs_dir:
            normalized_jobs_dir = str(Path(str(jobs_dir)).resolve()).casefold()
            if normalized_jobs_dir in seen_jobs_dirs:
                raise RuntimeError(f"{output}: jobs_dir is reused across conditions")
            seen_jobs_dirs.add(normalized_jobs_dir)
        result = _load(output / "result.json")
        result_controlled = result.get("controlled_pair_experiment")
        if not isinstance(result_controlled, dict) or result_controlled.get("harness_constant") is not True:
            raise RuntimeError(f"{output}: result lacks constant-harness provenance")
        pairs = result.get("pairs")
        if not isinstance(pairs, list):
            raise RuntimeError(f"{output}: result has no paired task rows")
        _validate_jobs_dir_result_paths(result, jobs_dir, output)
        task_names = tuple(str(pair.get("task_name")) for pair in pairs)
        if len(task_names) != len(set(task_names)):
            raise RuntimeError(f"{output}: result contains duplicate task rows")
        manifest_task_names = tuple(str(task.get("name")) for task in manifest.get("tasks", []))
        if manifest_task_names != task_names:
            raise RuntimeError(f"{output}: manifest/result task cohort differs")
        declared_task_names = tuple(str(name) for name in entry.get("task_names", ()))
        if declared_task_names and declared_task_names != manifest_task_names:
            raise RuntimeError(f"{output}: sweep entry task cohort differs from manifest")
        if common_tasks is None:
            common_tasks = task_names
        elif task_names != common_tasks:
            raise RuntimeError(
                "budget sweep task cohort/order differs; do not compare these outputs"
            )
        identity = _manifest_identity(manifest, output)
        result_identity = {
            key: result.get(key)
            for key in ("model", "reasoning_effort", "lhtb_source_commit")
        }
        if any(result_identity[key] != identity[key] for key in result_identity):
            raise RuntimeError(f"{output}: result identity differs from manifest")
        if result.get("agent_timeout_seconds") is not None and float(result["agent_timeout_seconds"]) != float(budget):
            raise RuntimeError(f"{output}: result budget does not match sweep entry")
        result_budget = result.get("budget_condition") or {}
        if result_budget.get("source") != "fixed_configured_agent_timeout":
            raise RuntimeError(f"{output}: result is not a fixed-budget condition")
        if result_budget.get("configured_agent_timeout_seconds") is not None and float(
            result_budget["configured_agent_timeout_seconds"]
        ) != float(budget):
            raise RuntimeError(f"{output}: result budget condition differs from sweep entry")
        if common_identity is None:
            common_identity = identity
        elif identity != common_identity:
            raise RuntimeError(
                f"{output}: model/runtime identity differs from the first condition"
            )
        profile = result.get("budget_tier_profiling") or {}
        tiers = profile.get("tiers") or []
        tier = next(
            (row for row in tiers if row.get("budget_seconds") == budget),
            None,
        )
        if tier is None:
            raise RuntimeError(f"{output}: missing budget tier {budget}")
        conditions.append(
            {
                "budget_seconds": budget,
                "output": str(output),
                "pair_count": tier.get("pair_count", 0),
                "continuation_pair_count": tier.get("continuation_pair_count", 0),
                "one_shot_pair_count": tier.get("one_shot_pair_count", 0),
                "result_eligible_pair_count": tier.get("result_eligible_pair_count", 0),
                "mechanism_eligible_pair_count": tier.get("mechanism_eligible_pair_count", 0),
                "outcome_itt": tier.get("outcome_itt", {}),
                "observed_reward_pairs": tier.get("observed_reward_pairs", {}),
                "provider_censored_pair_count": result.get("provider_censored_pair_count", 0),
                "execution_failure_pair_count": result.get("execution_failure_pair_count", 0),
                "infrastructure_resampled_pair_count": result.get("infrastructure_resampled_pair_count", 0),
                "uncontrolled_stochastic_pair_count": result.get("uncontrolled_stochastic_pair_count", 0),
                "manifest_sha256": actual_manifest_sha,
                "prepared_manifest_sha256": prepared_manifest_sha,
                "prepared_manifest": prepared_manifest_path,
                "provenance_identity_sha256": _stable_hash(identity),
            }
        )
    conditions.sort(key=lambda row: row["budget_seconds"])
    summary = {
        "schema_version": SCHEMA_VERSION,
        "official_score": False,
        "ranking_eligible": False,
        "claim_status": "custom_harness_budget_curve",
        "estimand": (
            "task-level paired ITT reward contrast (LHOS minus fresh) at fixed "
            "per-arm budget, under one custom DSH/Harbor harness"
        ),
        "official_leaderboard_reference_budget_seconds": 5400,
        "harness_constant": True,
        "provenance_validated": True,
        "provenance_scope": (
            "immutable prepared-manifest SHA (with legacy final-manifest fallback), "
            "task/content/image/verifier identity, runtime/Harbor, controlled-pair "
            "metadata, normalized paired YAML and jobs_dir result-path ownership"
        ),
        "common_identity": common_identity,
        "task_count": len(common_tasks or ()),
        "task_names": list(common_tasks or ()),
        "conditions": conditions,
        "zero_imputation": "missing_or_invalid_reward=0.0",
        "mechanism_note": "one-shot tasks are outcome-eligible but context-mechanism NA",
    }
    return summary


def render(summary: dict[str, Any]) -> str:
    lines = [
        "# DSH vs DSH + LHOS Budget Curve",
        "",
        "The custom DSH/Harbor harness, task cohort, model, image and verifier "
        "configuration are held fixed. Only the per-arm agent budget changes. "
        "Missing/invalid rewards are zero-imputed for the assignment-level ITT view.",
        "",
        "| Budget | Pairs | Continuation | One-shot | Fresh mean | LHOS mean | LHOS-Fresh | Fresh resolved | LHOS resolved |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for condition in summary["conditions"]:
        outcome = condition.get("outcome_itt") or {}
        lines.append(
            f"| {condition['budget_seconds']}s | {condition['pair_count']} | "
            f"{condition['continuation_pair_count']} | {condition['one_shot_pair_count']} | "
            f"{_fmt(outcome.get('fresh_mean_reward'))} | "
            f"{_fmt(outcome.get('lhos_mean_reward'))} | "
            f"{_fmt(outcome.get('lhos_minus_fresh_mean_reward'))} | "
            f"{_fmt(outcome.get('fresh_resolved_rate'))} | "
            f"{_fmt(outcome.get('lhos_resolved_rate'))} |"
        )
    lines.extend(
        [
            "",
            "Operational exclusions/diagnostics by budget:",
            "",
            "| Budget | Provider censored | Execution failures | Infrastructure resamples | Uncontrolled stochastic |",
            "|---:|---:|---:|---:|---:|",
        ]
    )
    for condition in summary["conditions"]:
        lines.append(
            f"| {condition['budget_seconds']}s | {condition['provider_censored_pair_count']} | "
            f"{condition['execution_failure_pair_count']} | "
            f"{condition['infrastructure_resampled_pair_count']} | "
            f"{condition['uncontrolled_stochastic_pair_count']} |"
        )
    lines.extend(
        [
            "",
            "These 600/900/1800/3600/5400 rows are fixed global custom-budget "
            "conditions; only 5400s matches the official leaderboard budget. "
            "They remain custom-harness measurements and are not leaderboard scores.",
            "At low budgets, a continuation/verifier boundary may never occur; "
            "such rows are outcome ITT only, with context-mechanism exposure marked NA.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sweep-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    summary = summarize(args.sweep_root)
    output = (args.output or args.sweep_root / "budget-sweep-summary.json").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output.with_name("BUDGET-SWEEP.md").write_text(render(summary) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
