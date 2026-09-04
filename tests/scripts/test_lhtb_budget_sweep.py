from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from scripts import prepare_lhtb_budget_sweep as prepare
from scripts import summarize_lhtb_budget_sweep as summary


def _write_condition(root: Path, budget: int, task_names: list[str]) -> None:
    output = root / f"budget-{budget:05d}s"
    output.mkdir(parents=True)
    configs_dir = output / "configs"
    configs_dir.mkdir()
    configs: dict[str, dict[str, str]] = {}
    for name in task_names:
        record: dict[str, str] = {}
        for arm, arm_value in (("dsh_fresh", "baseline"), ("lhos_resume", "lhos")):
            path = configs_dir / f"{name}.{arm}.yaml"
            path.write_text(
                "job_name: fixture\n"
                "agents:\n"
                "- override_timeout_sec: 600\n"
                "  kwargs:\n"
                f"    arm: {arm_value}\n",
                encoding="utf-8",
            )
            record[arm] = str(path)
        configs[name] = record
    tasks = [
        {
            "name": name,
            "task_name": name,
            "task_toml_sha256": f"toml-{name}",
            "task_content_sha256": f"content-{name}",
            "docker_image": f"image-{name}",
            "local_image_ids": {},
            "verifier_docker_image": f"image-{name}",
            "verifier_environment_mode": "same",
            "official_agent_timeout_seconds": 3600,
            "continue_until_timeout": True,
            "stochastic_verifier": False,
            "stochastic_pair_controlled": True,
            "paired_verifier_seed_sha256": None,
            "configured_time_slice_seconds": None,
            "time_slice_policy": "unsliced",
            "continuation_boundary_mode": "natural",
        }
        for name in task_names
    ]
    manifest = {
        "schema_version": "lhos-lhtb-dsh-controlled-pair.v3",
        "budget_sweep": {
            "schema_version": "lhos-lhtb-budget-sweep.v1",
            "sweep_id": "fixture",
            "budget_seconds": budget,
        },
        "budget_condition": {
            "source": "fixed_configured_agent_timeout",
            "configured_agent_timeout_seconds": budget,
        },
        "agent_timeout_mode": "global",
        "agent_timeout_seconds": budget,
        "time_slice_mode": "disabled",
        "time_slice_seconds": None,
        "time_slice_policy": {"mode": "disabled"},
        "controlled_pair_experiment": {"harness_constant": True},
        "model": "fixture-model",
        "reasoning_effort": "medium",
        "lhtb_source_commit": "fixture-commit",
        "agent": "fixture-agent",
        "runtime": {"runtime": "fixture"},
        "harbor": {"commit": "fixture-harbor"},
        "task_content": "official task payload",
        "semantic_context_control": {"enabled": True},
        "n_attempts": 1,
        "timeout_multiplier": 1.0,
        "environment_delete": False,
        "tasks": tasks,
        "configs": configs,
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, sort_keys=True), encoding="utf-8"
    )
    pairs = [
        {
            "task_name": name,
            "dsh_fresh": {"reward": 0.2},
            "lhos_resume": {"reward": 0.4},
            "continuation_boundary_mode": "natural",
            "complete": True,
            "result_eligible": True,
            "mechanism_eligible": True,
        }
        for name in task_names
    ]
    result = {
        "model": "fixture-model",
        "reasoning_effort": "medium",
        "lhtb_source_commit": "fixture-commit",
        "agent": "fixture-agent",
        "agent_timeout_seconds": budget,
        "budget_condition": {
            "source": "fixed_configured_agent_timeout",
            "configured_agent_timeout_seconds": budget,
        },
        "controlled_pair_experiment": {"harness_constant": True},
        "pairs": pairs,
        "budget_tier_profiling": {
            "tiers": [
                {
                    "budget_seconds": budget,
                    "pair_count": len(pairs),
                    "continuation_pair_count": len(pairs),
                    "one_shot_pair_count": 0,
                    "result_eligible_pair_count": len(pairs),
                    "mechanism_eligible_pair_count": len(pairs),
                    "outcome_itt": {
                        "fresh_mean_reward": 0.2,
                        "lhos_mean_reward": 0.4,
                        "lhos_minus_fresh_mean_reward": 0.2,
                        "fresh_resolved_rate": 0.0,
                        "lhos_resolved_rate": 0.0,
                    },
                    "observed_reward_pairs": {"pair_count": len(pairs)},
                }
            ]
        },
    }
    (output / "result.json").write_text(
        json.dumps(result),
        encoding="utf-8",
    )


def test_budget_sweep_summary_requires_same_task_cohort(tmp_path: Path) -> None:
    _write_condition(tmp_path, 600, ["a", "b"])
    _write_condition(tmp_path, 900, ["a", "b"])
    (tmp_path / "sweep.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-lhtb-budget-sweep.v1",
                "sweep_id": "fixture",
                "harness_constant": True,
                "budget_conditions": [
                    {
                        "budget_seconds": 600,
                        "output": str(tmp_path / "budget-00600s"),
                        "task_names": ["a", "b"],
                        "manifest_sha256": hashlib.sha256(
                            (tmp_path / "budget-00600s" / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "jobs_dir": str(tmp_path / "jobs-600"),
                    },
                    {
                        "budget_seconds": 900,
                        "output": str(tmp_path / "budget-00900s"),
                        "task_names": ["a", "b"],
                        "manifest_sha256": hashlib.sha256(
                            (tmp_path / "budget-00900s" / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "jobs_dir": str(tmp_path / "jobs-900"),
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    result = summary.summarize(tmp_path)
    assert [row["budget_seconds"] for row in result["conditions"]] == [600, 900]
    assert result["conditions"][0]["outcome_itt"]["lhos_minus_fresh_mean_reward"] == 0.2

    _write_condition(tmp_path, 1800, ["different"])
    (tmp_path / "sweep.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-lhtb-budget-sweep.v1",
                "sweep_id": "fixture",
                "harness_constant": True,
                "budget_conditions": [
                    {
                        "budget_seconds": 600,
                        "output": str(tmp_path / "budget-00600s"),
                        "task_names": ["a", "b"],
                        "manifest_sha256": hashlib.sha256(
                            (tmp_path / "budget-00600s" / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "jobs_dir": str(tmp_path / "jobs-600"),
                    },
                    {
                        "budget_seconds": 1800,
                        "output": str(tmp_path / "budget-01800s"),
                        "task_names": ["different"],
                        "manifest_sha256": hashlib.sha256(
                            (tmp_path / "budget-01800s" / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "jobs_dir": str(tmp_path / "jobs-1800"),
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    try:
        summary.summarize(tmp_path)
    except RuntimeError as exc:
        assert "task cohort/order differs" in str(exc)
    else:
        raise AssertionError("different budget cohorts must fail closed")


def test_fixed_budget_namespace_does_not_switch_to_task_declared_timeout(
    tmp_path: Path,
) -> None:
    args = SimpleNamespace(
        lhtb_root=tmp_path / "LHTB",
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch=tmp_path / "patch.yml",
        task_names=["a"],
        local_images_only=False,
        worker_timeout_seconds=1800,
        max_concurrency=1,
        resource_aware_pairs=False,
        pair_capacity_cpus=None,
        pair_capacity_memory_mb=None,
    )
    namespace = prepare._prepare_namespace(
        args,
        budget_seconds=600,
        output=tmp_path / "out",
        sweep_id="fixture",
        selected_mode="task_names",
    )
    assert namespace.agent_timeout_seconds == 600
    assert namespace.use_official_agent_timeouts is False
    assert namespace.time_to_verified is False
    assert namespace.no_time_slice is True


def test_summary_hashes_immutable_prepared_manifest_after_run_mutation(
    tmp_path: Path,
) -> None:
    _write_condition(tmp_path, 600, ["a"])
    output = tmp_path / "budget-00600s"
    prepared = output / "manifest.prepared.json"
    prepared.write_bytes((output / "manifest.json").read_bytes())
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    # The pair runner appends operational state to manifest.json while it
    # executes.  Those fields must not invalidate prepare-time provenance.
    manifest["run_arm"] = "both"
    manifest["run_arms"] = ["dsh_fresh", "lhos_resume"]
    manifest["pair_admission"] = {"enabled": False, "status": "completed"}
    (output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    prepared_sha = hashlib.sha256(prepared.read_bytes()).hexdigest()
    (tmp_path / "sweep.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-lhtb-budget-sweep.v1",
                "sweep_id": "fixture",
                "harness_constant": True,
                "budget_conditions": [
                    {
                        "budget_seconds": 600,
                        "output": str(output),
                        "task_names": ["a"],
                        # Historical alias plus the explicit immutable form.
                        "manifest_sha256": prepared_sha,
                        "prepared_manifest": str(prepared),
                        "prepared_manifest_sha256": prepared_sha,
                        "jobs_dir": str(tmp_path / "jobs-600"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = summary.summarize(tmp_path)
    assert result["provenance_validated"] is True
    assert result["conditions"][0]["prepared_manifest_sha256"] == prepared_sha


def test_summary_rejects_job_result_outside_declared_jobs_dir(tmp_path: Path) -> None:
    _write_condition(tmp_path, 600, ["a"])
    output = tmp_path / "budget-00600s"
    result_path = tmp_path / "outside" / "result.json"
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    result["pairs"][0]["dsh_fresh"]["job_result"] = str(result_path)
    (output / "result.json").write_text(
        json.dumps(result),
        encoding="utf-8",
    )
    (tmp_path / "sweep.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-lhtb-budget-sweep.v1",
                "sweep_id": "fixture",
                "harness_constant": True,
                "budget_conditions": [
                    {
                        "budget_seconds": 600,
                        "output": str(output),
                        "task_names": ["a"],
                        "manifest_sha256": hashlib.sha256(
                            (output / "manifest.json").read_bytes()
                        ).hexdigest(),
                        "jobs_dir": str(tmp_path / "jobs-600"),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    try:
        summary.summarize(tmp_path)
    except RuntimeError as exc:
        assert "job_result escapes jobs_dir" in str(exc)
    else:
        raise AssertionError("job_result outside jobs_dir must fail closed")
