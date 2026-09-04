from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts import run_lhtb_software5_pair as runner


def _task_roots(root: Path, names: list[str]) -> Path:
    tasks = root / "tasks"
    for name in names:
        task = tasks / name
        task.mkdir(parents=True)
        (task / "task.toml").write_text("[task]\nname = 'fixture'\n", encoding="utf-8")
    return tasks


def _forty_six_names() -> list[str]:
    defaults = [task.name for task in runner.SELECTED_TASKS]
    extras = [f"fixture-task-{index:02d}" for index in range(41)]
    return defaults + extras


def test_task_selection_keeps_default_and_discovers_all(tmp_path: Path) -> None:
    tasks_root = _task_roots(tmp_path, _forty_six_names())

    selected, selection = runner._select_tasks(tasks_root)
    assert [task.name for task in selected] == [task.name for task in runner.SELECTED_TASKS]
    assert selection["mode"] == "default_software5"
    assert selection["discovered_task_count"] == 46

    all_tasks, all_selection = runner._select_tasks(tasks_root, all_tasks=True)
    assert len(all_tasks) == 46
    assert [task.priority for task in all_tasks] == list(range(1, 47))
    assert all_selection["mode"] == "all"


def test_explicit_task_names_preserve_order_and_accept_commas(tmp_path: Path) -> None:
    names = ["alpha", "beta", "gamma"]
    tasks_root = _task_roots(tmp_path, names)
    selected, selection = runner._select_tasks(
        tasks_root,
        task_names=["gamma,alpha", "beta", "alpha"],
    )
    assert [task.name for task in selected] == ["gamma", "alpha", "beta"]
    assert selection["mode"] == "task_names"

    with pytest.raises(RuntimeError, match="missing"):
        runner._select_tasks(tasks_root, task_names=["missing"])


def test_image_inventory_reports_missing_and_local_only_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ids = {"local/a:1": "sha256:a", "local/c:1": "sha256:c"}
    monkeypatch.setattr(runner, "_docker_image_id", ids.get)
    tasks = [
        {
            "name": "task-a",
            "docker_image": "local/a:1",
            "required_docker_images": ["local/a:1"],
        },
        {
            "name": "task-b",
            "docker_image": "missing/b:1",
            "required_docker_images": ["missing/b:1"],
        },
        {
            "name": "task-c",
            "docker_image": "local/c:1",
            "required_docker_images": ["local/c:1", "missing/shared:1"],
        },
    ]
    inventory = runner._image_inventory(tasks)
    assert inventory["image_count"] == 4
    assert inventory["available_image_count"] == 2
    assert inventory["missing_image_count"] == 2
    assert inventory["available_task_names"] == ["task-a"]
    assert inventory["missing_task_names"] == ["task-b", "task-c"]


def test_dynamic_config_generation_is_paired_and_preserves_images(
    tmp_path: Path,
) -> None:
    selected = (
        runner.SelectedTask("alpha", 1, "fixture"),
        runner.SelectedTask("beta", 2, "fixture"),
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()
    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=75,
        runtime=runtime,
        job_prefix="lhtb-dsh",
    )
    assert set(configs) == {"alpha", "beta"}
    for name in configs:
        fresh = yaml.safe_load(Path(configs[name]["dsh_fresh"]).read_text(encoding="utf-8"))
        lhos = yaml.safe_load(Path(configs[name]["lhos_resume"]).read_text(encoding="utf-8"))
        assert fresh["environment"]["force_build"] is False
        assert fresh["environment"]["delete"] is False
        assert lhos["environment"] == fresh["environment"]
        assert fresh["agents"][0]["kwargs"]["controlled_pair_mode"] is True
        assert lhos["agents"][0]["kwargs"]["controlled_pair_mode"] is True
        assert fresh["agents"][0]["kwargs"]["arm"] == "baseline"
        assert lhos["agents"][0]["kwargs"]["arm"] == "lhos"
        assert fresh["agents"][0]["kwargs"]["time_slice_seconds"] == 75
        assert (
            fresh["agents"][0]["kwargs"]["semantic_context_control"]
            is True
        )
        assert (
            fresh["agents"][0]["kwargs"][
                "semantic_context_cache_tokens_per_call_threshold"
            ]
            == 24_000
        )
        assert (
            fresh["agents"][0]["kwargs"][
                "semantic_context_max_consecutive_max_tokens"
            ]
            == 2
        )
        assert (
            fresh["agents"][0]["kwargs"][
                "semantic_context_cumulative_cache_read_tokens_threshold"
            ]
            == 128_000
        )
        assert (
            fresh["agents"][0]["kwargs"][
                "semantic_context_no_progress_event_ratio_threshold"
            ]
            == 0.10
        )
        assert configs[name]["parity_sha256"]


def test_controlled_pair_credential_environment_is_identical_for_both_arms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STEPFUN_API_KEY", "fixture-secret")
    fresh_env, _ = runner._credential_env("STEPFUN_API_KEY", arm="dsh_fresh")
    lhos_env, _ = runner._credential_env("STEPFUN_API_KEY", arm="lhos_resume")
    assert fresh_env["HB_CONTINUE_MODE"] == "same_conversation"
    assert lhos_env["HB_CONTINUE_MODE"] == "same_conversation"
    assert fresh_env["HB_VERIFIER_FEEDBACK_MODE"] == "binary"
    assert lhos_env["HB_VERIFIER_FEEDBACK_MODE"] == "binary"


def test_official_leaderboard_contract_configs_validate_fail_closed(
    tmp_path: Path,
) -> None:
    selected = tuple(
        runner.SelectedTask(name, index, "fixture")
        for index, name in enumerate(_forty_six_names(), start=1)
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()
    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=runner.OFFICIAL_LEADERBOARD_AGENT_TIMEOUT_SECONDS,
        time_slice_seconds=None,
        runtime=runtime,
        job_prefix="lhtb-dsh",
        environment_delete=True,
        official_protocol_fields=True,
    )

    contract = runner._validate_official_leaderboard_contract(
        task_names=[task.name for task in selected],
        configs=configs,
    )
    assert contract["config_validated"] is True
    assert contract["task_count"] == 46
    assert contract["uniform_agent_timeout_seconds"] == 5400
    assert contract["environment_delete"] is True
    assert contract["parser_name"] == "json"
    assert contract["enable_summarize"] is True
    assert contract["proactive_summarization_threshold"] == 8000
    assert contract["reference_harness_match"] is False
    assert contract["official_score"] is False
    assert contract["alignment_scope"] == "shared_static_yaml_and_posthoc_metrics"
    assert contract["leaderboard_comparable"] is False
    assert contract["official_protocol_complete"] is False
    assert contract["reference_continue_until_timeout_task_count"] == 30
    assert contract["reference_interim_full_pass_reward"] == 1.0
    assert contract["configured_harbor_semantics_validated"] is False
    assert "historical snapshot" in contract["published_leaderboard_generation"]
    assert any(
        "same-conversation/binary Harbor path" in deviation
        for deviation in contract["protocol_deviations"]
    )

    tampered_path = Path(configs[selected[0].name]["dsh_fresh"])
    tampered = yaml.safe_load(tampered_path.read_text(encoding="utf-8"))
    tampered["environment"]["delete"] = False
    tampered_path.write_text(yaml.safe_dump(tampered, sort_keys=False), encoding="utf-8")
    with pytest.raises(RuntimeError, match=r"environment\.delete must be true"):
        runner._validate_official_leaderboard_contract(
            task_names=[task.name for task in selected],
            configs=configs,
        )

    tampered["environment"]["delete"] = True
    tampered["agents"][0]["kwargs"]["parser_name"] = "text"
    tampered_path.write_text(yaml.safe_dump(tampered, sort_keys=False), encoding="utf-8")
    with pytest.raises(RuntimeError, match="parser_name must be"):
        runner._validate_official_leaderboard_contract(
            task_names=[task.name for task in selected],
            configs=configs,
        )

    tampered["agents"][0]["kwargs"]["parser_name"] = "json"
    tampered["agents"][0]["kwargs"]["record_terminal_session"] = False
    tampered_path.write_text(yaml.safe_dump(tampered, sort_keys=False), encoding="utf-8")
    with pytest.raises(RuntimeError, match="record_terminal_session must be true"):
        runner._validate_official_leaderboard_contract(
            task_names=[task.name for task in selected],
            configs=configs,
        )


def test_official_model_yaml_reference_validates_git_payload_and_records_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = _forty_six_names()
    payload = {
        "job_name": "lhtb-reference",
        "jobs_dir": "./jobs",
        "n_attempts": 1,
        "n_concurrent_trials": 4,
        "timeout_multiplier": 1.0,
        "environment": {
            "type": "docker",
            "force_build": True,
            "delete": True,
        },
        "agents": [
            {
                "name": "terminus-2",
                "model_name": "openai/reference-model",
                "override_timeout_sec": 5_400,
                "kwargs": {
                    "parser_name": "json",
                    "enable_summarize": True,
                    "proactive_summarization_threshold": 8_000,
                    "record_terminal_session": True,
                },
            }
        ],
        "datasets": [{"path": "./tasks", "task_names": names}],
    }
    official_bytes = yaml.safe_dump(payload, sort_keys=False).encode("utf-8")
    lhtb_root = tmp_path / "LHTB"
    model_yaml = lhtb_root / "configs" / "leaderboard" / "reference.yaml"
    model_yaml.parent.mkdir(parents=True)
    model_yaml.write_bytes(official_bytes)
    monkeypatch.setattr(runner, "_git_blob", lambda _root, _object: official_bytes)

    reference = runner._validate_official_model_yaml_reference(
        lhtb_root=lhtb_root,
        model_yaml=model_yaml,
        expected_task_names=names,
    )

    assert reference["validated"] is True
    assert reference["repository_relative_path"] == (
        "configs/leaderboard/reference.yaml"
    )
    assert reference["agent_name"] == "terminus-2"
    assert reference["model_name"] == "openai/reference-model"
    assert reference["n_concurrent_trials"] == 4
    assert reference["force_build"] is True
    assert reference["record_terminal_session"] is True
    assert reference["datasets_path"] == "./tasks"
    assert reference["task_count"] == 46
    assert reference["task_names"] == names

    tampered_payload = dict(payload)
    tampered_payload["agents"] = [dict(payload["agents"][0])]
    tampered_payload["agents"][0]["kwargs"] = dict(
        payload["agents"][0]["kwargs"]
    )
    tampered_payload["agents"][0]["kwargs"]["record_terminal_session"] = False
    tampered_bytes = yaml.safe_dump(tampered_payload, sort_keys=False).encode()
    model_yaml.write_bytes(tampered_bytes)
    monkeypatch.setattr(runner, "_git_blob", lambda _root, _object: tampered_bytes)
    with pytest.raises(RuntimeError, match="record_terminal_session must be true"):
        runner._validate_official_model_yaml_reference(
            lhtb_root=lhtb_root,
            model_yaml=model_yaml,
            expected_task_names=names,
        )

    monkeypatch.setattr(runner, "_git_blob", lambda _root, _object: official_bytes)
    model_yaml.write_bytes(official_bytes + b"# dirty\n")
    with pytest.raises(RuntimeError, match="differs from Git HEAD"):
        runner._validate_official_model_yaml_reference(
            lhtb_root=lhtb_root,
            model_yaml=model_yaml,
            expected_task_names=names,
        )


def test_official_model_yaml_reference_rejects_non_reference_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = _forty_six_names()
    payload = {
        "n_attempts": 1,
        "n_concurrent_trials": 1,
        "timeout_multiplier": 1.0,
        "environment": {"type": "docker", "force_build": False, "delete": True},
        "agents": [
            {
                "name": "custom-agent",
                "model_name": "provider/model",
                "override_timeout_sec": 5_400,
                "kwargs": {
                    "parser_name": "json",
                    "enable_summarize": True,
                    "proactive_summarization_threshold": 8_000,
                    "record_terminal_session": True,
                },
            }
        ],
        "datasets": [{"path": "./tasks", "task_names": names}],
    }
    official_bytes = yaml.safe_dump(payload, sort_keys=False).encode("utf-8")
    lhtb_root = tmp_path / "LHTB"
    model_yaml = lhtb_root / "configs" / "leaderboard" / "bad.yaml"
    model_yaml.parent.mkdir(parents=True)
    model_yaml.write_bytes(official_bytes)
    monkeypatch.setattr(runner, "_git_blob", lambda _root, _object: official_bytes)

    with pytest.raises(RuntimeError, match=r"agent\.name must be 'terminus-2'"):
        runner._validate_official_model_yaml_reference(
            lhtb_root=lhtb_root,
            model_yaml=model_yaml,
            expected_task_names=names,
        )


def test_git_worktree_provenance_records_pull_policy_and_other_dirty_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    status = (
        " M harbor/src/harbor/environments/docker/docker-compose-prebuilt.yaml\n"
        " M tasks/example/task.toml\n"
        "?? configs/leaderboard/local-experiment.yaml\n"
    )
    monkeypatch.setattr(
        runner,
        "_run",
        lambda _args: SimpleNamespace(returncode=0, stdout=status, stderr=""),
    )

    provenance = runner._git_worktree_provenance(tmp_path)

    assert provenance["dirty"] is True
    assert provenance["entry_count"] == 3
    assert provenance["paths"] == [
        runner.HARBOR_PREBUILT_PULL_POLICY_PATH,
        "tasks/example/task.toml",
        "configs/leaderboard/local-experiment.yaml",
    ]
    assert provenance["known_runner_mutation_paths"] == [
        runner.HARBOR_PREBUILT_PULL_POLICY_PATH
    ]
    assert provenance["harbor_prebuilt_pull_policy_patch_present"] is True


def test_official_leaderboard_score_counts_error_rewards_as_zero() -> None:
    score = runner._official_leaderboard_score(
        [("a", 1.0), ("b", 0.95), ("c", None)],
        expected_task_count=3,
    )

    assert score["complete"] is True
    assert score["error_reward_count"] == 1
    assert score["mean_reward"] == 0.65
    assert score["solved_count"] == 2


def test_official_manifest_contract_rejects_timeout_or_slice_drift() -> None:
    manifest = {
        "official_leaderboard_contract": {
            "enabled": True,
            "alignment_scope": "shared_static_yaml_and_posthoc_metrics",
            "leaderboard_comparable": False,
            "official_protocol_complete": False,
            "source_worktree_dirty": False,
            "source_worktree_dirty_paths": [],
            "source_worktree": {
                "dirty": False,
                "paths": [],
            },
        },
        "tasks": [{} for _ in range(46)],
        "agent_timeout_seconds": 5_400,
        "agent_timeout_mode": "official_uniform_5400",
        "time_slice_seconds": None,
        "time_slice_mode": "disabled",
        "n_attempts": 1,
        "timeout_multiplier": 1.0,
        "environment_delete": True,
    }
    runner._validate_official_manifest_contract(manifest)
    manifest["time_slice_seconds"] = 75
    with pytest.raises(RuntimeError, match="time_slice_seconds must be null"):
        runner._validate_official_manifest_contract(manifest)
    manifest["time_slice_seconds"] = None
    manifest["official_leaderboard_contract"]["leaderboard_comparable"] = True
    with pytest.raises(RuntimeError, match="leaderboard_comparable must be false"):
        runner._validate_official_manifest_contract(manifest)


def test_nbody_configs_share_verifier_only_seed_and_persist_only_hash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner.secrets, "randbits", lambda _bits: 42)
    selected = (
        runner.SelectedTask(runner.NBODY_TASK_NAME, 1, "fixture"),
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()

    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=None,
        runtime=runtime,
        job_prefix="lhtb-dsh",
    )

    record = configs[runner.NBODY_TASK_NAME]
    payloads = [
        yaml.safe_load(Path(record[arm]).read_text(encoding="utf-8"))
        for arm in runner.ARMS
    ]
    assert {
        payload["verifier"]["env"][runner.NBODY_VERIFIER_SEED_ENV]
        for payload in payloads
    } == {"42"}
    assert all(
        runner.NBODY_VERIFIER_SEED_ENV not in payload["agents"][0]["env"]
        for payload in payloads
    )
    assert record["paired_verifier_seed_sha256"] == hashlib.sha256(
        b"42"
    ).hexdigest()
    assert "paired_verifier_seed" not in record
    assert record["parity_sha256"]


def test_generals_configs_share_python_hash_seed(
    tmp_path: Path,
) -> None:
    selected = (
        runner.SelectedTask(runner.GENERALS_TASK_NAME, 1, "fixture"),
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()

    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=None,
        runtime=runtime,
        job_prefix="lhtb-dsh",
        paired_verifier_seeds={runner.GENERALS_TASK_NAME: "1234"},
    )

    record = configs[runner.GENERALS_TASK_NAME]
    payloads = [
        yaml.safe_load(Path(record[arm]).read_text(encoding="utf-8"))
        for arm in runner.ARMS
    ]
    assert {
        payload["verifier"]["env"][runner.GENERALS_VERIFIER_SEED_ENV]
        for payload in payloads
    } == {"1234"}
    assert record["paired_verifier_seed_env"] == runner.GENERALS_VERIFIER_SEED_ENV
    assert record["paired_verifier_seed_sha256"] == hashlib.sha256(
        b"1234"
    ).hexdigest()


def test_tabular_configs_use_image_workdir(tmp_path: Path) -> None:
    task_name = "tabular-data-feature-covshift"
    selected = (runner.SelectedTask(task_name, 1, "fixture"),)
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()

    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=None,
        runtime=runtime,
        job_prefix="lhtb-dsh",
    )

    for arm in runner.ARMS:
        payload = yaml.safe_load(Path(configs[task_name][arm]).read_text(encoding="utf-8"))
        assert payload["agents"][0]["kwargs"]["workdir"] == "/workspace"


def test_task_toml_compatibility_override_is_hash_pinned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_name = "langchain-version-migration"
    old_command, new_command = runner.TASK_TOML_COMPATIBILITY_TRANSFORMS[
        task_name
    ]
    official = f"allow_internet = false\n{old_command}\n".encode()
    local = f"allow_internet = true\n{new_command}\n".encode()
    task_root = tmp_path / "tasks" / task_name
    task_root.mkdir(parents=True)
    (task_root / "task.toml").write_bytes(local)
    monkeypatch.setattr(
        runner,
        "_git_text",
        lambda *_args: f"tasks/{task_name}/task.toml\n",
    )
    monkeypatch.setattr(runner, "_git_blob", lambda *_args: official)
    override = {
        "kind": "preserve_separate_verifier_tests_after_healthcheck",
        "official_task_toml_sha256": hashlib.sha256(official).hexdigest(),
        "local_task_toml_sha256": hashlib.sha256(local).hexdigest(),
    }

    provenance = runner._verify_official_task_payload(
        lhtb_root=tmp_path,
        tasks_root=tmp_path / "tasks",
        task_name=task_name,
        declared_compatibility_override=override,
    )
    assert provenance["compatibility_override"] is True
    assert provenance["official_payload_matches"] is False

    override["local_task_toml_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="beyond declared overrides"):
        runner._verify_official_task_payload(
            lhtb_root=tmp_path,
            tasks_root=tmp_path / "tasks",
            task_name=task_name,
            declared_compatibility_override=override,
        )


def test_dynamic_config_generation_supports_per_task_official_timeouts(
    tmp_path: Path,
) -> None:
    selected = (
        runner.SelectedTask("short", 1, "fixture"),
        runner.SelectedTask("long", 2, "fixture"),
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()
    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=75,
        runtime=runtime,
        job_prefix="lhtb-dsh",
        task_timeout_overrides={"short": 3600, "long": 21600},
    )
    for name, expected in (("short", 3600), ("long", 21600)):
        fresh = yaml.safe_load(
            Path(configs[name]["dsh_fresh"]).read_text(encoding="utf-8")
        )
        lhos = yaml.safe_load(
            Path(configs[name]["lhos_resume"]).read_text(encoding="utf-8")
        )
        assert fresh["agents"][0]["override_timeout_sec"] == expected
        assert lhos["agents"][0]["override_timeout_sec"] == expected
        assert configs[name]["parity_sha256"]


def test_dynamic_config_generation_uses_mixed_per_task_time_slices(
    tmp_path: Path,
) -> None:
    selected = (
        runner.SelectedTask("continuation", 1, "fixture"),
        runner.SelectedTask("one-shot", 2, "fixture"),
    )
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "0.1.0-rc.8",
    }
    output = tmp_path / "output"
    output.mkdir()
    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=900,
        time_slice_seconds=60,
        runtime=runtime,
        job_prefix="lhtb-dsh",
        task_time_slice_overrides={
            "continuation": 60,
            "one-shot": None,
        },
    )

    for arm in runner.ARMS:
        continuation = yaml.safe_load(
            Path(configs["continuation"][arm]).read_text(encoding="utf-8")
        )
        one_shot = yaml.safe_load(
            Path(configs["one-shot"][arm]).read_text(encoding="utf-8")
        )
        assert continuation["agents"][0]["override_timeout_sec"] == 900
        assert continuation["agents"][0]["kwargs"]["time_slice_seconds"] == 60
        assert one_shot["agents"][0]["override_timeout_sec"] == 900
        assert one_shot["agents"][0]["kwargs"]["time_slice_seconds"] is None


@pytest.mark.parametrize(
    ("task", "expected"),
    [
        (
            {
                "continue_until_timeout": True,
                "configured_time_slice_seconds": None,
            },
            "natural",
        ),
        (
            {
                "continue_until_timeout": True,
                "configured_time_slice_seconds": 60,
            },
            "forced_time_slice",
        ),
        (
            {
                "continue_until_timeout": False,
                "configured_time_slice_seconds": None,
            },
            "one_shot",
        ),
    ],
)
def test_continuation_boundary_mode(
    task: dict[str, object],
    expected: str,
) -> None:
    assert runner._continuation_boundary_mode(task) == expected


def test_harbor_pull_policy_patch_is_safe_and_idempotent(tmp_path: Path) -> None:
    harbor = tmp_path / "harbor"
    compose = harbor / "src" / "harbor" / "environments" / "docker" / "docker-compose-prebuilt.yaml"
    compose.parent.mkdir(parents=True)
    compose.write_text(
        "services:\n"
        "  main:\n"
        "    image: ${PREBUILT_IMAGE_NAME}\n"
        '    command: [ "sh", "-c", "sleep infinity" ]\n',
        encoding="utf-8",
    )

    first = runner._ensure_harbor_pull_policy_never(harbor)
    second = runner._ensure_harbor_pull_policy_never(harbor)
    payload = yaml.safe_load(compose.read_text(encoding="utf-8"))
    assert first["changed"] is True
    assert second["changed"] is False
    assert payload["services"]["main"]["pull_policy"] == "never"

    compose.write_text(
        "services:\n  main:\n    image: ${PREBUILT_IMAGE_NAME}\n    pull_policy: always\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="refusing"):
        runner._ensure_harbor_pull_policy_never(harbor)


def test_prepare_parser_supports_all_explicit_and_local_only() -> None:
    parser = runner._parser()
    common = [
        "prepare",
        "--lhtb-root",
        "LHTB",
        "--output",
        "out",
    ]
    all_args = parser.parse_args([*common, "--all", "--local-images-only"])
    assert all_args.all is True
    assert all_args.local_images_only is True

    named = parser.parse_args([*common, "--task-names", "alpha", "beta"])
    assert named.task_names == ["alpha", "beta"]

    official = parser.parse_args(
        [*common, "--task-names", "alpha", "--official-timeouts", "--time-to-verified"]
    )
    assert official.use_official_agent_timeouts is True
    assert official.time_to_verified is True

    task_declared = parser.parse_args(
        [*common, "--task-names", "alpha", "--task-declared-timeouts"]
    )
    assert task_declared.use_official_agent_timeouts is True

    contract = parser.parse_args(
        [
            *common,
            "--official-leaderboard-contract",
            "--official-model-yaml",
            "configs/leaderboard/model.yaml",
        ]
    )
    assert contract.official_leaderboard_contract is True
    assert contract.official_model_yaml == Path("configs/leaderboard/model.yaml")
    protocol_alias = parser.parse_args([*common, "--official-protocol"])
    assert protocol_alias.official_leaderboard_contract is True

    resource_aware = parser.parse_args(
        [
            *common,
            "--task-names",
            "alpha",
            "--resource-aware-pairs",
            "--max-concurrency",
            "3",
            "--pair-capacity-cpus",
            "12",
            "--pair-capacity-memory-mb",
            "15360",
        ]
    )
    assert resource_aware.resource_aware_pairs is True
    assert resource_aware.max_concurrency == 3
    assert resource_aware.pair_capacity_cpus == 12
    assert resource_aware.pair_capacity_memory_mb == 15360

    run_args = parser.parse_args(
        [
            "run",
            "--output",
            "out",
            "--jobs-dir",
            "jobs",
            "--resource-aware-pairs",
            "--max-concurrency",
            "4",
        ]
    )
    assert run_args.resource_aware_pairs is True
    assert run_args.max_concurrency == 4
    assert run_args.arm is None

    arm_only = parser.parse_args(
        [
            "run",
            "--output",
            "out",
            "--jobs-dir",
            "jobs",
            "--arm",
            "fresh",
        ]
    )
    assert arm_only.arm == "fresh"

    merge_args = parser.parse_args(
        [
            "merge-arms",
            "--fresh-output",
            "fresh",
            "--lhos-output",
            "lhos",
            "--output",
            "merged",
        ]
    )
    assert merge_args.fresh_output == Path("fresh")
    assert merge_args.lhos_output == Path("lhos")

    materialize_args = parser.parse_args(
        [
            "materialize-arms",
            "--prepared-output",
            "prepared",
            "--fresh-output",
            "fresh",
            "--lhos-output",
            "lhos",
            "--task-names",
            "c,a",
            "b",
        ]
    )
    assert materialize_args.prepared_output == Path("prepared")
    assert materialize_args.task_names == ["c,a", "b"]

    no_slice = parser.parse_args(
        [*common, "--task-names", "alpha", "--no-time-slice"]
    )
    assert no_slice.no_time_slice is True
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                *common,
                "--task-names",
                "alpha",
                "--no-time-slice",
                "--time-slice-seconds",
                "60",
            ]
        )

    with pytest.raises(SystemExit):
        parser.parse_args([*common, "--all", "--task-names", "alpha"])


def _resource_task(
    name: str,
    priority: int,
    *,
    cpus: int | None = 2,
    memory_mb: int | None = 4096,
    verifier_environment_mode: str = "same",
    verifier_cpus: int | None = None,
    verifier_memory_mb: int | None = None,
) -> dict[str, object]:
    return {
        "name": name,
        "priority": priority,
        "cpus": cpus,
        "memory_mb": memory_mb,
        "verifier_environment_mode": verifier_environment_mode,
        "verifier_cpus": verifier_cpus,
        "verifier_memory_mb": verifier_memory_mb,
    }


def test_resource_aware_plan_packs_three_normal_tasks_on_12cpu_15gb() -> None:
    tasks = [
        _resource_task("normal-c", 3),
        _resource_task("normal-a", 1),
        _resource_task("normal-b", 2),
    ]
    plan = runner._resource_aware_pair_plan(
        tasks,
        capacity={"cpus": 12, "memory_mb": 15 * 1024},
        max_concurrency=3,
    )

    assert [wave["task_names"] for wave in plan["waves"]] == [
        ["normal-a", "normal-b", "normal-c"]
    ]
    assert plan["waves"][0]["cpus"] == 6
    assert plan["waves"][0]["memory_mb"] == 12 * 1024


def test_resource_aware_plan_separates_heavy_memory_and_backfills_normal() -> None:
    tasks = [
        _resource_task("heavy-a", 1, cpus=4, memory_mb=8192),
        _resource_task("heavy-b", 2, cpus=4, memory_mb=8192),
        _resource_task("normal", 3),
    ]
    plan = runner._resource_aware_pair_plan(
        tasks,
        capacity={"cpus": 12, "memory_mb": 15 * 1024},
        max_concurrency=3,
    )

    assert [wave["task_names"] for wave in plan["waves"]] == [
        ["heavy-a", "normal"],
        ["heavy-b"],
    ]
    heavy_b = next(
        decision for decision in plan["decisions"] if decision["task_name"] == "heavy-b"
    )
    assert heavy_b["rejected_waves"] == [
        {"wave": 1, "reasons": ["memory_capacity"]}
    ]


def test_resource_aware_plan_counts_nbody_separate_verifier_peak() -> None:
    tasks = [
        _resource_task(
            "nbody-accel-iterative",
            1,
            cpus=4,
            memory_mb=8192,
            verifier_environment_mode="separate",
            verifier_cpus=4,
            verifier_memory_mb=8192,
        ),
        _resource_task("normal", 2),
    ]
    plan = runner._resource_aware_pair_plan(
        tasks,
        capacity={"cpus": 12, "memory_mb": 15 * 1024},
        max_concurrency=3,
    )

    assert [wave["task_names"] for wave in plan["waves"]] == [["normal"]]
    decision = plan["decisions"][0]
    assert decision["admitted"] is False
    assert decision["wave"] is None
    assert decision["reason"] == "requirement_exceeds_capacity"
    assert decision["capacity_exceeded"] == ["memory_capacity"]
    assert plan["rejected_task_names"] == ["nbody-accel-iterative"]
    requirement = decision["resource_requirement"]
    assert requirement["cpus"] == 8
    assert requirement["memory_mb"] == 16384
    assert requirement["verifier"]["included_in_peak"] is True


def test_resource_aware_plan_is_deterministic_and_fails_closed_unknowns() -> None:
    tasks = [
        _resource_task("normal-b", 3),
        _resource_task("unknown", 2, memory_mb=None),
        _resource_task("normal-a", 1),
    ]
    capacity = {"cpus": 12, "memory_mb": 15 * 1024}
    first = runner._resource_aware_pair_plan(
        tasks,
        capacity=capacity,
        max_concurrency=3,
    )
    second = runner._resource_aware_pair_plan(
        list(reversed(tasks)),
        capacity=capacity,
        max_concurrency=3,
    )

    assert [wave["task_names"] for wave in first["waves"]] == [
        ["normal-a", "normal-b"],
        ["unknown"],
    ]
    assert first["waves"] == second["waves"]
    unknown = next(
        decision for decision in first["decisions"] if decision["task_name"] == "unknown"
    )
    assert unknown["exclusive"] is True
    assert unknown["reason"] == "exclusive_unknown_resources"
    assert unknown["resource_requirement"]["unknown_fields"] == [
        "environment.memory_mb"
    ]


def test_resource_metadata_backfill_recovers_separate_verifier_from_old_manifest(
    tmp_path: Path,
) -> None:
    tasks_root = tmp_path / "tasks"
    task_root = tasks_root / "nbody-accel-iterative"
    task_root.mkdir(parents=True)
    task_toml = task_root / "task.toml"
    task_toml.write_text(
        "[environment]\n"
        "cpus = 4\n"
        "memory_mb = 8192\n"
        "\n"
        "[verifier]\n"
        'environment_mode = "separate"\n'
        "\n"
        "[verifier.environment]\n"
        "cpus = 4\n"
        "memory_mb = 8192\n",
        encoding="utf-8",
    )
    task = {
        "name": "nbody-accel-iterative",
        "priority": 1,
        "cpus": 4,
        "memory_mb": 8192,
        "task_toml_sha256": runner._sha256_file(task_toml),
    }
    manifest = {"tasks_root": str(tasks_root), "tasks": [task]}

    runner._hydrate_manifest_task_resources(manifest)

    requirement = runner._task_pair_resource_requirement(task)
    assert task["resource_metadata_source"] == "run_task_toml_backfill"
    assert requirement["known"] is True
    assert requirement["cpus"] == 8
    assert requirement["memory_mb"] == 16384


def test_docker_pair_capacity_uses_engine_limits_and_partial_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        assert command == ["docker", "info", "--format", "{{json .}}"]
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {
                    "ID": "engine-id",
                    "Name": "docker-desktop",
                    "NCPU": 12,
                    "MemTotal": 15 * 1024 * 1024 * 1024,
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(runner, "_run", run)
    capacity = runner._docker_pair_capacity(cpus_override=10)
    assert capacity == {
        "cpus": 10,
        "memory_mb": 15 * 1024,
        "cpus_source": "override",
        "memory_mb_source": "docker_info.MemTotal",
        "docker_engine_id": "engine-id",
        "docker_engine_name": "docker-desktop",
    }


def test_run_pairs_persists_resource_admission_waves_in_manifest_and_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(
        output,
        task_names=["heavy-a", "heavy-b", "normal"],
    )
    manifest["max_concurrency"] = 3
    manifest["resource_aware_pairs"] = True
    manifest["tasks"] = [
        _resource_task("heavy-a", 1, cpus=4, memory_mb=8192),
        _resource_task("heavy-b", 2, cpus=4, memory_mb=8192),
        _resource_task("normal", 3),
    ]
    runner._write_json(output / "manifest.json", manifest)
    batches: list[tuple[list[str], int]] = []
    capacities: list[dict[str, object]] = []

    monkeypatch.setattr(runner, "_runtime_pins_match", lambda _manifest: None)
    monkeypatch.setattr(
        runner,
        "_image_ids_match",
        lambda _output, _manifest, strict=False: {},
    )

    def run_batch(
        *,
        tasks: list[dict[str, object]],
        max_workers: int,
        capacity: dict[str, object],
        **_: object,
    ) -> None:
        batches.append(([str(task["name"]) for task in tasks], max_workers))
        capacities.append(dict(capacity))

    monkeypatch.setattr(runner, "_run_task_batch_work_conserving", run_batch)
    result = runner.run_pairs(
        argparse.Namespace(
            output=output,
            jobs_dir=tmp_path / "jobs",
            credential_env="MISSING_FIXTURE_KEY",
            max_concurrency=3,
            resource_aware_pairs=True,
            pair_capacity_cpus=12,
            pair_capacity_memory_mb=15 * 1024,
        )
    )

    # Execution is work-conserving: one dynamic batch over all admitted
    # tasks (shortest-pair-first, then admission order), not barrier waves.
    assert batches == [(["heavy-a", "heavy-b", "normal"], 3)]
    assert [c["cpus"] for c in capacities] == [12]
    assert [c["memory_mb"] for c in capacities] == [15 * 1024]
    persisted_manifest = json.loads(
        (output / "manifest.json").read_text(encoding="utf-8")
    )
    assert persisted_manifest["pair_admission"]["status"] == "completed"
    assert result["resource_aware_pairs"] is True
    # The admission plan is still the static capacity proof of record.
    assert [wave["task_names"] for wave in result["pair_admission"]["waves"]] == [
        ["heavy-a", "normal"],
        ["heavy-b"],
    ]
    assert result["pair_admission"]["execution_policy"] == "work_conserving_backfill"


def test_run_pairs_skips_pair_that_exceeds_resource_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(
        output,
        task_names=[runner.NBODY_TASK_NAME],
    )
    manifest["max_concurrency"] = 3
    manifest["resource_aware_pairs"] = True
    manifest["tasks"] = [
        _resource_task(
            runner.NBODY_TASK_NAME,
            1,
            cpus=4,
            memory_mb=8192,
            verifier_environment_mode="separate",
            verifier_cpus=4,
            verifier_memory_mb=8192,
        )
    ]
    runner._write_json(output / "manifest.json", manifest)

    monkeypatch.setattr(runner, "_runtime_pins_match", lambda _manifest: None)
    monkeypatch.setattr(
        runner,
        "_image_ids_match",
        lambda _output, _manifest, strict=False: {},
    )
    monkeypatch.setattr(
        runner,
        "_run_task_batch_work_conserving",
        lambda **_kwargs: pytest.fail("capacity-rejected task must not run"),
    )

    result = runner.run_pairs(
        argparse.Namespace(
            output=output,
            jobs_dir=tmp_path / "jobs",
            credential_env="MISSING_FIXTURE_KEY",
            max_concurrency=3,
            resource_aware_pairs=True,
            pair_capacity_cpus=12,
            pair_capacity_memory_mb=15 * 1024,
        )
    )

    plan = result["pair_admission"]
    assert plan["waves"] == []
    assert plan["runnable_task_count"] == 0
    assert plan["skipped_task_names"] == [runner.NBODY_TASK_NAME]
    assert plan["decisions"][0]["admitted"] is False
    for arm in runner.ARMS:
        record = json.loads(
            (
                output
                / "runs"
                / runner.NBODY_TASK_NAME
                / f"{arm}.json"
            ).read_text(encoding="utf-8")
        )
        assert record["status"] == "failed"
        assert "exceeds configured capacity" in record["execution_error"]["message"]


def test_run_pairs_legacy_default_keeps_two_pair_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["a", "b", "c"])
    manifest["max_concurrency"] = 4
    runner._write_json(output / "manifest.json", manifest)
    observed_workers: list[int] = []

    monkeypatch.setattr(runner, "_runtime_pins_match", lambda _manifest: None)
    monkeypatch.setattr(
        runner,
        "_image_ids_match",
        lambda _output, _manifest, strict=False: {},
    )
    monkeypatch.setattr(
        runner,
        "_run_task_batch",
        lambda *, max_workers, **_: observed_workers.append(max_workers),
    )
    runner.run_pairs(
        argparse.Namespace(
            output=output,
            jobs_dir=tmp_path / "jobs",
            credential_env="MISSING_FIXTURE_KEY",
            max_concurrency=4,
            resource_aware_pairs=False,
            pair_capacity_cpus=None,
            pair_capacity_memory_mb=None,
        )
    )

    assert observed_workers == [2]


def test_run_pairs_arm_only_keeps_missing_counterpart_unselected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "fresh-output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["a", "b"])
    runner._write_json(output / "manifest.json", manifest)
    monkeypatch.setattr(runner, "_runtime_pins_match", lambda _manifest: None)
    monkeypatch.setattr(
        runner,
        "_image_ids_match",
        lambda _output, _manifest, strict=False: {},
    )

    def run_batch(*, tasks: list[dict[str, object]], **_: object) -> None:
        for task in tasks:
            name = str(task["name"])
            runner._write_json(
                output / "runs" / name / "dsh_fresh.json",
                {
                    "status": "completed",
                    "arm": "dsh_fresh",
                    "task_name": name,
                    "config_parity_sha256": "fixture-parity",
                    "docker_image_id": "sha256:fixture",
                    "metrics": {
                        "parse_valid": True,
                        "resource_measurement_valid": True,
                        "result_eligible": True,
                        "mechanism_eligible": False,
                        "comparison_eligible": False,
                        "reward": 1.0,
                        "resolved": True,
                        "verified": True,
                        "token_units": 10,
                        "model_calls": 1,
                        "tool_calls": 1,
                    },
                },
            )

    monkeypatch.setattr(runner, "_run_task_batch", run_batch)
    result = runner.run_pairs(
        argparse.Namespace(
            output=output,
            jobs_dir=tmp_path / "jobs",
            credential_env="MISSING_FIXTURE_KEY",
            arm="fresh",
            max_concurrency=2,
            resource_aware_pairs=False,
            pair_capacity_cpus=None,
            pair_capacity_memory_mb=None,
        )
    )

    assert result["schema_version"] == "lhos-lhtb-arm-result.v1"
    assert result["arm"] == "dsh_fresh"
    assert result["selected_arms"] == ["dsh_fresh"]
    assert result["completed_task_count"] == 2
    assert result["failed_task_count"] == 0
    assert result["pending_task_count"] == 0
    assert result["batch_timeline"]["arm_mode"] == "fresh"
    progress = json.loads(
        (output / "progress.json").read_text(encoding="utf-8")
    )
    assert progress["selected_arms"] == ["dsh_fresh"]
    assert progress["arm_count"] == 2
    assert progress["failed_arm_count"] == 0
    assert all(
        task["arms"]["lhos_resume"]["status"] == "not_selected"
        for task in progress["tasks"]
    )
    assert not (output / "runs" / "a" / "lhos_resume.json").exists()
    assert (output / "arm-result.json").is_file()
    assert (output / "batch-timeline.json").is_file()


def test_arm_summary_marks_missing_selected_arm_pending_not_invalid(
    tmp_path: Path,
) -> None:
    output = tmp_path / "fresh-output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["pending"])
    runner._set_manifest_run_arm(manifest, "fresh")
    runner._write_json(output / "manifest.json", manifest)

    result = runner.summarize_arm_output(output, "dsh_fresh")

    assert result["pending_task_count"] == 1
    assert result["invalid_task_count"] == 0
    assert result["tasks"][0]["status"] == "pending"


def _make_arm_batch_fixture(
    root: Path,
    *,
    arm: str,
    task_names: list[str],
) -> dict[str, object]:
    root.mkdir(parents=True)
    manifest = _minimal_manifest(root, task_names=task_names)
    mode = "fresh" if arm == "dsh_fresh" else "lhos"
    runner._set_manifest_run_arm(manifest, mode)
    runner._write_json(root / "manifest.json", manifest)
    for name in task_names:
        runner._write_json(
            root / "runs" / name / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": name,
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "metrics": {
                    "parse_valid": True,
                    "resource_measurement_valid": True,
                    "result_eligible": True,
                    "mechanism_eligible": arm == "lhos_resume",
                    "comparison_eligible": arm == "lhos_resume",
                    "parser_error": None,
                    "execution_error": None,
                    "provider_censored": False,
                    "reward": 1.0,
                    "resolved": True,
                    "verified": True,
                    "token_units": 10 if arm == "dsh_fresh" else 8,
                    "model_calls": 1,
                    "tool_calls": 1,
                    "continuation_gate": {"passed": arm == "lhos_resume"},
                },
            },
        )
    return runner.summarize_arm_output(root, arm)


def test_merge_arm_outputs_strictly_validates_and_generates_pair(
    tmp_path: Path,
) -> None:
    fresh = tmp_path / "fresh"
    lhos = tmp_path / "lhos"
    _make_arm_batch_fixture(
        fresh,
        arm="dsh_fresh",
        task_names=["a", "b"],
    )
    _make_arm_batch_fixture(
        lhos,
        arm="lhos_resume",
        task_names=["a", "b"],
    )

    merged = tmp_path / "merged"
    result = runner.merge_arm_outputs(
        fresh_output=fresh,
        lhos_output=lhos,
        output=merged,
    )

    assert result["pair_count"] == 2
    assert result["complete_pair_count"] == 2
    assert result["result_eligible_pair_count"] == 2
    assert result["arm_batch_merge"]["schema_version"] == "lhos-lhtb-arm-merge.v1"
    assert len(result["merge_validation"]) == 2
    assert result["batch_timeline"]["status"] == "merged"
    assert (merged / "runs" / "a" / "dsh_fresh.json").is_file()
    assert (merged / "runs" / "a" / "lhos_resume.json").is_file()
    assert (merged / "RESULTS.zh-CN.md").is_file()


def test_merge_arm_outputs_rejects_parity_mismatch(
    tmp_path: Path,
) -> None:
    fresh = tmp_path / "fresh"
    lhos = tmp_path / "lhos"
    _make_arm_batch_fixture(fresh, arm="dsh_fresh", task_names=["a"])
    _make_arm_batch_fixture(lhos, arm="lhos_resume", task_names=["a"])
    lhos_record = lhos / "runs" / "a" / "lhos_resume.json"
    payload = json.loads(lhos_record.read_text(encoding="utf-8"))
    payload["config_parity_sha256"] = "different"
    runner._write_json(lhos_record, payload)
    runner.summarize_arm_output(lhos, "lhos_resume")

    with pytest.raises(RuntimeError, match="config parity mismatch"):
        runner.merge_arm_outputs(
            fresh_output=fresh,
            lhos_output=lhos,
            output=tmp_path / "merged",
        )


def _make_prepared_materialization_fixture(
    root: Path,
    *,
    task_names: list[str],
) -> dict[str, object]:
    root.mkdir(parents=True)
    manifest = _minimal_manifest(root, task_names=task_names)
    manifest["resource_aware_pairs"] = True
    manifest["max_concurrency"] = 2
    manifest["pair_admission"] = {
        "schema_version": runner.PAIR_ADMISSION_SCHEMA_V1,
        "enabled": True,
        "capacity": {
            "cpus": 8,
            "memory_mb": 12 * 1024,
            "cpus_source": "override",
            "memory_mb_source": "override",
        },
        "max_concurrency": 2,
    }
    config_root = root / "configs"
    config_root.mkdir()
    inventory_images: list[dict[str, object]] = []
    prebuild_records: list[dict[str, object]] = []
    for index, task in enumerate(manifest["tasks"], start=1):
        name = str(task["name"])
        image = str(task["docker_image"])
        image_id = f"sha256:{name}"
        task.update(
            {
                "cpus": 2,
                "memory_mb": 4096,
                "verifier_environment_mode": "same",
                "verifier_cpus": None,
                "verifier_memory_mb": None,
                "continue_until_timeout": True,
                "configured_time_slice_seconds": None,
                "time_slice_policy": "unsliced",
                "continuation_boundary_mode": "natural",
                "task_toml_sha256": f"task-toml-{name}",
                "task_content_sha256": f"task-content-{name}",
                "official_agent_timeout_seconds": 900,
                "configured_agent_timeout_seconds": 900,
                "local_image_ids": {image: image_id},
                "paired_verifier_seed_sha256": (
                    "seed-hash" if index == len(task_names) else None
                ),
                "stochastic_verifier": index == len(task_names),
                "stochastic_pair_controlled": True,
            }
        )
        parity: set[str] = set()
        config_record: dict[str, object] = {}
        for arm in runner.ARMS:
            payload = {
                "job_name": f"{name}-{arm}",
                "agents": [
                    {
                        "kwargs": {
                            "arm": "baseline" if arm == "dsh_fresh" else "lhos"
                        }
                    }
                ],
                "datasets": [{"task_names": [name]}],
            }
            path = config_root / f"{name}.{arm}.yaml"
            path.write_text(
                yaml.safe_dump(payload, sort_keys=False),
                encoding="utf-8",
            )
            config_record[arm] = str(path)
            config_record[f"{arm}_job_name"] = payload["job_name"]
            parity.add(runner._parity_hash(payload))
        assert len(parity) == 1
        config_record["parity_sha256"] = parity.pop()
        manifest["configs"][name] = config_record
        inventory_images.append(
            {
                "image": image,
                "image_id": image_id,
                "available": True,
                "task_names": [name],
            }
        )
        prebuild_records.append(
            {
                "task_name": name,
                "image": image,
                "image_id": image_id,
                "exit_code": 0,
            }
        )
    runner._write_json(root / "manifest.json", manifest)
    runner._write_json(
        root / "image-inventory.json",
        {
            "schema_version": "lhos-lhtb-local-image-inventory.v1",
            "images": inventory_images,
            "image_count": len(inventory_images),
            "available_image_count": len(inventory_images),
            "missing_image_count": 0,
            "available_task_names": list(task_names),
            "missing_task_names": [],
        },
    )
    runner._write_json(
        root / "prebuild.json",
        {
            "schema_version": runner.PREBUILD_SCHEMA_V2,
            "complete": True,
            "records": prebuild_records,
        },
    )
    runner._write_json(
        root / "missing-local-images.json",
        {
            "missing_task_count": 0,
            "missing_task_names": [],
            "missing_image_count": 0,
            "missing_images": [],
        },
    )
    return manifest


def test_materialize_arms_creates_clean_identical_subsets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = tmp_path / "prepared"
    _make_prepared_materialization_fixture(
        prepared,
        task_names=["a", "b", "c"],
    )
    validated: list[dict[str, object]] = []
    monkeypatch.setattr(
        runner,
        "_runtime_pins_match",
        lambda manifest: validated.append(manifest),
    )
    fresh = tmp_path / "fresh"
    lhos = tmp_path / "lhos"

    result = runner.materialize_arm_outputs(
        prepared_output=prepared,
        fresh_output=fresh,
        lhos_output=lhos,
        task_names=["c,a"],
    )

    assert len(validated) == 1
    assert result["task_names"] == ["c", "a"]
    assert result["task_count"] == 2
    fresh_manifest = json.loads(
        (fresh / "manifest.json").read_text(encoding="utf-8")
    )
    lhos_manifest = json.loads(
        (lhos / "manifest.json").read_text(encoding="utf-8")
    )
    assert fresh_manifest["run_arm"] == "fresh"
    assert fresh_manifest["run_arms"] == ["dsh_fresh"]
    assert lhos_manifest["run_arm"] == "lhos"
    assert lhos_manifest["run_arms"] == ["lhos_resume"]
    assert [task["name"] for task in fresh_manifest["tasks"]] == ["c", "a"]
    assert [task["priority"] for task in fresh_manifest["tasks"]] == [1, 2]
    assert [task["source_priority"] for task in fresh_manifest["tasks"]] == [3, 1]
    assert fresh_manifest["tasks"] == lhos_manifest["tasks"]
    assert (
        fresh_manifest["materialization"]["common_identity_sha256"]
        == lhos_manifest["materialization"]["common_identity_sha256"]
    )
    assert (
        fresh_manifest["tasks"][0]["paired_verifier_seed_sha256"]
        == lhos_manifest["tasks"][0]["paired_verifier_seed_sha256"]
        == "seed-hash"
    )
    for name in ("c", "a"):
        assert (
            fresh_manifest["configs"][name]["parity_sha256"]
            == lhos_manifest["configs"][name]["parity_sha256"]
        )
        for arm in runner.ARMS:
            assert Path(fresh_manifest["configs"][name][arm]).is_file()
            assert Path(lhos_manifest["configs"][name][arm]).is_file()
    assert fresh_manifest["pair_admission"]["task_count"] == 2
    assert fresh_manifest["pair_admission"]["status"] == "prepared"
    assert not (fresh / "progress.json").exists()
    assert not (fresh / "result.json").exists()
    assert list((fresh / "runs").iterdir()) == []
    fresh_inventory = json.loads(
        (fresh / "image-inventory.json").read_text(encoding="utf-8")
    )
    assert fresh_inventory["available_task_names"] == ["c", "a"]
    assert {
        task_name
        for item in fresh_inventory["images"]
        for task_name in item["task_names"]
    } == {"a", "c"}
    fresh_prebuild = json.loads(
        (fresh / "prebuild.json").read_text(encoding="utf-8")
    )
    assert [row["task_name"] for row in fresh_prebuild["records"]] == [
        "a",
        "c",
    ]
    assert fresh_prebuild["complete"] is True


def test_materialize_arms_rejects_runtime_pin_mismatch_and_existing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = tmp_path / "prepared"
    _make_prepared_materialization_fixture(prepared, task_names=["a"])
    monkeypatch.setattr(
        runner,
        "_runtime_pins_match",
        lambda _manifest: (_ for _ in ()).throw(
            RuntimeError("runtime pin mismatch")
        ),
    )

    with pytest.raises(RuntimeError, match="runtime pin mismatch"):
        runner.materialize_arm_outputs(
            prepared_output=prepared,
            fresh_output=tmp_path / "fresh",
            lhos_output=tmp_path / "lhos",
        )
    assert not (tmp_path / "fresh").exists()
    assert not (tmp_path / "lhos").exists()

    monkeypatch.setattr(runner, "_runtime_pins_match", lambda _manifest: None)
    existing = tmp_path / "existing"
    existing.mkdir()
    with pytest.raises(RuntimeError, match="target already exists"):
        runner.materialize_arm_outputs(
            prepared_output=prepared,
            fresh_output=existing,
            lhos_output=tmp_path / "new-lhos",
        )
    assert not (tmp_path / "new-lhos").exists()


def test_official_worker_timeout_includes_agent_and_verifier_budget() -> None:
    task = {
        "official_agent_timeout_seconds": 3600,
        "official_verifier_timeout_seconds": 900,
    }
    manifest = {
        "agent_timeout_mode": "task_declared",
        "worker_timeout_seconds": 1800,
    }
    assert runner._task_worker_timeout_seconds(task, manifest) == 4800
    legacy_manifest = {**manifest, "agent_timeout_mode": "official_per_task"}
    assert runner._task_worker_timeout_seconds(task, legacy_manifest) == 4800


def test_fixed_budget_worker_timeout_includes_long_verifier() -> None:
    assert (
        runner._configured_worker_timeout_seconds(
            configured_agent_timeout_seconds=900,
            verifier_timeout_seconds=5400,
            base_worker_timeout_seconds=1800,
        )
        == 6600
    )
    assert (
        runner._configured_worker_timeout_seconds(
            configured_agent_timeout_seconds=120,
            verifier_timeout_seconds=300,
            base_worker_timeout_seconds=1800,
        )
        == 1800
    )


def test_trial_metrics_records_time_to_verified_and_first_usage(
    tmp_path: Path,
) -> None:
    job_root = tmp_path / "job"
    trial = job_root / "trial__verified"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {
            "stats": {
                "evals": {
                    "fixture": {
                        "reward_stats": {"reward": {"1.0": ["trial__verified"]}}
                    }
                }
            }
        },
    )
    runner._write_json(
        trial / "result.json",
        {
            "started_at": "2026-08-21T00:00:00Z",
            "finished_at": "2026-08-21T00:01:40Z",
            "agent_execution": {
                "started_at": "2026-08-21T00:00:05Z",
                "finished_at": "2026-08-21T00:01:30Z",
            },
            "verifier": {
                "started_at": "2026-08-21T00:01:30Z",
                "finished_at": "2026-08-21T00:01:40Z",
            },
            "agent_result": {
                "n_input_tokens": 10,
                "n_output_tokens": 5,
                "metadata": {},
            },
            "verifier_result": {"rewards": {"reward": 1.0}},
        },
    )
    runner._write_json(
        trial / "agent" / "dsh-observability.json",
        {
            "schema_version": "lhos-lhtb-dsh-harbor-agent.v1",
            "arm": "baseline",
            "controller": "none",
            "resume_count": 0,
            "session_reused": False,
            "invocation_count": 1,
            "usage": {
                "total_token_units": 15,
                "model_calls": 2,
                "tool_calls": 3,
            },
        },
    )
    metrics = runner._trial_metrics(job_root, "dsh_fresh")
    assert metrics["resolved"] is True
    assert metrics["verification_status"] == "verified"
    assert metrics["verification_observation"] == "final_verifier"
    assert metrics["time_to_verified_ms"] == 85000
    assert metrics["time_to_verified_source"] == "agent_phase_end"
    assert metrics["first_verified_token_units"] == 15
    assert metrics["first_verified_model_calls"] == 2
    assert metrics["first_verified_tool_calls"] == 3


def test_first_process_reward_checkpoint_excludes_verifier_time(tmp_path: Path) -> None:
    trial = tmp_path / "trial"
    trial.mkdir()
    records = [
        {
            "checkpoint_id": "checkpoint-01",
            "checkpoint_kind": "timed",
            "active_agent_time_sec": 12.5,
            "verifier_finished_at": "2026-08-21T00:00:20Z",
            "rewards": {"reward": 0.95},
        },
        {
            "checkpoint_id": "checkpoint-02",
            "checkpoint_kind": "timed",
            "active_agent_time_sec": 25.0,
            "verifier_finished_at": "2026-08-21T00:00:40Z",
            "rewards": {"reward": 1.0},
        },
    ]
    (trial / "process_reward_checkpoints" / "checkpoint-02").mkdir(parents=True)
    (trial / "process_reward.jsonl").write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    runner._write_json(
        trial / "process_reward_checkpoints" / "checkpoint-02" / "checkpoint.json",
        records[1],
    )

    first = runner._first_verified_process_reward(trial)
    assert first is not None
    assert first["checkpoint_id"] == "checkpoint-02"
    assert first["active_agent_time_sec"] == 25.0


def test_lhos_goal_closes_only_on_full_verifier_pass() -> None:
    partial = runner._lhos_verification_outcome(
        "fixture",
        {
            "reward": 0.95,
            "resolved": True,
            "verified": False,
        },
    )
    full = runner._lhos_verification_outcome(
        "fixture",
        {
            "reward": 1.0,
            "resolved": True,
            "verified": True,
        },
    )

    assert partial.passed is False
    assert full.passed is True


def test_time_to_verified_annotation_marks_budget_censoring() -> None:
    annotated = runner._annotate_time_to_verified(
        {
            "verified": False,
            "verification_status": "not_verified",
            "budget_exhausted": False,
            "agent_elapsed_ms": 3_599_500,
        },
        {"configured_agent_timeout_seconds": 3_600},
    )
    assert annotated["verification_status"] == "timeout"
    assert annotated["budget_exhausted"] is True
    assert (
        annotated["time_to_verified_censoring"]
        == "right_censored_at_agent_budget"
    )

    preserved = runner._annotate_time_to_verified(
        {
            "verified": True,
            "verification_observation": "final_verifier",
            "time_to_verified_source": "process_reward_checkpoint",
            "agent_elapsed_ms": 100,
        },
        {"configured_agent_timeout_seconds": 3_600},
    )
    assert preserved["time_to_verified_source"] == "process_reward_checkpoint"


def test_fmt_preserves_fractional_rewards() -> None:
    assert runner._fmt(0.085451037218) == "0.085451"
    assert runner._fmt(1.0) == "1"
    assert runner._fmt(1234.5) == "1,234.5"


def test_budget_tier_profiling_reports_itt_and_mechanism_denominators() -> None:
    pairs = [
        {
            "task_name": "short-a",
            "configured_agent_timeout_seconds": 600,
            "continuation_boundary_mode": "natural",
            "complete": True,
            "result_eligible": True,
            "mechanism_eligible": True,
            "dsh_fresh": {"reward": 0.0},
            "lhos_resume": {"reward": 0.8},
        },
        {
            "task_name": "short-b",
            "configured_agent_timeout_seconds": 600,
            "continuation_boundary_mode": "one_shot",
            "complete": True,
            "result_eligible": False,
            "mechanism_eligible": False,
            "dsh_fresh": {"reward": 1.0},
            "lhos_resume": {"reward": None},
        },
        {
            "task_name": "long-a",
            "configured_agent_timeout_seconds": 3600,
            "continuation_boundary_mode": "natural",
            "complete": True,
            "result_eligible": True,
            "mechanism_eligible": False,
            "dsh_fresh": {"reward": 0.5},
            "lhos_resume": {"reward": 1.0},
        },
    ]

    profiling = runner._budget_tier_profiling(pairs)
    assert [tier["budget_seconds"] for tier in profiling["tiers"]] == [600, 3600]
    short = profiling["tiers"][0]
    assert short["pair_count"] == 2
    assert short["continuation_pair_count"] == 1
    assert short["one_shot_pair_count"] == 1
    assert short["result_eligible_pair_count"] == 1
    assert short["mechanism_eligible_pair_count"] == 1
    assert short["outcome_itt"]["fresh_mean_reward"] == 0.5
    assert short["outcome_itt"]["lhos_mean_reward"] == 0.4
    assert short["outcome_itt"]["lhos_minus_fresh_mean_reward"] == -0.1
    assert short["observed_reward_pairs"]["pair_count"] == 1
    assert short["mechanism_cohort"]["task_names"] == ["short-a"]


def _minimal_manifest(output: Path, *, task_names: list[str]) -> dict[str, object]:
    tasks = [
        {
            "name": name,
            "priority": index,
            "docker_image": f"local/{name}:latest",
            "official_build_timeout_seconds": 1,
        }
        for index, name in enumerate(task_names, start=1)
    ]
    manifest = {
        "schema_version": runner.MANIFEST_SCHEMA_V2,
        "benchmark": "fixture",
        "official_score": False,
        "task_content": "fixture",
        "environment": "fixture",
        "lhtb_source_commit": "fixture",
        "model": "fixture",
        "reasoning_effort": "medium",
        "agent_timeout_seconds": 1,
        "max_concurrency": 1,
        "worker_timeout_seconds": 1,
        "harbor": {"version": "fixture", "commit": "fixture", "project": str(output)},
        "runtime": {},
        "tasks_root": str(output / "tasks"),
        "tasks": tasks,
        "configs": {
            name: {
                "dsh_fresh": str(output / f"{name}-fresh.yml"),
                "lhos_resume": str(output / f"{name}-lhos.yml"),
                "dsh_fresh_job_name": f"{name}-fresh",
                "lhos_resume_job_name": f"{name}-lhos",
                "parity_sha256": "fixture-parity",
            }
            for name in task_names
        },
        "attribution": "fixture",
    }
    runner._write_json(output / "manifest.json", manifest)
    return manifest


def test_write_json_is_atomic_when_replace_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "record.json"
    runner._write_json(path, {"version": 1})

    def fail_replace(_source: Path, _destination: Path) -> None:
        raise OSError("simulated interruption")

    monkeypatch.setattr(runner.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated interruption"):
        runner._write_json(path, {"version": 2})

    assert json.loads(path.read_text(encoding="utf-8")) == {"version": 1}
    assert list(tmp_path.glob(".*.tmp")) == []


@pytest.mark.parametrize(
    ("contents", "reason"),
    [
        (None, "missing_worker_status"),
        ("", "empty_worker_status"),
        ("{bad", "invalid_worker_status_json"),
    ],
)
def test_worker_status_corruption_is_infrastructure_interruption(
    tmp_path: Path,
    contents: str | None,
    reason: str,
) -> None:
    path = tmp_path / "status.json"
    if contents is not None:
        path.write_text(contents, encoding="utf-8")

    status = runner._sanitize_status(path, ())

    assert status["infrastructure_interrupted"] is True
    assert status["status_read_error"] == reason
    assert (
        runner._worker_infrastructure_reason(
            status,
            launcher_exit_code=0,
        )
        == reason
    )


@pytest.mark.parametrize(
    "exit_code",
    [runner.WINDOWS_CONTROL_C_EXIT, runner.WINDOWS_CONTROL_C_EXIT - (1 << 32)],
)
def test_windows_control_c_exit_is_infrastructure_interruption(
    exit_code: int,
) -> None:
    assert runner._windows_control_interrupted(exit_code) is True
    assert (
        runner._worker_infrastructure_reason(
            {"status": "finished", "exit_code": exit_code},
            launcher_exit_code=0,
        )
        == "harbor_control_c_exit"
    )


def test_infrastructure_cleanup_is_scoped_to_exact_compose_project(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queries: list[tuple[str, str]] = []
    commands: list[list[str]] = []

    def docker_ids(resource: str, project: str):
        queries.append((resource, project))
        return {
            "ps": (["container-1"], None),
            "network": (["network-1"], None),
            "volume": ([], None),
        }[resource]

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(runner, "_docker_ids", docker_ids)
    monkeypatch.setattr(runner, "_run", run)

    result = runner._cleanup_compose_project("fixture__abc123")

    assert result["complete"] is True
    assert queries == [
        ("ps", "fixture__abc123"),
        ("network", "fixture__abc123"),
        ("volume", "fixture__abc123"),
    ]
    assert commands == [
        ["docker", "rm", "-f", "container-1"],
        ["docker", "network", "rm", "network-1"],
    ]


def test_prebuild_timeout_keeps_processing_and_writes_all_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    _minimal_manifest(output, task_names=["timeout", "error", "success"])
    built: set[str] = set()
    calls: list[str] = []

    def image_id(image: str) -> str | None:
        return f"sha256:{image}" if image in built else None

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        image = command[command.index("--tag") + 1]
        calls.append(image)
        if image.endswith("timeout:latest"):
            raise subprocess.TimeoutExpired(command, timeout=1800)
        if image.endswith("error:latest"):
            raise FileNotFoundError("docker executable missing")
        built.add(image)
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(runner, "_docker_image_id", image_id)
    monkeypatch.setattr(runner, "_run", run)
    args = argparse.Namespace(output=output, missing_only=False)
    with pytest.raises(RuntimeError, match="prebuild did not complete"):
        runner.prebuild(args)

    payload = json.loads((output / "prebuild.json").read_text(encoding="utf-8"))
    assert calls == [
        "local/timeout:latest",
        "local/error:latest",
        "local/success:latest",
    ]
    assert len(payload["records"]) == 3
    assert payload["records"][0]["timed_out"] is True
    assert payload["records"][0]["execution_error"]["type"] == "TimeoutExpired"
    assert payload["records"][1]["exit_code"] == 125
    assert payload["records"][1]["execution_error"]["type"] == "FileNotFoundError"
    assert payload["records"][2]["exit_code"] == 0


def test_run_task_pair_persists_one_arm_failure_and_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    task = manifest["tasks"][0]

    def fail_fresh(**_: object) -> dict[str, object]:
        raise RuntimeError("fresh failed")

    async def run_lhos(**_: object) -> dict[str, object]:
        return {
            "status": "completed",
            "arm": "lhos_resume",
            "task_name": "fixture",
            "metrics": {
                "parse_valid": True,
                "comparison_eligible": False,
                "parser_error": None,
                "execution_error": None,
                "resolved": False,
            },
        }

    monkeypatch.setattr(runner, "_run_fresh_arm", fail_fresh)
    monkeypatch.setattr(runner, "_run_lhos_arm", run_lhos)
    monkeypatch.setattr(runner, "_docker_image_id", lambda _image: None)
    records = runner._run_task_pair(
        task=task,
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )

    assert {record["arm"] for record in records} == {"dsh_fresh", "lhos_resume"}
    fresh = json.loads((output / "runs" / "fixture" / "dsh_fresh.json").read_text(encoding="utf-8"))
    lhos = json.loads(
        (output / "runs" / "fixture" / "lhos_resume.json").read_text(encoding="utf-8")
    )
    assert fresh["execution_error"]["type"] == "RuntimeError"
    assert fresh["metrics"]["comparison_eligible"] is False
    assert lhos["status"] == "completed"
    progress = json.loads((output / "progress.json").read_text(encoding="utf-8"))
    assert progress["terminal_pair_count"] == 1
    assert progress["completed_pair_count"] == 0
    assert progress["failed_arm_count"] == 1
    assert progress["tasks"][0]["pair_status"] == "terminal_with_failure"


def test_run_task_pair_retries_infrastructure_once_and_preserves_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    task = manifest["tasks"][0]
    calls = {"fresh": 0, "lhos": 0, "cleanup": 0}

    def run_fresh(**_: object) -> dict[str, object]:
        calls["fresh"] += 1
        if calls["fresh"] == 1:
            raise runner.InfrastructureInterruptedError(
                "console interrupted",
                reason="harbor_control_c_exit",
                worker={"exit_code": runner.WINDOWS_CONTROL_C_EXIT},
                launcher_exit_code=0,
            )
        return {
            "status": "completed",
            "arm": "dsh_fresh",
            "task_name": "fixture",
            "metrics": {
                "parse_valid": True,
                "result_eligible": True,
                "mechanism_eligible": True,
                "comparison_eligible": True,
                "reward": 0.5,
                "token_units": 10,
            },
        }

    async def run_lhos(**_: object) -> dict[str, object]:
        calls["lhos"] += 1
        return {
            "status": "completed",
            "arm": "lhos_resume",
            "task_name": "fixture",
            "metrics": {
                "parse_valid": True,
                "result_eligible": True,
                "mechanism_eligible": True,
                "comparison_eligible": True,
                "reward": 0.5,
                "token_units": 8,
            },
        }

    def prepare_retry(**kwargs: object) -> dict[str, object]:
        calls["cleanup"] += 1
        return {
            "schema_version": "lhos-lhtb-controller-attempt.v1",
            "task_name": "fixture",
            "arm": kwargs["arm"],
            "controller_attempt": 1,
            "classification": "infrastructure_interrupted",
            "partial_metrics": {"token_units": 7, "model_calls": 1},
            "retry_authorized": True,
        }

    monkeypatch.setattr(runner, "_run_fresh_arm", run_fresh)
    monkeypatch.setattr(runner, "_run_lhos_arm", run_lhos)
    monkeypatch.setattr(runner, "_prepare_infrastructure_retry", prepare_retry)
    monkeypatch.setattr(runner, "_docker_image_id", lambda _image: "sha256:fixture")

    records = runner._run_task_pair(
        task=task,
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )

    assert calls == {"fresh": 2, "lhos": 1, "cleanup": 1}
    fresh = next(record for record in records if record["arm"] == "dsh_fresh")
    assert fresh["status"] == "completed"
    assert fresh["controller_attempt_count"] == 2
    assert fresh["infrastructure_retry_count"] == 1
    assert fresh["infrastructure_resampled"] is True
    assert fresh["controller_attempt_ledger"][0]["partial_metrics"] == {
        "token_units": 7,
        "model_calls": 1,
    }


def test_run_task_pair_never_retries_infrastructure_more_than_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    task = manifest["tasks"][0]
    calls = {"fresh": 0}

    def run_fresh(**_: object) -> dict[str, object]:
        calls["fresh"] += 1
        raise runner.InfrastructureInterruptedError(
            "console interrupted",
            reason="harbor_control_c_exit",
        )

    async def run_lhos(**_: object) -> dict[str, object]:
        return {
            "status": "completed",
            "arm": "lhos_resume",
            "task_name": "fixture",
            "metrics": {
                "parse_valid": True,
                "result_eligible": True,
                "mechanism_eligible": True,
                "comparison_eligible": True,
            },
        }

    monkeypatch.setattr(runner, "_run_fresh_arm", run_fresh)
    monkeypatch.setattr(runner, "_run_lhos_arm", run_lhos)
    monkeypatch.setattr(
        runner,
        "_prepare_infrastructure_retry",
        lambda **kwargs: {
            "classification": "infrastructure_interrupted",
            "controller_attempt": kwargs["controller_attempt"],
            "partial_metrics": {},
            "retry_authorized": True,
        },
    )
    monkeypatch.setattr(runner, "_docker_image_id", lambda _image: "sha256:fixture")

    records = runner._run_task_pair(
        task=task,
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )

    assert calls["fresh"] == 2
    fresh = next(record for record in records if record["arm"] == "dsh_fresh")
    assert fresh["status"] == "failed"
    assert fresh["failure_classification"] == "infrastructure_interrupted"
    assert fresh["controller_attempt_count"] == 2
    assert len(fresh["controller_attempt_ledger"]) == 2


def test_run_task_pair_replays_corrupt_record_and_skips_valid_terminal_arm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    task = manifest["tasks"][0]
    run_root = output / "runs" / "fixture"
    run_root.mkdir(parents=True)
    (run_root / "dsh_fresh.json").write_text("{bad", encoding="utf-8")
    runner._write_json(
        run_root / "lhos_resume.json",
        {
            "status": "completed",
            "arm": "lhos_resume",
            "task_name": "fixture",
            "config_parity_sha256": "fixture-parity",
            "metrics": {
                "parse_valid": True,
                "comparison_eligible": True,
                "reward": 0.5,
                "token_units": 20,
            },
        },
    )
    calls = {"fresh": 0, "lhos": 0}

    def run_fresh(**_: object) -> dict[str, object]:
        calls["fresh"] += 1
        return {
            "status": "completed",
            "arm": "dsh_fresh",
            "task_name": "fixture",
            "metrics": {
                "parse_valid": True,
                "comparison_eligible": True,
                "reward": 0.5,
                "token_units": 10,
            },
        }

    async def run_lhos(**_: object) -> dict[str, object]:
        calls["lhos"] += 1
        raise AssertionError("valid terminal arm must be skipped")

    monkeypatch.setattr(runner, "_run_fresh_arm", run_fresh)
    monkeypatch.setattr(runner, "_run_lhos_arm", run_lhos)
    monkeypatch.setattr(runner, "_docker_image_id", lambda _image: "sha256:fixture")

    records = runner._run_task_pair(
        task=task,
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )

    assert calls == {"fresh": 1, "lhos": 0}
    assert {record["arm"] for record in records} == {
        "dsh_fresh",
        "lhos_resume",
    }
    repaired = json.loads(
        (run_root / "dsh_fresh.json").read_text(encoding="utf-8")
    )
    assert repaired["status"] == "completed"
    progress = json.loads((output / "progress.json").read_text(encoding="utf-8"))
    assert progress["completed_pair_count"] == 1
    assert progress["completed_arm_count"] == 2
    by_arm = progress["tasks"][0]["arms"]
    assert by_arm["dsh_fresh"]["metrics"]["token_units"] == 10
    assert by_arm["lhos_resume"]["metrics"]["token_units"] == 20


def test_terminal_arm_identity_mismatch_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "arm.json"
    runner._write_json(
        path,
        {
            "status": "completed",
            "arm": "dsh_fresh",
            "task_name": "other-task",
            "config_parity_sha256": "parity",
        },
    )

    with pytest.raises(RuntimeError, match="identity mismatch: task_name"):
        runner._existing_terminal_arm_record(
            path,
            task_name="fixture",
            arm="dsh_fresh",
            parity_sha256="parity",
        )


def test_summary_distinguishes_parser_and_execution_failures(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["missing", "failed", "unresolved"])

    failed = runner._arm_failure_record(
        task_name="failed",
        arm="dsh_fresh",
        exc=RuntimeError("worker crashed"),
    )
    runner._write_json(output / "runs" / "failed" / "dsh_fresh.json", failed)
    runner._write_json(
        output / "runs" / "failed" / "lhos_resume.json",
        {
            "status": "completed",
            "arm": "lhos_resume",
            "metrics": {
                "parse_valid": True,
                "comparison_eligible": False,
                "resolved": False,
            },
        },
    )
    (output / "runs" / "missing").mkdir(parents=True)
    (output / "runs" / "missing" / "lhos_resume.json").write_text("{bad", encoding="utf-8")
    for arm in runner.ARMS:
        runner._write_json(
            output / "runs" / "unresolved" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "metrics": {
                    "parse_valid": True,
                    "comparison_eligible": False,
                    "parser_error": None,
                    "execution_error": None,
                    "resolved": False,
                    "reward": 0.0,
                },
            },
        )

    result = runner.summarize_output(output)
    by_name = {pair["task_name"]: pair for pair in result["pairs"]}
    assert by_name["missing"]["parser_error"] == runner.ARM_RECORD_PARSER_ERROR
    assert by_name["failed"]["parser_error"] is None
    assert by_name["failed"]["fresh_execution_error"]["type"] == "RuntimeError"
    assert by_name["failed"]["comparison_eligible"] is False
    assert by_name["unresolved"]["parser_error"] is None
    assert by_name["unresolved"]["complete"] is True


def test_trial_metrics_sums_step_agent_results(tmp_path: Path) -> None:
    job_root = tmp_path / "job"
    trial = job_root / "trial__abc"
    trial.mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {"stats": {"evals": {"fixture": {"reward_stats": {"reward": {"0.0": ["trial__abc"]}}}}}},
    )
    runner._write_json(
        trial / "result.json",
        {
            "step_results": [
                {"agent_result": {"n_input_tokens": 10, "n_cache_tokens": 2, "n_output_tokens": 3}},
                {"agent_result": {"n_input_tokens": 20, "n_cache_tokens": 4, "n_output_tokens": 5}},
            ],
            "verifier_result": {"reward": 0.0},
        },
    )
    metrics = runner._trial_metrics(job_root, "dsh_fresh")
    assert metrics["parse_valid"] is True
    assert metrics["input_tokens"] == 30
    assert metrics["cache_tokens"] == 6
    assert metrics["output_tokens"] == 8


def test_trial_metrics_classifies_provider_censorship_fail_closed(
    tmp_path: Path,
) -> None:
    job_root = tmp_path / "job"
    trial = job_root / "trial__censored"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {
            "stats": {
                "evals": {
                    "fixture": {
                        "reward_stats": {
                            "reward": {"0.0": ["trial__censored"]}
                        }
                    }
                }
            }
        },
    )
    runner._write_json(
        trial / "result.json",
        {
            "agent_result": {
                "n_input_tokens": 10,
                "n_output_tokens": 2,
            },
            "verifier_result": {"reward": 0.0},
            "exception_info": {
                "exception_type": "NonZeroAgentExitCodeError",
                "exception_message": "provider censorship_blocked status_code=451",
            },
        },
    )

    metrics = runner._trial_metrics(job_root, "dsh_fresh")

    assert metrics["provider_censored"] is True
    assert metrics["verification_status"] == "provider_censored"
    assert metrics["execution_error"]["type"] == "provider_censored"
    assert metrics["result_eligible"] is False
    assert metrics["mechanism_eligible"] is False
    assert metrics["comparison_eligible"] is False


def test_image_validation_reports_each_missing_task_without_aborting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["available", "missing"])
    monkeypatch.setattr(
        runner,
        "_docker_image_id",
        lambda image: "sha256:available" if "available" in image else None,
    )

    errors = runner._image_ids_match(output, manifest, strict=False)
    assert set(errors) == {"missing"}

    with pytest.raises(RuntimeError, match="required local image is missing"):
        runner._image_ids_match(output, manifest)


def test_profiling_uses_only_comparison_eligible_pairs() -> None:
    pairs = [
        {
            "task_name": "eligible",
            "result_eligible": True,
            "mechanism_eligible": True,
            "comparison_eligible": True,
            "dsh_fresh": {
                "reward": 1.0,
                "resolved": True,
                "token_units": 100,
                "tool_calls": 10,
            },
            "lhos_resume": {
                "reward": 1.0,
                "resolved": True,
                "token_units": 60,
                "tool_calls": 8,
            },
        },
        {
            "task_name": "excluded",
            "result_eligible": False,
            "mechanism_eligible": False,
            "comparison_eligible": False,
            "dsh_fresh": {
                "reward": 0.0,
                "resolved": False,
                "token_units": 1000,
                "tool_calls": 100,
            },
            "lhos_resume": {
                "reward": 0.0,
                "resolved": False,
                "token_units": 1,
                "tool_calls": 1,
            },
        },
    ]
    profile = runner._aggregate_profiling(pairs)
    tokens = profile["metrics"]["token_units"]
    assert profile["comparison_eligible_task_names"] == ["eligible"]
    assert tokens["fresh_total"] == 100
    assert tokens["lhos_total"] == 60
    assert tokens["saving_percent"] == 40.0
    assert tokens["lhos_wins"] == 1
    assert profile["same_reward_pair_count"] == 1

    full = runner._aggregate_profiling(
        pairs,
        eligibility_key="result_eligible",
    )
    assert full["eligible_task_names"] == ["eligible"]


def test_reward_fallback_matches_exact_trial_name() -> None:
    job_result = {
        "stats": {
            "evals": {
                "fixture": {
                    "metrics": [{"mean": 0.99}],
                    "reward_stats": {
                        "reward": {
                            "0.25": ["trial__old"],
                            "0.75": ["trial__current"],
                        }
                    },
                }
            }
        }
    }
    assert runner._reward(job_result, {}, "trial__current") == 0.75
    assert runner._reward(job_result, {}, "trial__missing") is None


def test_trial_dir_prefers_job_referenced_trial(tmp_path: Path) -> None:
    job_root = tmp_path / "job"
    old = job_root / "trial__old"
    current = job_root / "trial__current"
    old.mkdir(parents=True)
    current.mkdir(parents=True)
    runner._write_json(old / "result.json", {"id": "old"})
    runner._write_json(current / "result.json", {"id": "current"})
    job_result = {
        "stats": {"evals": {"fixture": {"reward_stats": {"reward": {"0.0": ["trial__current"]}}}}}
    }
    assert runner._trial_dir(job_root, job_result) == current


def test_resume_gate_requires_real_model_activity() -> None:
    observability = {
        "schema_version": "lhos-lhtb-dsh-harbor-agent.v1",
        "arm": "lhos",
        "controller": "longhorizonos",
        "resume_api": "ctx.agents.resume",
        "invocation_count": 2,
        "resume_count": 1,
        "session_reused": True,
        "session_file_count": 1,
        "session_id": "session-1234",
        "event_count": 10,
        "usage": {"model_calls": 0},
        "invocations": [
            {
                "resume": True,
                "status": "slice_preempted",
                "exit_code": 197,
            }
        ],
    }
    no_activity = runner._resume_gate(observability)
    assert no_activity["checks"]["model_activity"] is False
    assert no_activity["passed"] is False

    observability["usage"]["model_calls"] = 1
    with_activity = runner._resume_gate(observability)
    assert with_activity["checks"]["model_activity"] is True
    assert with_activity["passed"] is True


def test_strict_controlled_resume_gate_requires_observed_protocol_fields() -> None:
    observability = _natural_timeout_resume_observability()

    legacy = runner._resume_gate(
        observability,
        trial_exception_type="AgentTimeoutError",
        budget_exhausted=True,
    )
    assert legacy["passed"] is True

    strict_missing = runner._resume_gate(
        observability,
        trial_exception_type="AgentTimeoutError",
        budget_exhausted=True,
        controlled_pair_required=True,
    )
    assert strict_missing["passed"] is False
    assert strict_missing["checks"]["controlled_pair_mode"] is False
    assert strict_missing["checks"]["harbor_continue_mode"] is False
    assert strict_missing["checks"]["binary_feedback"] is False

    observability.update(
        {
            "controlled_pair_mode": True,
            "harbor_continue_mode": "same_conversation",
            "verifier_feedback_mode": "binary",
        }
    )
    strict_observed = runner._resume_gate(
        observability,
        trial_exception_type="AgentTimeoutError",
        budget_exhausted=True,
        controlled_pair_required=True,
    )
    assert strict_observed["passed"] is True


def test_v3_controlled_manifest_cannot_drop_control_block(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    manifest["schema_version"] = runner.MANIFEST_SCHEMA_V3

    with pytest.raises(RuntimeError, match="V3 requires"):
        runner._validate_controlled_pair_manifest(manifest)


def test_controlled_manifest_recomputes_config_parity_hash(tmp_path: Path) -> None:
    output = tmp_path / "output"
    output.mkdir()
    selected = (runner.SelectedTask("fixture", 1, "fixture"),)
    runtime = {
        "container_root": "/opt/lhtb-runtime",
        "node_container_path": "/opt/lhtb-runtime/node/bin/node",
        "dsh_container_path": "/opt/lhtb-runtime/dsh/lib/bin.js",
        "patch_container_path": "/opt/lhtb-patches/provider.yml",
        "dsh_version": "fixture",
    }
    configs = runner._write_configs(
        output,
        selected_tasks=selected,
        tasks_root=tmp_path / "tasks",
        runtime_root=tmp_path / "runtime",
        patch_path=tmp_path / "patches" / "provider.yml",
        agent_timeout_seconds=300,
        time_slice_seconds=75,
        runtime=runtime,
        job_prefix="fixture",
    )
    manifest = _minimal_manifest(output, task_names=["fixture"])
    manifest.update(
        {
            "schema_version": runner.MANIFEST_SCHEMA_V3,
            "agent": runner.AGENT_IMPORT_PATH,
            "runtime": runtime,
            "configs": configs,
            "controlled_pair_experiment": {
                "schema_version": runner.CONTROLLED_PAIR_SCHEMA_V1,
                "enabled": True,
                "harness_constant": True,
                "validity": "partial",
                "harbor_continue_mode": "same_conversation",
                "verifier_feedback_mode": "binary",
                "verifier_isolation_guaranteed": False,
                "verifier_artifact_isolation_guaranteed": False,
                "control_arm": "dsh_fresh",
                "treatment_arm": "lhos_resume",
            },
        }
    )
    runner._validate_controlled_pair_manifest(manifest)

    config_path = Path(configs["fixture"]["dsh_fresh"])
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["agents"][0]["kwargs"]["time_slice_seconds"] = 999
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(RuntimeError, match="parity hash is stale"):
        runner._validate_controlled_pair_manifest(manifest)


def test_resume_gate_accepts_adaptive_context_generations_and_max_tokens() -> None:
    observability = {
        "schema_version": "lhos-lhtb-dsh-harbor-agent.v1",
        "arm": "lhos",
        "controller": "longhorizonos",
        "resume_api": "ctx.agents.resume",
        "resume_evidence_mode": "adaptive_context_control",
        "semantic_context_control": True,
        "semantic_control_artifact": "dsh-semantic-control.json",
        "semantic_decision_count": 4,
        "invocation_count": 6,
        "resume_count": 4,
        "session_reused": True,
        "session_file_count": 2,
        "session_generation_count": 2,
        "controlled_restart_count": 1,
        "session_id": "session-generation-1",
        "event_count": 100,
        "usage": {"model_calls": 8},
        "invocations": [
            {
                "resume": True,
                "status": "max_tokens_checkpoint",
                "exit_code": 1,
                "max_tokens_checkpoint": True,
            }
        ],
    }
    gate = runner._resume_gate(observability)
    assert gate["passed"] is True
    assert gate["classification"] == "adaptive_context_control"
    assert gate["checks"]["session_topology_valid"] is True
    assert gate["checks"]["semantic_control_evidence"] is True

    observability["controlled_restart_count"] = 0
    invalid = runner._resume_gate(observability)
    assert invalid["passed"] is False
    assert invalid["checks"]["session_topology_valid"] is False


def _natural_timeout_resume_observability() -> dict[str, object]:
    session_id = "session-natural-123"
    return {
        "schema_version": "lhos-lhtb-dsh-harbor-agent.v1",
        "arm": "lhos",
        "controller": "longhorizonos",
        "resume_api": "ctx.agents.resume",
        "resume_evidence_mode": "adaptive_context_control",
        "semantic_context_control": True,
        "semantic_control_artifact": "dsh-semantic-control.json",
        "semantic_decision_count": 1,
        "semantic_decisions": [
            {
                "action": "resume",
                "event_count": 112,
                "session_id": session_id,
            }
        ],
        "invocation_count": 2,
        "resume_count": 1,
        "session_reused": True,
        "session_file_count": 1,
        "session_generation_count": 1,
        "controlled_restart_count": 0,
        "session_id": session_id,
        "event_count": 24_986,
        "usage": {"model_calls": 75},
        "last_invocation_status": "cancelled",
        "invocations": [
            {
                "resume": False,
                "status": "completed",
                "exit_code": 0,
            },
            {
                "resume": True,
                "status": "cancelled",
                "failure_type": "CancelledError",
                "resume_session_id": session_id,
                "exit_code": None,
            },
        ],
    }


def test_resume_gate_accepts_budget_cancelled_natural_boundary_resume() -> None:
    observability = _natural_timeout_resume_observability()

    gate = runner._resume_gate(
        observability,
        trial_exception_type="AgentTimeoutError",
        budget_exhausted=True,
    )

    assert gate["passed"] is True
    assert gate["classification"] == "natural_boundary_resume"
    assert gate["checks"]["durable_resume_invocation"] is True
    assert gate["natural_boundary_resume"]["passed"] is True
    assert (
        gate["natural_boundary_resume"]["checks"]["event_growth_after_decision"]
        is True
    )


@pytest.mark.parametrize(
    ("exception_type", "budget_exhausted", "event_count"),
    [
        ("CancelledError", False, 24_986),
        ("AgentTimeoutError", True, 112),
    ],
)
def test_resume_gate_rejects_external_cancel_or_no_natural_event_growth(
    exception_type: str,
    budget_exhausted: bool,
    event_count: int,
) -> None:
    observability = _natural_timeout_resume_observability()
    observability["event_count"] = event_count

    gate = runner._resume_gate(
        observability,
        trial_exception_type=exception_type,
        budget_exhausted=budget_exhausted,
    )

    assert gate["passed"] is False
    assert gate["natural_boundary_resume"]["passed"] is False
    assert (
        gate["natural_boundary_resume"]["checks"]["trial_budget_exhausted"]
        is (exception_type == "AgentTimeoutError" or budget_exhausted)
    )
    assert (
        gate["natural_boundary_resume"]["checks"]["event_growth_after_decision"]
        is (event_count > 112)
    )


def test_resume_gate_does_not_relabel_forced_slice_as_natural() -> None:
    observability = _natural_timeout_resume_observability()
    observability["effective_time_slice_seconds"] = 60

    gate = runner._resume_gate(
        observability,
        trial_exception_type="AgentTimeoutError",
        budget_exhausted=True,
    )

    assert gate["passed"] is False
    assert gate["natural_boundary_resume"]["checks"]["natural_boundary"] is False


def test_trial_metrics_accepts_budget_cancelled_natural_resume(
    tmp_path: Path,
) -> None:
    job_root = tmp_path / "job"
    trial = job_root / "trial__natural"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {
            "stats": {
                "evals": {
                    "fixture": {
                        "reward_stats": {
                            "reward": {"0.5": ["trial__natural"]}
                        }
                    }
                }
            }
        },
    )
    runner._write_json(
        trial / "result.json",
        {
            "agent_result": {
                "n_input_tokens": 100,
                "n_output_tokens": 10,
            },
            "verifier_result": {"reward": 0.5},
            "exception_info": {
                "exception_type": "AgentTimeoutError",
                "exception_message": "Agent execution timed out",
            },
        },
    )
    runner._write_json(
        trial / "agent" / "dsh-observability.json",
        _natural_timeout_resume_observability(),
    )

    metrics = runner._trial_metrics(job_root, "lhos_resume")

    assert metrics["result_eligible"] is True
    assert metrics["mechanism_eligible"] is True
    assert metrics["budget_exhausted"] is True
    assert metrics["control_classification"] == "natural_boundary_resume"
    assert metrics["natural_boundary_resume_evidence"]["passed"] is True


def test_refresh_arm_metrics_reparses_existing_job_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_root = tmp_path / "job"
    runner._write_json(job_root / "result.json", {"id": "job"})
    calls: list[tuple[Path, str]] = []
    refreshed = {
        "parse_valid": True,
        "resource_measurement_valid": True,
        "comparison_eligible": True,
        "budget_exhausted": True,
    }

    def trial_metrics(root: Path, arm: str) -> dict[str, object]:
        calls.append((root, arm))
        return refreshed

    monkeypatch.setattr(runner, "_trial_metrics", trial_metrics)
    record = {
        "status": "completed",
        "metrics": {
            "job_result": str(job_root / "result.json"),
            "budget_exhausted": False,
        },
    }
    assert runner._refresh_arm_metrics(record, "lhos_resume") == refreshed
    assert calls == [(job_root, "lhos_resume")]


def test_refresh_arm_metrics_never_overwrites_execution_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_root = tmp_path / "job"
    runner._write_json(job_root / "result.json", {"id": "job"})
    persisted = {
        "job_result": str(job_root / "result.json"),
        "parse_valid": False,
        "comparison_eligible": False,
        "execution_error": {"type": "RuntimeError", "message": "worker failed"},
    }

    def unexpected_refresh(_root: Path, _arm: str) -> dict[str, object]:
        pytest.fail("execution-level failed records must not be reparsed")

    monkeypatch.setattr(runner, "_trial_metrics", unexpected_refresh)
    record = {
        "status": "failed",
        "execution_error": persisted["execution_error"],
        "metrics": persisted,
    }
    assert runner._refresh_arm_metrics(record, "dsh_fresh") == persisted


def test_summarize_refreshes_metrics_and_preserves_launcher_elapsed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["fixture"])
    manifest["tasks"][0]["continue_until_timeout"] = True
    manifest["tasks"][0]["configured_time_slice_seconds"] = None
    manifest["tasks"][0]["time_slice_policy"] = "unsliced"
    runner._write_json(output / "manifest.json", manifest)
    jobs: dict[str, Path] = {}
    for arm, elapsed in (("dsh_fresh", 111.0), ("lhos_resume", 222.0)):
        job_root = tmp_path / f"job-{arm}"
        runner._write_json(job_root / "result.json", {"id": arm})
        jobs[arm] = job_root
        runner._write_json(
            output / "runs" / "fixture" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": "fixture",
                "elapsed_ms": elapsed,
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "metrics": {
                    "job_result": str(job_root / "result.json"),
                    "budget_exhausted": False,
                },
            },
        )

    def trial_metrics(root: Path, arm: str) -> dict[str, object]:
        assert root == jobs[arm]
        return {
            "parse_valid": True,
            "resource_measurement_valid": True,
            "result_eligible": True,
            "mechanism_eligible": True,
            "comparison_eligible": True,
            "parser_error": None,
            "execution_error": None,
            "budget_exhausted": True,
            "reward": 0.0,
            "resolved": False,
            "token_units": 10,
            "uncached_input_tokens": 5,
            "cache_read_tokens": 4,
            "output_tokens": 1,
            "model_calls": 1,
            "tool_calls": 1,
            "dsh_invocations": 1,
            "agent_elapsed_ms": 100.0,
            "harbor_total_elapsed_ms": 110.0,
            "continuation_gate": {
                "passed": arm == "lhos_resume",
            },
        }

    monkeypatch.setattr(runner, "_trial_metrics", trial_metrics)
    result = runner.summarize_output(output)
    pair = result["pairs"][0]
    assert pair["result_eligible"] is True
    assert pair["mechanism_eligible"] is True
    assert pair["comparison_eligible"] is True
    assert pair["continuation_boundary_mode"] == "natural"
    assert (
        result["continuation_boundary_profiling"]["natural"]["mechanism"][
            "eligible_pair_count"
        ]
        == 1
    )
    assert pair["dsh_fresh"]["budget_exhausted"] is True
    assert pair["lhos_resume"]["budget_exhausted"] is True
    assert pair["dsh_fresh"]["launcher_elapsed_ms"] == 111.0
    assert pair["lhos_resume"]["launcher_elapsed_ms"] == 222.0


def test_summary_includes_one_shot_in_full_suite_not_mechanism(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["one-shot"])
    manifest["tasks"][0]["time_slice_policy"] = "full_budget_one_shot"
    runner._write_json(output / "manifest.json", manifest)
    for arm, tokens in (("dsh_fresh", 100), ("lhos_resume", 90)):
        runner._write_json(
            output / "runs" / "one-shot" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": "one-shot",
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "metrics": {
                    "parse_valid": True,
                    "resource_measurement_valid": True,
                    "result_eligible": True,
                    "mechanism_eligible": False,
                    "comparison_eligible": False,
                    "parser_error": None,
                    "execution_error": None,
                    "provider_censored": False,
                    "reward": 0.5,
                    "resolved": False,
                    "token_units": tokens,
                    "model_calls": 1,
                    "tool_calls": 1,
                    "continuation_gate": {"passed": False},
                },
            },
        )

    result = runner.summarize_output(output)
    pair = result["pairs"][0]

    assert pair["result_eligible"] is True
    assert pair["mechanism_eligible"] is False
    assert pair["continuation_boundary_mode"] == "one_shot"
    assert pair["reported_lhos_mode"] == "one_shot_direct_compatibility"
    assert result["result_eligible_pair_count"] == 1
    assert result["mechanism_eligible_pair_count"] == 0
    assert result["full_suite_profiling"]["metrics"]["token_units"]["pair_count"] == 1
    assert result["mechanism_profiling"]["metrics"]["token_units"]["pair_count"] == 0
    assert (
        result["continuation_boundary_profiling"]["one_shot"]["full_suite"][
            "eligible_pair_count"
        ]
        == 1
    )


def test_summary_excludes_infrastructure_resample_but_counts_recovery(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["recovered"])
    task = manifest["tasks"][0]
    task["continue_until_timeout"] = True
    task["configured_time_slice_seconds"] = None
    task["continuation_boundary_mode"] = "natural"
    runner._write_json(output / "manifest.json", manifest)
    for arm, tokens in (("dsh_fresh", 100), ("lhos_resume", 90)):
        runner._write_json(
            output / "runs" / "recovered" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": "recovered",
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "controller_attempt_ledger": (
                    [
                        {
                            "partial_metrics": {
                                "token_units": 11,
                                "model_calls": 2,
                                "tool_calls": 3,
                                "agent_elapsed_ms": 400,
                                "harbor_total_elapsed_ms": 500,
                            },
                            "ledger_path": "attempt-1.json",
                        }
                    ]
                    if arm == "dsh_fresh"
                    else []
                ),
                "infrastructure_resampled": arm == "dsh_fresh",
                "metrics": {
                    "parse_valid": True,
                    "resource_measurement_valid": True,
                    "result_eligible": True,
                    "mechanism_eligible": True,
                    "comparison_eligible": True,
                    "parser_error": None,
                    "execution_error": None,
                    "provider_censored": False,
                    "reward": 1.0,
                    "resolved": True,
                    "verified": True,
                    "token_units": tokens,
                    "model_calls": 1,
                    "tool_calls": 1,
                    "continuation_gate": {
                        "passed": arm == "lhos_resume"
                    },
                },
            },
        )

    result = runner.summarize_output(output)
    pair = result["pairs"][0]

    assert pair["result_eligible"] is False
    assert pair["mechanism_eligible"] is False
    assert pair["infrastructure_resampled"] is True
    assert pair["operational_recovered"] is True
    assert result["result_eligible_pair_count"] == 0
    assert result["infrastructure_resampled_pair_count"] == 1
    assert result["operational_recovered_pair_count"] == 1
    assert result["operational_recovered_task_names"] == ["recovered"]
    assert result["infrastructure_retry_cost"]["combined"]["metrics"][
        "token_units"
    ] == 11.0
    assert result["full_suite_profiling"]["metrics"]["token_units"]["pair_count"] == 0
    report = (output / "RESULTS.zh-CN.md").read_text(encoding="utf-8")
    assert "## Infrastructure Retry Cost" in report
    assert "| `dsh_fresh` | 1 | 11 | 2 | 3 | 400 | 500 |" in report


def test_summary_excludes_uncontrolled_stochastic_cohort(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(
        output,
        task_names=["generals-bot-arena"],
    )
    task = manifest["tasks"][0]
    task["continue_until_timeout"] = True
    task["configured_time_slice_seconds"] = None
    task["stochastic_verifier"] = True
    task["stochastic_pair_controlled"] = False
    task["stochastic_control_mode"] = "uncontrolled_stochastic_cohort"
    runner._write_json(output / "manifest.json", manifest)
    for arm in runner.ARMS:
        runner._write_json(
            output / "runs" / "generals-bot-arena" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": "generals-bot-arena",
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "metrics": {
                    "parse_valid": True,
                    "resource_measurement_valid": True,
                    "result_eligible": True,
                    "mechanism_eligible": True,
                    "comparison_eligible": True,
                    "parser_error": None,
                    "execution_error": None,
                    "provider_censored": False,
                    "reward": 1.0,
                    "resolved": True,
                    "verified": True,
                    "token_units": 10,
                    "model_calls": 1,
                    "tool_calls": 1,
                    "continuation_gate": {
                        "passed": arm == "lhos_resume"
                    },
                },
            },
        )

    result = runner.summarize_output(output)
    pair = result["pairs"][0]

    assert pair["stochastic_verifier"] is True
    assert pair["stochastic_pair_controlled"] is False
    assert pair["result_eligible"] is False
    assert pair["mechanism_eligible"] is False
    assert result["uncontrolled_stochastic_pair_count"] == 1
    assert result["uncontrolled_stochastic_task_names"] == [
        "generals-bot-arena"
    ]


def test_summary_provider_censored_pair_is_invalid_without_one_arm_resample(
    tmp_path: Path,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=["censored"])
    manifest["tasks"][0]["time_slice_policy"] = "sliced_continuation"
    runner._write_json(output / "manifest.json", manifest)
    for arm in runner.ARMS:
        censored = arm == "dsh_fresh"
        execution_error = (
            {
                "type": "provider_censored",
                "message": "provider response was censored",
            }
            if censored
            else None
        )
        runner._write_json(
            output / "runs" / "censored" / f"{arm}.json",
            {
                "status": "completed",
                "arm": arm,
                "task_name": "censored",
                "config_parity_sha256": "fixture-parity",
                "docker_image_id": "sha256:fixture",
                "metrics": {
                    "parse_valid": True,
                    "resource_measurement_valid": True,
                    "result_eligible": not censored,
                    "mechanism_eligible": not censored,
                    "comparison_eligible": not censored,
                    "parser_error": None,
                    "execution_error": execution_error,
                    "provider_censored": censored,
                    "reward": 0.0,
                    "resolved": False,
                    "token_units": 10,
                    "model_calls": 1,
                    "tool_calls": 1,
                    "continuation_gate": {
                        "passed": arm == "lhos_resume"
                    },
                },
            },
        )

    result = runner.summarize_output(output)
    pair = result["pairs"][0]

    assert pair["provider_censored"] is True
    assert pair["result_eligible"] is False
    assert pair["mechanism_eligible"] is False
    assert pair["provider_resample"] == {
        "attempted": False,
        "max_pair_resamples": 1,
        "reason": "fail_closed_no_automatic_pair_resample",
    }
    assert result["provider_censored_pair_count"] == 1
    assert result["provider_censored_task_names"] == ["censored"]


def test_secret_scan_finds_exact_key_across_chunk_boundary(tmp_path: Path) -> None:
    secret = "secret-boundary-value"
    hit = tmp_path / "hit.bin"
    safe = tmp_path / "safe.txt"
    split_at = 1024 * 1024 - 7
    hit.write_bytes(b"x" * split_at + secret.encode() + b"tail")
    safe.write_text("secret-boundary-valuX", encoding="utf-8")

    report = runner._scan_secret((tmp_path,), secret)
    assert report["exact_key_hit_count"] == 1
    assert report["hits"] == [str(hit)]
    assert report["scanned_file_count"] == 2
    assert report["errors"] == []
    assert report["passed"] is False


def test_secret_scan_records_inaccessible_directory_and_continues(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "not-present"
    good = tmp_path / "good.txt"
    blocked = tmp_path / "blocked"
    good.write_text("ordinary file", encoding="utf-8")
    blocked.mkdir()
    real_scandir = os.scandir

    def scandir(path: str | os.PathLike[str]) -> os.ScandirIterator[str]:
        if Path(path) == blocked:
            raise PermissionError("access denied")
        return real_scandir(path)

    monkeypatch.setattr(runner.os, "scandir", scandir)
    report = runner._scan_secret((good, blocked), secret)
    assert report["scanned_file_count"] == 1
    assert report["exact_key_hit_count"] == 0
    assert report["passed"] is True
    assert report["scan_complete"] is False
    assert report["error_count"] == 1
    assert report["errors"][0]["path"] == str(blocked)
    assert report["errors"][0]["operation"] == "scandir"


def test_secret_scan_skips_windows_reparse_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    junction = tmp_path / "junction"
    junction.mkdir()
    real_lstat = Path.lstat
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))

    def lstat(path: Path) -> os.stat_result:
        if path == junction:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR,
                st_file_attributes=reparse_flag,
            )
        return real_lstat(path)

    monkeypatch.setattr(Path, "lstat", lstat)
    report = runner._scan_secret((junction,), "not-present")
    assert report["scanned_file_count"] == 0
    assert report["errors"] == []
    assert report["skipped"] == [
        {
            "path": str(junction),
            "reason": "reparse_point",
        }
    ]
    assert report["passed"] is True


def _req(
    cpus: int | None, memory_mb: int | None
) -> dict[str, object]:
    known = cpus is not None and memory_mb is not None
    return {"known": known, "cpus": cpus, "memory_mb": memory_mb}


def test_work_conserving_admission_never_exceeds_capacity_or_workers() -> None:
    pending = [
        _req(4, 4096),
        _req(4, 4096),
        _req(4, 4096),
        _req(8, 8192),
    ]
    admitted = runner._select_work_conserving_admissions(
        pending,
        [],
        capacity_cpus=12,
        capacity_memory_mb=12288,
        max_workers=3,
    )
    assert admitted == [0, 1, 2]  # fourth exceeds capacity; slots then full

    admitted = runner._select_work_conserving_admissions(
        pending,
        [],
        capacity_cpus=12,
        capacity_memory_mb=12288,
        max_workers=2,
    )
    assert admitted == [0, 1]  # worker cap binds first


def test_work_conserving_admission_backfills_around_oversized_head() -> None:
    # Head does not fit alongside the running task, but a later smaller task
    # does: backfill admits it instead of idling (barrier waves could not).
    admitted = runner._select_work_conserving_admissions(
        [_req(8, 8192), _req(2, 2048)],
        [_req(8, 8192)],
        capacity_cpus=12,
        capacity_memory_mb=12288,
        max_workers=5,
    )
    assert admitted == [1]


def test_work_conserving_admission_exclusive_drains_and_never_shares() -> None:
    exclusive = _req(None, None)
    # Exclusive head waits while anything runs (drain mode, anti-starvation).
    assert (
        runner._select_work_conserving_admissions(
            [exclusive, _req(2, 2048)],
            [_req(2, 2048)],
            capacity_cpus=12,
            capacity_memory_mb=12288,
            max_workers=5,
        )
        == []
    )
    # Exclusive head admits only onto an empty system.
    assert runner._select_work_conserving_admissions(
        [exclusive, _req(2, 2048)],
        [],
        capacity_cpus=12,
        capacity_memory_mb=12288,
        max_workers=5,
    ) == [0]
    # While an exclusive runs, nothing else may be admitted.
    assert (
        runner._select_work_conserving_admissions(
            [_req(2, 2048)],
            [exclusive],
            capacity_cpus=12,
            capacity_memory_mb=12288,
            max_workers=5,
        )
        == []
    )
    # An exclusive that is not at the head is skipped by backfill, keeping
    # utilization up; it advances to the head as earlier tasks finish.
    assert runner._select_work_conserving_admissions(
        [_req(2, 2048), exclusive],
        [],
        capacity_cpus=12,
        capacity_memory_mb=12288,
        max_workers=5,
    ) == [0]


def test_work_conserving_batch_runs_all_tasks_within_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    import time

    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=[])
    tasks = [
        _resource_task("slow-heavy", 1, cpus=4, memory_mb=4096),
        _resource_task("fast-a", 2, cpus=4, memory_mb=4096),
        _resource_task("fast-b", 3, cpus=4, memory_mb=4096),
        _resource_task("exclusive", 4, cpus=None, memory_mb=None),
    ]

    lock = threading.Lock()
    running: dict[str, None] = {}
    peak_cpu = 0.0
    peak_during_exclusive = 0.0
    completed: list[str] = []

    def fake_pair(*, task: dict[str, object], **_: object) -> list[dict[str, object]]:
        nonlocal peak_cpu, peak_during_exclusive
        name = str(task["name"])
        cpus = float(task["cpus"] or 0)
        with lock:
            running[name] = None
            current = sum(
                float(t["cpus"] or 0) for t in tasks if str(t["name"]) in running
            )
            peak_cpu = max(peak_cpu, current)
            if "exclusive" in running:
                peak_during_exclusive = max(peak_during_exclusive, current)
        time.sleep(0.05)
        with lock:
            del running[name]
            completed.append(name)
        return []

    monkeypatch.setattr(runner, "_run_task_pair", fake_pair)
    runner._run_task_batch_work_conserving(
        tasks=tasks,
        max_workers=3,
        capacity={"cpus": 12, "memory_mb": 12288},
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )
    assert sorted(completed) == ["exclusive", "fast-a", "fast-b", "slow-heavy"]
    assert peak_cpu <= 12.0
    # The exclusive task ran alone.
    assert peak_during_exclusive == 0.0
    # Work conservation: the three known tasks overlapped instead of draining
    # a wave barrier (peak equals all three running together).
    assert peak_cpu == 12.0


def test_work_conserving_batch_survives_task_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    manifest = _minimal_manifest(output, task_names=[])
    tasks = [
        _resource_task("bad", 1, cpus=2, memory_mb=2048),
        _resource_task("good", 2, cpus=2, memory_mb=2048),
    ]
    completed: list[str] = []

    def fake_pair(*, task: dict[str, object], **_: object) -> list[dict[str, object]]:
        if str(task["name"]) == "bad":
            raise RuntimeError("simulated pair failure")
        completed.append(str(task["name"]))
        return []

    monkeypatch.setattr(runner, "_run_task_pair", fake_pair)
    runner._run_task_batch_work_conserving(
        tasks=tasks,
        max_workers=2,
        capacity={"cpus": 12, "memory_mb": 12288},
        manifest=manifest,
        output=output,
        jobs_dir=tmp_path / "jobs",
        credential_env="MISSING_FIXTURE_KEY",
    )
    assert completed == ["good"]
    failure_records = list((output / "runs" / "bad").glob("*.json"))
    assert failure_records, "failure must be persisted for the crashed pair"


def test_arm_never_started_not_misled_by_crash_record_empty_metrics(
    tmp_path: Path,
) -> None:
    """Regression (v3-confirm foldseek): a ran-and-crashed arm whose failure
    record carries no parsed metrics must not be misclassified as
    never-connected -- the retry resample would wipe its scored result."""
    job_root = tmp_path / "job"
    trial = job_root / "trial__abc"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {
            "stats": {
                "evals": {
                    "fixture": {
                        "reward_stats": {"reward": {"0.333": ["trial__abc"]}}
                    }
                }
            }
        },
    )
    runner._write_json(
        trial / "result.json",
        {
            "agent_result": {"n_input_tokens": 750000, "n_output_tokens": 5000},
            "verifier_result": {"reward": 0.333},
            "exception_info": {
                "exception_type": "NonZeroAgentExitCodeError",
                "exception_message": "agent exited 1 after doing real work",
            },
        },
    )
    record = runner._arm_failure_record(
        task_name="fixture",
        arm="lhos_resume",
        exc=RuntimeError("lhos_resume Harbor worker exited 1"),
    )
    assert record["metrics"].get("token_units", 0) == 0  # the trap input
    assert runner._arm_never_started(record, job_root) is False


def test_arm_never_started_still_retries_genuine_zero_event_abort(
    tmp_path: Path,
) -> None:
    """The genuine connection-abort case (exit 134 before any agent event)
    must keep its resample authorization."""
    job_root = tmp_path / "job"
    trial = job_root / "trial__abc"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        trial / "agent" / "dsh-observability.json",
        {"event_count": 0, "invocations": []},
    )
    record = runner._arm_failure_record(
        task_name="fixture",
        arm="lhos_resume",
        exc=RuntimeError("exit 134 before first request"),
    )
    assert runner._arm_never_started(record, job_root) is True


def test_arm_never_started_no_artifacts_keeps_retry_authorized(
    tmp_path: Path,
) -> None:
    """No observability/result artifacts at all -> no proof of real work ->
    the zero-metrics record still classifies as never-connected."""
    job_root = tmp_path / "job"
    job_root.mkdir()
    record = runner._arm_failure_record(
        task_name="fixture",
        arm="dsh_fresh",
        exc=RuntimeError("compose never came up"),
    )
    assert runner._arm_never_started(record, job_root) is True


def _write_heartbeat(job_root: Path, events: int, tokens: int) -> None:
    agent_dir = job_root / "trial__x" / "agent"
    agent_dir.mkdir(parents=True, exist_ok=True)
    runner._write_json(
        agent_dir / "dsh-heartbeat.json",
        {
            "kind": "dsh-heartbeat",
            "event_count": events,
            "usage": {"total_token_units": tokens},
        },
    )


def test_monitor_no_progress_rebaselines_on_generation_reset(
    tmp_path: Path,
) -> None:
    """Compacted restarts reset per-generation heartbeat counters; the monitor
    must treat the decrease as lifecycle, not as a stall."""
    import asyncio

    job_root = tmp_path / "job"
    _write_heartbeat(job_root, events=100, tokens=1000)

    # Note: the monitor clamps step/window to >= 1.0s, so the fastest possible
    # cadence is one 1s poll and a 1s window.
    async def run() -> None:
        monitor = asyncio.create_task(
            runner._monitor_lhos(
                job_root,
                enabled=True,
                token_budget_units=0,
                control_inert_token_units=0,
                no_progress_window_seconds=1.0,
                poll_seconds=1.0,
            )
        )
        await asyncio.sleep(1.3)
        # Generation reset: counters drop, agent keeps working.
        _write_heartbeat(job_root, events=5, tokens=50)
        await asyncio.sleep(1.2)
        assert not monitor.done(), "counter reset must not count as a stall"
        # Now the new generation genuinely stalls (flat counters).
        await asyncio.sleep(2.0)
        assert monitor.done(), "a genuine post-reset stall must still kill"
        assert str(monitor.result()).startswith("no_progress")

    asyncio.run(run())


def test_monitor_no_progress_still_kills_genuine_stall(tmp_path: Path) -> None:
    import asyncio

    job_root = tmp_path / "job"
    _write_heartbeat(job_root, events=100, tokens=1000)

    async def run() -> None:
        monitor = asyncio.create_task(
            runner._monitor_lhos(
                job_root,
                enabled=True,
                token_budget_units=0,
                control_inert_token_units=0,
                no_progress_window_seconds=1.0,
                poll_seconds=1.0,
            )
        )
        await asyncio.sleep(3.5)
        assert monitor.done()
        assert str(monitor.result()).startswith("no_progress")

    asyncio.run(run())


def test_trial_metrics_falls_back_to_observability_usage_on_crash(
    tmp_path: Path,
) -> None:
    """Crash-terminated trials lose agent_result (harbor records only
    exception_info); token accounting must fall back to the agent-written
    cumulative observability usage instead of reporting zeros."""
    job_root = tmp_path / "job"
    trial = job_root / "trial__crash"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {"stats": {"evals": {"fixture": {"reward_stats": {"reward": {"0.0": ["trial__crash"]}}}}}},
    )
    runner._write_json(
        trial / "result.json",
        {
            "exception_info": {
                "exception_type": "NonZeroAgentExitCodeError",
                "exception_message": "agent exited 1",
            },
            "verifier_result": {"reward": 0.0},
        },
    )
    runner._write_json(
        trial / "agent" / "dsh-observability.json",
        {
            "usage": {
                "total_token_units": 45_900_000,
                "uncached_input_tokens": 1_200_000,
                "cache_read_tokens": 44_500_000,
                "cache_write_tokens": 0,
                "output_tokens": 241_000,
                "model_calls": 612,
                "tool_calls": 700,
            }
        },
    )
    metrics = runner._trial_metrics(job_root, "lhos_resume")
    assert metrics["output_tokens"] == 241_000
    assert metrics["input_tokens"] == 1_200_000 + 44_500_000
    assert metrics["cache_tokens"] == 44_500_000
    assert metrics["token_units"] == 45_900_000


def test_trial_metrics_agent_result_wins_over_observability(
    tmp_path: Path,
) -> None:
    """When agent_result fields exist they stay authoritative; the
    observability fallback only fires on zero/missing sums."""
    job_root = tmp_path / "job"
    trial = job_root / "trial__ok"
    (trial / "agent").mkdir(parents=True)
    runner._write_json(
        job_root / "result.json",
        {"stats": {"evals": {"fixture": {"reward_stats": {"reward": {"0.5": ["trial__ok"]}}}}}},
    )
    runner._write_json(
        trial / "result.json",
        {
            "agent_result": {"n_input_tokens": 10, "n_cache_tokens": 2, "n_output_tokens": 3},
            "verifier_result": {"reward": 0.5},
        },
    )
    runner._write_json(
        trial / "agent" / "dsh-observability.json",
        {
            "usage": {
                "total_token_units": 13,
                "uncached_input_tokens": 999,
                "cache_read_tokens": 999,
                "output_tokens": 999,
                "model_calls": 1,
            }
        },
    )
    metrics = runner._trial_metrics(job_root, "lhos_resume")
    assert metrics["input_tokens"] == 10
    assert metrics["cache_tokens"] == 2
    assert metrics["output_tokens"] == 3


def test_monitor_token_budget_reads_cumulative_across_generations(
    tmp_path: Path,
) -> None:
    """The per-task token ceiling must compare the run total, not the current
    generation: heartbeat usage resets on every compacted restart, so a
    multi-generation run would never trip the ceiling on per-generation
    values alone."""
    import asyncio

    job_root = tmp_path / "job"
    agent_dir = job_root / "trial__x" / "agent"
    agent_dir.mkdir(parents=True)
    runner._write_json(
        agent_dir / "dsh-heartbeat.json",
        {
            "kind": "dsh-heartbeat",
            "event_count": 5,
            # Current generation is tiny...
            "usage": {"total_token_units": 50},
            # ...but the run total long crossed the ceiling.
            "usage_cumulative": {"total_token_units": 90_000_000},
        },
    )

    async def run() -> None:
        monitor = asyncio.create_task(
            runner._monitor_lhos(
                job_root,
                enabled=True,
                token_budget_units=80_000_000,
                control_inert_token_units=0,
                no_progress_window_seconds=3600,
                poll_seconds=1.0,
            )
        )
        await asyncio.sleep(3.0)
        assert monitor.done()
        assert str(monitor.result()).startswith("token_budget_exceeded:90000000")

    asyncio.run(run())


def test_monitor_token_budget_ignores_legacy_heartbeat_below_ceiling(
    tmp_path: Path,
) -> None:
    """A legacy heartbeat without usage_cumulative keeps the old
    per-generation comparison (no regression for single-generation runs)."""
    import asyncio

    job_root = tmp_path / "job"
    agent_dir = job_root / "trial__x" / "agent"
    agent_dir.mkdir(parents=True)
    runner._write_json(
        agent_dir / "dsh-heartbeat.json",
        {
            "kind": "dsh-heartbeat",
            "event_count": 5,
            "usage": {"total_token_units": 50},
        },
    )

    async def run() -> None:
        monitor = asyncio.create_task(
            runner._monitor_lhos(
                job_root,
                enabled=True,
                token_budget_units=80_000_000,
                control_inert_token_units=0,
                no_progress_window_seconds=3600,
                poll_seconds=1.0,
            )
        )
        await asyncio.sleep(2.5)
        assert not monitor.done()
        monitor.cancel()

    asyncio.run(run())


def test_sweep_orphaned_trial_containers_reaps_only_live_orphans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jobs_dir = tmp_path / "jobs"
    trial = jobs_dir / "lhtb-dsh-foo-lhos-resume" / "foo__AbC123"
    trial.mkdir(parents=True)
    # A second trial whose containers are already gone, plus the archive dir
    # that must never be swept.
    (jobs_dir / "lhtb-dsh-bar-lhos-resume" / "bar__Xy9").mkdir(parents=True)
    (jobs_dir / "_lhos-controller-attempts" / "foo" / "lhos_resume" / "attempt-1").mkdir(
        parents=True
    )

    live_project = runner._compose_project_name("foo__AbC123")
    cleaned: list[str] = []

    def fake_cleanup(project: str) -> dict[str, object]:
        cleaned.append(project)
        return {"removed": {"containers": ["cid"], "networks": [], "volumes": []}}

    monkeypatch.setattr(runner, "_cleanup_compose_project", fake_cleanup)
    swept = runner._sweep_orphaned_trial_containers(
        jobs_dir, live_projects={live_project}
    )
    assert cleaned == [live_project]
    assert swept == [live_project]

    # Nothing live -> nothing swept (no docker calls beyond the listing).
    cleaned.clear()
    swept = runner._sweep_orphaned_trial_containers(jobs_dir, live_projects=set())
    assert cleaned == []
    assert swept == []


def test_sweep_orphaned_trial_containers_no_jobs_dir(tmp_path: Path) -> None:
    assert (
        runner._sweep_orphaned_trial_containers(
            tmp_path / "absent", live_projects={"anything"}
        )
        == []
    )
