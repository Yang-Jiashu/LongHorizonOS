"""Paired real-coding experiment: DeepSeek Harness static vs LongHorizonOS."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
)
from lhos.provenance import ExecutionContext
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.errors import ExecutionError

from .case import (
    AFFECTED_V2,
    PRESERVED_V2,
    TASK_ORDER,
    TASK_SPECS,
    apply_v2_mutation,
    artifact_content,
    benchmark_root,
    content_hashes,
    create_workspace,
    protected_files,
    task_prompt,
)

_PROVIDER_BASE_URLS = {
    "DEEPSEEK_API_KEY": ("DEEPSEEK_BASE_URL", "https://token.sensenova.cn/v1"),
    "STEPFUN_API_KEY": ("STEPFUN_BASE_URL", "https://api.stepfun.com/step_plan/v1"),
}
_MUTATED_INPUT_CONSUMERS: dict[str, tuple[str, ...]] = {
    "requirements/pricing_contract.json": ("pricing_core", "pricing_api"),
    "tests_public/test_core.py": ("pricing_core",),
    "tests_public/test_api.py": ("pricing_api",),
    "tests_public/test_integration.py": ("integration",),
}


class BenchmarkRunError(RuntimeError):
    """A benchmark arm failed correctness or execution."""


@dataclass(frozen=True, slots=True)
class DshConfig:
    node: Path
    dsh: Path
    patch: Path
    timeout_seconds: float
    max_concurrency: int
    max_attempts: int
    credential_env: str = "DEEPSEEK_API_KEY"
    model: str = "deepseek-v4-flash"
    reasoning: str = "low"
    provider_route: str = "sensenova/openai-completions"


@dataclass(frozen=True, slots=True)
class SharedV1Snapshot:
    snapshot_id: str
    path: Path
    manifest: tuple[dict[str, Any], ...]
    generation: dict[str, Any]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _benchmark_contract_hash() -> str:
    digest = hashlib.sha256()
    root = benchmark_root()
    for path in sorted(
        item
        for item in root.rglob("*")
        if item.is_file()
        and ".pytest_cache" not in item.parts
        and "__pycache__" not in item.parts
        and item.suffix not in {".pyc", ".pyo"}
    ):
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _is_generated_path(path: Path) -> bool:
    return (
        ".pytest_cache" in path.parts
        or "__pycache__" in path.parts
        or path.suffix in {".pyc", ".pyo"}
    )


def _workspace_manifest(workspace: Path) -> tuple[dict[str, Any], ...]:
    manifest: list[dict[str, Any]] = []
    for path in sorted(item for item in workspace.rglob("*") if item.is_file()):
        relative = path.relative_to(workspace)
        if _is_generated_path(relative):
            continue
        payload = path.read_bytes()
        manifest.append(
            {
                "path": relative.as_posix(),
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return tuple(manifest)


def _manifest_id(manifest: tuple[dict[str, Any], ...]) -> str:
    payload = json.dumps(
        manifest,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clone_snapshot(snapshot: SharedV1Snapshot, destination: Path) -> str:
    shutil.copytree(
        snapshot.path,
        destination,
        ignore=shutil.ignore_patterns(
            ".pytest_cache",
            "__pycache__",
            "*.pyc",
            "*.pyo",
        ),
    )
    cloned = _workspace_manifest(destination)
    cloned_id = _manifest_id(cloned)
    if cloned != snapshot.manifest or cloned_id != snapshot.snapshot_id:
        raise BenchmarkRunError("shared v1 snapshot clone failed manifest verification")
    return cloned_id


def _child_env(workspace: Path) -> dict[str, str]:
    env = dict(os.environ)
    python_path = str(_repo_root() / "src")
    current = env.get("PYTHONPATH", "").strip()
    env["PYTHONPATH"] = python_path if not current else os.pathsep.join((python_path, current))
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["LHOS_BENCHMARK_WORKSPACE"] = str(workspace)
    return env


def _credential_values(credential_env: str) -> tuple[str, ...]:
    """Read the selected key pool without passing credentials on argv."""

    raw = os.environ.get("LHOS_DSH_API_KEYS", "").strip()
    if not raw:
        raw = os.environ.get(credential_env, "").strip()
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    if not values:
        raise RuntimeError(f"LHOS_DSH_API_KEYS or {credential_env} is required")
    return values


def _provider_name(provider_route: str) -> str:
    return str(provider_route).split("/", 1)[0].strip()


def _key_slot_for_attempt(
    task_id: str,
    version: int,
    attempt_number: int,
    *,
    key_count: int,
    max_attempts: int,
) -> int:
    if key_count < 1:
        raise ValueError("key_count must be >= 1")
    task_index = TASK_ORDER.index(str(task_id))
    ordinal = (
        (max(1, int(version)) - 1) * len(TASK_ORDER) * max_attempts
        + task_index * max_attempts
        + max(0, int(attempt_number) - 1)
    )
    return ordinal % key_count


def _run_pytest(workspace: Path, targets: Iterable[Path | str]) -> dict[str, Any]:
    env = _child_env(workspace)
    workspace_src = str((workspace / "src").resolve())
    env["PYTHONPATH"] = os.pathsep.join((workspace_src, env["PYTHONPATH"]))
    command = [sys.executable, "-m", "pytest", "-q", *(str(item) for item in targets)]
    started = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=workspace,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=180,
        check=False,
    )
    return {
        "passed": completed.returncode == 0,
        "exit_code": int(completed.returncode),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "stdout_tail": (completed.stdout or "")[-4000:],
        "stderr_tail": (completed.stderr or "")[-4000:],
    }


def _target_verification(
    workspace: Path,
    task_id: str,
    expected_hashes: dict[str, str],
) -> dict[str, Any]:
    test_result = _run_pytest(workspace, (TASK_SPECS[task_id].public_test,))
    current_hashes = content_hashes(protected_files(workspace))
    protected_ok = current_hashes == expected_hashes
    return {
        **test_result,
        "passed": bool(test_result["passed"] and protected_ok),
        "protected_inputs_unchanged": protected_ok,
    }


def _normalize_adapter_attempt(record: dict[str, Any], path: Path) -> dict[str, Any]:
    trace = record.get("trace", {})
    trace = trace if isinstance(trace, dict) else {}
    raw_usage = trace.get("usage", {})
    raw_usage = raw_usage if isinstance(raw_usage, dict) else {}

    def counter(name: str) -> int:
        return max(0, int(raw_usage.get(name, 0) or 0))

    input_token_units = (
        counter("uncached_input_tokens")
        + counter("cache_read_tokens")
        + counter("cache_write_tokens")
    )
    usage = {
        "uncached_input_tokens": counter("uncached_input_tokens"),
        "output_tokens": counter("output_tokens"),
        "cache_read_tokens": counter("cache_read_tokens"),
        "cache_write_tokens": counter("cache_write_tokens"),
        "reasoning_tokens": counter("reasoning_tokens"),
        "input_token_units": input_token_units,
        "total_token_units": (
            input_token_units + counter("output_tokens") + counter("verification_tokens")
        ),
        "model_calls": counter("model_calls"),
        "tool_calls": counter("tool_calls"),
        "tool_names": [
            str(item.get("name", ""))
            for item in trace.get("tool_calls", ())
            if isinstance(item, dict)
        ],
        "turn_end_reasons": list(trace.get("turn_end_reasons", ()) or ()),
        "session_files": list(trace.get("session_files", ()) or ()),
        "session_event_count": max(0, int(trace.get("event_count", 0) or 0)),
    }
    binding = record.get("binding", {})
    binding = binding if isinstance(binding, dict) else {}
    process = record.get("process", {})
    process = process if isinstance(process, dict) else {}
    failure = record.get("failure")
    failure_summary = (
        str(failure.get("summary", "") or "") if isinstance(failure, dict) else str(failure or "")
    )
    exit_code = process.get("exit_code")
    if exit_code is None:
        exit_code = 0 if record.get("completed") is True else 1
    return {
        "schema_version": "dsh-dynamic-attempt.v1",
        "adapter_schema_version": str(record.get("schema_version", "")),
        "task_id": str(record.get("phase_id", "")),
        "requirement_version": int(record.get("phase_version", 1) or 1),
        "attempt_id": str(binding.get("attempt_id", "") or record.get("attempt_record_id", "")),
        "adapter_attempt_number": int(record.get("attempt_number", 1) or 1),
        "exit_code": int(exit_code),
        "elapsed_ms": float(process.get("elapsed_ms", 0.0) or 0.0),
        "usage": usage,
        "dsh_home": str(record.get("dsh_home", "")),
        "stdout_tail": str(record.get("stdout_tail", "")),
        "stderr_tail": str(record.get("stderr_tail", "")),
        "failure": failure_summary,
        "prompt_sha256": str(record.get("prompt_sha256", "")),
        "record_path": str(path),
    }


def _load_attempt_records(run_root: Path) -> list[dict[str, Any]]:
    records: dict[tuple[str, int, str, int], tuple[int, dict[str, Any]]] = {}
    for path in sorted((run_root / "attempts").glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema_version") == "deepseek-harness-attempt.v1":
            record = _normalize_adapter_attempt(raw, path)
            priority = 0
        elif raw.get("schema_version") == "dsh-dynamic-attempt.v1":
            record = dict(raw)
            record["record_path"] = str(path)
            record.setdefault("adapter_attempt_number", 1)
            priority = 1
        else:
            continue
        key = (
            str(record["task_id"]),
            int(record["requirement_version"]),
            str(record["attempt_id"]),
            int(record["adapter_attempt_number"]),
        )
        current = records.get(key)
        if current is None or priority > current[0]:
            records[key] = (priority, record)
    return [
        item[1]
        for item in sorted(
            records.values(),
            key=lambda item: (
                int(item[1]["requirement_version"]),
                str(item[1]["task_id"]),
                str(item[1]["attempt_id"]),
                int(item[1]["adapter_attempt_number"]),
            ),
        )
    ]


def _usage_summary(records: list[dict[str, Any]], *, version: int | None = None) -> dict[str, Any]:
    selected = [
        record
        for record in records
        if version is None or int(record["requirement_version"]) == version
    ]
    fields = (
        "uncached_input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "reasoning_tokens",
        "input_token_units",
        "total_token_units",
        "model_calls",
        "tool_calls",
    )
    totals = {field: 0 for field in fields}
    for record in selected:
        usage = record.get("usage", {})
        for field in fields:
            totals[field] += int(usage.get(field, 0) or 0)
    totals["attempts"] = len(selected)
    totals["wall_time_ms_sum"] = round(
        sum(float(record.get("elapsed_ms", 0.0)) for record in selected),
        3,
    )
    totals["tasks"] = [str(record["task_id"]) for record in selected]
    return totals


def _grade_final(workspace: Path, expected_hashes: dict[str, str]) -> dict[str, Any]:
    public_result = _run_pytest(workspace, ("tests_public",))
    hidden_result = _run_pytest(
        workspace,
        (benchmark_root() / "tests_hidden" / "test_final_v2.py",),
    )
    current_hashes = content_hashes(protected_files(workspace))
    protected_ok = current_hashes == expected_hashes
    return {
        "passed": bool(public_result["passed"] and hidden_result["passed"] and protected_ok),
        "public": public_result,
        "hidden": hidden_result,
        "protected_inputs_unchanged": protected_ok,
        "protected_hashes": current_hashes,
    }


def _run_static_task(
    *,
    workspace: Path,
    run_root: Path,
    task_id: str,
    version: int,
    expected_hashes: dict[str, str],
    config: DshConfig,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    credentials = _credential_values(config.credential_env)
    base_url_env, default_base_url = _PROVIDER_BASE_URLS.get(
        config.credential_env,
        ("LHOS_DSH_BASE_URL", "https://token.sensenova.cn/v1"),
    )
    base_url = (
        os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
    )
    for attempt_number in range(1, config.max_attempts + 1):
        key_slot = _key_slot_for_attempt(
            task_id,
            version,
            attempt_number,
            key_count=len(credentials),
            max_attempts=config.max_attempts,
        )
        spec = TASK_SPECS[task_id]
        phase = DeepSeekHarnessPhase(
            phase_id=task_id,
            prompt=task_prompt(task_id, version),
            version=version,
            inputs=spec.inputs,
            outputs=spec.target_files,
            artifact_id=spec.artifact_id,
            max_attempts=1,
        )
        adapter = DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=config.node,
                dsh=config.dsh,
                patch=config.patch,
                provider=_provider_name(config.provider_route),
                model=config.model,
                reasoning_effort=config.reasoning,
                credential_env=config.credential_env,
                base_url=base_url,
                base_url_env=base_url_env,
                credential_values=(credentials[key_slot],),
                profile="headless",
                timeout_seconds=config.timeout_seconds,
                retry=DeepSeekRetryPolicy(max_attempts=1),
                dsh_home_root=run_root / "dsh-sessions",
            ),
            workspace=workspace,
            run_root=run_root,
            phases=(phase,),
            extra_env=_child_env(workspace),
        )
        attempt_id = uuid.uuid4().hex
        context = ExecutionContext(
            "dsh-static-restart",
            task_id=task_id,
            attempt_id=attempt_id,
            semantic_epoch=max(0, version - 1),
            source="deepseek-harness-adapter",
        )
        context.graph_version = version
        context.agent_id = "dsh-static"
        context.process_id = f"static:{os.getpid()}"
        context.claim_id = f"static:{task_id}:v{version}:{attempt_id}"
        started = time.monotonic()
        failure = ""
        try:
            record = asyncio.run(adapter.execute(context, task_id))
            exit_code = int(record.process.get("exit_code") or 0)
        except ExecutionError as exc:
            record = adapter.latest_record(task_id)
            exit_code = 1
            failure = str(exc)
        except Exception as exc:
            record = adapter.latest_record(task_id)
            exit_code = 125
            failure = f"{type(exc).__name__}: {exc}"
        verification = _target_verification(workspace, task_id, expected_hashes)
        attempt = {
            "attempt_number": attempt_number,
            "exit_code": exit_code,
            "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
            "verification": verification,
            "stdout_tail": "" if record is None else record.stdout_tail[-2000:],
            "stderr_tail": "" if record is None else record.stderr_tail[-2000:],
            "failure": failure,
            "adapter_attempt_record_id": ("" if record is None else record.attempt_record_id),
            "key_slot": key_slot,
        }
        attempts.append(attempt)
        controller_dir = run_root / "controller_attempts"
        controller_dir.mkdir(parents=True, exist_ok=True)
        (controller_dir / f"{task_id}-v{version}-a{attempt_number}.json").write_text(
            json.dumps(attempt, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        if exit_code == 0 and verification["passed"]:
            return {"task_id": task_id, "passed": True, "attempts": attempts}
    return {"task_id": task_id, "passed": False, "attempts": attempts}


def _run_static_phase(
    *,
    workspace: Path,
    run_root: Path,
    version: int,
    task_ids: tuple[str, ...],
    expected_hashes: dict[str, str],
    config: DshConfig,
) -> list[dict[str, Any]]:
    pending = set(task_ids)
    completed: set[str] = set()
    outcomes: list[dict[str, Any]] = []
    while pending:
        ready = [
            task_id
            for task_id in TASK_ORDER
            if task_id in pending
            and all(
                dependency in completed or dependency not in pending
                for dependency in TASK_SPECS[task_id].dependencies
            )
        ]
        if not ready:
            raise BenchmarkRunError(f"static controller deadlocked with pending={sorted(pending)}")
        with ThreadPoolExecutor(max_workers=config.max_concurrency) as pool:
            futures = {
                pool.submit(
                    _run_static_task,
                    workspace=workspace,
                    run_root=run_root,
                    task_id=task_id,
                    version=version,
                    expected_hashes=expected_hashes,
                    config=config,
                ): task_id
                for task_id in ready
            }
            for future in as_completed(futures):
                outcome = future.result()
                outcomes.append(outcome)
                if not outcome["passed"]:
                    last = outcome["attempts"][-1]
                    raise BenchmarkRunError(
                        f"static task {outcome['task_id']!r} failed after "
                        f"{len(outcome['attempts'])} attempts; "
                        f"worker_exit={last['exit_code']} "
                        f"verification_passed={last['verification']['passed']}"
                    )
                task_id = str(outcome["task_id"])
                pending.remove(task_id)
                completed.add(task_id)
    return outcomes


def _build_shared_v1_snapshot(
    pair_root: Path,
    config: DshConfig,
) -> SharedV1Snapshot:
    run_root = pair_root / "shared-v1-generation"
    workspace = run_root / "workspace"
    run_root.mkdir(parents=True, exist_ok=False)
    create_workspace(workspace)
    template_manifest = _workspace_manifest(workspace)
    expected_hashes = content_hashes(protected_files(workspace))
    started = time.monotonic()
    outcomes = _run_static_phase(
        workspace=workspace,
        run_root=run_root,
        version=1,
        task_ids=TASK_ORDER,
        expected_hashes=expected_hashes,
        config=config,
    )
    public_grade = _run_pytest(workspace, ("tests_public",))
    protected_ok = content_hashes(protected_files(workspace)) == expected_hashes
    if not public_grade["passed"] or not protected_ok:
        raise BenchmarkRunError("shared v1 generation did not pass public verification")

    generated_manifest = _workspace_manifest(workspace)
    before_by_path = {item["path"]: item["sha256"] for item in template_manifest}
    after_by_path = {item["path"]: item["sha256"] for item in generated_manifest}
    changed = {
        path
        for path in set(before_by_path) | set(after_by_path)
        if before_by_path.get(path) != after_by_path.get(path)
    }
    allowed_targets = {path for spec in TASK_SPECS.values() for path in spec.target_files}
    unexpected = sorted(changed - allowed_targets)
    if unexpected:
        raise BenchmarkRunError(
            "shared v1 generation modified files outside declared task outputs: "
            + ", ".join(unexpected)
        )

    snapshot_path = pair_root / "shared-v1-snapshot"
    shutil.copytree(
        workspace,
        snapshot_path,
        ignore=shutil.ignore_patterns(
            ".pytest_cache",
            "__pycache__",
            "*.pyc",
            "*.pyo",
        ),
    )
    snapshot_manifest = _workspace_manifest(snapshot_path)
    snapshot_id = _manifest_id(snapshot_manifest)
    if snapshot_manifest != generated_manifest:
        raise BenchmarkRunError("shared v1 snapshot copy changed the semantic manifest")
    records = _load_attempt_records(run_root)
    generation = {
        "valid": True,
        "workspace": str(workspace),
        "snapshot": str(snapshot_path),
        "snapshot_id": snapshot_id,
        "wall_ms": round((time.monotonic() - started) * 1000, 3),
        "task_outcomes": outcomes,
        "usage": _usage_summary(records, version=1),
        "public_grade": public_grade,
        "protected_inputs_unchanged": protected_ok,
        "changed_files": sorted(changed),
    }
    (run_root / "summary.json").write_text(
        json.dumps(generation, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (pair_root / "shared-v1-manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "lhos-shared-v1-snapshot.v1",
                "snapshot_id": snapshot_id,
                "files": snapshot_manifest,
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return SharedV1Snapshot(
        snapshot_id=snapshot_id,
        path=snapshot_path,
        manifest=snapshot_manifest,
        generation=generation,
    )


def _prepare_arm(
    root: Path,
    arm: str,
    *,
    snapshot: SharedV1Snapshot,
    directory_name: str,
) -> tuple[Path, Path, str]:
    run_root = root / (directory_name or arm)
    workspace = run_root / "workspace"
    run_root.mkdir(parents=True, exist_ok=False)
    clone_id = _clone_snapshot(snapshot, workspace)
    return run_root, workspace, clone_id


def _run_static_arm(
    root: Path,
    config: DshConfig,
    snapshot: SharedV1Snapshot,
) -> dict[str, Any]:
    run_root, workspace, clone_id = _prepare_arm(
        root,
        "dsh_static_restart",
        snapshot=snapshot,
        directory_name="a",
    )
    started = time.monotonic()
    mutation_started = time.monotonic()
    apply_v2_mutation(workspace)
    pre_repair_manifest = _workspace_manifest(workspace)
    pre_repair_snapshot_id = _manifest_id(pre_repair_manifest)
    v2_hashes = content_hashes(protected_files(workspace))
    repair_outcomes = _run_static_phase(
        workspace=workspace,
        run_root=run_root,
        version=2,
        task_ids=TASK_ORDER,
        expected_hashes=v2_hashes,
        config=config,
    )
    final_grade = _grade_final(workspace, v2_hashes)
    ended = time.monotonic()
    records = _load_attempt_records(run_root)
    result = {
        "arm": "dsh_static_restart",
        "valid": bool(final_grade["passed"]),
        "workspace": str(workspace),
        "total_wall_ms": round((ended - started) * 1000, 3),
        "repair_wall_ms": round((ended - mutation_started) * 1000, 3),
        "shared_snapshot_id": clone_id,
        "pre_repair_snapshot_id": pre_repair_snapshot_id,
        "initial_task_outcomes": [],
        "repair_task_outcomes": repair_outcomes,
        "repair_tasks_executed": [str(item["task_id"]) for item in repair_outcomes],
        "initial_usage": _usage_summary([], version=1),
        "repair_usage": _usage_summary(records, version=2),
        "total_usage": _usage_summary(records),
        "final_grade": final_grade,
    }
    (run_root / "summary.json").write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result


async def _run_lhos_arm_async(
    root: Path,
    config: DshConfig,
    snapshot: SharedV1Snapshot,
) -> dict[str, Any]:
    # Keep provider-facing workspace/session paths neutral. Some hosted
    # providers apply content moderation to tool-visible machine paths; the
    # controller label remains in the result, while the execution directory
    # is deliberately not named after the controller.
    run_root, workspace, clone_id = _prepare_arm(
        root,
        "dsh_lhos",
        snapshot=snapshot,
        directory_name="b",
    )
    state = {
        "version": 1,
        "mode": "snapshot_adoption",
        "protected_hashes": content_hashes(protected_files(workspace)),
    }
    measured_usage: list[dict[str, Any]] = []
    credential_values = _credential_values(config.credential_env)
    invocation_counts: dict[tuple[str, int], int] = {}
    invocation_lock = Lock()
    base_url_env, default_base_url = _PROVIDER_BASE_URLS.get(
        config.credential_env,
        ("LHOS_DSH_BASE_URL", "https://token.sensenova.cn/v1"),
    )
    base_url = (
        os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
    )

    async def executor(context: Any, task_id: str) -> Any:
        """Run DSH directly under the real SDK ExecutionContext.

        This is the important boundary: AgentOS supplies the live claim,
        graph/epoch identity and cancellation token, while the adapter owns
        the Node DSH process and durable session trace.  No Python worker and
        no second JSONL parser sit between the scheduler and DSH.
        """

        if state["mode"] == "snapshot_adoption":
            current_manifest = _workspace_manifest(workspace)
            if (
                current_manifest != snapshot.manifest
                or _manifest_id(current_manifest) != snapshot.snapshot_id
            ):
                raise BenchmarkRunError("LHOS snapshot adoption observed a mutated shared v1 clone")
            return None

        task_version = 1 if task_id == "audit" else int(state["version"])
        invocation_key = (str(task_id), task_version)
        with invocation_lock:
            invocation_number = invocation_counts.get(invocation_key, 0) + 1
            invocation_counts[invocation_key] = invocation_number
        key_slot = _key_slot_for_attempt(
            str(task_id),
            task_version,
            invocation_number,
            key_count=len(credential_values),
            max_attempts=config.max_attempts,
        )
        spec = TASK_SPECS[str(task_id)]
        phase = DeepSeekHarnessPhase(
            phase_id=str(task_id),
            prompt=task_prompt(str(task_id), task_version),
            version=task_version,
            inputs=spec.inputs,
            outputs=spec.target_files,
            artifact_id=spec.artifact_id,
            max_attempts=1,
        )
        adapter = DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=config.node,
                dsh=config.dsh,
                patch=config.patch,
                provider=_provider_name(config.provider_route),
                model=config.model,
                reasoning_effort=config.reasoning,
                credential_env=config.credential_env,
                base_url=base_url,
                base_url_env=base_url_env,
                credential_values=(credential_values[key_slot],),
                profile="headless",
                timeout_seconds=config.timeout_seconds,
                retry=DeepSeekRetryPolicy(max_attempts=1),
                dsh_home_root=run_root / "dsh-sessions",
            ),
            workspace=workspace,
            run_root=run_root,
            phases=(phase,),
            extra_env=_child_env(workspace),
        )
        record = await adapter.execute(context, str(task_id))
        measured_usage.append(
            {
                "task_id": str(task_id),
                "attempt_id": record.binding.attempt_id,
                **record.trace.usage.model_dump(mode="json"),
                "elapsed_ms": record.process.get("elapsed_ms", 0),
                "terminated_by": record.process.get("terminated_by"),
                "key_slot": key_slot,
            }
        )
        return record

    runtime = AgentOS(":memory:")
    runtime.add_agent(
        Agent(
            "dsh",
            executor=executor,
            executor_api="context_v1",
            specializations=("python",),
            max_concurrency=config.max_concurrency,
        )
    )
    goal = Goal("real-dsh-dynamic-coding")
    tasks: dict[str, Any] = {}

    def verifier_for(task_id: str):
        def verify(_context: Any, dispatched_task_id: str) -> VerificationOutcome:
            if dispatched_task_id != task_id:
                raise BenchmarkRunError(f"verifier for {task_id!r} received {dispatched_task_id!r}")
            verification = _target_verification(
                workspace,
                task_id,
                dict(state["protected_hashes"]),
            )
            version = 1 if task_id == "audit" else int(state["version"])
            return VerificationOutcome(
                passed=bool(verification["passed"]),
                artifact_id=TASK_SPECS[task_id].artifact_id,
                version=version,
                content=artifact_content(workspace, task_id),
                evidence_note=(
                    f"pytest:{TASK_SPECS[task_id].public_test}:"
                    f"v{version}:exit={verification['exit_code']}"
                ),
                details=verification,
            )

        return verify

    for task_id in TASK_ORDER:
        spec = TASK_SPECS[task_id]
        dependencies = tuple(tasks[item] for item in spec.dependencies)
        tasks[task_id] = goal.task(
            task_id,
            agent="dsh",
            depends_on=dependencies,
            verify=verifier_for(task_id),
            required_specializations=("python",),
            max_attempts=config.max_attempts,
            executor_api="context_v1",
            inputs=spec.inputs,
            outputs=spec.target_files,
        )

    started = time.monotonic()
    try:
        goal.compile(runtime)
        previous_observations = {
            resource: runtime.observe_artifact(
                goal,
                resource,
                1,
                (workspace / resource).read_bytes(),
            )
            for resource in _MUTATED_INPUT_CONSUMERS
        }
        adoption_started = time.monotonic()
        initial = await runtime.run_async(
            goal,
            max_dispatches=16,
            max_steps=16,
            max_concurrency=config.max_concurrency,
            adaptive=True,
            max_parallelism=config.max_concurrency,
        )
        if initial.goal_state != "closed":
            raise BenchmarkRunError(
                f"LongHorizonOS initial Goal did not close: {initial.goal_state}"
            )
        adoption_wall_ms = round((time.monotonic() - adoption_started) * 1000, 3)
        if _workspace_manifest(workspace) != snapshot.manifest:
            raise BenchmarkRunError("LHOS snapshot adoption changed workspace bytes")
        adoption_records = _load_attempt_records(run_root)
        if adoption_records:
            raise BenchmarkRunError("LHOS snapshot adoption unexpectedly invoked DSH")
        mutation_started = time.monotonic()
        apply_v2_mutation(workspace)
        pre_repair_manifest = _workspace_manifest(workspace)
        pre_repair_snapshot_id = _manifest_id(pre_repair_manifest)
        state["version"] = 2
        state["mode"] = "repair"
        state["protected_hashes"] = content_hashes(protected_files(workspace))
        current_observations = {
            resource: runtime.observe_artifact(
                goal,
                resource,
                2,
                (workspace / resource).read_bytes(),
            )
            for resource in _MUTATED_INPUT_CONSUMERS
        }
        repair = runtime.reconcile_observations(
            goal,
            tuple(
                {
                    "previous_observation": previous_observations[resource],
                    "observation": current_observations[resource],
                    "affected_task_ids": consumers,
                    "resource_uri": f"workspace://{resource}",
                }
                for resource, consumers in _MUTATED_INPUT_CONSUMERS.items()
            ),
        )
        final = await runtime.run_async(
            goal,
            max_dispatches=16,
            max_steps=16,
            max_concurrency=config.max_concurrency,
            adaptive=True,
            max_parallelism=config.max_concurrency,
        )
        final_grade = _grade_final(workspace, dict(state["protected_hashes"]))
        ended = time.monotonic()
        records = _load_attempt_records(run_root)
        result = {
            "arm": "dsh_lhos",
            "valid": bool(final.goal_state == "closed" and final_grade["passed"]),
            "workspace": str(workspace),
            "total_wall_ms": round((ended - started) * 1000, 3),
            "repair_wall_ms": round((ended - mutation_started) * 1000, 3),
            "snapshot_adoption_wall_ms": adoption_wall_ms,
            "snapshot_adoption_model_calls": 0,
            "shared_snapshot_id": clone_id,
            "pre_repair_snapshot_id": pre_repair_snapshot_id,
            "initial_goal_state": initial.goal_state,
            "final_goal_state": final.goal_state,
            "repair": {
                "affected": sorted(repair.affected),
                "preserved": sorted(repair.preserved),
                "frontier": sorted(repair.frontier),
                "oracle_affected": list(AFFECTED_V2),
                "oracle_preserved": list(PRESERVED_V2),
                "under_invalidation": sorted(set(AFFECTED_V2) - set(repair.affected)),
                "over_invalidation": sorted(set(repair.affected) - set(AFFECTED_V2)),
            },
            "repair_tasks_executed": [
                task_id
                for task_id in TASK_ORDER
                if any(
                    str(record["task_id"]) == task_id and int(record["requirement_version"]) == 2
                    for record in records
                )
            ],
            "initial_usage": _usage_summary([], version=1),
            "repair_usage": _usage_summary(records, version=2),
            "total_usage": _usage_summary(records),
            "subprocess_usage": measured_usage,
            "final_grade": final_grade,
        }
        (run_root / "summary.json").write_text(
            json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return result
    finally:
        runtime.close()


def _run_lhos_arm(
    root: Path,
    config: DshConfig,
    snapshot: SharedV1Snapshot,
) -> dict[str, Any]:
    return asyncio.run(_run_lhos_arm_async(root, config, snapshot))


def _comparison(static: dict[str, Any], lhos: dict[str, Any]) -> dict[str, Any]:
    static_usage = static.get("repair_usage", {})
    lhos_usage = lhos.get("repair_usage", {})
    static_tokens = int(static_usage.get("total_token_units", 0) or 0)
    lhos_tokens = int(lhos_usage.get("total_token_units", 0) or 0)
    static_calls = int(static_usage.get("model_calls", 0) or 0)
    lhos_calls = int(lhos_usage.get("model_calls", 0) or 0)
    static_wall = float(static.get("repair_wall_ms", 0.0) or 0.0)
    lhos_wall = float(lhos.get("repair_wall_ms", 0.0) or 0.0)
    return {
        "pair_valid": bool(static["valid"] and lhos["valid"]),
        "static_error": str(static.get("error", "")),
        "lhos_error": str(lhos.get("error", "")),
        "repair_token_saving_ratio": (
            None if static_tokens <= 0 else round(1.0 - (lhos_tokens / static_tokens), 6)
        ),
        "repair_model_call_saving_ratio": (
            None if static_calls <= 0 else round(1.0 - (lhos_calls / static_calls), 6)
        ),
        "repair_wall_speedup": (None if lhos_wall <= 0 else round(static_wall / lhos_wall, 6)),
        "static_repair_tokens": static_tokens,
        "lhos_repair_tokens": lhos_tokens,
        "static_repair_model_calls": static_calls,
        "lhos_repair_model_calls": lhos_calls,
        "static_repair_wall_ms": static_wall,
        "lhos_repair_wall_ms": lhos_wall,
        "static_repair_tasks": list(static.get("repair_tasks_executed", ())),
        "lhos_repair_tasks": list(lhos.get("repair_tasks_executed", ())),
    }


def _failed_arm(arm: str, exc: Exception) -> dict[str, Any]:
    return {
        "arm": arm,
        "valid": False,
        "error": f"{type(exc).__name__}: {exc}",
        "repair_wall_ms": 0.0,
        "repair_tasks_executed": [],
        "repair_usage": {
            "total_token_units": 0,
            "model_calls": 0,
            "tool_calls": 0,
            "attempts": 0,
        },
    }


def _experiment_result_payload(
    *,
    config: DshConfig,
    repeat: int,
    pairs: list[dict[str, Any]],
    pair_index_offset: int,
) -> dict[str, Any]:
    return {
        "benchmark": "real_dsh_dynamic_coding",
        "benchmark_version": 3,
        "benchmark_contract_sha256": _benchmark_contract_hash(),
        "model": config.model,
        "reasoning_effort": config.reasoning,
        "provider_route": config.provider_route,
        "credential_env": config.credential_env,
        "controller_difference_only": True,
        "independent_initial_trajectories": False,
        "shared_initial_snapshot": True,
        "repair_comparison_conditional_on_verified_shared_v1": True,
        "same_process_adapter": True,
        "same_task_prompts": True,
        "same_concurrency": config.max_concurrency,
        "same_retry_limit": config.max_attempts,
        "key_assignment_policy": "deterministic(task,version,attempt)",
        "repeat": repeat,
        "pair_index_offset": pair_index_offset,
        "completed_pairs": len(pairs),
        "pairs": pairs,
    }


def run_experiment(
    *,
    output_dir: Path,
    config: DshConfig,
    repeat: int = 1,
    pair_index_offset: int = 0,
) -> dict[str, Any]:
    if repeat < 1:
        raise ValueError("repeat must be >= 1")
    if pair_index_offset < 0:
        raise ValueError("pair_index_offset must be >= 0")
    output_dir.mkdir(parents=True, exist_ok=True)
    pairs: list[dict[str, Any]] = []
    for index in range(repeat):
        pair_number = pair_index_offset + index + 1
        pair_root = output_dir / f"pair-{pair_number:02d}-{uuid.uuid4().hex[:8]}"
        pair_root.mkdir(parents=True, exist_ok=False)
        snapshot = _build_shared_v1_snapshot(pair_root, config)
        order = (
            ("dsh_static_restart", "dsh_lhos")
            if (pair_number - 1) % 2 == 0
            else (
                "dsh_lhos",
                "dsh_static_restart",
            )
        )
        results: dict[str, dict[str, Any]] = {}
        for arm in order:
            try:
                if arm == "dsh_static_restart":
                    results[arm] = _run_static_arm(pair_root, config, snapshot)
                else:
                    results[arm] = _run_lhos_arm(pair_root, config, snapshot)
            except Exception as exc:
                results[arm] = _failed_arm(arm, exc)
        static = results["dsh_static_restart"]
        lhos = results["dsh_lhos"]
        pairs.append(
            {
                "pair": pair_number,
                "order": list(order),
                "static": static,
                "lhos": lhos,
                "comparison": _comparison(static, lhos),
                "shared_v1_generation": snapshot.generation,
                "shared_snapshot_id": snapshot.snapshot_id,
            }
        )
        checkpoint = _experiment_result_payload(
            config=config,
            repeat=repeat,
            pairs=pairs,
            pair_index_offset=pair_index_offset,
        )
        (output_dir / "result.json").write_text(
            json.dumps(checkpoint, ensure_ascii=True, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    result = _experiment_result_payload(
        config=config,
        repeat=repeat,
        pairs=pairs,
        pair_index_offset=pair_index_offset,
    )
    (output_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--dsh", type=Path, required=True)
    parser.add_argument(
        "--patch",
        type=Path,
        default=benchmark_root() / "sensenova-pi-ai.cordis.patch.yml",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_repo_root() / "artifacts" / "real-dsh-dynamic-coding-20260819",
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--pair-index-offset", type=int, default=0)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument("--credential-env", default="DEEPSEEK_API_KEY")
    parser.add_argument("--model", default="deepseek-v4-flash")
    parser.add_argument("--reasoning", default="low")
    parser.add_argument(
        "--provider-route",
        default="sensenova/openai-completions via DeepSeek Harness llm-pi-ai",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    config = DshConfig(
        node=args.node.resolve(),
        dsh=args.dsh.resolve(),
        patch=args.patch.resolve(),
        timeout_seconds=float(args.timeout_seconds),
        max_concurrency=int(args.max_concurrency),
        max_attempts=int(args.max_attempts),
        credential_env=str(args.credential_env),
        model=str(args.model),
        reasoning=str(args.reasoning),
        provider_route=str(args.provider_route),
    )
    for path, label in ((config.node, "node"), (config.dsh, "dsh"), (config.patch, "patch")):
        if not path.is_file():
            raise SystemExit(f"{label} path does not exist: {path}")
    if not (
        os.environ.get("LHOS_DSH_API_KEYS", "").strip()
        or os.environ.get(config.credential_env, "").strip()
    ):
        raise SystemExit(f"LHOS_DSH_API_KEYS or {config.credential_env} is required")
    result = run_experiment(
        output_dir=args.output_dir.resolve(),
        config=config,
        repeat=args.repeat,
        pair_index_offset=args.pair_index_offset,
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if all(pair["comparison"]["pair_valid"] for pair in result["pairs"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
