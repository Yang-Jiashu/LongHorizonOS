"""No-Docker SWE-bench host-native smoke runner.

This module intentionally does not claim official SWE-bench evaluator
equivalence. It uses the public task metadata, a local repository checkout,
the public test patch, and the public FAIL_TO_PASS/PASS_TO_PASS commands.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
    inspect_deepseek_patch,
    parse_deepseek_sessions,
)
from lhos.provenance import ExecutionContext
from lhos.sdk import Agent, AgentOS, Goal, VerificationOutcome
from lhos.sdk.errors import ExecutionError

DEFAULT_CREDENTIAL_ENV = "DEEPSEEK_API_KEY"
_CREDENTIAL_ENV_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PROVIDER_BASE_URLS = {
    "DEEPSEEK_API_KEY": (
        "DEEPSEEK_BASE_URL",
        "https://token.sensenova.cn/v1",
    ),
    "STEPFUN_API_KEY": (
        "STEPFUN_BASE_URL",
        "https://api.stepfun.com/step_plan/v1",
    ),
}


@dataclass(frozen=True, slots=True)
class HostNativeCase:
    instance_id: str
    repo: str
    base_commit: str
    problem_statement: str
    hints: str
    fail_to_pass: tuple[str, ...]
    pass_to_pass: tuple[str, ...]


DEFAULT_CASE = HostNativeCase(
    instance_id="pytest-dev__pytest-11143",
    repo="pytest-dev/pytest",
    base_commit="6995257cf470d2143ad1683824962de4071c0eb7",
    problem_statement=(
        "Rewrite fails when the first expression of a Python file is a number "
        "and it is mistaken for a module docstring. Do not pass a non-string AST "
        "constant to the docstring marker check."
    ),
    hints="",
    fail_to_pass=(
        "testing/test_assertrewrite.py::TestIssue11140::test_constant_not_picked_as_module_docstring",
    ),
    pass_to_pass=(
        "testing/test_assertrewrite.py::TestAssertionRewrite::test_place_initial_imports",
    ),
)


def _load_case(path: Path | None) -> HostNativeCase:
    if path is None:
        return DEFAULT_CASE
    raw = json.loads(path.read_text(encoding="utf-8"))
    return HostNativeCase(
        instance_id=str(raw["instance_id"]),
        repo=str(raw["repo"]),
        base_commit=str(raw["base_commit"]),
        problem_statement=str(raw["problem_statement"]),
        hints=str(raw.get("hints_text", "")),
        fail_to_pass=tuple(str(item) for item in raw["FAIL_TO_PASS"]),
        pass_to_pass=tuple(str(item) for item in raw["PASS_TO_PASS"]),
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_dsh_patch() -> Path:
    return (
        _repo_root() / "benchmarks" / "real_dsh_dynamic_coding" / "sensenova-pi-ai.cordis.patch.yml"
    )


def _credential_env_name(value: str | None = None) -> str:
    name = (value or os.environ.get("LHOS_DSH_CREDENTIAL_ENV") or DEFAULT_CREDENTIAL_ENV).strip()
    if not _CREDENTIAL_ENV_PATTERN.fullmatch(name):
        raise ValueError(f"invalid credential environment variable name: {name!r}")
    return name


def _keys(credential_env: str | None = None) -> list[str]:
    credential_env = _credential_env_name(credential_env)
    raw = (
        os.environ.get("LHOS_DSH_API_KEYS", "").strip()
        or os.environ.get(credential_env, "").strip()
    )
    return [item.strip() for item in raw.split(",") if item.strip()]


def _missing_credentials_message(credential_env: str | None = None) -> str:
    credential_env = _credential_env_name(credential_env)
    return f"LHOS_DSH_API_KEYS or {credential_env} is required"


def _safe_text(value: str, secrets: list[str], limit: int = 3000) -> str:
    text = value[-limit:]
    for secret in secrets:
        text = text.replace(secret, "[REDACTED]")
    return text.encode("ascii", errors="backslashreplace").decode("ascii")


def _usage_from_logs(session_root: Path) -> dict[str, Any]:
    """Compatibility view backed by the shared DeepSeek trace parser."""

    trace = parse_deepseek_sessions(session_root, workspace=session_root.parent.parent)
    usage = trace.usage
    return {
        "uncached_input_tokens": usage.uncached_input_tokens,
        "output_tokens": usage.output_tokens,
        "cache_read_tokens": usage.cache_read_tokens,
        "cache_write_tokens": usage.cache_write_tokens,
        "reasoning_tokens": usage.reasoning_tokens,
        "input_token_units": usage.input_token_units,
        "total_token_units": usage.total_token_units,
        "model_calls": usage.model_calls,
        "tool_calls": usage.tool_calls,
        "tool_names": [tool.name for tool in trace.tool_calls],
        "turn_end_reasons": list(trace.turn_end_reasons),
        "session_files": list(trace.session_files),
        "session_event_count": trace.event_count,
    }


def _attempt_result_view(
    record: Any,
    *,
    failure_override: str = "",
) -> dict[str, Any]:
    usage = record.usage
    failure = record.failure.summary if record.failure is not None else failure_override
    exit_code = record.process.get("exit_code")
    if exit_code is None:
        exit_code = 0 if record.completed else 1
    if record.failure is not None and int(exit_code) == 0:
        exit_code = 126 if record.failure.failure_class.value == "protocol_malformed" else 1
    return {
        "schema_version": "swe-dsh-attempt-view.v1",
        "adapter_schema_version": record.schema_version,
        "exit_code": int(exit_code),
        "elapsed_ms": float(record.process.get("elapsed_ms", 0.0) or 0.0),
        "usage": {
            "uncached_input_tokens": usage.uncached_input_tokens,
            "output_tokens": usage.output_tokens,
            "cache_read_tokens": usage.cache_read_tokens,
            "cache_write_tokens": usage.cache_write_tokens,
            "reasoning_tokens": usage.reasoning_tokens,
            "input_token_units": usage.input_token_units,
            "total_token_units": usage.total_token_units,
            "model_calls": usage.model_calls,
            "tool_calls": usage.tool_calls,
            "tool_names": [tool.name for tool in record.trace.tool_calls],
            "turn_end_reasons": list(record.trace.turn_end_reasons),
            "session_files": list(record.trace.session_files),
            "session_event_count": record.trace.event_count,
        },
        "dsh_home": record.dsh_home,
        "stdout_tail": record.stdout_tail,
        "stderr_tail": record.stderr_tail,
        "failure": failure,
        "command_sha256": record.command_sha256,
        "attempt_record": record.model_dump(mode="json"),
    }


def _task_prompt(case: HostNativeCase) -> str:
    return f"""# SWE-bench task: {case.instance_id}

Fix this repository-level bug without modifying tests:

> {case.problem_statement}

Repository facts:

- This checkout is the base commit for the public SWE-bench Lite instance
  `{case.instance_id}`.
{f"- Public issue hints: {case.hints}" if case.hints else ""}

Inspect the implementation and existing tests, make the smallest correct source
change, and run any useful local checks. Do not edit files under `tests/` or
`testing/`, do not weaken tests, and do not use web search or subagents.
"""


def _env(
    workspace: Path,
    key: str | None,
    credential_env: str | None = None,
) -> dict[str, str]:
    credential_env = _credential_env_name(credential_env)
    env = dict(os.environ)
    env.pop("LHOS_DSH_API_KEYS", None)
    for name in {DEFAULT_CREDENTIAL_ENV, "STEPFUN_API_KEY", credential_env}:
        env.pop(name, None)
    if key:
        env[credential_env] = key
    provider_base_url = _PROVIDER_BASE_URLS.get(credential_env)
    if provider_base_url is not None:
        base_url_env, default_base_url = provider_base_url
        env[base_url_env] = (
            os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
        )
    env["PYTHONPATH"] = str((workspace / "src").resolve())
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    env["SETUPTOOLS_SCM_PRETEND_VERSION_FOR_PYTEST"] = "8.0.0"
    env["PYTHONIOENCODING"] = "utf-8"
    env["DSH_PERMISSION_MODE"] = "workspace-write"
    env["DSH_TELEMETRY_DISABLED"] = "1"
    env["DSH_TOOLS_MODE"] = "native"
    return env


def _dsh_home() -> Path:
    root = Path(tempfile.gettempdir()) / "lhos-swe-dsh"
    root.mkdir(parents=True, exist_ok=True)
    return root / uuid.uuid4().hex[:12]


def _run_dsh(
    *,
    workspace: Path,
    run_root: Path,
    node: Path,
    dsh: Path,
    patch: Path,
    key: str,
    credential_env: str | None,
    timeout_seconds: float,
    case: HostNativeCase,
    eval_python: Path | None = None,
    validate_runtime_versions: bool = True,
) -> dict[str, Any]:
    for label, path in (("node", node), ("dsh", dsh), ("patch", patch)):
        if not Path(path).is_file():
            raise FileNotFoundError(f"{label} path does not exist: {path}")
    route = inspect_deepseek_patch(patch)

    dsh_home = _dsh_home()
    credential_env = _credential_env_name(credential_env)
    base_url_env, default_base_url = _PROVIDER_BASE_URLS.get(
        credential_env,
        ("LHOS_DSH_BASE_URL", "https://token.sensenova.cn/v1"),
    )
    base_url = (
        os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
    )
    extra_env = _env(workspace, None, credential_env)
    if eval_python is not None:
        extra_env["PATH"] = str(eval_python.parent) + os.pathsep + extra_env.get("PATH", "")
    attempt_id = uuid.uuid4().hex
    phase = DeepSeekHarnessPhase(
        phase_id=case.instance_id,
        prompt=_task_prompt(case),
        version=1,
        artifact_id=f"swe://{case.repo}/{case.instance_id}",
        resume_dsh_home=dsh_home,
    )
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=node,
            dsh=dsh,
            patch=patch,
            provider=route.provider,
            model=route.model,
            reasoning_effort=route.reasoning_effort,
            credential_env=credential_env,
            base_url=base_url,
            base_url_env=base_url_env,
            credential_values=(key,),
            profile="headless",
            timeout_seconds=timeout_seconds,
            retry=DeepSeekRetryPolicy(max_attempts=1),
            dsh_home_root=dsh_home.parent,
            validate_runtime_versions=validate_runtime_versions,
        ),
        workspace=workspace,
        run_root=run_root,
        phases=(phase,),
        extra_env=extra_env,
    )
    context = ExecutionContext(
        f"swebench:{case.repo}",
        task_id=case.instance_id,
        attempt_id=attempt_id,
        semantic_epoch=0,
        source="deepseek-harness-adapter",
    )
    context.graph_version = 1
    context.agent_id = "deepseek-harness"
    context.process_id = f"swe-worker:{os.getpid()}"
    context.claim_id = f"swe:{case.instance_id}:{attempt_id}"
    record_model = None
    failure = ""
    try:
        record_model = asyncio.run(adapter.execute(context, case.instance_id))
        exit_code = int(record_model.process.get("exit_code") or 0)
    except ExecutionError as exc:
        record_model = adapter.latest_record(case.instance_id)
        exit_code = 1
        failure = str(exc)
    except Exception as exc:
        exit_code = 125
        failure = f"{type(exc).__name__}: {exc}"
    if record_model is None:
        return {
            "schema_version": "swe-dsh-attempt-view.v1",
            "exit_code": exit_code,
            "elapsed_ms": 0.0,
            "usage": {
                "input_token_units": 0,
                "total_token_units": 0,
                "model_calls": 0,
                "tool_calls": 0,
            },
            "dsh_home": str(dsh_home),
            "stdout_tail": "",
            "stderr_tail": "",
            "failure": _safe_text(failure, [key]),
            "attempt_record": {},
        }
    return _attempt_result_view(
        record_model,
        failure_override=_safe_text(failure, [key]),
    )


def _run_pytest(
    workspace: Path,
    tests: tuple[str, ...],
    eval_python: Path | None = None,
    credential_env: str | None = None,
) -> dict[str, Any]:
    env = _env(workspace, None, credential_env)
    python_executable = str(eval_python or Path(sys.executable))
    command = [python_executable, "-m", "pytest", "-q", *tests]
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


def _git(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=workspace,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _apply_test_patch(workspace: Path, patch_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        ["git", "apply", "--whitespace=nowarn", str(patch_path)],
        cwd=workspace,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return {
        "passed": completed.returncode == 0,
        "exit_code": int(completed.returncode),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _evaluate(
    workspace: Path,
    test_patch: Path,
    case: HostNativeCase,
    eval_python: Path | None = None,
    credential_env: str | None = None,
) -> dict[str, Any]:
    before = _git(workspace, "diff", "--name-only")
    changed_before_test_patch = {
        line.strip().replace("\\", "/") for line in before.stdout.splitlines() if line.strip()
    }
    status = _git(workspace, "status", "--porcelain", "--untracked-files=all")
    changed_before_test_patch.update(
        line[3:].strip().replace("\\", "/")
        for line in status.stdout.splitlines()
        if len(line) >= 4 and line[3:].strip()
    )
    changed_before_test_patch_list = sorted(changed_before_test_patch)
    agent_patch = _git(workspace, "--no-pager", "diff", "--binary").stdout
    source_only_before_test_patch = all(
        not (line.startswith("tests/") or line.startswith("testing/"))
        for line in changed_before_test_patch_list
    )
    applied = _apply_test_patch(workspace, test_patch)
    if not applied["passed"]:
        return {
            "passed": False,
            "source_only_before_test_patch": source_only_before_test_patch,
            "changed_before_test_patch": changed_before_test_patch_list,
            "agent_patch": agent_patch,
            "test_patch": applied,
        }
    target = _run_pytest(
        workspace,
        case.fail_to_pass + case.pass_to_pass,
        eval_python,
        credential_env,
    )
    return {
        "passed": bool(target["passed"] and source_only_before_test_patch),
        "source_only_before_test_patch": source_only_before_test_patch,
        "changed_before_test_patch": changed_before_test_patch_list,
        "agent_patch": agent_patch,
        "test_patch": applied,
        "target_tests": target,
        "git_diff_stat": _git(workspace, "--no-pager", "diff", "--stat").stdout,
        "git_diff_name_only": _git(workspace, "diff", "--name-only").stdout.splitlines(),
    }


def _copy_workspace(source: Path, target: Path) -> None:
    shutil.copytree(source, target)


def _load_worker_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--run-root", type=Path)
    parser.add_argument("--node", type=Path)
    parser.add_argument("--dsh", type=Path)
    parser.add_argument("--patch", type=Path)
    parser.add_argument("--case-json", type=Path)
    parser.add_argument("--eval-python", type=Path)
    parser.add_argument("--key-slot", type=int, default=0)
    parser.add_argument(
        "--credential-env",
        help=(
            "Environment variable containing the provider API key. "
            "Defaults to LHOS_DSH_CREDENTIAL_ENV, then DEEPSEEK_API_KEY."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=900.0)
    parser.add_argument("--source-repo", type=Path)
    parser.add_argument("--test-patch", type=Path)
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def _worker(args: argparse.Namespace) -> int:
    credential_env = _credential_env_name(args.credential_env)
    keys = _keys(credential_env)
    if not keys:
        raise SystemExit(_missing_credentials_message(credential_env))
    key_slot = int(args.key_slot) % len(keys)
    record = _run_dsh(
        workspace=args.workspace.resolve(),
        run_root=args.run_root.resolve(),
        node=args.node.resolve(),
        dsh=args.dsh.resolve(),
        patch=args.patch.resolve(),
        key=keys[key_slot],
        credential_env=credential_env,
        timeout_seconds=args.timeout_seconds,
        case=_load_case(args.case_json),
        eval_python=args.eval_python,
    )
    path = args.run_root.resolve() / "dsh-attempt.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, ensure_ascii=True, indent=2), encoding="utf-8")
    print(
        f"SWE_DSH_RESULT exit={record['exit_code']} "
        f"model_calls={record['usage']['model_calls']} "
        f"tokens={record['usage']['total_token_units']}",
        flush=True,
    )
    return int(record["exit_code"])


def _run_static(
    *,
    workspace: Path,
    run_root: Path,
    node: Path,
    dsh: Path,
    patch: Path,
    test_patch: Path,
    timeout_seconds: float,
    case: HostNativeCase,
    eval_python: Path | None = None,
    key_slot: int = 0,
    credential_env: str | None = None,
) -> dict[str, Any]:
    credential_env = _credential_env_name(credential_env)
    keys = _keys(credential_env)
    if not keys:
        raise RuntimeError(_missing_credentials_message(credential_env))
    started = time.monotonic()
    dsh_result = _run_dsh(
        workspace=workspace,
        run_root=run_root,
        node=node,
        dsh=dsh,
        patch=patch,
        key=keys[key_slot % len(keys)],
        credential_env=credential_env,
        timeout_seconds=timeout_seconds,
        case=case,
        eval_python=eval_python,
    )
    evaluation = _evaluate(workspace, test_patch, case, eval_python, credential_env)
    return {
        "arm": "dsh_static",
        "valid": bool(dsh_result["exit_code"] == 0 and evaluation["passed"]),
        "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
        "dsh": dsh_result,
        "evaluation": evaluation,
    }


async def _run_lhos(
    *,
    workspace: Path,
    run_root: Path,
    node: Path,
    dsh: Path,
    patch: Path,
    test_patch: Path,
    timeout_seconds: float,
    case: HostNativeCase,
    eval_python: Path | None = None,
    case_json: Path | None = None,
    key_slot: int = 1,
    credential_env: str | None = None,
) -> dict[str, Any]:
    credential_env = _credential_env_name(credential_env)
    keys = _keys(credential_env)
    if not keys:
        raise RuntimeError(_missing_credentials_message(credential_env))
    selected_key = keys[key_slot % len(keys)]
    base_url_env, default_base_url = _PROVIDER_BASE_URLS.get(
        credential_env,
        ("LHOS_DSH_BASE_URL", "https://token.sensenova.cn/v1"),
    )
    base_url = (
        os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
    )
    route = inspect_deepseek_patch(patch)
    evaluation: dict[str, Any] = {}
    record_holder: dict[str, Any] = {}

    extra_env = _env(workspace, None, credential_env)
    if eval_python is not None:
        extra_env["PATH"] = str(eval_python.parent) + os.pathsep + extra_env.get("PATH", "")

    async def executor(context: Any, task_id: str) -> Any:
        phase = DeepSeekHarnessPhase(
            phase_id=case.instance_id,
            prompt=_task_prompt(case),
            version=1,
            artifact_id=f"swe://{case.repo}/{case.instance_id}",
            inputs=(f"swe://{case.repo}/{case.instance_id}/base",),
            outputs=(f"swe://{case.repo}/{case.instance_id}/patch",),
        )
        adapter = DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=node,
                dsh=dsh,
                patch=patch,
                provider=route.provider,
                model=route.model,
                reasoning_effort=route.reasoning_effort,
                credential_env=credential_env,
                base_url=base_url,
                base_url_env=base_url_env,
                credential_values=(selected_key,),
                profile="headless",
                timeout_seconds=timeout_seconds,
                retry=DeepSeekRetryPolicy(max_attempts=1),
                dsh_home_root=run_root / "dsh-sessions",
            ),
            workspace=workspace,
            run_root=run_root,
            phases=(phase,),
            extra_env=extra_env,
        )
        try:
            record = await adapter.execute(context, str(task_id))
        except BaseException:
            partial = adapter.latest_record(str(task_id))
            if partial is not None:
                record_holder["record"] = _attempt_result_view(partial)
            raise
        record_holder["record"] = _attempt_result_view(record)
        return record

    runtime = AgentOS(":memory:")
    runtime.add_agent(
        Agent("swe-dsh", executor=executor, executor_api="context_v1", max_concurrency=1)
    )
    goal = Goal(f"swebench-goal-{case.instance_id}")

    def verify(_context: Any, _task_id: str) -> VerificationOutcome:
        nonlocal evaluation
        evaluation = _evaluate(workspace, test_patch, case, eval_python, credential_env)
        return VerificationOutcome(
            passed=bool(evaluation["passed"]),
            artifact_id=f"swe://{case.repo}/{case.instance_id}",
            version=1,
            content=json.dumps(evaluation, sort_keys=True),
            evidence_note="host-native SWE-bench target tests",
            details=evaluation,
        )

    goal.task(
        case.instance_id,
        agent="swe-dsh",
        verify=verify,
        executor_api="context_v1",
        max_attempts=1,
        inputs=(f"swe://{case.repo}/{case.instance_id}/base",),
        outputs=(f"swe://{case.repo}/{case.instance_id}/patch",),
    )
    started = time.monotonic()
    try:
        result = await runtime.run_async(
            goal,
            max_dispatches=1,
            max_steps=2,
            max_concurrency=1,
            adaptive=True,
            max_parallelism=1,
        )
    finally:
        runtime.close()
    elapsed_ms = round((time.monotonic() - started) * 1000, 3)
    dsh_result = record_holder.get("record", {})
    record_path = run_root / "dsh-attempt.json"
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(json.dumps(dsh_result, ensure_ascii=True, indent=2), encoding="utf-8")
    return {
        "arm": "dsh_lhos",
        "valid": bool(result.goal_state == "closed" and evaluation.get("passed", False)),
        "elapsed_ms": elapsed_ms,
        "run_result": result.as_dict(),
        "dsh": dsh_result,
        "evaluation": evaluation,
    }


def run_pair(
    *,
    source_repo: Path,
    test_patch: Path,
    node: Path,
    dsh: Path,
    patch: Path,
    output_dir: Path,
    timeout_seconds: float = 900.0,
    case: HostNativeCase = DEFAULT_CASE,
    eval_python: Path | None = None,
    case_json: Path | None = None,
    credential_env: str | None = None,
) -> dict[str, Any]:
    credential_env = _credential_env_name(credential_env)
    head = _git(source_repo, "rev-parse", "HEAD")
    if head.returncode != 0 or head.stdout.strip() != case.base_commit:
        raise ValueError(f"source_repo HEAD must equal SWE-bench base_commit {case.base_commit}")
    status = _git(source_repo, "status", "--porcelain", "--untracked-files=all")
    if status.returncode != 0 or status.stdout.strip():
        raise ValueError("source_repo must be a clean checkout before run_pair")
    output_dir.mkdir(parents=True, exist_ok=False)
    case_json = case_json or (output_dir / "case.json")
    case_json.write_text(
        json.dumps(
            {
                "instance_id": case.instance_id,
                "repo": case.repo,
                "base_commit": case.base_commit,
                "problem_statement": case.problem_statement,
                "hints_text": case.hints,
                "FAIL_TO_PASS": list(case.fail_to_pass),
                "PASS_TO_PASS": list(case.pass_to_pass),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    static_workspace = output_dir / "dsh_static_workspace"
    lhos_workspace = output_dir / "dsh_lhos_workspace"
    _copy_workspace(source_repo, static_workspace)
    _copy_workspace(source_repo, lhos_workspace)
    static = _run_static(
        workspace=static_workspace,
        run_root=output_dir / "dsh_static",
        node=node,
        dsh=dsh,
        patch=patch,
        test_patch=test_patch,
        timeout_seconds=timeout_seconds,
        case=case,
        eval_python=eval_python,
        key_slot=0,
        credential_env=credential_env,
    )
    lhos = asyncio.run(
        _run_lhos(
            workspace=lhos_workspace,
            run_root=output_dir / "dsh_lhos",
            node=node,
            dsh=dsh,
            patch=patch,
            test_patch=test_patch,
            timeout_seconds=timeout_seconds,
            case=case,
            eval_python=eval_python,
            case_json=case_json,
            key_slot=0,
            credential_env=credential_env,
        )
    )
    static_tokens = int(static.get("dsh", {}).get("usage", {}).get("total_token_units", 0))
    lhos_tokens = int(lhos.get("dsh", {}).get("usage", {}).get("total_token_units", 0))
    result = {
        "benchmark": "SWE-bench Lite host-native smoke/proxy",
        "instance_id": case.instance_id,
        "repo": case.repo,
        "base_commit": case.base_commit,
        "official_evaluator": False,
        "docker_used": False,
        "same_dsh_executor": True,
        "static": static,
        "lhos": lhos,
        "comparison": {
            "pair_valid": bool(static["valid"] and lhos["valid"]),
            "static_tokens": static_tokens,
            "lhos_tokens": lhos_tokens,
            "token_ratio": None if static_tokens == 0 else round(lhos_tokens / static_tokens, 6),
            "static_elapsed_ms": static["elapsed_ms"],
            "lhos_elapsed_ms": lhos["elapsed_ms"],
            "wall_ratio": None
            if lhos["elapsed_ms"] == 0
            else round(static["elapsed_ms"] / lhos["elapsed_ms"], 6),
        },
    }
    (output_dir / "result.json").write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return result


def main() -> int:
    args = _load_worker_args()
    if args.worker:
        return _worker(args)
    credential_env = _credential_env_name(args.credential_env)
    required = (args.source_repo, args.test_patch, args.node, args.dsh, args.patch)
    if any(item is None for item in required):
        raise SystemExit("--source-repo, --test-patch, --node, --dsh and --patch are required")
    if not _keys(credential_env):
        raise SystemExit(_missing_credentials_message(credential_env))
    if args.output_dir is None:
        raise SystemExit("--output-dir is required")
    result = run_pair(
        source_repo=args.source_repo.resolve(),
        test_patch=args.test_patch.resolve(),
        node=args.node.resolve(),
        dsh=args.dsh.resolve(),
        patch=args.patch.resolve(),
        output_dir=args.output_dir.resolve(),
        timeout_seconds=args.timeout_seconds,
        case=_load_case(args.case_json),
        eval_python=args.eval_python.resolve() if args.eval_python else None,
        case_json=args.case_json.resolve() if args.case_json else None,
        credential_env=credential_env,
    )
    print(json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if result["comparison"]["pair_valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
