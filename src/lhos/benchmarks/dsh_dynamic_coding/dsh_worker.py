"""One DeepSeek Harness task attempt with provider-reported usage export."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
    parse_deepseek_sessions,
)
from lhos.provenance import ExecutionContext
from lhos.sdk.errors import ExecutionError
from lhos.sdk.harness_child import format_usage_line

from .case import benchmark_root, task_prompt

DEFAULT_CREDENTIAL_ENV = "DEEPSEEK_API_KEY"
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


def _credential_env_name(value: str | None = None) -> str:
    name = (value or os.environ.get("LHOS_DSH_CREDENTIAL_ENV") or DEFAULT_CREDENTIAL_ENV).strip()
    if (
        not name
        or not (name[0].isalpha() or name[0] == "_")
        or not all(char.isalnum() or char == "_" for char in name)
    ):
        raise ValueError(f"invalid credential environment variable name: {name!r}")
    return name


def _keys(credential_env: str | None = None) -> list[str]:
    credential_env = _credential_env_name(credential_env)
    raw = os.environ.get("LHOS_DSH_API_KEYS", "").strip()
    if not raw:
        raw = os.environ.get(credential_env, "").strip()
    keys = [item.strip() for item in raw.split(",") if item.strip()]
    if not keys:
        raise RuntimeError(f"LHOS_DSH_API_KEYS or {credential_env} is required")
    return keys


def _redact(value: str, secrets: list[str]) -> str:
    result = value
    for secret in secrets:
        result = result.replace(secret, "[REDACTED]")
    return result


def _usage_from_logs(session_root: Path, *, workspace: Path | None = None) -> dict[str, Any]:
    trace = parse_deepseek_sessions(
        session_root,
        workspace=(workspace or session_root.parent.parent),
    )
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--version", type=int, choices=(1, 2), required=True)
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--dsh", type=Path, required=True)
    parser.add_argument(
        "--patch",
        type=Path,
        default=benchmark_root() / "sensenova-pi-ai.cordis.patch.yml",
    )
    parser.add_argument("--timeout-seconds", type=float, default=600.0)
    parser.add_argument("--key-slot", type=int, default=0)
    parser.add_argument("--arm", required=True)
    parser.add_argument("--credential-env")
    parser.add_argument("--model", default=os.environ.get("LHOS_DSH_MODEL", "deepseek-v4-flash"))
    parser.add_argument("--reasoning", default=os.environ.get("LHOS_DSH_REASONING", "low"))
    parser.add_argument("--provider", default=os.environ.get("LHOS_DSH_PROVIDER", "deepseek"))
    return parser


def main() -> int:
    args = _parser().parse_args()
    workspace = args.workspace.resolve()
    run_root = args.run_root.resolve()
    credential_env = _credential_env_name(args.credential_env)
    secrets = _keys(credential_env)
    selected_key = secrets[args.key_slot % len(secrets)]
    attempt_id = uuid.uuid4().hex
    dsh_home_root = Path(
        os.environ.get("LHOS_DSH_HOME_ROOT", Path(tempfile.gettempdir()) / "lhos-dsh")
    ).resolve()
    dsh_home = dsh_home_root / (
        f"{args.arm[:12]}-{args.task_id[:16]}-v{args.version}-{attempt_id[:12]}"
    )
    dsh_home.mkdir(parents=True, exist_ok=True)
    attempts_dir = run_root / "attempts"
    attempts_dir.mkdir(parents=True, exist_ok=True)
    record_path = attempts_dir / f"{args.task_id}-v{args.version}-{attempt_id}.json"

    base_url_env, default_base_url = _PROVIDER_BASE_URLS.get(
        credential_env,
        ("LHOS_DSH_BASE_URL", "https://token.sensenova.cn/v1"),
    )
    base_url = (
        os.environ.get("LHOS_DSH_BASE_URL") or os.environ.get(base_url_env) or default_base_url
    )
    prompt = task_prompt(args.task_id, args.version)
    phase = DeepSeekHarnessPhase(
        phase_id=args.task_id,
        prompt=prompt,
        version=args.version,
        artifact_id=args.task_id,
        resume_dsh_home=dsh_home,
    )
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=args.node.resolve(),
            dsh=args.dsh.resolve(),
            patch=args.patch.resolve(),
            provider=str(args.provider).split("/", 1)[0].strip(),
            model=str(args.model),
            reasoning_effort=str(args.reasoning),
            credential_env=credential_env,
            base_url=base_url,
            base_url_env=base_url_env,
            credential_values=(selected_key,),
            profile="headless",
            timeout_seconds=float(args.timeout_seconds),
            retry=DeepSeekRetryPolicy(max_attempts=1),
            dsh_home_root=dsh_home_root,
        ),
        workspace=workspace,
        run_root=run_root,
        phases=(phase,),
    )
    context = ExecutionContext(
        f"{args.arm}:dynamic-coding",
        task_id=args.task_id,
        attempt_id=attempt_id,
        semantic_epoch=max(0, int(args.version) - 1),
        source="deepseek-harness-adapter",
    )
    context.graph_version = int(args.version)
    context.agent_id = "deepseek-harness"
    context.process_id = f"worker:{os.getpid()}"
    context.claim_id = f"{args.arm}:{args.task_id}:v{args.version}:{attempt_id}"
    try:
        record_model = asyncio.run(adapter.execute(context, args.task_id))
        failure = ""
        exit_code = int(record_model.process.get("exit_code") or 0)
    except ExecutionError as exc:
        record_model = adapter.latest_record(args.task_id)
        failure = str(exc)
        exit_code = 1
    except Exception as exc:
        record_model = adapter.latest_record(args.task_id)
        failure = f"{type(exc).__name__}: {exc}"
        exit_code = 125
    if record_model is None:
        usage = _usage_from_logs(dsh_home / "sessions")
        elapsed_ms = 0.0
        process = {"exit_code": exit_code}
        stdout_tail = ""
        stderr_tail = ""
    else:
        usage_model = record_model.trace.usage
        usage = {
            "uncached_input_tokens": usage_model.uncached_input_tokens,
            "output_tokens": usage_model.output_tokens,
            "cache_read_tokens": usage_model.cache_read_tokens,
            "cache_write_tokens": usage_model.cache_write_tokens,
            "reasoning_tokens": usage_model.reasoning_tokens,
            "input_token_units": usage_model.input_token_units,
            "total_token_units": usage_model.total_token_units,
            "model_calls": usage_model.model_calls,
            "tool_calls": usage_model.tool_calls,
            "tool_names": [tool.name for tool in record_model.trace.tool_calls],
            "turn_end_reasons": list(record_model.trace.turn_end_reasons),
            "session_files": list(record_model.trace.session_files),
            "session_event_count": record_model.trace.event_count,
        }
        process = dict(record_model.process)
        elapsed_ms = float(process.get("elapsed_ms", 0.0) or 0.0)
        stdout_tail = record_model.stdout_tail
        stderr_tail = record_model.stderr_tail
        if record_model.failure is not None and not failure:
            failure = record_model.failure.summary
        if exit_code == 0 and record_model.failure is not None:
            exit_code = 1
    record = {
        "schema_version": "dsh-dynamic-attempt.v1",
        "adapter_schema_version": "deepseek-harness-attempt.v1",
        "arm": args.arm,
        "task_id": args.task_id,
        "requirement_version": args.version,
        "attempt_id": attempt_id,
        "exit_code": exit_code,
        "elapsed_ms": elapsed_ms,
        "usage": usage,
        "dsh_home": str(dsh_home),
        "process": process,
        "stdout_tail": _redact(stdout_tail[-2000:], secrets),
        "stderr_tail": _redact(stderr_tail[-2000:], secrets),
        "failure": _redact(failure, secrets),
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "trace": {} if record_model is None else record_model.trace.model_dump(mode="json"),
    }
    record_path.write_text(
        json.dumps(record, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(
        format_usage_line(
            tokens_in=int(usage["input_token_units"]),
            tokens_out=int(usage["output_tokens"]),
            cost_microusd=0,
        ),
        flush=True,
    )
    print(
        f"DSH_TASK_RESULT task={args.task_id} version={args.version} "
        f"exit={exit_code} record={record_path}",
        flush=True,
    )
    if exit_code != 0:
        message = failure or f"DeepSeek Harness exited with code {exit_code}"
        safe_ascii = _redact(message, secrets).encode("ascii", errors="backslashreplace").decode()
        print(safe_ascii, file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
