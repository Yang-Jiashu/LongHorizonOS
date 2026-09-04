"""Run a real DeepSeek Harness adapter smoke without accepting keys on argv."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
    inspect_deepseek_patch,
)
from lhos.sdk import AgentOS, VerificationOutcome

_DEFAULT_BASE_URLS = {
    "sensenova": "https://token.sensenova.cn/v1",
    "stepfun": "https://api.stepfun.com/step_plan/v1",
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--dsh", type=Path, required=True)
    parser.add_argument("--patch", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    return parser


def _base_url(route: Any) -> tuple[str, str]:
    expression = str(route.base_url_expression)
    if expression.startswith(("http://", "https://")):
        return f"LHOS_{route.provider.upper()}_BASE_URL", expression
    match = re.fullmatch(r"process\.env\.([A-Za-z_][A-Za-z0-9_]*)", expression)
    if match is None:
        raise RuntimeError(f"unsupported Cordis baseURL expression: {expression!r}")
    env_name = match.group(1)
    value = (
        os.environ.get("LHOS_DSH_BASE_URL", "").strip()
        or os.environ.get(env_name, "").strip()
        or _DEFAULT_BASE_URLS.get(route.provider, "")
    )
    if not value:
        raise RuntimeError(f"{env_name} or LHOS_DSH_BASE_URL is required")
    return env_name, value


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    node = args.node.resolve()
    dsh = args.dsh.resolve()
    patch = args.patch.resolve()
    run_root = args.run_root.resolve()
    workspace = run_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=False)
    route = inspect_deepseek_patch(patch)
    credential_pool_env = (
        "LHOS_DSH_API_KEYS" if os.environ.get("LHOS_DSH_API_KEYS", "").strip() else None
    )
    credential_source = credential_pool_env or route.credential_env
    if not os.environ.get(credential_source, "").strip():
        raise RuntimeError(
            f"{credential_source} is required; the smoke runner never accepts keys on argv"
        )
    base_url_env, base_url = _base_url(route)
    output = workspace / "lhos_dsh_smoke.txt"

    def verify(_context: Any, _task_id: str) -> VerificationOutcome:
        content = output.read_text(encoding="utf-8") if output.is_file() else ""
        passed = content == "LHOS_DSH_SMOKE_OK\n"
        return VerificationOutcome(
            passed=passed,
            artifact_id="smoke://deepseek-harness/output",
            version=1,
            content=content,
            evidence_note="exact smoke file content",
            details={"path": str(output), "exact_content": passed},
        )

    phase = DeepSeekHarnessPhase(
        phase_id="deepseek-adapter-real-smoke",
        prompt=(
            "Create exactly one file named `lhos_dsh_smoke.txt` in the current "
            "workspace. Its complete UTF-8 content must be exactly "
            "`LHOS_DSH_SMOKE_OK` followed by one newline. Do not modify any "
            "other file. Finish only after reading the file back."
        ),
        outputs=("lhos_dsh_smoke.txt",),
        artifact_id="smoke://deepseek-harness/output",
        verifier=verify,
        max_attempts=1,
        harness_max_attempts=1,
    )
    adapter = DeepSeekHarnessAdapter(
        DeepSeekHarnessConfig(
            node=node,
            dsh=dsh,
            patch=patch,
            provider=route.provider,
            model=route.model,
            reasoning_effort=route.reasoning_effort,
            credential_env=route.credential_env,
            credential_pool_env=credential_pool_env,
            base_url=base_url,
            base_url_env=base_url_env,
            timeout_seconds=float(args.timeout_seconds),
            retry=DeepSeekRetryPolicy(max_attempts=1),
            dsh_home_root=run_root / "dsh-home",
        ),
        workspace=workspace,
        run_root=run_root,
        phases=(phase,),
    )
    runtime = AgentOS(":memory:")
    runtime.add_agent(adapter.agent("deepseek-harness", max_concurrency=1))
    goal = adapter.goal(
        "deepseek-adapter-real-smoke-goal",
        agent_name="deepseek-harness",
    )
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
    record = adapter.latest_record(phase.phase_id)
    summary = {
        "schema_version": "deepseek-adapter-real-smoke.v1",
        "valid": bool(
            result.goal_state == "closed"
            and output.is_file()
            and output.read_text(encoding="utf-8") == "LHOS_DSH_SMOKE_OK\n"
        ),
        "goal_state": result.goal_state,
        "verified": sorted(result.verified),
        "node_version": "" if record is None else record.node_version,
        "dsh_version": "" if record is None else record.dsh_version,
        "patch_sha256": "" if record is None else record.patch_sha256,
        "provider": "" if record is None else record.trace.provider,
        "model": "" if record is None else record.trace.model,
        "model_calls": 0 if record is None else record.usage.model_calls,
        "tool_calls": 0 if record is None else record.usage.tool_calls,
        "token_units": 0 if record is None else record.usage.total_token_units,
        "wall_time_ms": 0 if record is None else record.usage.wall_time_ms,
        "event_count": 0 if record is None else len(record.trace.events),
        "failure_class": (
            None if record is None or record.failure is None else record.failure.failure_class.value
        ),
    }
    (run_root / "smoke-summary.json").write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def main() -> int:
    args = _parser().parse_args()
    summary = asyncio.run(_run(args))
    print(json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True))
    return 0 if summary["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
