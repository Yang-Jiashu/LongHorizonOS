"""Real DSH/AgentOS success smoke using a local OpenAI-compatible SSE server."""

from __future__ import annotations

import argparse
import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

from lhos.integrations.harness import (
    DeepSeekHarnessAdapter,
    DeepSeekHarnessConfig,
    DeepSeekHarnessPhase,
    DeepSeekRetryPolicy,
)
from lhos.sdk import AgentOS, VerificationOutcome

_EXPECTED = "LHOS_DSH_LOCAL_OK"


class _Handler(BaseHTTPRequestHandler):
    request_paths: ClassVar[list[str]] = []

    def log_message(self, _format: str, *args: Any) -> None:
        del args

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        self.rfile.read(content_length)
        type(self).request_paths.append(self.path)
        created = int(time.time())
        chunks = (
            {
                "id": "chatcmpl-lhos-local",
                "object": "chat.completion.chunk",
                "created": created,
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "role": "assistant",
                            "content": _EXPECTED,
                        },
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl-lhos-local",
                "object": "chat.completion.chunk",
                "created": created,
                "model": "deepseek-v4-flash",
                "choices": [
                    {
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 5,
                    "completion_tokens": 3,
                    "total_tokens": 8,
                },
            },
        )
        payload = (
            "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)
        self.wfile.flush()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", type=Path, required=True)
    parser.add_argument("--dsh", type=Path, required=True)
    parser.add_argument("--patch", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    return parser


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    run_root = args.run_root.resolve()
    workspace = run_root / "workspace"
    workspace.mkdir(parents=True, exist_ok=False)
    _Handler.request_paths = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    adapter: DeepSeekHarnessAdapter | None = None
    runtime: AgentOS | None = None
    try:
        base_url = f"http://127.0.0.1:{server.server_address[1]}/v1"

        def verify(_context: Any, _task_id: str) -> VerificationOutcome:
            record = None if adapter is None else adapter.latest_record("local-provider-smoke")
            output = "" if record is None else record.stdout_tail.strip()
            return VerificationOutcome(
                passed=output == _EXPECTED,
                artifact_id="smoke://deepseek-harness/local-provider-output",
                version=1,
                content=output,
                evidence_note="exact local provider assistant output",
                details={"exact_output": output == _EXPECTED},
            )

        phase = DeepSeekHarnessPhase(
            phase_id="local-provider-smoke",
            prompt=f"Reply with exactly {_EXPECTED}. Do not call tools.",
            artifact_id="smoke://deepseek-harness/local-provider-output",
            verifier=verify,
            max_attempts=1,
        )
        adapter = DeepSeekHarnessAdapter(
            DeepSeekHarnessConfig(
                node=args.node.resolve(),
                dsh=args.dsh.resolve(),
                patch=args.patch.resolve(),
                provider="sensenova",
                model="deepseek-v4-flash",
                reasoning_effort="low",
                credential_env="DEEPSEEK_API_KEY",
                base_url=base_url,
                base_url_env="DEEPSEEK_BASE_URL",
                credential_values=("local-placeholder",),
                timeout_seconds=90,
                retry=DeepSeekRetryPolicy(max_attempts=1),
                dsh_home_root=run_root / "dsh-home",
                extra_env={
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
                },
            ),
            workspace=workspace,
            run_root=run_root,
            phases=(phase,),
        )
        runtime = AgentOS(":memory:")
        runtime.add_agent(adapter.agent("deepseek-harness", max_concurrency=1))
        goal = adapter.goal(
            "deepseek-local-provider-smoke-goal",
            agent_name="deepseek-harness",
        )
        result = await runtime.run_async(
            goal,
            max_dispatches=1,
            max_steps=2,
            max_concurrency=1,
            adaptive=True,
            max_parallelism=1,
        )
        record = adapter.latest_record(phase.phase_id)
        summary = {
            "schema_version": "deepseek-adapter-local-provider-smoke.v1",
            "valid": bool(
                result.goal_state == "closed"
                and record is not None
                and record.stdout_tail.strip() == _EXPECTED
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
            "event_count": 0 if record is None else len(record.trace.events),
            "request_paths": list(_Handler.request_paths),
        }
    finally:
        if runtime is not None:
            runtime.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
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
