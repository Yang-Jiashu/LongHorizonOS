"""Fake/scripted worker (spec section 12; no real LLM).

Driven by ``node.metadata["script"]``:

.. code-block:: yaml

    script:
      status: claimed_done        # default; "failed" | "waiting" also allowed
      fail_times: 0               # first N attempts report failure
      crash_on_attempt: null      # raise SimulatedCrashError on this attempt
      crash_after_tool_calls: null# crash right after the Nth tool call finishes
      attempts:                   # per-attempt overrides merged over the script
        "1": {produced_artifacts: [...]}
      summary: "..."
      input_tokens: 120
      output_tokens: 40
      exit_code: 0
      tool_calls:
        - {tool_name: fake, arguments: {...}}
      produced_artifacts:
        - {path: out.txt, content: "..."}
      verification_request: null  # overrides node.verification_spec
      graph_patch: []
      environment_events:         # external change injection (spec 15);
        - {type: artifact_updated, node_id: art, new_hash: v2}   # fired once,
                                  # on attempt 1, unless ..._every_attempt: true

Crash-injection flags interpreted by the controller (spec 26.2):
``crash_before_execution``, ``crash_before_verification``,
``crash_after_verified``.

Idempotency keys are deterministic per node + call + content, so a re-run
after a crash replays recorded tool results instead of re-executing, while a
retry producing DIFFERENT content still executes (spec 13.3, 16.3).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, Field

from lhos.domain.errors import SimulatedCrashError
from lhos.domain.models import GraphNode
from lhos.ports.tools import ToolRequest
from lhos.runtime.context_compiler import ContextPacket

VALID_STATUSES = {"claimed_done", "failed", "waiting", "verified"}


def _content_key(prefix: str, material: Any) -> str:
    digest = hashlib.sha256(
        json.dumps(material, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"{prefix}:{digest}"


class WorkerResult(BaseModel):
    """Structured worker output (spec 12.1-12.2)."""

    status: str = "claimed_done"
    summary: str = ""
    produced_artifacts: list[dict[str, Any]] = Field(default_factory=list)
    verification_request: dict[str, Any] | None = None
    graph_patch: list[dict[str, Any]] = Field(default_factory=list)
    environment_events: list[dict[str, Any]] = Field(default_factory=list)
    exit_code: int | None = None
    input_tokens: int = 0
    output_tokens: int = 0
    tool_call_count: int = 0


class FakeWorker:
    def __init__(self, tool_runtime=None):
        self._tool_runtime = tool_runtime

    def execute(self, node: GraphNode, context: ContextPacket) -> WorkerResult:
        script: dict[str, Any] = dict(node.metadata.get("script", {}))
        attempt = node.attempt_count

        if script.get("crash_on_attempt") == attempt:
            raise SimulatedCrashError(f"simulated crash on node {node.id} attempt {attempt}")

        # Per-attempt overrides: "first attempt broken, retry fixes it" (26.3).
        overrides = script.get("attempts", {}).get(str(attempt))
        if overrides:
            script.update(overrides)

        fail_times = int(script.get("fail_times", 0))
        status = script.get("status")
        if status is None:
            status = "failed" if attempt <= fail_times else "claimed_done"
        if status not in VALID_STATUSES:
            status = "claimed_done"

        tool_calls = 0
        crash_after = script.get("crash_after_tool_calls")
        crash_attempt = int(script.get("crash_after_tool_calls_attempt", 1))
        if self._tool_runtime is not None:
            for i, call in enumerate(script.get("tool_calls", [])):
                arguments = call.get("arguments", {})
                request = ToolRequest(
                    tool_name=call.get("tool_name", "fake"),
                    arguments=arguments,
                    timeout_seconds=call.get("timeout_seconds", 30),
                    idempotency_key=call.get(
                        "idempotency_key",
                        _content_key(f"{node.id}:tool{i}", arguments),
                    ),
                )
                self._tool_runtime.execute(node.run_id, node.id, request)
                tool_calls += 1
                if crash_after == tool_calls and attempt == crash_attempt:
                    # Process death AFTER the tool completed and its events
                    # were persisted, before the claim was written (26.2).
                    raise SimulatedCrashError(
                        f"simulated crash after tool call {tool_calls} on {node.id}"
                    )
            for artifact in script.get("produced_artifacts", []):
                if "content" in artifact and "path" in artifact:
                    request = ToolRequest(
                        tool_name="filesystem",
                        arguments={
                            "op": "write",
                            "path": artifact["path"],
                            "content": artifact["content"],
                        },
                        timeout_seconds=30,
                        idempotency_key=_content_key(
                            f"{node.id}:write:{artifact['path']}",
                            artifact["content"],
                        ),
                    )
                    self._tool_runtime.execute(node.run_id, node.id, request)
                    tool_calls += 1
                    if crash_after == tool_calls and attempt == crash_attempt:
                        raise SimulatedCrashError(
                            f"simulated crash after tool call {tool_calls} on {node.id}"
                        )

        environment_events: list[dict[str, Any]] = []
        if attempt == 1 or script.get("environment_events_every_attempt"):
            environment_events = list(script.get("environment_events", []))

        return WorkerResult(
            status=status,
            summary=script.get("summary", f"fake execution of {node.title}"),
            produced_artifacts=[
                {k: v for k, v in a.items() if k != "content"}
                for a in script.get("produced_artifacts", [])
            ],
            verification_request=script.get("verification_request"),
            graph_patch=script.get("graph_patch", []),
            environment_events=environment_events,
            exit_code=script.get("exit_code"),
            input_tokens=script.get("input_tokens", context.estimated_tokens),
            output_tokens=script.get("output_tokens", 50),
            tool_call_count=tool_calls,
        )
