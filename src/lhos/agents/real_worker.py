"""Real Node Worker (spec Phase 2C-C2).

Uses ``LLMClient`` to execute a single node. The worker can either request
a tool call or claim completion. It never self-verifies.

The worker receives only local context (compiled by the Context Compiler) —
no full transcript, no unrelated siblings, no hidden oracle.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from lhos.agents.prompt_manager import PromptManager
from lhos.domain.errors import StructuredOutputError
from lhos.infrastructure.llm.structured_output import parse_structured
from lhos.ports.llm import LLMClient, LLMRequest
from lhos.runtime.context_compiler import ContextPacket
from lhos.runtime.worker import WorkerResult

PROMPT_NAME = "node_worker"
PROMPT_VERSION = "v1"


class WorkerToolRequest(BaseModel):
    tool_name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 60


class WorkerOutput(BaseModel):
    """Structured output from the Node Worker."""

    action_type: str  # tool_call | claim_done
    summary: str = ""
    tool_request: WorkerToolRequest | None = None
    produced_artifacts: list[dict[str, Any]] = Field(default_factory=list)
    verification_request: dict[str, Any] | None = None
    suggested_graph_patch: list[dict[str, Any]] = Field(default_factory=list)


class RealNodeWorker:
    """Real worker backed by an LLM.

    The worker makes ONE decision per LLM call: either request a tool or
    claim done. The controller calls this in a loop until the worker claims
    done or the budget is exhausted.
    """

    def __init__(
        self,
        client: LLMClient,
        model_id: str = "sensenova-6.7-flash-lite",
        max_output_tokens: int = 4096,
        prompt_manager: PromptManager | None = None,
    ):
        self._client = client
        self._model_id = model_id
        self._max_output_tokens = max_output_tokens
        self._pm = prompt_manager or PromptManager()
        self._prompt_info = self._pm.load(PROMPT_NAME, PROMPT_VERSION)

    @property
    def prompt_info(self):
        return self._prompt_info

    def execute(self, node, context: ContextPacket) -> WorkerResult:
        """Execute a single node.

        Returns a ``WorkerResult`` with status 'claimed_done' (the worker
        believes it's done) or 'waiting' (needs a tool call first).

        The controller handles the tool_call -> result -> next_call loop.
        """
        # Build the context message from the ContextPacket.
        dep_text = "\n".join(f"- {d}" for d in context.dependency_summaries) or "None"
        constraint_text = "\n".join(f"- {c}" for c in context.constraints) or "None"
        failure_text = "\n".join(f"- {f}" for f in context.previous_failures) or "None"
        verification_text = (
            "\n".join(context.verification_requirements) or "No specific verification"
        )

        user_content = (
            f"## Context Packet\n\n"
            f"Global goal: {context.global_goal}\n"
            f"Current task: {context.current_task}\n"
            f"Node version: {node.version}\n"
            f"Attempt: {node.attempt_count}\n"
            f"Dependencies:\n{dep_text}\n"
            f"Constraints:\n{constraint_text}\n"
            f"Previous failures:\n{failure_text}\n"
            f"Verification requirements:\n{verification_text}\n"
            f"Available tools: filesystem (read/write/list/exists), shell (command)\n"
        )

        request = LLMRequest(
            role="worker",
            messages=[
                {"role": "system", "content": self._prompt_info.content},
                {"role": "user", "content": user_content},
            ],
            response_schema={"type": "object"},
            temperature=0.0,
            max_output_tokens=self._max_output_tokens,
            metadata={
                "prompt_name": self._prompt_info.name,
                "prompt_version": self._prompt_info.version,
                "prompt_file_hash": self._prompt_info.file_hash,
                "node_id": node.id,
                "node_version": node.version,
            },
        )

        response = self._client.generate(request)

        try:
            parsed = parse_structured(response.text, WorkerOutput)
        except StructuredOutputError:
            # Fallback: try the old ExecutorOutput format.
            try:
                import json

                raw = json.loads(response.text.strip())
                if "status" in raw:
                    # Old format: status/summary/produced_artifacts/...
                    return WorkerResult(
                        status=raw.get("status", "claimed_done"),
                        summary=raw.get("summary", ""),
                        produced_artifacts=raw.get("produced_artifacts", []),
                        verification_request=raw.get("verification_request"),
                        graph_patch=raw.get("graph_patch", []),
                        input_tokens=response.usage.input_tokens,
                        output_tokens=response.usage.output_tokens,
                    )
                raise
            except Exception as exc:
                raise StructuredOutputError(f"Worker output could not be parsed: {exc}") from exc

        if parsed.action_type == "tool_call":
            # The worker wants to call a tool first.
            # We return a 'waiting' status with the tool request in the result.
            # The controller will execute the tool and call the worker again.
            return WorkerResult(
                status="waiting",
                summary=parsed.summary,
                produced_artifacts=[],
                verification_request=None,
                graph_patch=parsed.suggested_graph_patch,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                tool_call_count=0,
            )
        elif parsed.action_type == "claim_done":
            return WorkerResult(
                status="claimed_done",
                summary=parsed.summary,
                produced_artifacts=parsed.produced_artifacts,
                verification_request=parsed.verification_request,
                graph_patch=parsed.suggested_graph_patch,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                tool_call_count=0,
            )
        else:
            raise StructuredOutputError(
                f"Worker returned unknown action_type: {parsed.action_type}"
            )
