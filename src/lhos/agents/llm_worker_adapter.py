"""LLM worker adapter: bridges RealNodeWorker with the RuntimeController.

The controller expects a worker with ``execute(node, context) -> WorkerResult``.
The ``RealNodeWorker`` returns ``status="waiting"`` when it wants to call a
tool. This adapter handles the tool-call loop internally:

1. Call RealNodeWorker.execute(node, context)
2. If status == "waiting" (tool_call): execute the tool via ToolRuntime,
   add the tool result to the conversation, and call the worker again.
3. If status == "claimed_done": return the result to the controller.

This keeps the controller's loop unchanged while enabling real LLM-driven
tool use.

Step 4 fixes:
- Structured trace logging for each worker iteration.
- Tool name normalization (Step 6 contract).
- Proper error feedback to the worker when tool execution fails.
- Diagnostic logging of action types, tool names, and execution status.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from lhos.agents.real_worker import RealNodeWorker, WorkerOutput
from lhos.domain.models import GraphNode
from lhos.infrastructure.llm.structured_output import parse_structured
from lhos.infrastructure.tools.registry import normalize_tool_name
from lhos.ports.llm import LLMClient, LLMRequest
from lhos.ports.tools import ToolRequest
from lhos.runtime.context_compiler import ContextPacket
from lhos.runtime.duplicate_work import DuplicateWorkDetector
from lhos.runtime.node_budget import NodeExecutionBudget
from lhos.runtime.worker import WorkerResult

_MAX_TOOL_ROUNDS = 20

logger = logging.getLogger(__name__)


class LLMWorkerAdapter:
    """Wraps RealNodeWorker to handle the tool-call loop.

    Implements the same interface as FakeWorker: ``execute(node, context)``.
    """

    def __init__(
        self,
        client: LLMClient,
        tool_runtime: Any,
        model_id: str = "sensenova-6.7-flash-lite",
        max_output_tokens: int = 4096,
    ):
        self._worker = RealNodeWorker(
            client=client,
            model_id=model_id,
            max_output_tokens=max_output_tokens,
        )
        self._tool_runtime = tool_runtime
        self._client = client
        self._parse_failures = 0
        self._total_llm_calls = 0

    @property
    def prompt_info(self) -> Any:
        return self._worker.prompt_info

    def execute(self, node: GraphNode, context: ContextPacket) -> WorkerResult:
        """Execute a node with the LLM worker, handling tool calls."""
        total_input_tokens = 0
        total_output_tokens = 0
        tool_call_count = 0
        conversation_messages: list[dict[str, str]] = []
        trace_entries: list[dict[str, Any]] = []
        dup_detector = DuplicateWorkDetector()
        parse_failure_count = 0
        # Step 7: per-node budget enforcement inside the worker loop.
        node_budget = NodeExecutionBudget()
        node_budget.start_node(node.id)

        # Build initial context (same as RealNodeWorker).
        dep_text = "\n".join(f"- {d}" for d in context.dependency_summaries) or "None"
        constraint_text = "\n".join(f"- {c}" for c in context.constraints) or "None"
        failure_text = "\n".join(f"- {f}" for f in context.previous_failures) or "None"
        verification_text = (
            "\n".join(context.verification_requirements) or "No specific verification"
        )

        system_prompt = self._worker.prompt_info.content
        initial_user_content = (
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

        conversation_messages.append({"role": "system", "content": system_prompt})
        conversation_messages.append({"role": "user", "content": initial_user_content})

        for round_num in range(_MAX_TOOL_ROUNDS):
            # Step 7: check per-node budget at the start of each iteration.
            if node_budget.is_exhausted(node.id):
                reason = node_budget.get_exhaustion_reason(node.id) or "unknown"
                logger.warning(
                    "Worker %s: per-node budget exhausted at round %d: %s",
                    node.id,
                    round_num,
                    reason,
                )
                trace_entries.append(
                    {
                        "worker_iteration": round_num,
                        "node_id": node.id,
                        "action_type": "node_budget_exhausted",
                        "exhaustion_reason": reason,
                        "total_tool_calls": tool_call_count,
                    }
                )
                return WorkerResult(
                    status="failed",
                    summary=f"Per-node budget exhausted: {reason}",
                    produced_artifacts=[],
                    verification_request=None,
                    graph_patch=[],
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    tool_call_count=tool_call_count,
                )

            request = LLMRequest(
                role="worker",
                messages=list(conversation_messages),
                response_schema={"type": "object"},
                temperature=0.0,
                max_output_tokens=self._worker._max_output_tokens,
                metadata={
                    "prompt_name": self._worker.prompt_info.name,
                    "prompt_version": self._worker.prompt_info.version,
                    "prompt_file_hash": self._worker.prompt_info.file_hash,
                    "node_id": node.id,
                    "node_version": node.version,
                    "round": round_num,
                },
            )

            # Set context on logged client if available.
            if hasattr(self._client, "set_context"):
                self._client.set_context(
                    node_id=node.id,
                    prompt_name=self._worker.prompt_info.name,
                    prompt_version=self._worker.prompt_info.version,
                    prompt_file_hash=self._worker.prompt_info.file_hash,
                )

            response = self._client.generate(request)
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            # Step 7: record model call in per-node budget.
            node_budget.record_model_call(
                node.id, response.usage.input_tokens, response.usage.output_tokens
            )

            # Add assistant response to conversation.
            conversation_messages.append({"role": "assistant", "content": response.text})

            # Parse structured output.
            try:
                parsed = parse_structured(response.text, WorkerOutput)
            except Exception as exc:
                parse_failure_count += 1
                self._parse_failures += 1
                self._total_llm_calls += 1
                # Log parse failure.
                trace_entries.append(
                    {
                        "worker_iteration": round_num,
                        "node_id": node.id,
                        "action_type": "parse_failed",
                        "error": str(exc)[:500],
                        "raw_response_length": len(response.text),
                    }
                )
                logger.warning(
                    "Worker %s round %d: parse failure: %s",
                    node.id,
                    round_num,
                    str(exc)[:200],
                )
                # Step 4: Return parse_failed status instead of claim_done.
                # This lets the controller handle it without counting as a full attempt.
                return WorkerResult(
                    status="parse_failed",
                    summary=f"Structured output parse failure: {str(exc)[:200]}",
                    produced_artifacts=[],
                    verification_request=None,
                    graph_patch=[],
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    tool_call_count=tool_call_count,
                )

            if parsed.action_type == "tool_call" and parsed.tool_request:
                # Normalize tool name (Step 6).
                raw_tool_name = parsed.tool_request.tool_name
                canonical_tool_name = normalize_tool_name(raw_tool_name)

                tool_req = parsed.tool_request
                arguments = dict(tool_req.arguments)
                arguments.setdefault("timeout_seconds", tool_req.timeout_seconds)

                # Step 6: Check for duplicate work before executing.
                dup_result = dup_detector.check_and_record(
                    node_id=node.id,
                    tool_name=canonical_tool_name,
                    arguments=arguments,
                )
                if dup_result.blocked:
                    # Block this call and provide feedback to the worker.
                    conversation_messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Tool call blocked: {dup_result.feedback}\n"
                                f"You have repeated this exact action too many times. "
                                f"Try a different approach or use claim_done if the task is complete."
                            ),
                        }
                    )
                    trace_entries.append(
                        {
                            "worker_iteration": round_num,
                            "node_id": node.id,
                            "action_type": "tool_call_blocked",
                            "requested_tool": raw_tool_name,
                            "normalized_tool": canonical_tool_name,
                            "block_reason": dup_result.feedback,
                        }
                    )
                    continue
                elif dup_result.duplicate and dup_result.feedback:
                    # Provide duplicate warning feedback to the worker.
                    conversation_messages.append(
                        {
                            "role": "user",
                            "content": f"Duplicate warning: {dup_result.feedback}",
                        }
                    )

                # Generate idempotency key.
                idem_key = hashlib.sha256(
                    f"{node.id}:tool{tool_call_count}:{json.dumps(arguments, sort_keys=True)}".encode()
                ).hexdigest()[:16]

                tr = ToolRequest(
                    tool_name=canonical_tool_name,
                    arguments=arguments,
                    timeout_seconds=tool_req.timeout_seconds,
                    idempotency_key=idem_key,
                )

                tool_execution_status = "unknown"
                produced_artifacts: list[str] = []

                try:
                    result = self._tool_runtime.execute(node.run_id, node.id, tr)
                    tool_call_count += 1
                    # Step 7: record tool call in per-node budget.
                    node_budget.record_tool_call(node.id)
                    tool_execution_status = "completed" if result.success else "failed"

                    # Track produced artifacts.
                    if result.environment_delta.get("file_written"):
                        produced_artifacts.append(result.environment_delta["file_written"])

                    # Add tool result to conversation.
                    tool_result_text = result.stdout or result.stderr or "(no output)"
                    if len(tool_result_text) > 5000:
                        tool_result_text = tool_result_text[:5000] + "...[truncated]"
                    conversation_messages.append(
                        {
                            "role": "user",
                            "content": f"Tool result ({canonical_tool_name}):\n{tool_result_text}",
                        }
                    )

                    trace_entries.append(
                        {
                            "worker_iteration": round_num,
                            "node_id": node.id,
                            "action_type": "tool_call",
                            "requested_tool": raw_tool_name,
                            "normalized_tool": canonical_tool_name,
                            "tool_arguments_hash": idem_key,
                            "tool_execution_status": tool_execution_status,
                            "produced_artifacts": produced_artifacts,
                            "tool_exit_code": result.exit_code,
                        }
                    )

                    logger.info(
                        "Worker %s round %d: tool_call %s -> %s",
                        node.id,
                        round_num,
                        canonical_tool_name,
                        tool_execution_status,
                    )

                except Exception as exc:
                    tool_execution_status = "error"
                    error_msg = str(exc)[:500]
                    conversation_messages.append(
                        {
                            "role": "user",
                            "content": (
                                f"Tool error ({canonical_tool_name}): {error_msg}\n"
                                f"This tool call failed. Please try a different approach "
                                f"or use a different tool."
                            ),
                        }
                    )

                    trace_entries.append(
                        {
                            "worker_iteration": round_num,
                            "node_id": node.id,
                            "action_type": "tool_call",
                            "requested_tool": raw_tool_name,
                            "normalized_tool": canonical_tool_name,
                            "tool_arguments_hash": idem_key,
                            "tool_execution_status": tool_execution_status,
                            "error": error_msg,
                        }
                    )

                    logger.warning(
                        "Worker %s round %d: tool_call %s failed: %s",
                        node.id,
                        round_num,
                        canonical_tool_name,
                        error_msg,
                    )

            elif parsed.action_type == "claim_done":
                trace_entries.append(
                    {
                        "worker_iteration": round_num,
                        "node_id": node.id,
                        "action_type": "claim_done",
                        "summary": parsed.summary[:200],
                        "produced_artifacts": parsed.produced_artifacts,
                        "total_tool_calls": tool_call_count,
                    }
                )

                logger.info(
                    "Worker %s round %d: claim_done (tool_calls=%d)",
                    node.id,
                    round_num,
                    tool_call_count,
                )

                # Log the full trace for debugging.
                for entry in trace_entries:
                    logger.debug("Worker trace: %s", json.dumps(entry, default=str))

                return WorkerResult(
                    status="claimed_done",
                    summary=parsed.summary,
                    produced_artifacts=parsed.produced_artifacts,
                    verification_request=parsed.verification_request,
                    graph_patch=parsed.suggested_graph_patch,
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    tool_call_count=tool_call_count,
                )
            else:
                # Unknown action — treat as claim_done.
                trace_entries.append(
                    {
                        "worker_iteration": round_num,
                        "node_id": node.id,
                        "action_type": f"unknown:{parsed.action_type}",
                        "summary": parsed.summary[:200],
                    }
                )

                logger.warning(
                    "Worker %s round %d: unknown action_type %s",
                    node.id,
                    round_num,
                    parsed.action_type,
                )

                return WorkerResult(
                    status="claimed_done",
                    summary=parsed.summary or "Unknown action, claiming done.",
                    produced_artifacts=parsed.produced_artifacts,
                    verification_request=parsed.verification_request,
                    graph_patch=parsed.suggested_graph_patch,
                    input_tokens=total_input_tokens,
                    output_tokens=total_output_tokens,
                    tool_call_count=tool_call_count,
                )

        # Budget exhausted: return what we have.
        trace_entries.append(
            {
                "worker_iteration": _MAX_TOOL_ROUNDS,
                "node_id": node.id,
                "action_type": "budget_exhausted",
                "total_tool_calls": tool_call_count,
            }
        )

        logger.warning(
            "Worker %s: exhausted %d tool rounds without claiming done",
            node.id,
            _MAX_TOOL_ROUNDS,
        )

        return WorkerResult(
            status="failed",
            summary=f"Exceeded {_MAX_TOOL_ROUNDS} tool rounds without claiming done.",
            produced_artifacts=[],
            verification_request=None,
            graph_patch=[],
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            tool_call_count=tool_call_count,
        )
