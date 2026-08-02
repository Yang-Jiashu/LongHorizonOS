"""Executor agent shell (Phase 2 preview).

Wraps the LLM port with the execute_node prompt and parses the structured
worker output of spec 12.1. When no LLM is attached it falls back to the
node's scripted metadata (identical to the FakeWorker contract), so tests and
the CLI never need a real model.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from lhos.infrastructure.llm.structured_output import parse_with_retry
from lhos.runtime.worker import WorkerResult

PROMPT_PATH = Path(__file__).parent / "prompts" / "execute_node.md"
PROMPT_VERSION = "execute_node.v1"


class ExecutorOutput(BaseModel):
    status: str = "claimed_done"
    summary: str = ""
    produced_artifacts: list[dict] = Field(default_factory=list)
    verification_request: dict | None = None
    graph_patch: list[dict] = Field(default_factory=list)


class ExecutorAgent:
    def __init__(self, llm, model: str = "mock-worker"):  # noqa: ANN001
        self._llm = llm
        self._model = model
        self._template = PROMPT_PATH.read_text(encoding="utf-8")

    def execute(self, node, context) -> WorkerResult:  # noqa: ANN001
        prompt = (
            f"{self._template}\n\n## Context Packet\n\n"
            f"Global goal: {context.global_goal}\n"
            f"Current task: {context.current_task}\n"
            f"Constraints: {context.constraints}\n"
            f"Dependencies: {context.dependency_summaries}\n"
            f"Verification: {context.verification_requirements}\n"
            f"Previous failures: {context.previous_failures}\n"
        )
        output = parse_with_retry(
            self._llm, prompt, ExecutorOutput, model=self._model, temperature=0.0
        )
        return WorkerResult(
            status=output.status,
            summary=output.summary,
            produced_artifacts=output.produced_artifacts,
            verification_request=output.verification_request,
            graph_patch=output.graph_patch,
        )
