"""Initial planner shell (Phase 2 preview; works with the MockLLM today).

Given a natural-language goal it renders the initial_plan prompt and parses
the section 8.1 JSON spec. With MockLLM the response is scripted, so the
whole path is testable without a real model.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lhos.infrastructure.llm.structured_output import parse_structured
from lhos.domain.errors import StructuredOutputError
from pydantic import BaseModel

PROMPT_PATH = Path(__file__).parent / "prompts" / "initial_plan.md"
PROMPT_VERSION = "initial_plan.v1"


class InitialPlanOutput(BaseModel):
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]


class InitialPlanner:
    def __init__(self, llm, model: str = "mock-planner"):  # noqa: ANN001
        self._llm = llm
        self._model = model
        self._template = PROMPT_PATH.read_text(encoding="utf-8")

    def plan(
        self,
        goal: str,
        environment: str = "",
        tools: str = "",
        budget: str = "",
        constraints: str = "",
    ) -> dict[str, Any]:
        prompt = (
            f"{self._template}\n\n## Task\n\nGoal: {goal}\n"
            f"Environment: {environment}\nTools: {tools}\n"
            f"Budget: {budget}\nConstraints: {constraints}\n"
        )
        response = self._llm.complete(prompt, model=self._model, temperature=0.0)
        plan = parse_structured(response.text, InitialPlanOutput)
        if not plan.nodes:
            raise StructuredOutputError("planner returned an empty graph")
        return plan.model_dump()
