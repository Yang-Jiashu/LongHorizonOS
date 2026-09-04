"""Real Initial Planner (spec Phase 2C-C1).

Uses ``LLMClient`` to generate an initial task graph from a natural-language
goal. The planner only sees public information — no hidden oracle, no hidden
tests, no future failures.

The planner output is a Graph Patch (list of add_node / add_edge operations)
that is validated by the Patch Validator before being applied.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from lhos.agents.prompt_manager import PromptManager
from lhos.domain.errors import StructuredOutputError
from lhos.infrastructure.llm.structured_output import parse_structured
from lhos.ports.llm import LLMClient, LLMRequest

PROMPT_NAME = "initial_planner"
PROMPT_VERSION = "v1"


class PlannerOperation(BaseModel):
    op: str
    payload: dict[str, Any]


class PlannerOutput(BaseModel):
    """Structured output from the Initial Planner."""

    operations: list[PlannerOperation]
    planning_summary: str = ""
    open_questions: list[str] = Field(default_factory=list)


class RealInitialPlanner:
    """Real planner backed by an LLM.

    Parameters
    ----------
    client : LLMClient
        The LLM client (SenseNovaClient, MockLLMClient, etc.).
    model_id : str
        Exact model identifier (never silently changed).
    """

    def __init__(
        self,
        client: LLMClient,
        model_id: str = "sensenova-6.7-flash-lite",
        max_output_tokens: int = 8192,
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

    def plan(
        self,
        goal: str,
        environment: str = "",
        tools: str = "",
        budget: str = "",
        constraints: str = "",
    ) -> dict[str, Any]:
        """Generate an initial task graph.

        Returns a dict with 'operations', 'planning_summary', 'open_questions'.
        The operations are a Graph Patch to be validated by the Patch Validator.
        """
        user_content = (
            f"## Task\n\n"
            f"Goal: {goal}\n"
            f"Environment: {environment}\n"
            f"Tools: {tools}\n"
            f"Budget: {budget}\n"
            f"Constraints: {constraints}\n"
        )

        request = LLMRequest(
            role="planner",
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
            },
        )

        response = self._client.generate(request)

        try:
            parsed = parse_structured(response.text, PlannerOutput)
        except StructuredOutputError:
            # If the LLM returned the old format (nodes/edges), adapt it.
            import json

            try:
                raw = json.loads(response.text.strip())
                if "nodes" in raw or "edges" in raw:
                    ops = []
                    for node in raw.get("nodes", []):
                        ops.append({"op": "add_node", "payload": node})
                    for edge in raw.get("edges", []):
                        ops.append({"op": "add_edge", "payload": edge})
                    parsed = PlannerOutput(
                        operations=[PlannerOperation(**op) for op in ops],
                        planning_summary="adapted from nodes/edges format",
                    )
                else:
                    raise
            except Exception as exc:
                raise StructuredOutputError(f"Planner output could not be parsed: {exc}") from exc

        if not parsed.operations:
            raise StructuredOutputError("planner returned an empty graph")

        return parsed.model_dump()
