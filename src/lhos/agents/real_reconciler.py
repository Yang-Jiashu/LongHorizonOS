"""Real Semantic Reconciler (spec Phase 2C-C3).

Uses ``LLMClient`` to handle events that deterministic rules cannot classify.
Only invoked when explicit conditions are met (unmapped evidence, ambiguous
requirement change, etc.). Never called every round.

Output is a local Graph Patch with affected node IDs and a confidence score.
Low-confidence patches do not include destructive operations.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from pydantic import BaseModel, Field

from lhos.agents.prompt_manager import PromptManager
from lhos.domain.errors import StructuredOutputError
from lhos.infrastructure.llm.structured_output import parse_structured
from lhos.ports.llm import LLMClient, LLMRequest

PROMPT_NAME = "semantic_reconciler"
PROMPT_VERSION = "v1"


class ReconcilerOperation(BaseModel):
    op: str
    target_id: str | None = None
    expected_version: int | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ReconcilerOutput(BaseModel):
    """Structured output from the Semantic Reconciler."""

    operations: list[ReconcilerOperation]
    affected_node_ids: list[str] = Field(default_factory=list)
    explanation: str = ""
    confidence: float = 0.0


class RealSemanticReconciler:
    """Real reconciler backed by an LLM.

    Only called when deterministic rules cannot handle an event.
    """

    # Invocation conditions (spec 2C-C3).
    INVOKE_REASONS: ClassVar[frozenset[str]] = frozenset(
        {
            "unmapped_semantic_evidence",
            "ambiguous_requirement_change",
            "newly_discovered_necessary_subtask",
            "uncertain_invalidation_scope",
            "user_goal_modification",
        }
    )

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
        self._invocation_count = 0

    @property
    def prompt_info(self):
        return self._prompt_info

    @property
    def invocation_count(self) -> int:
        return self._invocation_count

    def should_invoke(self, reason: str) -> bool:
        """Check if the reconciler should be invoked for this reason."""
        return reason in self.INVOKE_REASONS

    def reconcile(
        self,
        event: dict[str, Any],
        subgraph: dict[str, Any],
        evidence: list[dict[str, Any]] | None = None,
        constraints: list[str] | None = None,
        affected_versions: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """Reconcile an event that deterministic rules couldn't handle.

        Returns a dict with 'operations', 'affected_node_ids', 'explanation',
        'confidence'. Operations are a Graph Patch to be validated.
        """
        self._invocation_count += 1

        user_content = (
            f"## Event\n{json.dumps(event, indent=2, default=str)}\n\n"
            f"## Local Subgraph\n{json.dumps(subgraph, indent=2, default=str)}\n\n"
            f"## Evidence\n{json.dumps(evidence or [], indent=2, default=str)}\n\n"
            f"## Active Constraints\n{json.dumps(constraints or [], indent=2, default=str)}\n\n"
            f"## Affected Node Versions\n{json.dumps(affected_versions or {}, indent=2, default=str)}\n"
        )

        request = LLMRequest(
            role="reconciler",
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
                "invocation_count": self._invocation_count,
            },
        )

        response = self._client.generate(request)

        try:
            parsed = parse_structured(response.text, ReconcilerOutput)
        except StructuredOutputError as exc:
            raise StructuredOutputError(f"Reconciler output could not be parsed: {exc}") from exc

        # Enforce low-confidence safety: filter destructive ops if confidence < 0.5.
        if parsed.confidence < 0.5:
            destructive_ops = {"mark_stale", "mark_invalidated", "remove_node", "remove_edge"}
            filtered = [op for op in parsed.operations if op.op not in destructive_ops]
            if len(filtered) < len(parsed.operations):
                parsed.explanation += (
                    " [NOTE: destructive operations were filtered due to low confidence]"
                )
            parsed.operations = filtered

        return parsed.model_dump()
