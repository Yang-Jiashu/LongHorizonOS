"""llm_judge verifier: last resort only (spec 14.2, 29).

Disabled by default (``verification.allow_llm_judge: false``); raises unless
explicitly enabled in config. Even when enabled, the MVP only accepts an
injected LLM port — real API calls are out of scope.
"""

from __future__ import annotations

from lhos.domain.errors import LlmJudgeDisabledError, VerificationError
from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext


class LlmJudgeVerifier:
    verifier_type = "llm_judge"

    def __init__(self, allow_llm_judge: bool = False, llm=None):  # noqa: ANN001
        self._allowed = allow_llm_judge
        self._llm = llm

    def verify(self, node: GraphNode, spec: VerificationSpec,
               context: VerificationContext) -> VerificationResult:
        if not self._allowed:
            raise LlmJudgeDisabledError(
                "llm_judge is disabled (verification.allow_llm_judge: false)"
            )
        if self._llm is None:
            raise VerificationError("llm_judge enabled but no LLM port injected")
        rubric = spec.parameters.get("rubric", "Is the task complete?")
        prompt = (
            f"Rubric: {rubric}\n\nNode: {node.title}\n{node.specification}\n\n"
            f"Worker summary: {context.worker_result.get('summary', '')}\n\n"
            "Answer PASS or FAIL followed by one sentence."
        )
        response = self._llm.complete(prompt, model="judge", temperature=0.0)
        passed = response.text.strip().upper().startswith("PASS")
        return VerificationResult(
            passed=passed,
            summary=f"llm_judge: {response.text.strip()[:200]}",
            evidence=[
                {
                    "evidence_type": "llm_judge",
                    "summary": response.text.strip()[:400],
                    "metadata": {"rubric": rubric},
                }
            ]
            if passed
            else [],
        )
