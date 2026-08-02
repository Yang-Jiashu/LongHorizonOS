"""Semantic reconciler stub (spec 8.3): deterministic rules come first.

This shell renders the reconcile_event prompt and would parse a graph patch
from an LLM. In the MVP it is never invoked unless explicitly wired in, and it
raises without an LLM port so accidental semantic reconciliation cannot slip
into deterministic experiments.
"""

from __future__ import annotations

from pathlib import Path

from lhos.domain.errors import LhosError

PROMPT_PATH = Path(__file__).parent / "prompts" / "reconcile_event.md"
PROMPT_VERSION = "reconcile_event.v1"


class SemanticReconcilerStub:
    def __init__(self, llm=None, model: str = "mock-reconciler"):  # noqa: ANN001
        self._llm = llm
        self._model = model
        self._template = PROMPT_PATH.read_text(encoding="utf-8")

    def reconcile(self, run_id: str, event) -> bool:  # noqa: ANN001
        if self._llm is None:
            raise LhosError(
                "semantic reconciler has no LLM configured; deterministic "
                "reconciliation should have handled this event"
            )
        raise NotImplementedError("semantic reconciliation arrives in Phase 2")
