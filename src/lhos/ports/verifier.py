"""Verifier port (spec section 14)."""

from typing import Any, Protocol

from pydantic import BaseModel, Field

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec


class VerificationContext(BaseModel):
    """Everything a deterministic verifier needs."""

    model_config = {"arbitrary_types_allowed": True}

    run_id: str
    workspace_dir: str
    worker_result: dict[str, Any] = Field(default_factory=dict)
    baseline_hashes: dict[str, str | None] = Field(default_factory=dict)


class Verifier(Protocol):
    verifier_type: str

    def verify(
        self,
        node: GraphNode,
        spec: VerificationSpec,
        context: VerificationContext,
    ) -> VerificationResult: ...
