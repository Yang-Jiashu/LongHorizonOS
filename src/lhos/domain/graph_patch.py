"""Graph patch operations (spec section 8.2).

LLMs may only emit patches; they may never rewrite the whole graph.
"""

from typing import Any

from pydantic import BaseModel, Field

from lhos.domain.enums import PatchOperationType


class GraphPatchOperation(BaseModel):
    op: PatchOperationType
    target_id: str | None = None
    expected_version: int | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
