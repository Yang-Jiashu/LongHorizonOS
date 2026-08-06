"""GraphPatchProposal + PatchOperation union + helpers.

The patch is the sole legal state-transition mechanism for VPG semantic state.
Every operation passes through the full validation pipeline.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator

from .models import ArtifactVersionBinding


def _uuid() -> str:
    from uuid import uuid4

    return uuid4().hex


def _utcnow() -> datetime:
    return datetime.now(UTC)


class PatchOpType(StrEnum):
    ADD_NODE = "add_node"
    ADD_EDGE = "add_edge"
    ATTACH_ARTIFACT = "attach_artifact"
    ATTACH_EVIDENCE = "attach_evidence"


class AddNodeOp(BaseModel):
    op_type: Literal[PatchOpType.ADD_NODE] = PatchOpType.ADD_NODE

    node_id: str
    graph_id: str
    node_type: str  # "goal" | "task" | "artifact_ref" | "verification" | "evidence"
    created_by_pid: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    # specialized payloads
    title: str = ""
    description: str = ""
    task_kind: str = ""
    execution_spec: dict[str, Any] = Field(default_factory=dict)
    required_verification_count: int = 1

    # artifact_ref fields
    canonical_uri: str = ""
    artifact_id: str = ""
    version: int | None = None
    content_hash: str = ""
    media_type: str = "application/octet-stream"

    # verification fields
    verification_kind: str = "command_result"
    obligation: dict[str, Any] = Field(default_factory=dict)
    source_action_id: str | None = None

    # evidence fields
    evidence_kind: str = "command_result"
    result: str = "inconclusive"  # "pass" | "fail" | "inconclusive"
    source_verification_id: str | None = None
    evidence_source_action_id: str | None = None
    source_event_ids: tuple[str, ...] = ()
    artifact_bindings: tuple[ArtifactVersionBinding, ...] = ()
    evidence_content_ref: ArtifactVersionBinding | None = None
    evidence_hash: str = ""
    produced_by_pid: str = ""

    @field_validator("execution_spec")
    @classmethod
    def _execution_spec_no_secrets(cls, v: dict[str, Any]) -> dict[str, Any]:
        """Forbid host paths / code objects in execution_spec."""
        for key in v:
            lk = key.lower()
            if lk in {"callback", "callable", "fn", "function", "lambda_code", "exec"}:
                raise ValueError(f"execution_spec key '{key}' is forbidden")
        for val in v.values():
            if callable(val):
                raise ValueError("execution_spec may not contain callables")
            if isinstance(val, str) and val.strip().startswith("/"):
                raise ValueError("execution_spec may not contain host absolute paths")
        return v

    @field_validator("metadata")
    @classmethod
    def _metadata_no_secrets(cls, v: dict[str, Any]) -> dict[str, Any]:
        for val in v.values():
            if callable(val):
                raise ValueError("metadata may not contain callables")
        return v


class AddEdgeOp(BaseModel):
    op_type: Literal[PatchOpType.ADD_EDGE] = PatchOpType.ADD_EDGE

    edge_id: str = Field(default_factory=_uuid)
    edge_type: str  # "depends_on" | "produces" | "verifies"
    source_node_id: str
    target_node_id: str
    created_by_pid: str


class AttachArtifactOp(BaseModel):
    """Bind an existing committed ArtifactVersion to a Task node via produces."""

    op_type: Literal[PatchOpType.ATTACH_ARTIFACT] = PatchOpType.ATTACH_ARTIFACT

    task_node_id: str
    artifact: ArtifactVersionBinding
    created_by_pid: str
    edge_id: str = Field(default_factory=_uuid)


class AttachEvidenceOp(BaseModel):
    """Attach an existing EvidenceNode to a VerificationNode via produces."""

    op_type: Literal[PatchOpType.ATTACH_EVIDENCE] = PatchOpType.ATTACH_EVIDENCE

    verification_node_id: str
    evidence_node_id: str
    created_by_pid: str
    edge_id: str = Field(default_factory=_uuid)


PatchOperation = Annotated[
    AddNodeOp | AddEdgeOp | AttachArtifactOp | AttachEvidenceOp,
    Field(discriminator=None),  # runtime dispatches via op_type
]


class GraphPatchProposal(BaseModel):
    """A proposed patch — applied atomically or rejected whole."""

    patch_id: str = Field(default_factory=_uuid)
    graph_id: str

    expected_graph_version: int
    author_pid: str

    # operations to apply within this patch
    operations: tuple[PatchOperation, ...] = ()

    reason: str = ""
    causation_ids: tuple[str, ...] = ()
    idempotency_key: str

    created_at: datetime = Field(default_factory=_utcnow)

    @property
    def composite_key(self) -> tuple[str, str, str]:
        return (self.author_pid, self.graph_id, self.idempotency_key)


class PatchCommitResult(BaseModel):
    """Committed patch — includes the new GraphVersion and derived events."""

    graph_id: str
    patch_id: str
    committed_graph_version: int
    patch_applied: bool
    idempotent_replay: bool = False
    applied_at: datetime = Field(default_factory=_utcnow)
