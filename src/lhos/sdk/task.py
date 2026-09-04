"""LongHorizonOS Public SDK — Task developer abstraction (E1).

A `Task` is a DTO that compiles into a real VPG Task node + depends_on edges +
an optional verification/evidence guardian.  It is NOT a second graph/semantic
store — the VPG remains the semantic authority.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal

from pydantic import ValidationError

from lhos.agent_os.context.models import ContextManifest
from lhos.provenance import CoveragePolicy
from lhos.runtimes.multi_agent import ResourceVector

from .errors import ConfigurationError
from .verification import Verifier

ExecutorAPI = Literal["legacy_task_id", "context_v1"]


def _coerce_executor_api(
    value: ExecutorAPI | str | None,
    *,
    field_name: str = "executor_api",
    allow_none: bool = False,
) -> ExecutorAPI | None:
    """Validate the small public executor calling convention enum."""

    if value is None and allow_none:
        return None
    if value is None:
        return "legacy_task_id"
    normalized = str(value).strip().lower()
    if normalized not in {"legacy_task_id", "context_v1"}:
        raise ConfigurationError(
            f"{field_name} must be 'legacy_task_id' or 'context_v1', got {value!r}"
        )
    return normalized  # type: ignore[return-value]


def _coerce_string_tuple(
    value: Iterable[str] | Mapping[str, Any] | str | None,
    *,
    field_name: str,
) -> tuple[str, ...]:
    """Normalize declared provenance resource identifiers deterministically."""

    if value is None:
        return ()
    if isinstance(value, str):
        values: Iterable[Any] = (value,)
    elif isinstance(value, Mapping):
        values = value.keys()
    else:
        values = value
    result = tuple(sorted({str(item).strip() for item in values if str(item).strip()}))
    return result


def _coerce_provenance_policy(
    value: CoveragePolicy | str | None,
    *,
    field_name: str = "provenance_policy",
) -> CoveragePolicy:
    if value is None:
        return CoveragePolicy.LEGACY
    try:
        return value if isinstance(value, CoveragePolicy) else CoveragePolicy(str(value).lower())
    except ValueError as exc:
        raise ConfigurationError(
            f"{field_name} must be 'legacy', 'audit', or 'strict', got {value!r}"
        ) from exc


def _coerce_resources(
    value: ResourceVector | dict[str, Any] | None,
    *,
    field_name: str,
) -> ResourceVector:
    """Validate an SDK resource vector without accepting silent coercions."""
    if value is None:
        return ResourceVector()
    if isinstance(value, ResourceVector):
        raw = value.model_dump(mode="python")
    elif isinstance(value, dict):
        raw = dict(value)
    else:
        raise ConfigurationError(
            f"{field_name} must be a ResourceVector or dict, got {type(value).__name__}"
        )

    unknown = sorted(set(raw) - set(ResourceVector.model_fields))
    if unknown:
        raise ConfigurationError(
            f"{field_name} contains unknown resource fields: {', '.join(unknown)}"
        )
    try:
        return ResourceVector.model_validate(raw, strict=True)
    except ValidationError as exc:
        raise ConfigurationError(f"invalid {field_name}", cause=exc) from exc


def _coerce_context_manifest(
    value: ContextManifest | dict[str, Any] | None,
    *,
    field_name: str,
) -> ContextManifest | None:
    """Validate an explicit Context VM manifest without inferring dependencies."""

    if value is None:
        return None
    if isinstance(value, ContextManifest):
        return value
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"{field_name} must be a ContextManifest or dict, got {type(value).__name__}"
        )
    try:
        return ContextManifest.model_validate(value)
    except ValidationError as exc:
        raise ConfigurationError(f"invalid {field_name}", cause=exc) from exc


class Task:
    def __init__(
        self,
        task_id: str,
        *,
        agent: str = "",
        depends_on: tuple[Task, ...] = (),
        verify: Verifier | None = None,
        task_kind: str = "task",
        required_specializations: tuple[str, ...] | None = None,
        required_tools: tuple[str, ...] = (),
        max_attempts: int | None = 3,
        metadata: dict[str, Any] | None = None,
        resources: ResourceVector | dict[str, Any] | None = None,
        inputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        outputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        provenance_policy: CoveragePolicy | str | None = None,
        executor_api: ExecutorAPI | str | None = None,
        context_manifest: ContextManifest | dict[str, Any] | None = None,
    ) -> None:
        self.task_id = task_id
        self.agent = agent  # preferred agent id ("" = any eligible)
        self.depends_on: tuple[Task, ...] = tuple(depends_on)
        self.verify = verify  # optional verifier / executor
        self.task_kind = task_kind
        self.required_specializations = required_specializations or ("python",)
        self.required_tools = tuple(required_tools)
        self.max_attempts = max_attempts
        self.metadata = dict(metadata or {})
        self.inputs = _coerce_string_tuple(inputs, field_name="Task.inputs")
        self.outputs = _coerce_string_tuple(outputs, field_name="Task.outputs")
        self.provenance_policy = _coerce_provenance_policy(provenance_policy)
        self.executor_api = _coerce_executor_api(
            executor_api,
            field_name="Task.executor_api",
            allow_none=True,
        )
        self.context_manifest = _coerce_context_manifest(
            context_manifest,
            field_name="Task.context_manifest",
        )
        resource_input = resources
        scheduler_metadata = self.metadata.get("scheduler")
        if resource_input is None and isinstance(scheduler_metadata, dict):
            resource_input = scheduler_metadata.get("resources")
        self.resources = _coerce_resources(resource_input, field_name="Task.resources")

    @property
    def dependency_ids(self) -> tuple[str, ...]:
        return tuple(t.task_id for t in self.depends_on)

    @property
    def declared_inputs(self) -> tuple[str, ...]:
        """Compatibility alias used by provenance adapters."""

        return self.inputs

    @property
    def declared_outputs(self) -> tuple[str, ...]:
        """Compatibility alias used by provenance adapters."""

        return self.outputs

    def __repr__(self) -> str:
        return f"Task(task_id={self.task_id!r})"
