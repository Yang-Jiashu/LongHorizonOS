"""LongHorizonOS Public SDK — Task developer abstraction (E1).

A `Task` is a DTO that compiles into a real VPG Task node + depends_on edges +
an optional verification/evidence guardian.  It is NOT a second graph/semantic
store — the VPG remains the semantic authority.
"""

from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from lhos.runtimes.multi_agent import ResourceVector

from .errors import ConfigurationError
from .verification import Verifier


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
        resource_input = resources
        scheduler_metadata = self.metadata.get("scheduler")
        if resource_input is None and isinstance(scheduler_metadata, dict):
            resource_input = scheduler_metadata.get("resources")
        self.resources = _coerce_resources(resource_input, field_name="Task.resources")

    @property
    def dependency_ids(self) -> tuple[str, ...]:
        return tuple(t.task_id for t in self.depends_on)

    def __repr__(self) -> str:
        return f"Task(task_id={self.task_id!r})"
