"""LongHorizonOS Public SDK — Task developer abstraction (E1).

A `Task` is a DTO that compiles into a real VPG Task node + depends_on edges +
an optional verification/evidence guardian.  It is NOT a second graph/semantic
store — the VPG remains the semantic authority.
"""

from __future__ import annotations

from typing import Any

from .verification import Verifier


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
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.task_id = task_id
        self.agent = agent  # preferred agent id ("" = any eligible)
        self.depends_on: tuple[Task, ...] = tuple(depends_on)
        self.verify = verify  # optional verifier / executor
        self.task_kind = task_kind
        self.required_specializations = required_specializations or ("python",)
        self.required_tools = tuple(required_tools)
        self.metadata = dict(metadata or {})

    @property
    def dependency_ids(self) -> tuple[str, ...]:
        return tuple(t.task_id for t in self.depends_on)

    def __repr__(self) -> str:
        return f"Task(task_id={self.task_id!r})"
