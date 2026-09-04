"""LongHorizonOS Public SDK — Goal developer abstraction (E1).

A `Goal` holds Tasks and compiles into a real VPG Goal node + Tasks + depends_on
Edges via a single `GraphPatchProposal`.  The VPG remains the semantic authority.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from lhos.agent_os.context.models import ContextManifest
from lhos.provenance import CoveragePolicy
from lhos.runtimes.multi_agent import ResourceVector

from .task import ExecutorAPI, Task, _coerce_executor_api, _coerce_provenance_policy

if TYPE_CHECKING:
    from .os import AgentOS


class Goal:
    def __init__(
        self,
        goal_id: str,
        *,
        tasks: tuple[Task, ...] = (),
        inputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        outputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        provenance_policy: CoveragePolicy | str | None = None,
        executor_api: ExecutorAPI | str | None = None,
    ) -> None:
        self.goal_id = goal_id
        self.inputs = (
            tuple(inputs)
            if inputs is not None and not isinstance(inputs, str)
            else ((inputs,) if isinstance(inputs, str) else ())
        )
        if isinstance(inputs, Mapping):
            self.inputs = tuple(inputs.keys())
        self.outputs = (
            tuple(outputs)
            if outputs is not None and not isinstance(outputs, str)
            else ((outputs,) if isinstance(outputs, str) else ())
        )
        if isinstance(outputs, Mapping):
            self.outputs = tuple(outputs.keys())
        self.provenance_policy = _coerce_provenance_policy(provenance_policy)
        self.executor_api = _coerce_executor_api(
            executor_api,
            field_name="Goal.executor_api",
            allow_none=True,
        )
        self.tasks: list[Task] = []
        for t in tasks:
            self.add_task(t)

    def task(
        self,
        task_id: str,
        *,
        agent: str = "",
        depends_on: tuple[Task, ...] = (),
        verify=None,
        task_kind: str = "task",
        required_specializations: tuple[str, ...] | None = None,
        required_tools: tuple[str, ...] = (),
        max_attempts: int | None = 3,
        metadata: dict | None = None,
        resources: ResourceVector | dict[str, Any] | None = None,
        inputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        outputs: Iterable[str] | Mapping[str, Any] | str | None = None,
        provenance_policy: CoveragePolicy | str | None = None,
        executor_api: ExecutorAPI | str | None = None,
        context_manifest: ContextManifest | dict[str, Any] | None = None,
    ) -> Task:
        if inputs is None:
            inputs = self.inputs
        if outputs is None:
            outputs = self.outputs
        if provenance_policy is None:
            provenance_policy = self.provenance_policy
        if executor_api is None:
            executor_api = self.executor_api
        t = Task(
            task_id,
            agent=agent,
            depends_on=depends_on,
            verify=verify,
            task_kind=task_kind,
            required_specializations=required_specializations,
            required_tools=required_tools,
            max_attempts=max_attempts,
            metadata=metadata,
            resources=resources,
            inputs=inputs,
            outputs=outputs,
            provenance_policy=provenance_policy,
            executor_api=executor_api,
            context_manifest=context_manifest,
        )
        self.add_task(t)
        return t

    def add_task(self, task: Task) -> None:
        self.tasks.append(task)

    def compile(self, os: AgentOS):
        """Compile this Goal + Tasks into a real VPG GraphPatch for the OS facade."""
        return os._compile_goal(self)

    def __repr__(self) -> str:
        return f"Goal(goal_id={self.goal_id!r}, tasks={[t.task_id for t in self.tasks]!r})"
