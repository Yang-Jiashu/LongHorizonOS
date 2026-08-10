"""LongHorizonOS Public SDK — Goal developer abstraction (E1).

A `Goal` holds Tasks and compiles into a real VPG Goal node + Tasks + depends_on
Edges via a single `GraphPatchProposal`.  The VPG remains the semantic authority.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .task import Task

if TYPE_CHECKING:
    from .os import AgentOS


class Goal:
    def __init__(self, goal_id: str, *, tasks: tuple[Task, ...] = ()) -> None:
        self.goal_id = goal_id
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
    ) -> Task:
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
