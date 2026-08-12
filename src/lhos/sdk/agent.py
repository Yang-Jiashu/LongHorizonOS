"""LongHorizonOS Public SDK — Agent developer abstraction (E1).

An `Agent` is a developer-facing object that maps onto Core primitives:
- a real Kernel Process (`_process_service.spawn`),
- an `AgentDescriptor` in the Registry (specializations, concurrency, cost),
- a capability set (optional),
- an executor (callable or Verifier) that performs the task when scheduled.

The SDK registers this Agent with the D2 Scheduler; eligibility + matching +
Kernel-Lease ownership are all Core/D2 — the SDK does NOT bypass them.
"""

from __future__ import annotations

import inspect
from typing import Any

from lhos.runtimes.multi_agent import ResourceVector

from .errors import ConfigurationError
from .task import _coerce_resources


class Agent:
    """A developer-facing agent definition.

    Parameters
    ----------
    name : str
        unique agent id.
    executor : callable | None
        Optional sync or async executor: ``callable(task_id) -> object``.
        ``AgentOS.run`` accepts only synchronous executors; use
        ``await AgentOS.run_async(...)`` when this callable is asynchronous.
        A returned ``VerificationOutcome`` remains the legacy combined
        executor/verifier path when the Task has no independent verifier.
    specializations : tuple[str, ...]
        matched against task required_specializations for deterministic matching.
    supported_task_kinds : tuple[str, ...] | None
        None => ("*",) (any kind).
    supported_tools : tuple[str, ...] | None
        Tools this Agent can execute for scheduler eligibility.
    capabilities : tuple[str, ...] | None
        resource patterns to grant (e.g. ("shell", "filesystem")).  If None, the
        agent gets a default broad grant for demo simplicity.
    max_concurrency : int
    cost_weight : float
    resource_capacity : ResourceVector | dict[str, Any] | None
        Total CPU/RAM/GPU/VRAM/model-slot capacity schedulable on this agent.
    """

    def __init__(
        self,
        name: str,
        *,
        executor: Any = None,
        specializations: tuple[str, ...] = ("python",),
        supported_task_kinds: tuple[str, ...] | None = None,
        supported_tools: tuple[str, ...] | None = None,
        capabilities: tuple[str, ...] | None = None,
        max_concurrency: int = 4,
        cost_weight: float = 1.0,
        model: str | None = None,
        resource_capacity: ResourceVector | dict[str, Any] | None = None,
    ) -> None:
        if not name:
            raise ConfigurationError("Agent name must be non-empty")
        self.name = name
        self.executor = executor
        self.specializations = tuple(specializations)
        self.supported_task_kinds = supported_task_kinds or ("*",)
        self.supported_tools = None if supported_tools is None else tuple(supported_tools)
        self.capabilities = capabilities
        self.max_concurrency = max_concurrency
        self.cost_weight = cost_weight
        self.model = model
        self.resource_capacity = _coerce_resources(
            resource_capacity,
            field_name="Agent.resource_capacity",
        )
        self._process_id: str | None = None

    @property
    def process_id(self) -> str | None:
        return self._process_id

    @property
    def executor_is_async(self) -> bool:
        """Whether the configured executor requires ``AgentOS.run_async``."""
        return self.executor is not None and _is_async_callable(self.executor)

    def _bind_process(self, process_id: str) -> None:
        self._process_id = process_id

    def __repr__(self) -> str:
        return f"Agent(name={self.name!r}, specializations={self.specializations!r})"


def _is_async_callable(value: Any) -> bool:
    """Return whether ``value`` is an async function or async callable object."""
    if inspect.iscoroutinefunction(value):
        return True
    if not callable(value):
        return False
    return inspect.iscoroutinefunction(type(value).__call__)
