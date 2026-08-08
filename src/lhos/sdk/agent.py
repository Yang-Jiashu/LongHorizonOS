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

from typing import Any

from .errors import ConfigurationError


class Agent:
    """A developer-facing agent definition.

    Parameters
    ----------
    name : str
        unique agent id.
    executor : callable | None
        Optional deterministic executor: ``callable(task_id) -> VerificationOutcome``
        (or a Verifier).  When None, a scripted executor is used so no API key is
        required for demos.
    specializations : tuple[str, ...]
        matched against task required_specializations for deterministic matching.
    supported_task_kinds : tuple[str, ...] | None
        None => ("*",) (any kind).
    capabilities : tuple[str, ...] | None
        resource patterns to grant (e.g. ("shell", "filesystem")).  If None, the
        agent gets a default broad grant for demo simplicity.
    max_concurrency : int
    cost_weight : float
    """

    def __init__(
        self,
        name: str,
        *,
        executor: Any = None,
        specializations: tuple[str, ...] = ("python",),
        supported_task_kinds: tuple[str, ...] | None = None,
        capabilities: tuple[str, ...] | None = None,
        max_concurrency: int = 4,
        cost_weight: float = 1.0,
        model: str | None = None,
    ) -> None:
        if not name:
            raise ConfigurationError("Agent name must be non-empty")
        self.name = name
        self.executor = executor
        self.specializations = tuple(specializations)
        self.supported_task_kinds = supported_task_kinds or ("*",)
        self.capabilities = capabilities
        self.max_concurrency = max_concurrency
        self.cost_weight = cost_weight
        self.model = model
        self._process_id: str | None = None

    @property
    def process_id(self) -> str | None:
        return self._process_id

    def _bind_process(self, process_id: str) -> None:
        self._process_id = process_id

    def __repr__(self) -> str:
        return f"Agent(name={self.name!r}, specializations={self.specializations!r})"
