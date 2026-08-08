"""Agent Registry.

Section 9: the registry holds scheduling metadata, NOT process liveness.
Whether a process is really alive is always looked up via the injected
ProcessProvider at eligibility time.
"""

from __future__ import annotations

import threading
from typing import Any

from .models import AgentDescriptor


class AgentRegistry:
    """Thread-safe in-memory agent registry with CRUD operations.

    A registration MAY validate the process_id and required scheduler
    capability against a live ProcessProvider / CapabilityProvider at
    register() time — but validation is optional and injector-driven.
    """

    def __init__(self) -> None:
        self._agents: dict[str, AgentDescriptor] = {}
        self._lock = threading.RLock()

    def register(self, agent: AgentDescriptor) -> AgentDescriptor:
        with self._lock:
            if agent.agent_id in self._agents:
                raise ValueError(
                    f"agent_id {agent.agent_id!r} already registered"
                )
            self._agents[agent.agent_id] = agent
            return agent

    def get(self, agent_id: str) -> AgentDescriptor | None:
        with self._lock:
            return self._agents.get(agent_id)

    def list(
        self,
        *,
        enabled_only: bool = False,
    ) -> list[AgentDescriptor]:
        with self._lock:
            agents = list(self._agents.values())
        if enabled_only:
            agents = [a for a in agents if a.enabled]
        return agents

    def update(self, agent_id: str, fields: dict[str, Any] | None = None, **kw: Any) -> AgentDescriptor:
        """Patch agent fields; returns the updated descriptor.

        Prefer the ``fields`` dict — passing ``agent_id`` as a keyword would
        collide with the positional arg.
        """
        merged: dict[str, Any] = dict(fields or {})
        merged.update(kw)
        with self._lock:
            cur = self._agents.get(agent_id)
            if cur is None:
                raise KeyError(agent_id)
            data = cur.model_dump()
            for k, v in merged.items():
                if k == "agent_id":
                    raise ValueError("agent_id is immutable")
                if k not in data:
                    raise ValueError(f"unknown field: {k}")
                data[k] = v
            updated = AgentDescriptor(**data)
            self._agents[agent_id] = updated
            return updated

    def set_enabled(self, agent_id: str, enabled: bool) -> AgentDescriptor:
        return self.update(agent_id, enabled=enabled)

    def enable(self, agent_id: str) -> AgentDescriptor:
        return self.set_enabled(agent_id, True)

    def disable(self, agent_id: str) -> AgentDescriptor:
        return self.set_enabled(agent_id, False)

    def remove(self, agent_id: str) -> AgentDescriptor:
        with self._lock:
            if agent_id not in self._agents:
                raise KeyError(agent_id)
            return self._agents.pop(agent_id)

    def clear(self) -> None:
        with self._lock:
            self._agents.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._agents)

    def __contains__(self, agent_id: str) -> bool:
        with self._lock:
            return agent_id in self._agents

    def snapshot(self) -> dict[str, AgentDescriptor]:
        """Return a shallow copy of the registry contents."""
        with self._lock:
            return dict(self._agents)
