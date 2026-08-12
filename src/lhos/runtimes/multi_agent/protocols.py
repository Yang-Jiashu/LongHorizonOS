"""Dependency-injection provider protocols for the Multi-Agent Scheduler.

The Scheduler package NEVER imports Agent OS Kernel internals directly.
Tests and demos inject concrete providers backed by the real AgentKernel.
This boundary keeps the dependency direction strict (Section 6).

Each protocol models exactly what the Scheduler needs from the underlying
Process / Lease / Capability authority — no more, no less.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Protocol, runtime_checkable


# ── Process liveness / scheduling state ──────────────────────────────────────
@runtime_checkable
class ProcessInfo(Protocol):
    pid: str
    state: str
    capability_set_id: str
    program_id: str


class ProcessProvider(Protocol):
    """Process authority: answers whether a process exists and whether its
    current state allows scheduling a new TaskClaim on it."""

    def get(self, pid: str) -> ProcessInfo | None: ...
    def list_all(self) -> list[ProcessInfo]: ...


# ── Capability authority ─────────────────────────────────────────────────────
class CapabilityProvider(Protocol):
    """Capability authority: answers whether a pid holds a (resource, op) grant."""

    def check(self, pid: str, resource: str, operation: str) -> bool: ...
    def capabilities_for(self, pid: str) -> list[Any]: ...


# ── Lease authority ──────────────────────────────────────────────────────────
class LeaseInfo(Protocol):
    lease_id: str
    resource_id: str
    owner_pid: str
    mode: str
    fencing_token: int
    expires_at: datetime | str | None


class LeaseProvider(Protocol):
    """Lease authority: grant/release/query the Kernel's exclusive-resource
    leases.  The Scheduler never constructs leases directly — it asks the
    provider, because real ownership linearizes at Kernel Lease acquisition."""

    def acquire_exclusive(
        self,
        pid: str,
        resource_id: str,
        ttl: timedelta,
    ) -> LeaseInfo | None:
        """Acquire an exclusive lease. Returns the lease on success, None on
        refusal (another owner already holds it, or another exclusive)."""

    def release(self, lease_id: str) -> bool: ...
    def release_all_for_pid(self, pid: str) -> int: ...
    def get(self, lease_id: str) -> LeaseInfo | None: ...
    def list_for_resource(self, resource_id: str) -> list[LeaseInfo]: ...
    def list_for_pid(self, pid: str) -> list[LeaseInfo]: ...
    def reclaim_expired(self) -> int: ...


# ── Dispatcher protocol ──────────────────────────────────────────────────────
class DispatchResult(Protocol):
    """Result of dispatching a task's execution to an Agent."""

    attempt_id: str
    dispatched: bool
    error: str | None


class AgentDispatcher(Protocol):
    """Harness-independent adapter that delivers a TaskClaim + TaskDispatch
    to an external Agent process.  The Demo layer wires concrete dispatchers
    (ScriptedAgentDispatcher, SIGKILL-capable dispatcher, etc.)."""

    async def dispatch(
        self,
        *,
        agent_id: str,
        task_id: str,
        task_kind: str,
        claim_id: str,
        execution_spec: dict[str, Any],
    ) -> DispatchResult: ...
