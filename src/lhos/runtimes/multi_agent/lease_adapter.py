"""Lease Adapter — freezes the claim resource URI format and translates
Scheduler operations over the injected LeaseProvider (Section 15-16).

Resource URI scheme (frozen):
    vpg://<graph-id>/task/<task-id>/claim

The Scheduler never constructs a ResourceLease directly — it asks the
LeaseProvider to acquire/release, because real ownership linearizes at
Kernel Lease acquisition.
"""

from __future__ import annotations

from datetime import UTC, timedelta
from typing import Any

from .protocols import LeaseProvider

DEFAULT_CLAIM_TTL = timedelta(minutes=30)


def claim_resource_uri(graph_id: str, task_id: str) -> str:
    """Canonical claim URI for a task in a graph."""
    return f"vpg://{graph_id}/task/{task_id}/claim"


class LeaseAdapter:
    """Thin adapter over a LeaseProvider with the Scheduler's resource-URI
    conventions baked in."""

    def __init__(
        self,
        provider: LeaseProvider,
        ttl: timedelta = DEFAULT_CLAIM_TTL,
    ) -> None:
        self._provider = provider
        self._ttl = ttl

    def acquire(
        self,
        graph_id: str,
        task_id: str,
        pid: str,
    ) -> Any | None:
        resource = claim_resource_uri(graph_id, task_id)
        return self._provider.acquire_exclusive(pid, resource, self._ttl)

    def release(self, lease_id: str) -> bool:
        return self._provider.release(lease_id)

    def release_all_for_pid(self, pid: str) -> int:
        return self._provider.release_all_for_pid(pid)

    def get(self, lease_id: str) -> Any | None:
        return self._provider.get(lease_id)

    def list_for_task(self, graph_id: str, task_id: str) -> list[Any]:
        resource = claim_resource_uri(graph_id, task_id)
        return self._provider.list_for_resource(resource)

    def list_for_pid(self, pid: str) -> list[Any]:
        return self._provider.list_for_pid(pid)

    def reclaim_expired(self) -> int:
        return self._provider.reclaim_expired()

    def is_lease_active(self, lease: Any | None) -> bool:
        """A lease is active iff it exists AND has not expired."""
        if lease is None:
            return False
        exp = getattr(lease, "expires_at", None)
        if exp is None:
            return True  # no expiry = permanent
        from datetime import datetime

        if isinstance(exp, str):
            exp = datetime.fromisoformat(exp)
        return datetime.now(UTC).timestamp() <= exp.timestamp()
