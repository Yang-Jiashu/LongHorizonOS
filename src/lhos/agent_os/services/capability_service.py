"""Capability Service — capability-based access control.

Phase B supports:
- device:model/mock
- device:tool/mock
- resource:workspace/*
- process:signal/*

Namespace isolation rules:
- P1 cannot access P2's private resources by default.
- Parent does not automatically inherit child capabilities.
- Child capabilities must be a subset of parent capabilities.
"""

from __future__ import annotations

from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import Capability, CapabilitySet, KernelEvent
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage

# Default capability templates for Phase B
DEFAULT_CAPABILITIES: dict[str, list[Capability]] = {
    "full": [
        Capability(
            resource_pattern="device:model/mock",
            operations={"invoke"},
        ),
        Capability(
            resource_pattern="device:tool/mock",
            operations={"invoke"},
        ),
        Capability(
            resource_pattern="resource:workspace/*",
            operations={"acquire", "release"},
        ),
        Capability(
            resource_pattern="process:signal/*",
            operations={"send"},
        ),
    ],
    "restricted": [
        Capability(
            resource_pattern="device:model/mock",
            operations={"invoke"},
        ),
    ],
}


class CapabilityService:
    """Manages capability sets and access checks."""

    def __init__(
        self,
        storage: SQLiteStorage,
        journal: JournalService,
    ):
        self._storage = storage
        self._journal = journal

    def create_capability_set(
        self,
        pid: str,
        capabilities: list[Capability] | None = None,
    ) -> CapabilitySet:
        cap_set = CapabilitySet(pid=pid, capabilities=capabilities or [])
        self._upsert_capability_set(cap_set)

        ev = KernelEvent(
            pid=pid,
            event_type="CAPABILITY_SET_CREATED",
            payload=cap_set.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        return cap_set

    def grant(
        self,
        pid: str,
        capability: Capability,
    ) -> None:
        cap_set = self.get_capability_set(pid)
        if cap_set is None:
            cap_set = self.create_capability_set(pid)
        cap_set.capabilities.append(capability)
        self._upsert_capability_set(cap_set)

        ev = KernelEvent(
            pid=pid,
            event_type="CAPABILITY_GRANTED",
            payload={"pid": pid, "capability": capability.model_dump(mode="json")},
        )
        self._journal.append_event(ev)

    def check(
        self,
        pid: str,
        resource: str,
        operation: str,
    ) -> bool:
        """Check if pid has the capability for resource+operation."""
        cap_set = self.get_capability_set(pid)
        if cap_set is None:
            return False
        return cap_set.check(resource, operation)

    def enforce(
        self,
        pid: str,
        resource: str,
        operation: str,
    ) -> None:
        """Check capability; raise CapabilityDenied if not held.

        Also journals a CAPABILITY_DENIED event on failure.
        """
        if self.check(pid, resource, operation):
            return

        ev = KernelEvent(
            pid=pid,
            event_type="CAPABILITY_DENIED",
            payload={
                "pid": pid,
                "resource": resource,
                "operation": operation,
            },
        )
        self._journal.append_event(ev)
        raise CapabilityDenied(pid, resource, operation)

    def check_namespace_isolation(
        self,
        caller_pid: str,
        caller_namespace: str,
        target_resource: str,
        target_namespace: str,
    ) -> bool:
        """Check if caller can access target resource across namespaces."""
        if caller_namespace == target_namespace:
            return True
        # Cross-namespace: only allowed if explicitly granted
        return self.check(caller_pid, f"resource:{target_namespace}/*", "acquire")

    def get_capability_set(self, pid: str) -> CapabilitySet | None:
        row = self._storage.query_one(
            "SELECT * FROM capability_sets WHERE pid = ?",
            (pid,),
        )
        if not row:
            return None
        caps_data = SQLiteStorage.loads(row["capabilities_json"])
        capabilities = [Capability(**c) for c in caps_data]
        return CapabilitySet(
            set_id=row["set_id"],
            pid=row["pid"],
            capabilities=capabilities,
        )

    def verify_child_subset(self, parent_pid: str, child_pid: str) -> bool:
        """Verify child capabilities are a subset of parent."""
        parent = self.get_capability_set(parent_pid)
        child = self.get_capability_set(child_pid)
        if parent is None:
            return child is None or len(child.capabilities) == 0
        if child is None:
            return True

        parent_patterns = {
            (c.resource_pattern, frozenset(c.operations)) for c in parent.capabilities
        }
        for c in child.capabilities:
            if (c.resource_pattern, frozenset(c.operations)) not in parent_patterns:
                return False
        return True

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        if ev.event_type == "CAPABILITY_SET_CREATED":
            cap_set = CapabilitySet(**ev.payload)
            self._upsert_capability_set(cap_set)
        elif ev.event_type == "CAPABILITY_GRANTED":
            pid = ev.payload["pid"]
            cap = Capability(**ev.payload["capability"])
            cap_set = self.get_capability_set(pid)
            if cap_set is None:
                cap_set = self.create_capability_set(pid)
            cap_set.capabilities.append(cap)
            self._upsert_capability_set(cap_set)

    def _upsert_capability_set(self, cap_set: CapabilitySet) -> None:
        caps_json = SQLiteStorage.dumps([c.model_dump(mode="json") for c in cap_set.capabilities])
        with self._storage.transaction() as tx:
            tx.execute(
                """INSERT INTO capability_sets (set_id, pid, capabilities_json)
                   VALUES (?, ?, ?)
                   ON CONFLICT(set_id) DO UPDATE SET capabilities_json = excluded.capabilities_json""",
                (cap_set.set_id, cap_set.pid, caps_json),
            )
            # Also upsert by pid
            tx.execute(
                """INSERT INTO capability_sets (set_id, pid, capabilities_json)
                   VALUES (?, ?, ?)
                   ON CONFLICT(set_id) DO NOTHING""",
                (cap_set.set_id, cap_set.pid, caps_json),
            )
