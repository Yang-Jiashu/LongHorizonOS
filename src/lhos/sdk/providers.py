# mypy: disable-error-code="no-any-return,attr-defined"
"""LongHorizonOS Public SDK — provider adapters (E1, experimental).

These are the public composition adapters that wire a real Agent Kernel into the
VPG + D2 Scheduler + D3.  They are the supported, shipped equivalent of what the
Core audit previously relied on through test helpers; shipping them as public SDK
code moves the wiring from "test-only" to "supported composition".

They reach Kernel service internals (`_process_service` / `_lease_service` /
`_capability_service`) for integration — the documented, supported composition
surface (see artifacts/oss_productization_e1/*).  The SDK does NOT bypass Core
authority: ownership still comes from the Kernel Lease, semantic state from VPG.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from lhos.agent_os.kernel.models import Capability


class _ProcInfo:
    def __init__(self, pid: str, state: str) -> None:
        self.pid = pid
        self.state = state
        self.capability_set_id = ""
        self.program_id = ""


class KernelProcessProvider:
    """Adapts the Kernel ProcessService into the D2 ProcessProvider protocol."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def get(self, pid: str) -> Any | None:
        pcb = self._k._process_service.get_process(pid)
        if pcb is None:
            return None
        info = _ProcInfo(pcb.pid, pcb.state.value)
        info.capability_set_id = pcb.capability_set_id
        info.program_id = pcb.program_id
        return info

    def list_all(self) -> list[Any]:
        out: list[Any] = []
        for pcb in self._k._process_service.list_all():
            info = _ProcInfo(pcb.pid, pcb.state.value)
            info.capability_set_id = pcb.capability_set_id
            info.program_id = pcb.program_id
            out.append(info)
        return out

    def spawn(self, program_id: str | None = None) -> str:
        """Spawn a real Kernel process for an agent (returns pid)."""
        return self._k._process_service.spawn(program_id or "agent").pid

    def set_failed(self, pid: str) -> None:
        from lhos.agent_os.kernel.models import ProcessState

        self._k._process_service.transition(pid, ProcessState.FAILED)


class KernelLeaseProvider:
    """Adapts Kernel LeaseService atomic_acquire into the D2 LeaseProvider."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: timedelta) -> Any | None:
        leases = self._k._lease_service.atomic_acquire(
            pid,
            [{"resource_id": resource_id, "mode": "exclusive"}],
            ttl=ttl,
        )
        if not leases:
            return None
        return leases[0]

    def release(self, lease_id: str) -> bool:
        return self._k._lease_service.release([lease_id]) == 1

    def release_all_for_pid(self, pid: str) -> int:
        return self._k._lease_service.release_all_for_pid(pid)

    def get(self, lease_id: str) -> Any | None:
        return self._k._lease_service.get_lease(lease_id)

    def list_for_resource(self, resource_id: str) -> list[Any]:
        return self._k._lease_service.list_active_leases_for_resource(resource_id)

    def list_for_pid(self, pid: str) -> list[Any]:
        return self._k._lease_service.list_leases_for_pid(pid)

    def reclaim_expired(self) -> int:
        return self._k._lease_service.reclaim_expired(self._k._clock.now())


class KernelCapabilityProvider:
    """Adapts Kernel CapabilityService into the D2 CapabilityProvider."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def check(self, pid: str, resource: str, operation: str) -> bool:
        try:
            return self._k._capability_service.check(pid, resource, operation)
        except Exception:
            return False

    def capabilities_for(self, pid: str) -> list[Any]:
        cs = self._k._capability_service.get_capability_set(pid)
        if cs is None:
            return []
        return list(cs.capabilities)


class VPGFacade:
    """The exact VPG public surface the D2 Scheduler consumes (duck-typed vpg)."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt

    def ready_frontier(self, graph_id: str) -> list[Any]:
        return list(self._rt.query_ready_frontier(graph_id))

    def current_graph_version(self, graph_id: str) -> int:
        return self._rt.get_graph(graph_id).current_version

    def task_node_payload(self, graph_id: str, task_id: str) -> dict | None:
        node = self._rt.inspect_node(graph_id, task_id)
        if node is None:
            return None
        return node.model_dump(mode="json")

    def task_validity(self, graph_id: str, task_id: str) -> str | None:
        node = self._rt.inspect_node(graph_id, task_id)
        if node is None:
            return None
        return node.validity.value


class FactsProvider:
    """Versioned Artifact + committed-Action facts for VPG evidence derivation.

    A user/workload writes artifacts + commits actions here; VPG's
    verification then has a valid source action + exact ArtifactVersion binding
    so it can DERIVE VERIFIED.  This is simulation/scripted support — a real
    E2 integration would back these with a real Artifact CAS + Journal.
    """

    def __init__(self) -> None:
        self._versions: dict[str, list[int]] = {}
        self._hashes: dict[tuple[str, int], str] = {}
        self._actions: dict[str, Any] = {}

    # Artifact
    def artifact_exists(self, pid: str, uri: str, version: int) -> bool:
        return uri in self._versions and version in self._versions[uri]

    def read_hash(self, pid: str, uri: str, version: int) -> str | None:
        return self._hashes.get((uri, version))

    def verify_binding(self, pid: str, binding: Any) -> bool:
        if binding is None:
            return True
        # check canonical_uri or artifact_id keyed hash
        expect = self._hashes.get((binding.canonical_uri, binding.version))
        if expect is None:
            expect = self._hashes.get((binding.artifact_id, binding.version))
        return expect is not None and expect == binding.content_hash

    def can_read(self, pid: str, artifact_id: str, version: int) -> bool:
        return True

    def add_version(self, artifact_id: str, version: int, content: str) -> None:
        self._versions.setdefault(artifact_id, []).append(version)
        h = f"hash:{content}"
        self._hashes[(artifact_id, version)] = h
        self._hashes[(f"vpg://{artifact_id}", version)] = h

    def versions(self) -> dict[str, list[int]]:
        """Public read-only view of known artifact versions."""
        return {k: list(v) for k, v in self._versions.items()}

    def latest(self, artifact_id: str) -> int | None:
        vs = self._versions.get(artifact_id)
        return max(vs) if vs else None

    # Kernel events/actions
    def commit_action(self, action_id: str, *, pid: str = "sdk-agent", exit_code: int = 0) -> None:
        self._actions[action_id] = _CommittedAction(action_id, pid, exit_code)

    def get_action(self, action_id: str) -> Any | None:
        return self._actions.get(action_id)

    def has_event(self, event_id: str) -> bool:
        return False

    def list_events_for_pid(self, pid: str) -> list[Any]:
        return []


class _CommittedAction:
    def __init__(self, action_id: str, pid: str, exit_code: int) -> None:
        self.action_id = action_id
        self.pid = pid
        self.state = "committed"  # terminal action state (frozenset check uses "committed")
        self.result = {"exit_code": exit_code}
        self.artifact_refs = ()


def make_capability(resource: str, ops: tuple[str, ...]) -> Capability:
    return Capability(resource_pattern=resource, operations=set(ops))
