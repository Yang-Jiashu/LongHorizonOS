"""Real-Kernel-backed providers for D2 integration tests and demos.

These classes adapt the Agent OS Kernel services into the injected
Process / Lease / Capability provider protocols the Scheduler expects,
WITHOUT the Scheduler package itself importing any Kernel internals
(keeps the Section-6 dependency boundary strict — D2 imports Kernel here,
but only in the test/demo support layer).

Real VPG + real AgentKernel share a single SQLite file so that Scheduler,
VPG, and Kernel projections are all in the same database.  For crash/SIGKILL
tests, callers pass a file-backed path; for unit tests, ":memory:" is fine.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from typing import Any

from lhos.agent_os.kernel.models import Capability


# ---------------------------------------------------------------------------
# Provider adapters
# ---------------------------------------------------------------------------
class _ProcInfo:
    def __init__(self, pid: str, state: str) -> None:
        self.pid = pid
        self.state = state
        self.capability_set_id = ""
        self.program_id = ""


class KernelProcessProvider:
    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def get(self, pid: str) -> Any:
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


class KernelLeaseProvider:
    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def acquire_exclusive(
        self,
        pid: str,
        resource_id: str,
        ttl: timedelta,
    ) -> Any | None:
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


# ---------------------------------------------------------------------------
# VPG adapter (Scheduler's VPG surface)
# ---------------------------------------------------------------------------
class VPGAdapter:
    """Exposes exactly the VPG public surface the Scheduler consumes."""

    def __init__(self, rt: Any) -> None:
        self._rt = rt
        self._validity_cache: dict[tuple[str, str], str] = {}

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


# ---------------------------------------------------------------------------
# Facts provider helpers
# ---------------------------------------------------------------------------
class FakeFactsMinimal:
    """No-op artifact/kernel facts provider — lets VPG artifact-binding
    checks succeed without a real Artifact CAS."""

    def __init__(self, artifacts: dict | None = None) -> None:
        self._artifacts: dict = dict(artifacts or {})

    def get_action(self, action_id: str) -> Any:
        return None

    def has_event(self, event_id: str) -> bool:
        return False

    def artifact_exists(self, pid: str, uri: str, v: int) -> bool:
        return (uri, v) in self._artifacts

    def read_hash(self, pid: str, uri: str, v: int) -> Any:
        return self._artifacts.get((uri, v))

    def verify_binding(self, pid: str, binding: Any) -> bool:
        if binding is None:
            return True
        return self._artifacts.get((binding.canonical_uri, binding.version)) == binding.content_hash

    def can_read(self, pid: str, aid: str, v: int) -> bool:
        return True


class FakeFactsWithCommittedAction(FakeFactsMinimal):
    """Facts provider simulating a committed Kernel action so VPG can derive
    Task -> VERIFIED once matching artifact bindings are attached."""

    def __init__(
        self,
        artifacts: dict,
        action_id: str = "test-action",
        agent_pid: str = "agent-1",
    ) -> None:
        super().__init__(artifacts)
        self._action_id = action_id
        self._agent_pid = agent_pid

    def get_action(self, action_id: str) -> Any:
        if action_id == self._action_id:

            class _A:
                pass

            a = _A()
            a.action_id = self._action_id
            a.pid = self._agent_pid
            a.state = "committed"
            a.result = {"exit_code": 0}
            a.artifact_refs = ()
            return a
        return None


def make_capability(resource: str, ops: tuple[str, ...]) -> Capability:
    return Capability(resource_pattern=resource, operations=set(ops))


# ---------------------------------------------------------------------------
# Child-process helpers for SIGKILL / crash-reassignment tests & demos
# ---------------------------------------------------------------------------
SIGKILL_TIMEOUT = 60.0

# Windows has no SIGKILL; there os.kill(pid, SIGTERM) maps to TerminateProcess,
# which is equally uncatchable, so the child still dies without cleanup.
HARD_KILL_SIGNAL = getattr(signal, "SIGKILL", signal.SIGTERM)


def spawn_worker(args: list[str], **kw: Any) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, *args], **kw)


def kill_and_wait(proc: subprocess.Popen, *, timeout: float = SIGKILL_TIMEOUT) -> int:
    with contextlib.suppress(ProcessLookupError, OSError):
        os.kill(proc.pid, HARD_KILL_SIGNAL)
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return -9


def wait_for_file(path: str, *, timeout: float = 10.0, poll: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(poll)
    return False


def make_temp_db(name: str = "d2") -> str:
    d = tempfile.mkdtemp(prefix=f"lhos-d2-{name}-")
    return os.path.join(d, "kernel.sqlite")


def cleanup_temp_db(path: str) -> None:
    d = os.path.dirname(path)
    try:
        shutil.rmtree(d, ignore_errors=True)
    except Exception:
        logging.getLogger(__name__).exception("cleanup_temp_db failed for %s", d)


def wait_for_child_exit(
    proc: subprocess.Popen,
    *,
    timeout: float = SIGKILL_TIMEOUT,
) -> int:
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        return -9
