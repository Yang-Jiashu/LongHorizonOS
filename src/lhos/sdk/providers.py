# mypy: disable-error-code="no-any-return,attr-defined"
"""Public provider adapters for Kernel, Scheduler, and VPG composition."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any

from lhos.agent_os.kernel.models import Capability


class _ProcInfo:
    def __init__(self, pid: str, state: str) -> None:
        self.pid = pid
        self.state = state
        self.capability_set_id = ""
        self.program_id = ""


class KernelProcessProvider:
    """Adapt the Kernel ProcessService to the Scheduler protocol."""

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
        return self._k._process_service.spawn(program_id or "agent").pid

    def set_failed(self, pid: str) -> None:
        from lhos.agent_os.kernel.models import ProcessState

        self._k._process_service.transition(pid, ProcessState.FAILED)


class KernelLeaseProvider:
    """Adapt Kernel LeaseService atomic acquisition to the Scheduler protocol."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: timedelta) -> Any | None:
        leases = self._k._lease_service.atomic_acquire(
            pid,
            [{"resource_id": resource_id, "mode": "exclusive"}],
            ttl=ttl,
        )
        return leases[0] if leases else None

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
    """Adapt Kernel CapabilityService to Scheduler eligibility checks."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def check(self, pid: str, resource: str, operation: str) -> bool:
        try:
            return self._k._capability_service.check(pid, resource, operation)
        except Exception:
            return False

    def capabilities_for(self, pid: str) -> list[Any]:
        capability_set = self._k._capability_service.get_capability_set(pid)
        if capability_set is None:
            return []
        return list(capability_set.capabilities)


class VPGFacade:
    """Expose only the authoritative VPG surface consumed by Scheduler."""

    def __init__(self, runtime: Any) -> None:
        self._rt = runtime

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
    """Durable ArtifactVersion and Kernel Action facts consumed by VPG."""

    def __init__(
        self,
        db_path: str = ":memory:",
        *,
        read_only: bool = False,
        action_service: Any | None = None,
    ) -> None:
        self._read_only = read_only
        self._action_service = action_service
        self._versions: dict[str, list[int]] = {}
        self._hashes: dict[tuple[str, int], str] = {}
        self._actions: dict[str, Any] = {}
        self._conn: sqlite3.Connection | None = None
        self._has_persistent_facts = False
        if db_path != ":memory:":
            resolved = Path(db_path).resolve()
            if read_only:
                self._conn = sqlite3.connect(
                    f"file:{resolved.as_posix()}?mode=ro",
                    uri=True,
                    check_same_thread=False,
                )
                row = self._conn.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'sdk_artifact_facts'
                    """
                ).fetchone()
                self._has_persistent_facts = row is not None
            else:
                self._conn = sqlite3.connect(str(resolved), check_same_thread=False)
                self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sdk_artifact_facts (
                        artifact_id TEXT NOT NULL,
                        canonical_uri TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        content_hash TEXT NOT NULL,
                        PRIMARY KEY (artifact_id, version),
                        UNIQUE (canonical_uri, version)
                    )
                    """
                )
                self._conn.commit()
                self._has_persistent_facts = True

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def artifact_exists(self, pid: str, uri: str, version: int) -> bool:
        if self._conn is not None and self._has_persistent_facts:
            row = self._conn.execute(
                """
                SELECT 1 FROM sdk_artifact_facts
                WHERE version = ? AND (artifact_id = ? OR canonical_uri = ?)
                """,
                (version, uri, uri),
            ).fetchone()
            return row is not None
        return uri in self._versions and version in self._versions[uri]

    def read_hash(self, pid: str, uri: str, version: int) -> str | None:
        if self._conn is not None and self._has_persistent_facts:
            row = self._conn.execute(
                """
                SELECT content_hash FROM sdk_artifact_facts
                WHERE version = ? AND (artifact_id = ? OR canonical_uri = ?)
                """,
                (version, uri, uri),
            ).fetchone()
            return str(row[0]) if row is not None else None
        return self._hashes.get((uri, version))

    def verify_binding(self, pid: str, binding: Any) -> bool:
        if binding is None:
            return True
        expected = self.read_hash(pid, binding.canonical_uri, binding.version)
        if expected is None:
            expected = self.read_hash(pid, binding.artifact_id, binding.version)
        return expected is not None and expected == binding.content_hash

    def can_read(self, pid: str, artifact_id: str, version: int) -> bool:
        return self.artifact_exists(pid, artifact_id, version)

    def add_version(self, artifact_id: str, version: int, content: str) -> None:
        if self._read_only:
            raise RuntimeError("read-only FactsProvider cannot add artifact versions")
        artifact_id = artifact_id.removeprefix("vpg://")
        canonical_uri = f"vpg://{artifact_id}"
        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        if self._conn is not None and self._has_persistent_facts:
            existing = self._conn.execute(
                """
                SELECT content_hash FROM sdk_artifact_facts
                WHERE artifact_id = ? AND version = ?
                """,
                (artifact_id, version),
            ).fetchone()
            if existing is not None:
                if existing[0] != content_hash:
                    raise ValueError(
                        f"ArtifactVersion is immutable: {artifact_id}@{version} already exists"
                    )
                return
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO sdk_artifact_facts
                    (artifact_id, canonical_uri, version, content_hash)
                    VALUES (?, ?, ?, ?)
                    """,
                    (artifact_id, canonical_uri, version, content_hash),
                )
            return
        existing = self._hashes.get((artifact_id, version))
        if existing is not None:
            if existing != content_hash:
                raise ValueError(
                    f"ArtifactVersion is immutable: {artifact_id}@{version} already exists"
                )
            return
        self._versions.setdefault(artifact_id, []).append(version)
        self._hashes[(artifact_id, version)] = content_hash
        self._hashes[(canonical_uri, version)] = content_hash

    def versions(self) -> dict[str, list[int]]:
        if self._conn is not None and self._has_persistent_facts:
            rows = self._conn.execute(
                """
                SELECT artifact_id, version FROM sdk_artifact_facts
                ORDER BY artifact_id, version
                """
            ).fetchall()
            out: dict[str, list[int]] = {}
            for artifact_id, version in rows:
                out.setdefault(str(artifact_id), []).append(int(version))
            return out
        return {key: list(versions) for key, versions in self._versions.items()}

    def latest(self, artifact_id: str) -> int | None:
        artifact_id = artifact_id.removeprefix("vpg://")
        if self._conn is not None and self._has_persistent_facts:
            row = self._conn.execute(
                "SELECT MAX(version) FROM sdk_artifact_facts WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
            return int(row[0]) if row is not None and row[0] is not None else None
        versions = self._versions.get(artifact_id)
        return max(versions) if versions else None

    def commit_action(
        self,
        action_id: str,
        *,
        pid: str = "sdk-agent",
        exit_code: int = 0,
    ) -> str:
        if self._read_only:
            raise RuntimeError("read-only FactsProvider cannot commit actions")
        if self._action_service is not None:
            existing = self._action_service.get_action(action_id)
            if existing is not None:
                if getattr(existing.state, "value", existing.state) != "committed":
                    raise ValueError(f"Action {action_id} already exists but is not committed")
                return action_id
            action = self._action_service.submit(
                pid,
                "sdk",
                "verification",
                arguments={"exit_code": exit_code},
                idempotency_key=action_id,
                action_id=action_id,
            )
            self._action_service.admit(action.action_id)
            self._action_service.dispatch(action.action_id)
            self._action_service.commit(action.action_id, {"exit_code": exit_code})
            return action.action_id
        self._actions[action_id] = _CommittedAction(action_id, pid, exit_code)
        return action_id

    def get_action(self, action_id: str) -> Any | None:
        if self._action_service is not None:
            return self._action_service.get_action(action_id)
        if self._conn is not None:
            row = self._conn.execute(
                """
                SELECT action_id, pid, state, result_json
                FROM actions_projection WHERE action_id = ?
                """,
                (action_id,),
            ).fetchone()
            if row is not None:
                result = json.loads(row[3]) if row[3] else {}
                return _CommittedAction(
                    str(row[0]),
                    str(row[1]),
                    int(result.get("exit_code", 0)),
                    state=str(row[2]),
                )
        return self._actions.get(action_id)

    def has_event(self, event_id: str) -> bool:
        if self._conn is not None:
            row = self._conn.execute(
                "SELECT 1 FROM journal_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            return row is not None
        return False

    def list_events_for_pid(self, pid: str) -> list[Any]:
        if self._conn is not None:
            return list(
                self._conn.execute(
                    """
                    SELECT event_id, event_type, payload_json, created_at
                    FROM journal_events WHERE pid = ? ORDER BY journal_offset
                    """,
                    (pid,),
                ).fetchall()
            )
        return []


class _CommittedAction:
    def __init__(
        self,
        action_id: str,
        pid: str,
        exit_code: int,
        *,
        state: str = "committed",
    ) -> None:
        self.action_id = action_id
        self.pid = pid
        self.state = state
        self.result = {"exit_code": exit_code}
        self.artifact_refs = ()


def make_capability(resource: str, ops: tuple[str, ...]) -> Capability:
    return Capability(resource_pattern=resource, operations=set(ops))
