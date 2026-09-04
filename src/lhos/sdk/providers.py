# mypy: disable-error-code="no-any-return,attr-defined"
"""Public provider adapters for Kernel, Scheduler, and VPG composition."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from lhos.agent_os.kernel.models import Capability, RecoveryPolicy, SideEffectClass

from .observation import ObservationToken, make_observation_token


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

        # Terminal transition first closes new lease admission; cleanup then
        # releases any ownership held by the failed process.  Keeping this
        # ordering aligned with the kernel's failure paths prevents a racing
        # same-PID acquire from creating a post-failure lease.
        self._k._process_service.transition(pid, ProcessState.FAILED)
        self._k._lease_service.release_all_for_pid(pid)


class KernelLeaseProvider:
    """Adapt Kernel LeaseService atomic acquisition to the Scheduler protocol."""

    def __init__(self, kernel: Any) -> None:
        self._k = kernel

    def acquire_exclusive(self, pid: str, resource_id: str, ttl: timedelta) -> Any | None:
        # Ordinary contention is represented as ``None`` by the Scheduler
        # provider protocol, while the Kernel expresses that same expected
        # race with its domain exception.  Normalize only that exception;
        # infrastructure and programming errors must still propagate.
        from lhos.agent_os.kernel.errors import LeaseAcquisitionFailed

        try:
            leases = self._k._lease_service.atomic_acquire(
                pid,
                [{"resource_id": resource_id, "mode": "exclusive"}],
                ttl=ttl,
            )
        except LeaseAcquisitionFailed:
            return None
        return leases[0] if leases else None

    def release(self, lease_id: str) -> bool:
        return self._k._lease_service.release([lease_id]) == 1

    def renew(self, lease_id: str, ttl: timedelta) -> Any | None:
        """Renew a still-live Kernel lease without changing its fence token."""
        return self._k._lease_service.renew(lease_id, ttl=ttl)

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

    def task_evidence_bindings(self, graph_id: str, task_id: str) -> list[dict[str, Any]]:
        """Return valid Evidence ownership bindings for one task.

        This is deliberately a narrow audit/query surface: semantic validity
        remains owned by VPG's verification predicate, while the Scheduler
        uses the causal identifiers to ensure a VERIFIED observation belongs
        to the currently active claim/attempt/epoch.
        """
        from lhos.runtimes.verified_progress.models import (
            EdgeType,
            EvidenceNode,
        )
        from lhos.runtimes.verified_progress.verification import validate_evidence

        nodes, edges = self._rt.snapshot_projection(graph_id)
        out: list[dict[str, Any]] = []
        verification_ids = {
            edge.source_node_id
            for edge in edges
            if edge.edge_type == EdgeType.VERIFIES and edge.target_node_id == task_id
        }
        for edge in edges:
            if edge.edge_type != EdgeType.PRODUCES or edge.source_node_id not in verification_ids:
                continue
            evidence = nodes.get(edge.target_node_id)
            if not isinstance(evidence, EvidenceNode):
                continue
            check = validate_evidence(
                evidence,
                existing_nodes=nodes,
                existing_edges=edges,
                facts_artifact=getattr(self._rt, "facts_artifact", None),
                facts_kernel=getattr(self._rt, "facts_kernel", None),
            )
            if not check.valid:
                continue
            out.append(
                {
                    "evidence_id": evidence.node_id,
                    "claim_id": evidence.claim_id,
                    "attempt_id": evidence.attempt_id,
                    "semantic_epoch": evidence.semantic_epoch,
                    "lease_fencing_token": evidence.lease_fencing_token,
                    "provenance_digest": evidence.provenance_digest,
                    "context_snapshot_id": evidence.context_snapshot_id,
                    "context_manifest_id": evidence.context_manifest_id,
                    "context_manifest_hash": evidence.context_manifest_hash,
                    "context_working_set_hash": evidence.context_working_set_hash,
                    "context_materialized_hash": evidence.context_materialized_hash,
                }
            )
        return out


class FactsProvider:
    """Durable ArtifactVersion and Kernel Action facts consumed by VPG."""

    @staticmethod
    def content_hash(content: str | bytes) -> str:
        """Return the canonical SHA-256 digest for an artifact payload.

        Artifact facts are content-addressed even though the public
        compatibility API still accepts a caller-selected integer version.
        Keeping this helper on the authority object gives workspace/tool
        adapters one canonical hashing rule instead of each adapter inventing
        its own encoding or digest format.
        """

        payload = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def normalize_artifact_id(artifact_id: str) -> str:
        """Normalize the SDK's ``vpg://`` alias to one artifact identity."""

        value = str(artifact_id).strip().removeprefix("vpg://")
        if not value:
            raise ValueError("artifact_id must be non-empty")
        return value

    @staticmethod
    def validate_version(version: int) -> int:
        """Validate a version token before it can enter the facts authority."""

        # ``bool`` is an ``int`` subclass, but accepting True as version 1 is
        # an authority ambiguity and has caused subtle caller bugs in the past.
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValueError("ArtifactVersion version must be a positive integer")
        return version

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
        self._contents: dict[tuple[str, int], bytes] = {}
        self._observation_tokens: dict[str, ObservationToken] = {}
        self._observation_lock = RLock()
        self._actions: dict[str, Any] = {}
        self._conn: sqlite3.Connection | None = None
        self._has_persistent_facts = False
        self._has_observation_tokens = False
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
                token_row = self._conn.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'sdk_observation_tokens'
                    """
                ).fetchone()
                self._has_observation_tokens = token_row is not None
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
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sdk_observation_tokens (
                        token_id TEXT PRIMARY KEY,
                        schema_version TEXT NOT NULL,
                        artifact_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        content_hash TEXT NOT NULL,
                        graph_id TEXT NOT NULL,
                        issued_at TEXT NOT NULL,
                        token_digest TEXT NOT NULL,
                        UNIQUE (artifact_id, version, graph_id, content_hash, token_digest)
                    )
                    """
                )
                self._conn.commit()
                self._has_persistent_facts = True
                self._has_observation_tokens = True

    def close(self) -> None:
        with self._observation_lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def artifact_exists(self, pid: str, uri: str, version: int) -> bool:
        if self._conn is not None and self._has_persistent_facts:
            # ``sdk_observation_tokens`` was added after the original
            # ``sdk_artifact_facts`` table.  Artifact fact reads must remain
            # compatible with a read-only connection to an older database
            # that has facts but no token table; token availability is a
            # separate capability checked by ``get_observation``.
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

    def read_version(
        self,
        *,
        artifact_id: str,
        version: int,
        canonical_uri: str,
    ) -> bytes:
        """Return exact bytes for Context VM materialization in this process.

        Facts persistence stores hashes, not payload bytes. Callers that need
        restart-durable non-empty Context VM pages must inject a content
        supplier-backed ContextService.
        """

        normalized = self.normalize_artifact_id(artifact_id)
        content = self._contents.get((normalized, version))
        if content is None:
            content = self._contents.get((canonical_uri, version))
        if content is None:
            raise FileNotFoundError(f"{normalized}@{version} content bytes are unavailable")
        return content

    def read_version_size(self, *, artifact_id: str, version: int) -> int:
        return len(
            self.read_version(
                artifact_id=artifact_id,
                version=version,
                canonical_uri=f"vpg://{self.normalize_artifact_id(artifact_id)}",
            )
        )

    def add_version(self, artifact_id: str, version: int, content: str | bytes) -> None:
        if self._read_only:
            raise RuntimeError("read-only FactsProvider cannot add artifact versions")
        artifact_id = self.normalize_artifact_id(artifact_id)
        version = self.validate_version(version)
        canonical_uri = f"vpg://{artifact_id}"
        payload = content.encode("utf-8") if isinstance(content, str) else bytes(content)
        content_hash = self.content_hash(content)
        if self._conn is not None and self._has_persistent_facts and self._has_observation_tokens:
            # Check monotonicity and insert under one writer transaction.
            # Without BEGIN IMMEDIATE, two FactsProvider instances could both
            # observe the same latest version and race to publish conflicting
            # version order.
            self._conn.execute("BEGIN IMMEDIATE")
            try:
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
                    self._conn.commit()
                    self._contents[(artifact_id, version)] = payload
                    self._contents[(canonical_uri, version)] = payload
                    return
                latest_row = self._conn.execute(
                    "SELECT MAX(version) FROM sdk_artifact_facts WHERE artifact_id = ?",
                    (artifact_id,),
                ).fetchone()
                latest = int(latest_row[0]) if latest_row and latest_row[0] is not None else None
                if latest is not None and version < latest:
                    raise ValueError(
                        f"ArtifactVersion must be monotonic: {artifact_id}@{version} "
                        f"is older than current @{latest}"
                    )
                self._conn.execute(
                    """
                    INSERT INTO sdk_artifact_facts
                    (artifact_id, canonical_uri, version, content_hash)
                    VALUES (?, ?, ?, ?)
                    """,
                    (artifact_id, canonical_uri, version, content_hash),
                )
                self._conn.commit()
                self._contents[(artifact_id, version)] = payload
                self._contents[(canonical_uri, version)] = payload
            except BaseException:
                self._conn.rollback()
                raise
            return
        existing = self._hashes.get((artifact_id, version))
        if existing is not None:
            if existing != content_hash:
                raise ValueError(
                    f"ArtifactVersion is immutable: {artifact_id}@{version} already exists"
                )
            self._contents[(artifact_id, version)] = payload
            self._contents[(canonical_uri, version)] = payload
            return
        latest_versions = self._versions.get(artifact_id, [])
        latest = max(latest_versions) if latest_versions else None
        if latest is not None and version < latest:
            raise ValueError(
                f"ArtifactVersion must be monotonic: {artifact_id}@{version} "
                f"is older than current @{latest}"
            )
        self._versions.setdefault(artifact_id, []).append(version)
        self._hashes[(artifact_id, version)] = content_hash
        self._hashes[(canonical_uri, version)] = content_hash
        self._contents[(artifact_id, version)] = payload
        self._contents[(canonical_uri, version)] = payload

    def observe_version(
        self,
        artifact_id: str,
        version: int,
        content: str | bytes,
        *,
        expected_hash: str | None = None,
    ) -> str:
        """Validate and register one observed artifact snapshot.

        ``expected_hash`` is useful when a watcher or workspace CAS supplied a
        digest independently.  A mismatch fails closed before any fact is
        persisted.  The returned digest is the value bound to Evidence.
        """

        digest = self.content_hash(content)
        if expected_hash is not None and str(expected_hash).lower() != digest:
            raise ValueError(
                f"artifact observation hash mismatch for {self.normalize_artifact_id(artifact_id)!r}"
            )
        self.add_version(artifact_id, version, content)
        return digest

    def issue_observation(
        self,
        artifact_id: str,
        version: int,
        *,
        graph_id: str = "",
        content: str | bytes | None = None,
        expected_hash: str | None = None,
    ) -> ObservationToken:
        """Issue an immutable token for one authoritative artifact observation.

        A token can only be issued for bytes supplied to this authority or for
        a hash already registered in ``sdk_artifact_facts``.  The method never
        invents content from an integer version.  Supplying ``content`` for a
        new version registers it using the normal monotonic/immutability rules.
        """

        # ``check_same_thread=False`` permits callers to share the provider,
        # but SQLite transactions and the in-memory semantic check/insert both
        # still require caller-side serialization.  Keep version registration
        # and token persistence in one critical section so an exact concurrent
        # observation cannot mint multiple identities or nest transactions on
        # this provider's connection.
        with self._observation_lock:
            artifact_id = self.normalize_artifact_id(artifact_id)
            version = self.validate_version(version)
            graph_id = str(graph_id).strip()
            digest: str | None
            if content is not None:
                digest = self.observe_version(
                    artifact_id,
                    version,
                    content,
                    expected_hash=expected_hash,
                )
            else:
                if expected_hash is not None:
                    expected_hash = str(expected_hash).strip().lower()
                digest = self.read_hash("sdk-observation", artifact_id, version)
                if digest is None:
                    raise ValueError(
                        f"cannot issue observation for unregistered artifact "
                        f"{artifact_id!r}@{version} without content"
                    )
                if expected_hash is not None and digest != expected_hash:
                    raise ValueError(
                        f"artifact observation hash mismatch for {artifact_id!r}@{version}"
                    )
            token = make_observation_token(
                token_id=uuid4().hex,
                artifact_id=artifact_id,
                version=version,
                content_hash=digest,
                graph_id=graph_id,
            )
            # Observation tokens identify an exact authoritative observation, not
            # an invocation of ``issue_observation``.  Reusing the durable token
            # for the same artifact/version/hash/graph is important for retry
            # safety: a caller may have committed a semantic repair and then lost
            # its response (for example, after a process/network interruption).
            # Re-observing the unchanged bytes must converge to the same token and
            # therefore to the same reconciliation idempotency key, rather than
            # manufacturing a second D3 transition.
            return self._persist_observation(token)

    @staticmethod
    def _observation_from_row(row: Any) -> ObservationToken:
        """Decode one durable observation row into its immutable DTO."""
        # FactsProvider's standalone sqlite connection intentionally keeps the
        # default tuple row factory for backwards compatibility, while some
        # injected connections expose mapping-like rows.  Accept both shapes.
        if isinstance(row, Mapping):
            values = {
                "schema_version": row["schema_version"],
                "token_id": row["token_id"],
                "artifact_id": row["artifact_id"],
                "version": row["version"],
                "content_hash": row["content_hash"],
                "graph_id": row["graph_id"],
                "issued_at": row["issued_at"],
                "token_digest": row["token_digest"],
            }
        else:
            (
                values_schema,
                values_token,
                values_artifact,
                values_version,
                values_hash,
                values_graph,
                values_issued,
                values_digest,
            ) = row
            values = {
                "schema_version": values_schema,
                "token_id": values_token,
                "artifact_id": values_artifact,
                "version": values_version,
                "content_hash": values_hash,
                "graph_id": values_graph,
                "issued_at": values_issued,
                "token_digest": values_digest,
            }
        return ObservationToken(
            schema_version=str(values["schema_version"]),
            token_id=str(values["token_id"]),
            artifact_id=str(values["artifact_id"]),
            version=int(values["version"]),
            content_hash=str(values["content_hash"]),
            graph_id=str(values["graph_id"]),
            issued_at=datetime.fromisoformat(str(values["issued_at"])),
            token_digest=str(values["token_digest"]),
        )

    def _persist_observation(self, token: ObservationToken) -> ObservationToken:
        """Persist one token, reusing an exact semantic observation.

        The persistent path takes a writer transaction before checking the
        semantic identity, so two ``FactsProvider`` instances sharing one
        SQLite database cannot race into distinct tokens for the same exact
        observation.  Existing databases may contain duplicate rows from
        older versions; the deterministic oldest row is selected and reused.
        """

        if not token.is_self_consistent():
            raise ValueError("observation token integrity digest mismatch")
        if self._read_only:
            raise RuntimeError("read-only FactsProvider cannot issue observations")
        if self._conn is not None and self._has_persistent_facts and self._has_observation_tokens:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    """
                    SELECT schema_version, token_id, artifact_id, version,
                           content_hash, graph_id, issued_at, token_digest
                    FROM sdk_observation_tokens WHERE token_id = ?
                    """,
                    (token.token_id,),
                ).fetchone()
                if existing is not None:
                    stored = self._observation_from_row(existing)
                    if stored != token:
                        raise ValueError(
                            f"observation token {token.token_id!r} already exists with different payload"
                        )
                    self._conn.commit()
                    self._observation_tokens[stored.token_id] = stored
                    return stored

                # ``token_id`` is generated afresh above, but semantic
                # identity is stable across retries.  Reuse the first durable
                # row for this exact observation.  The writer transaction
                # serializes this lookup with concurrent issuers.
                semantic = self._conn.execute(
                    """
                    SELECT schema_version, token_id, artifact_id, version,
                           content_hash, graph_id, issued_at, token_digest
                    FROM sdk_observation_tokens
                    WHERE artifact_id = ? AND version = ? AND content_hash = ?
                      AND graph_id = ?
                    ORDER BY issued_at ASC, token_id ASC
                    LIMIT 1
                    """,
                    (
                        token.artifact_id,
                        token.version,
                        token.content_hash,
                        token.graph_id,
                    ),
                ).fetchone()
                if semantic is not None:
                    stored = self._observation_from_row(semantic)
                    self._conn.commit()
                    self._observation_tokens[stored.token_id] = stored
                    return stored

                self._conn.execute(
                    """
                    INSERT INTO sdk_observation_tokens
                    (token_id, schema_version, artifact_id, version, content_hash,
                     graph_id, issued_at, token_digest)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        token.token_id,
                        token.schema_version,
                        token.artifact_id,
                        token.version,
                        token.content_hash,
                        token.graph_id,
                        token.issued_at.isoformat(),
                        token.token_digest,
                    ),
                )
                self._conn.commit()
                self._observation_tokens[token.token_id] = token
                return token
            except BaseException:
                self._conn.rollback()
                raise
        # In-memory FactsProvider has no cross-instance concurrency boundary,
        # but still reuses exact observations to preserve the same retry
        # semantics as the durable implementation.
        existing = self._observation_tokens.get(token.token_id)
        if existing is not None and existing != token:
            raise ValueError(
                f"observation token {token.token_id!r} already exists with different payload"
            )
        if existing is not None:
            return existing
        for stored in sorted(
            self._observation_tokens.values(),
            key=lambda item: (item.issued_at, item.token_id),
        ):
            if (
                stored.artifact_id == token.artifact_id
                and stored.version == token.version
                and stored.content_hash == token.content_hash
                and stored.graph_id == token.graph_id
            ):
                return stored
        self._observation_tokens[token.token_id] = token
        return token

    def get_observation(self, token_id: str) -> ObservationToken | None:
        """Load an issued token by id, if present."""

        token_id = str(token_id).strip()
        if self._conn is not None and self._has_persistent_facts and self._has_observation_tokens:
            row = self._conn.execute(
                """
                SELECT schema_version, token_id, artifact_id, version, content_hash,
                       graph_id, issued_at, token_digest
                FROM sdk_observation_tokens WHERE token_id = ?
                """,
                (token_id,),
            ).fetchone()
            if row is None:
                return None
            return ObservationToken(
                schema_version=str(row[0]),
                token_id=str(row[1]),
                artifact_id=str(row[2]),
                version=int(row[3]),
                content_hash=str(row[4]),
                graph_id=str(row[5]),
                issued_at=datetime.fromisoformat(str(row[6])),
                token_digest=str(row[7]),
            )
        return self._observation_tokens.get(token_id)

    def validate_observation(
        self,
        token: ObservationToken,
        *,
        graph_id: str | None = None,
    ) -> ObservationToken:
        """Validate token integrity, durable authority, and current hash."""

        if not isinstance(token, ObservationToken):
            raise TypeError("token must be an ObservationToken")
        if not token.is_self_consistent():
            raise ValueError("observation token integrity digest mismatch")
        if graph_id is not None and token.graph_id != str(graph_id).strip():
            raise ValueError(
                f"observation token graph mismatch: token is bound to {token.graph_id!r}, "
                f"expected {str(graph_id).strip()!r}"
            )
        stored = self.get_observation(token.token_id)
        if stored is None or stored != token:
            raise ValueError("observation token is not issued by this FactsProvider")
        stored_hash = self.read_hash("sdk-observation", token.artifact_id, token.version)
        if stored_hash is None:
            raise ValueError(
                f"observation artifact {token.artifact_id!r}@{token.version} is not registered"
            )
        if stored_hash != token.content_hash:
            raise ValueError(
                f"observation token hash mismatch for {token.artifact_id!r}@{token.version}"
            )
        return token

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
                side_effect_class=SideEffectClass.PURE,
                recovery_policy=RecoveryPolicy.RETRY,
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
