"""Capability-scoped workspace access with provenance capture.

This gateway closes one explicit execution boundary: file bytes accessed
through it are confined to a ``WorkspaceTool`` root and recorded against the
current ``ExecutionContext``. It cannot observe direct ``Path`` or ``open``
calls made outside the gateway, so it is a mediated primitive rather than
automatic dependency discovery.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from lhos.provenance import ExecutionContext, ProvenanceOperation

from .workspace import WorkspaceTool

_WORKSPACE_SCHEMES = ("workspace://", "vpg://workspace/")


class WorkspaceGatewayError(RuntimeError):
    """A mediated workspace operation could not be completed safely."""


class WorkspaceAccessDenied(PermissionError):
    """A path is outside the gateway's declared read/write capability."""


class WorkspaceVersionValidationError(WorkspaceGatewayError):
    """A claimed workspace ArtifactVersion could not be proven."""


class WorkspaceReadSetValidationError(WorkspaceGatewayError):
    """The mediated workspace read-set is not current at a commit boundary."""

    def __init__(self, report: WorkspaceReadSetValidationReport) -> None:
        details = (
            *report.stale_resources,
            *report.unavailable_resources,
            *report.unknown_resources,
        )
        suffix = f": {', '.join(details[:8])}" if details else ""
        if report.truncated:
            suffix += " (resource limit exceeded)"
        super().__init__(f"workspace read-set is not current{suffix}")
        self.report = report


@dataclass(frozen=True, slots=True)
class WorkspaceSnapshot:
    """Immutable identity of bytes observed through the workspace gateway."""

    artifact_id: str
    resource_uri: str
    content_hash: str
    size: int
    # A gateway never invents a semantic ArtifactVersion.  Callers may pass an
    # authoritative version obtained from Facts/ArtifactFS; when omitted this
    # remains ``None`` and the content hash is the only trusted identity.
    version: int | None = None


@dataclass(frozen=True, slots=True)
class WorkspaceReadSetValidationReport:
    """Bounded point-in-time validation of mediated workspace reads.

    This report compares the latest exact hash recorded for each file read
    through one gateway with bytes currently visible through the same
    root-confined ``WorkspaceTool``. It is intentionally not a filesystem
    transaction or lock: a caller should run it immediately before semantic
    commit, while the VPG/Facts and ownership fences remain authoritative.
    """

    current: bool
    checked_resources: tuple[str, ...] = ()
    stale_resources: tuple[str, ...] = ()
    unavailable_resources: tuple[str, ...] = ()
    unknown_resources: tuple[str, ...] = ()
    resource_limit: int = 256
    truncated: bool = False

    @property
    def checked_count(self) -> int:
        return len(self.checked_resources)


WorkspaceVersionValidator = Callable[[WorkspaceSnapshot], bool]


class WorkspaceProvenanceGateway:
    """Root-confined workspace reads/writes bound to an execution attempt.

    In strict mode, reads and writes are admitted only when their exact
    relative path appears in ``readable`` or ``writable``. In compatibility
    mode, other paths are allowed but remain visible as undeclared provenance
    when the normal coverage report is built.
    """

    def __init__(
        self,
        workspace: WorkspaceTool,
        context: ExecutionContext,
        *,
        readable: Iterable[str] = (),
        writable: Iterable[str] = (),
        strict: bool | None = None,
        version_validator: WorkspaceVersionValidator | None = None,
        version_authority: Any | None = None,
    ) -> None:
        self.workspace = workspace
        self.context = context
        requested_strict = context.secure_mode if strict is None else bool(strict)
        if context.secure_mode and not requested_strict:
            raise WorkspaceGatewayError(
                "a secure ExecutionContext cannot disable workspace strict mode"
            )
        self.strict = requested_strict
        if version_validator is not None and not callable(version_validator):
            raise WorkspaceGatewayError("version_validator must be callable")
        if version_validator is not None and version_authority is not None:
            raise WorkspaceGatewayError(
                "configure either version_validator or version_authority, not both"
            )
        if version_authority is not None and not callable(
            getattr(version_authority, "read_hash", None)
        ):
            raise WorkspaceGatewayError(
                "version_authority must expose read_hash(pid, uri, version)"
            )
        self._version_validator = version_validator
        self._version_authority = version_authority
        self._readable = self._build_capability_map(readable, capability="read")
        self._writable = self._build_capability_map(writable, capability="write")
        root_identity = str(self.workspace.root).encode("utf-8", errors="surrogatepass")
        self.gateway_id = f"workspace-{hashlib.sha256(root_identity).hexdigest()[:24]}"
        registered = tuple(getattr(self.context, "_workspace_provenance_gateways", ()) or ())
        if all(existing is not self for existing in registered):
            self.context._workspace_provenance_gateways = (*registered, self)

    @classmethod
    def for_task(
        cls,
        workspace: WorkspaceTool,
        context: ExecutionContext,
        task: Any,
        *,
        strict: bool | None = None,
        version_validator: WorkspaceVersionValidator | None = None,
        version_authority: Any | None = None,
    ) -> WorkspaceProvenanceGateway:
        """Bind workspace capabilities from one SDK Task's inputs/outputs."""

        return cls(
            workspace,
            context,
            readable=_workspace_resources(getattr(task, "inputs", ())),
            writable=_workspace_resources(getattr(task, "outputs", ())),
            strict=strict,
            version_validator=version_validator,
            version_authority=version_authority,
        )

    def _build_capability_map(
        self,
        resources: Iterable[str],
        *,
        capability: str,
    ) -> dict[str, str]:
        result: dict[str, str] = {}
        for resource in resources:
            original = str(resource).strip()
            if not original:
                continue
            artifact_id = self._artifact_id(original)
            existing = result.get(artifact_id)
            if existing is not None and existing != original:
                raise WorkspaceGatewayError(
                    f"workspace {capability} capability aliases the same path twice: "
                    f"{existing!r} and {original!r}"
                )
            result[artifact_id] = original
        return result

    def _artifact_id(self, resource: str) -> str:
        value = str(resource).strip()
        original = value
        for prefix in _WORKSPACE_SCHEMES:
            if value.startswith(prefix):
                value = value[len(prefix) :]
                # ``workspace:///relative/path`` is a common URI spelling
                # emitted by the Artifact/Context APIs.  The third slash is
                # URI syntax, not an instruction to escape the gateway root.
                if original.startswith("workspace:///"):
                    value = value.lstrip("/")
                break
        else:
            if "://" in value:
                raise WorkspaceGatewayError(f"unsupported workspace resource URI: {resource!r}")
        if not value:
            raise WorkspaceGatewayError("workspace resource must identify a file")
        try:
            resolved = self.workspace.resolve(value)
            relative = resolved.relative_to(self.workspace.root).as_posix()
        except (OSError, ValueError, PermissionError) as exc:
            raise WorkspaceAccessDenied(str(exc)) from exc
        if relative in {"", "."}:
            raise WorkspaceGatewayError("workspace resource must identify a file")
        return relative

    def _authorize(
        self,
        resource: str,
        *,
        capability: str,
    ) -> tuple[str, str]:
        artifact_id = self._artifact_id(resource)
        allowed = self._readable if capability == "read" else self._writable
        declared_uri = allowed.get(artifact_id)
        if declared_uri is None and self.strict:
            raise WorkspaceAccessDenied(
                f"workspace {capability} denied for undeclared path {artifact_id!r}"
            )
        return artifact_id, declared_uri or f"workspace://{artifact_id}"

    @staticmethod
    def _snapshot(
        *,
        artifact_id: str,
        resource_uri: str,
        payload: bytes,
        version: int | None = None,
    ) -> WorkspaceSnapshot:
        return WorkspaceSnapshot(
            artifact_id=artifact_id,
            resource_uri=resource_uri,
            content_hash=hashlib.sha256(payload).hexdigest(),
            size=len(payload),
            version=version,
        )

    @staticmethod
    def _normalize_version(version: int | None) -> int | None:
        """Validate an optional authoritative ArtifactVersion token.

        ``None`` is intentionally allowed: a plain workspace has no durable
        version authority of its own.  Rejecting booleans and non-positive
        values prevents callers from accidentally turning an ad-hoc counter
        into semantic truth.
        """

        if version is None:
            return None
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise WorkspaceGatewayError("workspace version must be a positive integer or None")
        return version

    def _preflight_version_validation(self, version: int | None) -> None:
        """Reject unverifiable strict version claims before filesystem access."""

        if (
            self.strict
            and version is not None
            and self._version_validator is None
            and self._version_authority is None
        ):
            raise WorkspaceVersionValidationError(
                "strict workspace version binding requires a version_validator or version_authority"
            )

    def _validate_snapshot_version(self, snapshot: WorkspaceSnapshot) -> str:
        """Validate an exact ``version + content_hash`` binding.

        Returns the source label persisted in provenance metadata.  With no
        configured authority, compatibility/audit mode retains the historical
        caller-supplied version behavior; strict mode is rejected by
        :meth:`_preflight_version_validation`.
        """

        if snapshot.version is None:
            return "content_hash"
        validator = self._version_validator
        if validator is not None:
            try:
                accepted = validator(snapshot)
            except Exception as exc:
                raise WorkspaceVersionValidationError(
                    f"workspace version validator failed for "
                    f"{snapshot.artifact_id!r}@{snapshot.version}: {exc}"
                ) from exc
            if accepted is not True:
                raise WorkspaceVersionValidationError(
                    f"workspace version validator rejected "
                    f"{snapshot.artifact_id!r}@{snapshot.version}"
                )
            return "validator"

        authority = self._version_authority
        if authority is None:
            return "caller"
        read_hash = authority.read_hash
        authority_pid = (
            str(getattr(self.context, "claim_id", "")).strip()
            or str(getattr(self.context, "attempt_id", "")).strip()
            or "workspace-gateway"
        )
        expected_hash: str | None = None
        try:
            # Prefer the declared URI because it preserves namespaces such as
            # ``vpg://workspace/...``.  FactsProvider-like authorities also
            # accept an artifact ID, which is used only when the URI is not
            # registered.
            expected_hash = read_hash(
                authority_pid,
                snapshot.resource_uri,
                snapshot.version,
            )
            if expected_hash is None and snapshot.artifact_id != snapshot.resource_uri:
                expected_hash = read_hash(
                    authority_pid,
                    snapshot.artifact_id,
                    snapshot.version,
                )
        except Exception as exc:
            raise WorkspaceVersionValidationError(
                f"workspace version authority failed for "
                f"{snapshot.artifact_id!r}@{snapshot.version}: {exc}"
            ) from exc
        if expected_hash is None:
            raise WorkspaceVersionValidationError(
                f"workspace version authority has no registered binding for "
                f"{snapshot.artifact_id!r}@{snapshot.version}"
            )
        try:
            normalized_expected = _normalize_hash(str(expected_hash))
        except WorkspaceGatewayError as exc:
            raise WorkspaceVersionValidationError(
                f"workspace version authority returned an invalid content hash for "
                f"{snapshot.artifact_id!r}@{snapshot.version}"
            ) from exc
        if normalized_expected != snapshot.content_hash:
            raise WorkspaceVersionValidationError(
                f"workspace version/content hash mismatch for "
                f"{snapshot.artifact_id!r}@{snapshot.version}"
            )
        return "authority"

    def _read_payload_snapshot(
        self,
        resource: str,
        *,
        version: int | None = None,
    ) -> tuple[bytes, WorkspaceSnapshot]:
        """Read one byte sequence and record exactly that sequence.

        Keeping the read and hash in one operation is important: a previous
        implementation read a file once, then read it again while constructing
        ``snapshot()``.  A concurrent mutation could therefore make the
        returned hash differ from the bytes actually observed by the executor.
        """

        artifact_id, resource_uri = self._authorize(resource, capability="read")
        resolved_version = self._normalize_version(version)
        self._preflight_version_validation(resolved_version)
        try:
            payload = self.workspace.read_bytes(artifact_id)
        except Exception as exc:
            # An admitted but missing/unreadable input is itself useful
            # provenance: strict verification must not mistake a failed read
            # for an absent dependency.
            self.context.observe_unknown(
                op=ProvenanceOperation.READ,
                resource_hint=resource_uri,
                artifact_id=artifact_id,
                gateway="workspace_v1",
                gateway_id=self.gateway_id,
                reason="workspace_read_failed",
                error=str(exc),
            )
            raise WorkspaceGatewayError(
                f"workspace read failed for {artifact_id!r}: {exc}"
            ) from exc
        snapshot = self._snapshot(
            artifact_id=artifact_id,
            resource_uri=resource_uri,
            payload=payload,
            version=resolved_version,
        )
        try:
            version_source = self._validate_snapshot_version(snapshot)
        except WorkspaceVersionValidationError as exc:
            self.context.record(
                ProvenanceOperation.READ,
                resource_uri=snapshot.resource_uri,
                artifact_id=snapshot.artifact_id,
                version=snapshot.version,
                content_hash=snapshot.content_hash,
                source="workspace-gateway",
                known=False,
                metadata={
                    "gateway": "workspace_v1",
                    "gateway_id": self.gateway_id,
                    "hash_algorithm": "sha256",
                    "size": snapshot.size,
                    "reason": "workspace_version_unverified",
                    "error": str(exc),
                },
            )
            raise
        self.context.record(
            ProvenanceOperation.READ,
            resource_uri=snapshot.resource_uri,
            artifact_id=snapshot.artifact_id,
            version=snapshot.version,
            content_hash=snapshot.content_hash,
            source="workspace-gateway",
            metadata={
                "gateway": "workspace_v1",
                "gateway_id": self.gateway_id,
                "hash_algorithm": "sha256",
                "size": snapshot.size,
                **({"version_source": version_source} if snapshot.version is not None else {}),
            },
        )
        return payload, snapshot

    def read_bytes(self, resource: str, *, version: int | None = None) -> bytes:
        """Read bytes once and record the exact payload hash in the read-set."""

        payload, _snapshot = self._read_payload_snapshot(resource, version=version)
        return payload

    def read_text(
        self,
        resource: str,
        *,
        encoding: str = "utf-8",
        version: int | None = None,
    ) -> str:
        """Read and record a text file through the same byte-level boundary."""

        payload, _snapshot = self._read_payload_snapshot(resource, version=version)
        return payload.decode(encoding)

    def snapshot(self, resource: str, *, version: int | None = None) -> WorkspaceSnapshot:
        """Read once and return the exact immutable identity that was recorded."""

        _payload, snapshot = self._read_payload_snapshot(resource, version=version)
        return snapshot

    def write_bytes(
        self,
        resource: str,
        content: bytes,
        *,
        expected_hash: str | None = None,
        version: int | None = None,
    ) -> WorkspaceSnapshot:
        """Atomically replace one declared output and record its resulting hash.

        ``expected_hash`` is a compare-before-write guard provided by
        ``WorkspaceTool``. The filesystem write and provenance append are not a
        cross-system transaction; an append failure is surfaced to the caller
        and must not be interpreted as a verified output.
        """

        artifact_id, resource_uri = self._authorize(resource, capability="write")
        payload = bytes(content)
        normalized_expected = _normalize_hash(expected_hash)
        resolved_version = self._normalize_version(version)
        self._preflight_version_validation(resolved_version)
        proposed_snapshot = self._snapshot(
            artifact_id=artifact_id,
            resource_uri=resource_uri,
            payload=payload,
            version=resolved_version,
        )
        # Validate a claimed existing ArtifactVersion before touching the
        # workspace.  Registering a brand-new version belongs to the
        # Artifact/Facts authority and should happen through its own commit
        # protocol rather than being invented by this gateway.
        version_source = self._validate_snapshot_version(proposed_snapshot)
        result = self.workspace.write_atomic(
            artifact_id,
            payload,
            expected_hash=normalized_expected,
        )
        if not result.ok:
            raise WorkspaceGatewayError(
                f"workspace write failed for {artifact_id!r}: {result.error}"
            )
        try:
            persisted = self.workspace.read_bytes(artifact_id)
        except Exception as exc:
            self.context.observe_unknown(
                op=ProvenanceOperation.WRITE,
                resource_hint=resource_uri,
                artifact_id=artifact_id,
                gateway="workspace_v1",
                gateway_id=self.gateway_id,
                reason="post_write_read_failed",
                error=str(exc),
            )
            raise WorkspaceGatewayError(
                f"workspace write could not be verified for {artifact_id!r}: {exc}"
            ) from exc
        if persisted != payload:
            self.context.observe_unknown(
                op=ProvenanceOperation.WRITE,
                resource_hint=resource_uri,
                artifact_id=artifact_id,
                gateway="workspace_v1",
                gateway_id=self.gateway_id,
                reason="post_write_content_mismatch",
            )
            raise WorkspaceGatewayError(f"workspace write verification failed for {artifact_id!r}")
        snapshot = self._snapshot(
            artifact_id=artifact_id,
            resource_uri=resource_uri,
            payload=persisted,
            version=resolved_version,
        )
        self.context.record(
            ProvenanceOperation.WRITE,
            resource_uri=snapshot.resource_uri,
            artifact_id=snapshot.artifact_id,
            version=snapshot.version,
            content_hash=snapshot.content_hash,
            source="workspace-gateway",
            metadata={
                "gateway": "workspace_v1",
                "gateway_id": self.gateway_id,
                "hash_algorithm": "sha256",
                "size": snapshot.size,
                "expected_previous_hash": normalized_expected,
                **({"version_source": version_source} if snapshot.version is not None else {}),
            },
        )
        return snapshot

    def write_text(
        self,
        resource: str,
        content: str,
        *,
        encoding: str = "utf-8",
        expected_hash: str | None = None,
        version: int | None = None,
    ) -> WorkspaceSnapshot:
        """Encode, write, verify, and record one declared text output."""

        return self.write_bytes(
            resource,
            content.encode(encoding),
            expected_hash=expected_hash,
            version=version,
        )

    @property
    def read_set(self) -> tuple[WorkspaceSnapshot, ...]:
        """Return the deduplicated workspace read-set observed by this context."""

        return self._event_set(ProvenanceOperation.READ)

    @property
    def write_set(self) -> tuple[WorkspaceSnapshot, ...]:
        """Return the deduplicated workspace write-set observed by this context."""

        return self._event_set(ProvenanceOperation.WRITE)

    def validate_read_set_current(
        self,
        *,
        max_resources: int = 256,
    ) -> WorkspaceReadSetValidationReport:
        """Re-read the latest mediated read-set and compare exact byte hashes.

        The result is bounded and fail-closed. A missing/unreadable file,
        unidentified/unknown latest observation, or more than
        ``max_resources`` distinct reads makes ``current`` false. The method
        validates only accesses that crossed this gateway; direct ``open`` /
        ``Path`` calls and other workspaces remain outside its coverage.
        """

        if (
            isinstance(max_resources, bool)
            or not isinstance(max_resources, int)
            or max_resources < 1
            or max_resources > 4096
        ):
            raise WorkspaceGatewayError("max_resources must be an integer between 1 and 4096")

        latest: dict[str, Any] = {}
        truncated = False
        for event in reversed(self.context.events):
            if event.op is not ProvenanceOperation.READ:
                continue
            metadata = event.metadata or {}
            if event.source != "workspace-gateway" and metadata.get("gateway") != "workspace_v1":
                continue
            event_gateway_id = str(metadata.get("gateway_id", "")).strip()
            if event_gateway_id and event_gateway_id != self.gateway_id:
                continue
            identity = str(event.artifact_id or event.resource_uri or "").strip()
            if not identity or identity in latest:
                continue
            if len(latest) >= max_resources:
                truncated = True
                break
            latest[identity] = event

        checked: list[str] = []
        stale: list[str] = []
        unavailable: list[str] = []
        unknown: list[str] = []
        for identity in sorted(latest):
            event = latest[identity]
            artifact_id = str(event.artifact_id or "").strip()
            expected_hash = str(event.content_hash or "").strip().lower()
            if not event.known or not artifact_id or not expected_hash:
                unknown.append(identity)
                continue
            try:
                payload = self.workspace.read_bytes(artifact_id)
            except Exception:
                unavailable.append(identity)
                continue
            checked.append(identity)
            if hashlib.sha256(payload).hexdigest() != expected_hash:
                stale.append(identity)

        return WorkspaceReadSetValidationReport(
            current=not (stale or unavailable or unknown or truncated),
            checked_resources=tuple(checked),
            stale_resources=tuple(stale),
            unavailable_resources=tuple(unavailable),
            unknown_resources=tuple(unknown),
            resource_limit=max_resources,
            truncated=truncated,
        )

    def require_read_set_current(
        self,
        *,
        max_resources: int = 256,
    ) -> WorkspaceReadSetValidationReport:
        """Return a current report or raise before semantic commit."""

        report = self.validate_read_set_current(max_resources=max_resources)
        if not report.current:
            raise WorkspaceReadSetValidationError(report)
        return report

    def _event_set(self, operation: ProvenanceOperation) -> tuple[WorkspaceSnapshot, ...]:
        # Track the *latest event*, including unknown events.  If a later
        # observation for an artifact cannot be verified, an earlier trusted
        # snapshot must not remain visible as the current read/write binding.
        # Otherwise callers could accidentally commit against stale
        # cognition while the coverage report correctly says UNKNOWN.
        latest: dict[str, Any] = {}
        for event in self.context.events:
            if event.op is not operation or (
                event.source != "workspace-gateway"
                and event.metadata.get("gateway") != "workspace_v1"
            ):
                continue
            event_gateway_id = str(event.metadata.get("gateway_id", "")).strip()
            if event_gateway_id and event_gateway_id != self.gateway_id:
                continue
            artifact_id = event.artifact_id or event.resource_uri
            if not artifact_id:
                continue
            latest[artifact_id] = event

        trusted: list[WorkspaceSnapshot] = []
        for artifact_id in sorted(latest):
            event = latest[artifact_id]
            if not event.known or not event.content_hash:
                continue
            try:
                size = int(event.metadata.get("size", 0) or 0)
            except (TypeError, ValueError):
                size = 0
            trusted.append(
                WorkspaceSnapshot(
                    artifact_id=event.artifact_id or artifact_id,
                    resource_uri=event.resource_uri,
                    content_hash=event.content_hash,
                    size=size,
                    version=event.version,
                )
            )
        return tuple(trusted)


def _workspace_resources(resources: Iterable[str]) -> tuple[str, ...]:
    """Select workspace-backed Task resources without claiming other schemes."""

    selected: list[str] = []
    for resource in resources:
        value = str(resource).strip()
        if not value:
            continue
        if value.startswith(_WORKSPACE_SCHEMES) or "://" not in value:
            selected.append(value)
    return tuple(selected)


def _normalize_hash(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized.startswith("sha256:"):
        normalized = normalized.removeprefix("sha256:")
    if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
        raise WorkspaceGatewayError("expected_hash must be a SHA-256 digest")
    return normalized


__all__ = [
    "WorkspaceAccessDenied",
    "WorkspaceGatewayError",
    "WorkspaceProvenanceGateway",
    "WorkspaceReadSetValidationError",
    "WorkspaceReadSetValidationReport",
    "WorkspaceSnapshot",
    "WorkspaceVersionValidationError",
    "WorkspaceVersionValidator",
]
