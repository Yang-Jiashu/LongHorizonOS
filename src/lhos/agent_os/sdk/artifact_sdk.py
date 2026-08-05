"""Artifact SDK — convenience facade for agent processes.

Provides a clean, high-level API for reading and writing versioned artifacts,
managing handles, mounts, snapshots, watches, and quotas.

Usage:
    from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK

    sdk = ArtifactSDK(service, namespace_service)
    sdk.write("workspace:///report.md", b"# Report")
    data = sdk.read("workspace:///report.md")
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from lhos.agent_os.artifacts.models import (
    ArtifactHandle,
    ArtifactWatch,
    NamespaceMount,
    NamespaceSnapshot,
    WriteTransaction,
)
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.service import ArtifactFSService


class ArtifactSDK:
    """High-level SDK for artifact file system operations.

    Wraps ArtifactFSService and NamespaceService with a simplified API
    that automatically resolves workspace:/// URIs to the caller's namespace.
    """

    def __init__(
        self,
        service: ArtifactFSService,
        namespace_service: NamespaceService,
    ) -> None:
        self._service = service
        self._ns_service = namespace_service

    # ── File Operations ──────────────────────────────────────────────────

    def read(self, pid: str, uri: str, version: int | None = None) -> bytes:
        """Read artifact content as bytes."""
        return self._service.read(pid, uri, version)

    def read_text(self, pid: str, uri: str, version: int | None = None) -> str:
        """Read artifact content as decoded UTF-8 text."""
        return self._service.read(pid, uri, version).decode("utf-8")

    def write(
        self,
        pid: str,
        uri: str,
        content: bytes | str,
        idempotency_key: str | None = None,
        expected_version: int | None = None,
    ) -> WriteTransaction:
        """Write content to an artifact atomically.

        If idempotency_key is None, a UUID-based key is generated.
        """
        from uuid import uuid4

        key = idempotency_key or f"auto-{uuid4().hex}"
        return self._service.write(pid, uri, content, key, expected_version)

    def stat(self, pid: str, uri: str) -> dict[str, Any]:
        """Get artifact metadata (version, size, hash, timestamps)."""
        return self._service.read_metadata(pid, uri)

    def list(self, pid: str, uri_prefix: str | None = None) -> Sequence[dict[str, Any]]:
        """List artifacts in the caller's namespace."""
        return self._service.list_artifacts(pid, uri_prefix)

    def list_versions(self, pid: str, uri: str) -> Sequence[dict[str, Any]]:
        """List all versions of an artifact."""
        return self._service.list_versions(pid, uri)

    def delete(self, pid: str, uri: str) -> bool:
        """Soft-delete an artifact."""
        return self._service.delete(pid, uri)

    def exists(self, pid: str, uri: str) -> bool:
        """Check if an artifact exists (non-deleted, has at least one version)."""
        try:
            self._service.read_metadata(pid, uri)
            return True
        except Exception:
            return False

    # ── Handle Operations ────────────────────────────────────────────────

    def open(
        self,
        pid: str,
        uri: str,
        mode: str = "read",
        expected_version: int | None = None,
    ) -> ArtifactHandle:
        """Open a handle to an artifact."""
        return self._service.open(pid, uri, mode, expected_version)

    def close(self, handle_id: str) -> bool:
        """Close an open handle."""
        return self._service.close(handle_id)

    def close_all(self, pid: str) -> int:
        """Close all open handles for a process."""
        return self._service.close_all_for_pid(pid)

    # ── Namespace Operations ─────────────────────────────────────────────

    def create_namespace(
        self,
        pid: str,
        quota_bytes: int | None = None,
        max_open_handles: int | None = None,
    ):
        """Create a private namespace for a process."""
        return self._ns_service.create_namespace(pid, quota_bytes, max_open_handles)

    def get_namespace(self, pid: str):
        """Get the namespace for a process."""
        return self._ns_service.get_namespace_for_pid(pid)

    def set_quota(self, namespace_id: str, quota_bytes: int) -> None:
        """Set storage quota for a namespace."""
        self._ns_service.set_quota(namespace_id, quota_bytes)

    def get_usage(self, pid: str) -> dict[str, Any]:
        """Get storage usage statistics for a process's namespace."""
        namespace_id = f"ns-{pid}"
        return self._service.get_namespace_usage(namespace_id)

    # ── Mount Operations ─────────────────────────────────────────────────

    def mount(
        self,
        target_pid: str,
        mount_point: str,
        source_pid: str,
        source_prefix: str = "",
        mode: str = "shared_readonly",
    ) -> NamespaceMount:
        """Mount a source process's namespace into the target process's namespace."""
        source_ns_id = f"ns-{source_pid}"
        return self._ns_service.mount(
            target_pid, mount_point, source_ns_id, source_prefix, mode
        )

    def unmount(self, target_pid: str, mount_point: str) -> bool:
        """Remove a mount from a process's namespace."""
        return self._ns_service.unmount(target_pid, mount_point)

    def list_mounts(self, pid: str) -> Sequence[NamespaceMount]:
        """List all mounts in a process's namespace."""
        return self._ns_service.list_mounts(pid)

    # ── Snapshot Operations ──────────────────────────────────────────────

    def snapshot(self, pid: str, namespace_id: str | None = None) -> NamespaceSnapshot:
        """Create an immutable snapshot of a namespace."""
        return self._ns_service.create_snapshot(pid, namespace_id)

    def get_snapshot(self, snapshot_id: str) -> NamespaceSnapshot | None:
        """Retrieve a snapshot by ID."""
        return self._ns_service.get_snapshot(snapshot_id)

    # ── Watch Operations ─────────────────────────────────────────────────

    def watch(self, pid: str, uri_prefix: str) -> ArtifactWatch:
        """Register a watch for artifact changes matching uri_prefix."""
        return self._service.watch(pid, uri_prefix)

    def unwatch(self, watch_id: str) -> bool:
        """Deactivate a watch."""
        return self._service.unwatch(watch_id)

    def list_watches(self, pid: str) -> Sequence[ArtifactWatch]:
        """List all active watches for a process."""
        return self._service.list_watches(pid)

    # ── Recovery ─────────────────────────────────────────────────────────

    def recover(self) -> dict[str, Any]:
        """Run crash recovery for artifact transactions."""
        return self._service.recover()
