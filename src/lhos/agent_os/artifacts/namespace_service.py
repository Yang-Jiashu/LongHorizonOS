"""Namespace Service — manages artifact namespaces per process.

Each process gets a private namespace on spawn: ns-<pid>.
Namespaces can be shared via mounts (Phase C1 Commit 5).
"""

from __future__ import annotations

from lhos.agent_os.artifacts.models import ArtifactNamespace
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.kernel.models import KernelEvent
from lhos.agent_os.services.journal import JournalService


class NamespaceService:
    """Manages artifact namespaces."""

    def __init__(
        self,
        projections: ArtifactProjections,
        journal: JournalService,
    ) -> None:
        self._projections = projections
        self._journal = journal

    def create_namespace(
        self,
        pid: str,
        quota_bytes: int | None = None,
        max_open_handles: int | None = None,
    ) -> ArtifactNamespace:
        """Create a private namespace for a process.

        Namespace ID: ns-<pid>
        Root URI: artifact://ns-<pid>/
        """
        namespace_id = f"ns-{pid}"
        existing = self._projections.get_namespace(namespace_id)
        if existing:
            return existing

        ns = ArtifactNamespace(
            namespace_id=namespace_id,
            owner_pid=pid,
            root_uri=f"artifact://{namespace_id}",
            quota_bytes=quota_bytes,
            max_open_handles=max_open_handles,
        )
        self._projections.upsert_namespace(ns)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_NAMESPACE_CREATED",
            payload=ns.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        return ns

    def get_namespace(self, namespace_id: str) -> ArtifactNamespace | None:
        return self._projections.get_namespace(namespace_id)

    def get_namespace_for_pid(self, pid: str) -> ArtifactNamespace | None:
        return self._projections.get_namespace(f"ns-{pid}")

    def delete_namespace(self, pid: str) -> bool:
        """Delete a process's namespace (on process exit)."""
        namespace_id = f"ns-{pid}"
        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            return False

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_NAMESPACE_DELETED",
            payload={"namespace_id": namespace_id},
        )
        self._journal.append_event(ev)
        return True

    def set_quota(self, namespace_id: str, quota_bytes: int) -> None:
        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            raise ValueError(f"Namespace not found: {namespace_id}")
        ns.quota_bytes = quota_bytes
        self._projections.upsert_namespace(ns)

        ev = KernelEvent(
            pid=ns.owner_pid,
            event_type="ARTIFACT_NAMESPACE_QUOTA_SET",
            payload={"namespace_id": namespace_id, "quota_bytes": quota_bytes},
        )
        self._journal.append_event(ev)

    # ── Projection ─────────────────────────────────────────────────────────

    def handle_event(self, ev: KernelEvent) -> None:
        if ev.event_type == "ARTIFACT_NAMESPACE_CREATED":
            ns = ArtifactNamespace(**ev.payload)
            self._projections.upsert_namespace(ns)
        elif ev.event_type == "ARTIFACT_NAMESPACE_QUOTA_SET":
            existing_ns: ArtifactNamespace | None = self._projections.get_namespace(
                ev.payload["namespace_id"]
            )
            if existing_ns:
                existing_ns.quota_bytes = ev.payload["quota_bytes"]
                self._projections.upsert_namespace(existing_ns)
