"""Namespace Service — manages artifact namespaces per process.

Each process gets a private namespace on spawn: ns-<pid>.
Namespaces can be shared via mounts (Phase C1 Commit 5).
"""

from __future__ import annotations

from lhos.agent_os.artifacts.models import ArtifactNamespace, NamespaceMount, NamespaceSnapshot
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

    # ── Mounts ────────────────────────────────────────────────────────────

    def mount(
        self,
        target_pid: str,
        mount_point: str,
        source_namespace_id: str,
        source_prefix: str = "",
        mode: str = "shared_readonly",
    ) -> NamespaceMount:
        """Mount a source namespace prefix into the target process's namespace.

        Modes:
        - private: mount is only visible to target process
        - shared_readonly: target can read but not write
        - copy_on_write: target gets a copy on first write
        - shared_readwrite: target can read and write (deferred)
        """
        target_ns_id = f"ns-{target_pid}"
        ns = self._projections.get_namespace(target_ns_id)
        if not ns:
            raise ValueError(f"Target namespace not found: {target_ns_id}")

        # Normalize mount_point (remove leading/trailing slashes)
        mount_point = mount_point.strip("/")
        source_prefix = source_prefix.strip("/")

        mnt = NamespaceMount(
            namespace_id=target_ns_id,
            mount_point=mount_point,
            source_namespace_id=source_namespace_id,
            source_prefix=source_prefix,
            mode=mode,  # type: ignore[arg-type]
        )
        self._projections.upsert_mount(mnt)

        ev = KernelEvent(
            pid=target_pid,
            event_type="ARTIFACT_MOUNT_CREATED",
            payload=mnt.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        return mnt

    def unmount(self, target_pid: str, mount_point: str) -> bool:
        """Remove a mount from the target namespace."""
        target_ns_id = f"ns-{target_pid}"
        mount_point = mount_point.strip("/")
        mounts = self._projections.list_mounts(target_ns_id)
        for m in mounts:
            if m.mount_point == mount_point:
                with self._projections._storage.transaction() as tx:
                    tx.execute(
                        "DELETE FROM mounts_projection WHERE mount_id = ?",
                        (m.mount_id,),
                    )
                ev = KernelEvent(
                    pid=target_pid,
                    event_type="ARTIFACT_MOUNT_REMOVED",
                    payload={"mount_id": m.mount_id, "mount_point": mount_point},
                )
                self._journal.append_event(ev)
                return True
        return False

    def list_mounts(self, pid: str) -> list[NamespaceMount]:
        """List all mounts in a process's namespace."""
        return self._projections.list_mounts(f"ns-{pid}")

    def resolve_mount(self, target_namespace_id: str, path: str) -> tuple[str, str, str]:
        """Resolve a path through mounts.

        Returns (resolved_namespace_id, resolved_path, mount_mode).
        If no mount matches, returns (target_namespace_id, path, "private").
        """
        mounts = self._projections.list_mounts(target_namespace_id)
        for mnt in mounts:
            if path == mnt.mount_point or path.startswith(mnt.mount_point + "/"):
                # Path is under this mount
                relative = path[len(mnt.mount_point) :].lstrip("/")
                resolved_path = (
                    f"{mnt.source_prefix}/{relative}".strip("/") if mnt.source_prefix else relative
                )
                return mnt.source_namespace_id, resolved_path, mnt.mode
        return target_namespace_id, path, "private"

    # ── Snapshots ──────────────────────────────────────────────────────────

    def create_snapshot(
        self,
        pid: str,
        namespace_id: str | None = None,
    ) -> NamespaceSnapshot:
        """Create an immutable snapshot of a namespace.

        Captures artifact_versions: {canonical_uri: version} and
        content_refs: {"artifact_id:version": content_ref}.
        """
        if namespace_id is None:
            namespace_id = f"ns-{pid}"

        ns = self._projections.get_namespace(namespace_id)
        if not ns:
            raise ValueError(f"Namespace not found: {namespace_id}")

        artifacts = self._projections.list_artifacts(namespace_id)
        artifact_versions: dict[str, int] = {}
        content_refs: dict[str, str] = {}

        for a in artifacts:
            if a.current_version > 0:
                artifact_versions[a.canonical_uri] = a.current_version
                ver = self._projections.get_version(a.artifact_id, a.current_version)
                if ver:
                    content_refs[f"{a.artifact_id}:{ver.version}"] = ver.content_ref

        snap = NamespaceSnapshot(
            namespace_id=namespace_id,
            artifact_versions=artifact_versions,
            content_refs=content_refs,
            created_by_pid=pid,
        )
        self._projections.upsert_snapshot(snap)

        ev = KernelEvent(
            pid=pid,
            event_type="ARTIFACT_SNAPSHOT_CREATED",
            payload=snap.model_dump(mode="json"),
        )
        self._journal.append_event(ev)
        return snap

    def get_snapshot(self, snapshot_id: str) -> NamespaceSnapshot | None:
        return self._projections.get_snapshot(snapshot_id)

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
        elif ev.event_type == "ARTIFACT_MOUNT_CREATED":
            mnt = NamespaceMount(**ev.payload)
            self._projections.upsert_mount(mnt)
        elif ev.event_type == "ARTIFACT_MOUNT_REMOVED":
            with self._projections._storage.transaction() as tx:
                tx.execute(
                    "DELETE FROM mounts_projection WHERE mount_id = ?",
                    (ev.payload["mount_id"],),
                )
        elif ev.event_type == "ARTIFACT_SNAPSHOT_CREATED":
            snap = NamespaceSnapshot(**ev.payload)
            self._projections.upsert_snapshot(snap)
