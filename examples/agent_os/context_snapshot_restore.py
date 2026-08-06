"""Demo 4: Snapshot Restore.

创建 Context Snapshot → 关闭/重启 (simulated by fresh ContextService over same
ArtifactFS) → restore Snapshot → materialized hash 完全一致.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class _AllowsAllCaps:
    def can_context_operation(self, **kwargs) -> bool:
        return True

    def can_artifact_read(self, **kwargs) -> bool:
        return True


class _ArtifactSupplier:
    """Adapter from ArtifactFSService to Context VM content supplier."""

    def __init__(self, svc: ArtifactFSService) -> None:
        self._svc = svc

    def read_version(self, *, artifact_id: str, version: int, canonical_uri: str) -> bytes:
        return self._svc.read(pid="p1", uri=canonical_uri, version=version)


def main() -> None:
    tmpdir = tempfile.mkdtemp()
    db_path = str(Path(tmpdir) / "state.db")
    storage = SQLiteStorage(db_path)
    journal = JournalService(storage)
    cas_root = Path(tmpdir) / "cas"
    driver = LocalArtifactStorageDriver(cas_root)
    projections = ArtifactProjections(storage)
    cap_svc = CapabilityService(storage, journal)
    ns_svc = NamespaceService(projections, journal)
    service = ArtifactFSService(projections, driver, journal, capability_service=cap_svc)
    sdk = ArtifactSDK(service, ns_svc)
    ns_svc.create_namespace("p1")
    cap_svc.grant(
        "p1", Capability(resource_pattern="artifact://ns-p1/**", operations={"read", "write"})
    )
    sdk.write("p1", "workspace:///doc.md", b"snapshot content\n", "ksnap")
    ver = next(iter(sdk.list_versions("p1", "workspace:///doc.md")))
    art_map = {a["canonical_uri"]: a["artifact_id"] for a in service.list_artifacts("p1")}

    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk = ContextSDK(ctx_svc)
    manifest = ContextManifest(
        owner_pid="p1",
        refs=(
            ContentRef(
                ref_id="doc",
                canonical_uri="artifact://ns-p1/doc.md",
                artifact_id=art_map["artifact://ns-p1/doc.md"],
                version=ver["version"],
                content_hash=ver["content_hash"],
                media_type="text/markdown",
                priority=10,
                required=True,
            ),
        ),
        token_budget=10_000,
        page_size_bytes=64,
    )
    handle, loaded = ctx_sdk.load(pid="p1", manifest=manifest)
    snap = ctx_sdk.snapshot(pid="p1", context_id=loaded.context_id)

    # Simulate restart: new ContextService instance, same ArtifactFS via
    # supplier pointing at the same underlying storage+CAS.
    ctx_svc2 = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk2 = ContextSDK(ctx_svc2)
    # Move snapshot into new service (snapshots are app-level state; here we
    # simulate by copying via the snapshot record itself).
    ctx_svc2._snaps[snap.snapshot_id] = snap
    h2, restored = ctx_sdk2.restore_snapshot(pid="p1", snapshot_id=snap.snapshot_id)

    print(
        json.dumps(
            {
                "demo": "snapshot_restore",
                "original_materialized_hash": loaded.materialized_hash,
                "restored_materialized_hash": restored.materialized_hash,
                "hashes_match": loaded.materialized_hash == restored.materialized_hash,
                "original_pages": [
                    p.content.decode("utf-8", errors="replace") for p in loaded.ordered_pages
                ],
                "restored_pages": [
                    p.content.decode("utf-8", errors="replace") for p in restored.ordered_pages
                ],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
