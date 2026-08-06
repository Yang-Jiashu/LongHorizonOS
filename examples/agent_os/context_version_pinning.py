"""Demo 3: Version Pinning.

读取 document@v1 构建 Context → Artifact 更新到 v2 → 原 Context 仍读取 v1 →
新 Context 明确加载 v2.
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

    sdk.write("p1", "workspace:///doc.md", b"v1 content here\n", "kv1")
    sdk.write("p1", "workspace:///doc.md", b"v2 content here much longer\n", "kv2")

    art_map = {a["canonical_uri"]: a["artifact_id"] for a in service.list_artifacts("p1")}
    v1 = next(v for v in sdk.list_versions("p1", "workspace:///doc.md") if v["version"] == 1)
    v2 = next(v for v in sdk.list_versions("p1", "workspace:///doc.md") if v["version"] == 2)

    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk = ContextSDK(ctx_svc)

    art_id = art_map["artifact://ns-p1/doc.md"]
    manifest_v1 = ContextManifest(
        owner_pid="p1",
        refs=(
            ContentRef(
                ref_id="doc",
                canonical_uri="artifact://ns-p1/doc.md",
                artifact_id=art_id,
                version=v1["version"],
                content_hash=v1["content_hash"],
                media_type="text/markdown",
                priority=10,
                required=True,
            ),
        ),
        token_budget=10_000,
        page_size_bytes=64,
    )
    handle_v1, loaded_v1 = ctx_sdk.load(pid="p1", manifest=manifest_v1)

    manifest_v2 = ContextManifest(
        owner_pid="p1",
        refs=(
            ContentRef(
                ref_id="doc",
                canonical_uri="artifact://ns-p1/doc.md",
                artifact_id=art_id,
                version=v2["version"],
                content_hash=v2["content_hash"],
                media_type="text/markdown",
                priority=10,
                required=True,
            ),
        ),
        token_budget=10_000,
        page_size_bytes=64,
    )
    handle_v2, loaded_v2 = ctx_sdk.load(pid="p1", manifest=manifest_v2)

    print(
        json.dumps(
            {
                "demo": "version_pinning",
                "v1_pages_decoded": [
                    p.content.decode("utf-8", errors="replace") for p in loaded_v1.ordered_pages
                ],
                "v2_pages_decoded": [
                    p.content.decode("utf-8", errors="replace") for p in loaded_v2.ordered_pages
                ],
                "v1_materialized_hash": loaded_v1.materialized_hash,
                "v2_materialized_hash": loaded_v2.materialized_hash,
                "v1_still_v1_after_v2_commit": (
                    loaded_v1.ordered_pages[0].content == b"v1 content here\n"
                ),
                "v2_reads_v2": b"v2 content" in loaded_v2.ordered_pages[0].content,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
