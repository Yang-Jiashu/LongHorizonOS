"""Demo 1: Basic Load.

Process 创建两个 ArtifactVersion → 构造 version-pinned Manifest → 加载 Context
→ 输出选中页面 → 输出 token/byte accounting.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import (
    ContentRef,
    ContextManifest,
)
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Capability, KernelEvent
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


class _AllowsAllCaps:
    def can_context_operation(self, **kwargs) -> bool:
        return True

    def can_artifact_read(self, **kwargs) -> bool:
        return True


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
    service = ArtifactFSService(
        projections, driver, journal, capability_service=cap_svc
    )
    sdk = ArtifactSDK(service, ns_svc)

    ns_svc.create_namespace("p1")
    cap_svc.grant(
        "p1",
        Capability(resource_pattern="artifact://ns-p1/**",
                   operations={"read", "write"}),
    )

    sdk.write("p1", "workspace:///doc.md", b"# Hello\nWorld\n", "k1")
    sdk.write("p1", "workspace:///doc.md", b"# Hello v2\nMore stuff here\n", "k2")
    sdk.write("p1", "workspace:///other.md", b"Other document\n", "o1")

    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk = ContextSDK(ctx_svc)

    # build a uri -> artifact_id lookup from projection
    art_map = {a["canonical_uri"]: a["artifact_id"]
               for a in service.list_artifacts("p1")}

    v1_row = None
    v2_row = None
    for v in sdk.list_versions("p1", "workspace:///doc.md"):
        if v["version"] == 1:
            v1_row = v
        elif v["version"] == 2:
            v2_row = v

    other_row = next(iter(sdk.list_versions("p1", "workspace:///other.md")))

    manifest = ContextManifest(
        owner_pid="p1",
        refs=(
            ContentRef(
                ref_id="doc-v1",
                canonical_uri="artifact://ns-p1/doc.md",
                artifact_id=art_map["artifact://ns-p1/doc.md"],
                version=v1_row["version"],
                content_hash=v1_row["content_hash"],
                media_type="text/markdown",
                priority=100,
                required=True,
            ),
            ContentRef(
                ref_id="other",
                canonical_uri="artifact://ns-p1/other.md",
                artifact_id=art_map["artifact://ns-p1/other.md"],
                version=other_row["version"],
                content_hash=other_row["content_hash"],
                media_type="text/plain",
                priority=50,
                required=False,
            ),
        ),
        token_budget=10_000,
        page_size_bytes=64,
    )

    handle, loaded = ctx_sdk.load(pid="p1", manifest=manifest)
    print(json.dumps({
        "demo": "basic_load",
        "context_id": loaded.context_id,
        "manifest_hash": loaded.manifest_hash,
        "materialized_hash": loaded.materialized_hash,
        "selected_pages": len(loaded.ordered_pages),
        "omitted_refs": [o.ref_id for o in loaded.omitted_refs],
        "tokens_used": loaded.tokens_used,
        "bytes_used": loaded.bytes_used,
        "version_bindings": [
            {"page_id": vb.page_id, "artifact_id": vb.artifact_id,
             "version": vb.version, "content_hash": vb.content_hash}
            for vb in loaded.version_bindings
        ],
    }, indent=2))


class _ArtifactSupplier:
    """Adapter from ArtifactFSService to Context VM content supplier."""

    def __init__(self, svc: ArtifactFSService) -> None:
        self._svc = svc

    def read_version(self, *, artifact_id: str, version: int,
                     canonical_uri: str) -> bytes:
        return self._svc.read(pid="p1", uri=canonical_uri, version=version)


if __name__ == "__main__":
    main()
