"""Demo 2: Budget + Eviction.

Manifest 请求超过预算的 optional pages → required pages 全部加载 →
optional pages 确定性选择 → pin 一个 page → eviction 不得移除 pinned page.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from lhos.agent_os.context.estimator import DeterministicByteTokenEstimator
from lhos.agent_os.context.models import ContentRef, ContextManifest
from lhos.agent_os.context.sdk import ContextSDK
from lhos.agent_os.context.service import ContextService
from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
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

    def read_version(self, *, artifact_id: str, version: int,
                     canonical_uri: str) -> bytes:
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
    service = ArtifactFSService(projections, driver, journal,
                                capability_service=cap_svc)
    sdk = ArtifactSDK(service, ns_svc)
    ns_svc.create_namespace("p1")
    cap_svc.grant("p1", Capability(resource_pattern="artifact://ns-p1/**",
                                    operations={"read", "write"}))

    sdk.write("p1", "workspace:///req.md",
              b"x" * 500, "k-req")
    sdk.write("p1", "workspace:///opt1.md",
              b"y" * 5000, "k-opt1")
    sdk.write("p1", "workspace:///opt2.md",
              b"z" * 5000, "k-opt2")

    service._ns_resolver = ns_svc  # type: ignore[attr-defined]

    ver_req = next(iter(sdk.list_versions("p1", "workspace:///req.md")))
    ver_o1 = next(iter(sdk.list_versions("p1", "workspace:///opt1.md")))
    ver_o2 = next(iter(sdk.list_versions("p1", "workspace:///opt2.md")))

    # URI -> artifact_id lookup
    art_map = {a["canonical_uri"]: a["artifact_id"]
               for a in service.list_artifacts("p1")}

    byte_budget = 520
    manifest = ContextManifest(
        owner_pid="p1",
        refs=(
            ContentRef(
                ref_id="req",
                canonical_uri="artifact://ns-p1/req.md",
                artifact_id=art_map["artifact://ns-p1/req.md"],
                version=ver_req["version"],
                content_hash=ver_req["content_hash"],
                media_type="text/markdown",
                priority=100,
                required=True,
            ),
            ContentRef(
                ref_id="opt1",
                canonical_uri="artifact://ns-p1/opt1.md",
                artifact_id=art_map["artifact://ns-p1/opt1.md"],
                version=ver_o1["version"],
                content_hash=ver_o1["content_hash"],
                media_type="text/markdown",
                priority=50,
                required=False,
            ),
            ContentRef(
                ref_id="opt2",
                canonical_uri="artifact://ns-p1/opt2.md",
                artifact_id=art_map["artifact://ns-p1/opt2.md"],
                version=ver_o2["version"],
                content_hash=ver_o2["content_hash"],
                media_type="text/markdown",
                priority=50,
                required=False,
            ),
        ),
        token_budget=100_000,
        byte_budget=byte_budget,
        page_size_bytes=512,
    )
    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk = ContextSDK(ctx_svc)
    handle, loaded = ctx_sdk.load(pid="p1", manifest=manifest)

    all_page_ids = [p.page_id for p in loaded.ordered_pages]
    pinned_page = all_page_ids[0]
    ctx_sdk.pin(pid="p1", handle_id=handle.handle_id,
                page_ids=[pinned_page])

    # set up valid pid for eviction by manipulating handle ownership
    result = ctx_svc.evict(
        pid="p1",
        working_set_id=handle.working_set_id,
        target_tokens=100_000,
    )

    print(json.dumps({
        "demo": "budget_eviction",
        "selected_pages": [p.page_id for p in loaded.ordered_pages],
        "omitted_refs": [o.ref_id for o in loaded.omitted_refs],
        "bytes_used": loaded.bytes_used,
        "byte_budget": byte_budget,
        "pinned_page": pinned_page,
        "evicted_pages": result["evicted_pages"],
        "eviction_blocked_pinned": pinned_page not in result["evicted_pages"],
    }, indent=2))


if __name__ == "__main__":
    main()
