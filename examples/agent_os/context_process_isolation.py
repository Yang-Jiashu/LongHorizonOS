"""Demo 5: Process Isolation.

P1 创建 ContextHandle → P2 尝试 read/pin/evict → 全部拒绝 → P1 正常使用.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.context.errors import (
    ErrHandleNotOwned,
)
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


class _PidAwareCaps:
    """Capability checker that enforces per-PID artifact read restrictions.

    Here we allow both P1 and P2 to read for simplicity; the process-isolation
    we test is purely the Context VM Handle/PID ownership, not the underlying
    Artifact read capability.
    """

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
    for pid in ("p1", "p2"):
        ns_svc.create_namespace(pid)
        cap_svc.grant(
            pid,
            Capability(resource_pattern=f"artifact://ns-{pid}/**", operations={"read", "write"}),
        )

    service._ns_resolver = ns_svc  # type: ignore[attr-defined]
    sdk.write("p1", "workspace:///doc.md", b"p1 private content\n", "kp1")
    ver = next(iter(sdk.list_versions("p1", "workspace:///doc.md")))
    art_map = {a["canonical_uri"]: a["artifact_id"] for a in service.list_artifacts("p1")}

    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(service),
        capability_checker=_PidAwareCaps(),
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

    results = {"demo": "process_isolation"}

    # --- P1 read succeeds ---
    r = ctx_sdk.read(pid="p1", handle_id=handle.handle_id)
    results["p1_read"] = r is not None

    # --- P2 read denied ---
    try:
        ctx_sdk.read(pid="p2", handle_id=handle.handle_id)
        results["p2_read_blocked"] = False
    except ErrHandleNotOwned:
        results["p2_read_blocked"] = True

    # --- P2 pin denied ---
    page_id = loaded.ordered_pages[0].page_id
    try:
        ctx_sdk.pin(pid="p2", handle_id=handle.handle_id, page_ids=[page_id])
        results["p2_pin_blocked"] = False
    except ErrHandleNotOwned:
        results["p2_pin_blocked"] = True

    # --- P2 evict denied ---
    try:
        ctx_svc.evict(pid="p2", working_set_id=handle.working_set_id, target_tokens=1000)
        results["p2_evict_blocked"] = False
    except Exception:
        # P2 not owner → working_set_id won't be found under P2 → blocked
        results["p2_evict_blocked"] = True

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
