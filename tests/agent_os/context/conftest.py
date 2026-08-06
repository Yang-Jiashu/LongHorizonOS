"""Shared test fixtures for Context VM (Phase C2).

Provides:
- An in-memory fully-wired ArtifactFS + Namespace + Journal + Capability service.
- A ContextService wired with an ``_ArtifactSupplier`` adapter and an
  ``_AllowsAllCaps`` capability checker.
- An ``_ArtifactSupplier`` that deflects reads to the ArtifactFS service for a
  given pid (version-bound).
- Helper to build manifests and write artifacts in one step.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Any, Callable

import pytest

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
    def can_context_operation(self, **kwargs: Any) -> bool:
        return True

    def can_artifact_read(self, **kwargs: Any) -> bool:
        return True


class _DenyAllCaps:
    def can_context_operation(self, **kwargs: Any) -> bool:
        return False

    def can_artifact_read(self, **kwargs: Any) -> bool:
        return False


class _ArtifactSupplier:
    """Adapts an ``ArtifactFSService`` to the Context VM content supplier.

    Reads are dispatched to the ArtifactFS using the supplier's pid, so the
    Context VM never touches the storage layer directly (spec enforcement of
    L3 boundary).
    """

    def __init__(self, svc: ArtifactFSService, pid: str) -> None:
        self._svc = svc
        self._pid = pid

    def read_version(
        self,
        *,
        artifact_id: str,
        version: int,
        canonical_uri: str,
    ) -> bytes:
        return self._svc.read(pid=self._pid, uri=canonical_uri, version=version)


@pytest.fixture()
def tmp_cas():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture()
def env(tmp_cas: Path):
    """Wired environment for one test."""
    storage = SQLiteStorage(":memory:")
    journal = JournalService(storage)
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(tmp_cas / "cas")
    cap_svc = CapabilityService(storage, journal)
    ns_svc = NamespaceService(projections, journal)
    artifact_svc = ArtifactFSService(
        projections, driver, journal, capability_service=cap_svc
    )
    artifact_svc._ns_resolver = ns_svc  # type: ignore[attr-defined]
    ns_svc.create_namespace("p1")
    cap_svc.grant(
        "p1",
        Capability(resource_pattern="artifact://ns-p1/**",
                   operations={"read", "write"}),
    )
    artifact_sdk = ArtifactSDK(artifact_svc, ns_svc)
    ctx_svc = ContextService(
        content_supplier=_ArtifactSupplier(artifact_svc, pid="p1"),
        capability_checker=_AllowsAllCaps(),
        estimator=DeterministicByteTokenEstimator(),
    )
    ctx_sdk = ContextSDK(ctx_svc)
    return {
        "storage": storage,
        "journal": journal,
        "projections": projections,
        "driver": driver,
        "cap_svc": cap_svc,
        "ns_svc": ns_svc,
        "artifact_svc": artifact_svc,
        "artifact_sdk": artifact_sdk,
        "ctx_svc": ctx_svc,
        "ctx_sdk": ctx_sdk,
        "pid": "p1",
    }


def write_artifacts_and_build_manifest(
    *,
    env: dict[str, Any],
    pid: str,
    artifacts: list[tuple[str, bytes, str]],
    token_budget: int = 10_000,
    byte_budget: int | None = None,
    page_size_bytes: int = 64,
    required_map: dict[str, bool] | None = None,
    priority_map: dict[str, int] | None = None,
    start_byte_map: dict[str, int | None] | None = None,
    end_byte_map: dict[str, int | None] | None = None,
    media_type_map: dict[str, str] | None = None,
    ref_id_map: dict[str, str] | None = None,
) -> ContextManifest:
    """Write several artifacts and build a version-pinned ContextManifest.

    ``artifacts`` is a list of (workspace_uri, content, idem_key)."""
    required_map = required_map or {}
    priority_map = priority_map or {}
    start_byte_map = start_byte_map or {}
    end_byte_map = end_byte_map or {}
    media_type_map = media_type_map or {}
    ref_id_map = ref_id_map or {}

    art_sdk: ArtifactSDK = env["artifact_svc"]  # we use the internal svc below
    ctx_internal: ArtifactFSService = env["artifact_svc"]
    ns_svc: NamespaceService = env["ns_svc"]

    refs: list[ContentRef] = []
    for idx, (ws_uri, content, idem_key) in enumerate(artifacts):
        # write via ArtifactSDK (namespace-aware)
        env["artifact_sdk"].write(pid, ws_uri, content, idem_key)
        # resolve the version row via list_versions (keyed by ws_uri)
        versions = env["artifact_sdk"].list_versions(pid, ws_uri)
        vrow = next(iter(versions))
        # find the matching artifact row by artifact_id so multi-artifact
        # manifests correctly pair each version with its own artifact row.
        rows = ctx_internal.list_artifacts(pid)
        art_row = next(r for r in rows if r["artifact_id"] == vrow["artifact_id"])
        ref_id = ref_id_map.get(ws_uri, f"r{idx}")
        canonical_uri = art_row["canonical_uri"]
        refs.append(
            ContentRef(
                ref_id=ref_id,
                canonical_uri=canonical_uri,
                artifact_id=art_row["artifact_id"],
                version=vrow["version"],
                content_hash=vrow["content_hash"],
                media_type=media_type_map.get(ws_uri, "application/octet-stream"),
                priority=priority_map.get(ws_uri, 0),
                required=required_map.get(ws_uri, True),
                start_byte=start_byte_map.get(ws_uri),
                end_byte=end_byte_map.get(ws_uri),
            )
        )
    return ContextManifest(
        owner_pid=pid,
        refs=tuple(refs),
        token_budget=token_budget,
        byte_budget=byte_budget,
        page_size_bytes=page_size_bytes,
    )


__all__ = [
    "_AllowsAllCaps",
    "_DenyAllCaps",
    "_ArtifactSupplier",
    "tmp_cas",
    "env",
    "write_artifacts_and_build_manifest",
]
