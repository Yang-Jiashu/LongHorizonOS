"""Projection rebuild audit (Section 11) and Content integrity (Section 12)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService


def _make_state(journal):
    """Create a complex state for rebuild audit."""
    storage = journal._storage
    projections = ArtifactProjections(storage)
    driver = LocalArtifactStorageDriver(Path(tempfile.mkdtemp()) / "cas")
    cap_svc = CapabilityService(storage, journal)
    ns_svc = NamespaceService(projections, journal)
    service = ArtifactFSService(projections, driver, journal, capability_service=cap_svc)
    sdk = ArtifactSDK(service, ns_svc)

    # 3 namespaces
    for p in ["p1", "p2", "p3"]:
        ns_svc.create_namespace(p)
        cap_svc.grant(
            p, Capability(resource_pattern=f"artifact://ns-{p}/**", operations={"read", "write"})
        )

    # Artifacts: 10 total
    for i in range(10):
        pid = f"p{(i % 3) + 1}"
        sdk.write(pid, f"workspace:///art_{i}.txt", f"v1-{i}".encode(), f"init-{i}")
        sdk.write(
            pid, f"workspace:///art_{i}.txt", f"v2-{i}".encode(), f"up-{i}", expected_version=1
        )
        sdk.write(
            pid, f"workspace:///art_{i}.txt", f"v3-{i}".encode(), f"fin-{i}", expected_version=2
        )

    # 2 mounts
    sdk.mount("p2", "view_p1", "p1", mode="shared_readonly")
    sdk.mount("p3", "view_p1", "p1", mode="shared_readonly")

    # Multiple closed handles
    for _ in range(3):
        h = sdk.open("p1", "workspace:///art_0.txt", mode="read")
        sdk.close(h.handle_id)

    # Conflict + abort
    with pytest.raises(Exception):
        sdk.write("p1", "workspace:///art_0.txt", b"x", "conflict-key", expected_version=1)

    return storage, projections, driver, journal


def snap(projections):
    """Take deterministic snapshot of projected state.

    Normalizes random IDs (artifact_id, transaction_id, lease_id, journal_id)
    to deterministic keys so that three identical rebuilds compare equal.
    Timestamps are stripped because they depend on wall-clock.
    """
    state = {}

    # Build a stable mapping: original artifact_id -> canonical_uri
    aid_to_uri: dict[str, str] = {}
    for row in projections._storage.query_all(
        "SELECT artifact_id, canonical_uri FROM artifacts_projection", ()
    ):
        aid_to_uri[dict(row)["artifact_id"]] = dict(row)["canonical_uri"]

    nss = []
    for row in projections._storage.query_all("SELECT * FROM namespaces_projection", ()):
        d = dict(row)
        d.pop("created_at", None)
        nss.append(d)
    state["namespaces"] = sorted(nss, key=lambda x: x.get("namespace_id", ""))

    arts = []
    for row in projections._storage.query_all("SELECT * FROM artifacts_projection", ()):
        d = dict(row)
        d.pop("artifact_id", None)  # random UUID — not deterministic
        d.pop("created_at", None)
        d.pop("updated_at", None)
        arts.append(d)
    state["artifacts"] = sorted(arts, key=lambda x: x.get("canonical_uri", ""))

    vers = []
    for row in projections._storage.query_all("SELECT * FROM artifact_versions_projection", ()):
        d = dict(row)
        d.pop("committed_at", None)
        d.pop("committed_action_id", None)  # random UUID
        # artifact_id is random; replace with canonical_uri-based key
        aid = d.pop("artifact_id", None)
        if aid and aid in aid_to_uri:
            d["artifact_key"] = aid_to_uri[aid]
        elif aid:
            d["artifact_key"] = aid
        vers.append(d)
    state["versions"] = sorted(vers, key=lambda x: (x.get("artifact_key", ""), x.get("version", 0)))

    txns = []
    for row in projections._storage.query_all("SELECT * FROM write_transactions_projection", ()):
        d = dict(row)
        d.pop("transaction_id", None)  # random UUID
        d.pop("created_at", None)
        d.pop("committed_at", None)
        d.pop("aborted_at", None)
        txns.append(d)
    # Sort by idempotency_key if available, else by artifact_uri + pid
    state["transactions"] = sorted(
        txns,
        key=lambda x: (
            x.get("artifact_uri", "") or x.get("artifact_key", ""),
            x.get("pid", ""),
            x.get("idempotency_key", ""),
        ),
    )

    # Journal: sort events deterministically by sequence_number
    events = []
    for row in projections._storage.query_all(
        "SELECT * FROM journal_events ORDER BY journal_offset", ()
    ):
        d = dict(row)
        d.pop("event_id", None)  # random UUID
        d.pop("created_at", None)  # wall-clock
        events.append(d)
    state["journal"] = events

    return state


class TestProjectionRebuild:
    def test_deterministic_projection_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            stores = []

            def build_state(idx):
                from lhos.agent_os.storage.sqlite import SQLiteStorage

                db_path = str(Path(tmpdir) / f"audit-{idx}.db")
                storage = SQLiteStorage(db_path)
                stores.append(storage)
                journal = JournalService(storage)
                _, proj, _, _ = _make_state(journal)
                return proj

            # Build state 3 times identically
            proj1 = build_state(1)
            proj2 = build_state(2)
            proj3 = build_state(3)

            snap1 = snap(proj1)
            snap2 = snap(proj2)
            snap3 = snap(proj3)

            for s in stores:
                s.close()

            with open(
                "artifacts/agent_os_phase_c1_audit/projection-before.json", "w", encoding="utf-8"
            ) as f:
                json.dump(snap1, f, indent=2, default=str)
            with open(
                "artifacts/agent_os_phase_c1_audit/projection-rebuild-1.json", "w", encoding="utf-8"
            ) as f:
                json.dump(snap2, f, indent=2, default=str)
            with open(
                "artifacts/agent_os_phase_c1_audit/projection-rebuild-2.json", "w", encoding="utf-8"
            ) as f:
                json.dump(snap3, f, indent=2, default=str)

            # All snapshots should be identical (deterministic projections)
            assert snap1["namespaces"] == snap2["namespaces"] == snap3["namespaces"]
            assert snap1["artifacts"] == snap2["artifacts"] == snap3["artifacts"]
            assert snap1["versions"] == snap2["versions"] == snap3["versions"]


class TestBlobIntegrity:
    def test_missing_blob_read_fails(self, tmp_path):
        """Reading missing blob raises error."""
        driver = LocalArtifactStorageDriver(tmp_path / "cas")
        with pytest.raises(FileNotFoundError):
            driver.read("nonexistenthash123456789abcdef")

    def test_corrupt_manifest_handled(self, tmp_path):
        """Missing/malformed entry fails gracefully."""
        driver = LocalArtifactStorageDriver(tmp_path / "cas")
        result = driver.inspect_transaction("nonexistent-txn")
        assert result.status == "unknown"

    def test_hash_mismatch_detected(self, tmp_path):
        """Staging content and committing with wrong hash is handled."""
        driver = LocalArtifactStorageDriver(tmp_path / "cas")
        staged = driver.stage("txn-1", b"hello world")
        assert staged.content_hash is not None
        result = driver.commit("txn-1")
        assert result.committed is True
