"""P0-2 observation-authority guards for Artifact facts and workspace writes."""

from __future__ import annotations

import hashlib
import sqlite3

import pytest

from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk.providers import FactsProvider


def test_read_only_facts_provider_reads_legacy_db_without_observation_table(tmp_path) -> None:
    """Fact reads remain compatible with pre-ObservationToken databases."""

    db = tmp_path / "legacy-facts.sqlite"
    digest = hashlib.sha256(b"legacy-v1").hexdigest()
    conn = sqlite3.connect(db)
    conn.execute(
        """
        CREATE TABLE sdk_artifact_facts (
            artifact_id TEXT NOT NULL,
            canonical_uri TEXT NOT NULL,
            version INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            PRIMARY KEY (artifact_id, version),
            UNIQUE (canonical_uri, version)
        )
        """
    )
    conn.execute(
        "INSERT INTO sdk_artifact_facts VALUES (?, ?, ?, ?)",
        ("legacy.txt", "vpg://legacy.txt", 1, digest),
    )
    conn.commit()
    conn.close()

    facts = FactsProvider(str(db), read_only=True)
    try:
        assert facts.artifact_exists("pid", "legacy.txt", 1)
        assert facts.read_hash("pid", "legacy.txt", 1) == digest
        assert facts.versions() == {"legacy.txt": [1]}
        assert facts.latest("legacy.txt") == 1
        # Observation tokens are intentionally unavailable on this old
        # schema; the artifact-fact read surface must still work.
        assert facts.get_observation("missing") is None
    finally:
        facts.close()


def test_facts_provider_rejects_invalid_and_non_monotonic_versions() -> None:
    facts = FactsProvider(":memory:")
    with pytest.raises(ValueError, match="positive integer"):
        facts.add_version("a.txt", 0, "zero")
    with pytest.raises(ValueError, match="positive integer"):
        facts.add_version("a.txt", True, "bool is not a version")

    facts.add_version("a.txt", 2, "v2")
    with pytest.raises(ValueError, match="monotonic"):
        facts.add_version("a.txt", 1, "v1")


def test_facts_provider_same_version_is_hash_immutable() -> None:
    facts = FactsProvider(":memory:")
    facts.add_version("a.txt", 1, "same")
    # Re-registering an identical observation is idempotent.
    facts.add_version("a.txt", 1, "same")
    assert facts.read_hash("pid", "a.txt", 1) == hashlib.sha256(b"same").hexdigest()
    with pytest.raises(ValueError, match="immutable"):
        facts.add_version("a.txt", 1, "tampered")


def test_facts_provider_observe_version_checks_external_digest_and_bytes() -> None:
    facts = FactsProvider(":memory:")
    digest = hashlib.sha256(b"payload").hexdigest()
    assert facts.observe_version("vpg://blob", 1, b"payload", expected_hash=digest) == digest
    assert facts.read_hash("pid", "blob", 1) == digest
    with pytest.raises(ValueError, match="hash mismatch"):
        facts.observe_version("blob", 2, b"changed", expected_hash=digest)


def test_persistent_facts_rollback_and_same_version_are_fail_closed(tmp_path) -> None:
    db = tmp_path / "facts.sqlite"
    facts = FactsProvider(str(db))
    facts.add_version("artifact", 1, "v1")
    facts.add_version("artifact", 3, "v3")
    with pytest.raises(ValueError, match="monotonic"):
        facts.add_version("artifact", 2, "v2")
    facts.close()

    reopened = FactsProvider(str(db))
    assert reopened.versions()["artifact"] == [1, 3]
    with pytest.raises(ValueError, match="immutable"):
        reopened.add_version("artifact", 3, "different")
    reopened.close()


def test_workspace_write_is_atomic_and_snapshot_is_hash_bound(tmp_path) -> None:
    ws = WorkspaceTool(tmp_path)
    assert ws.write("input.txt", "old").ok
    snapshot = ws.snapshot("input.txt")
    assert snapshot["content_hash"] == ws.content_hash("input.txt")

    failed = ws.write_atomic("input.txt", "new", expected_hash="wrong")
    assert failed.ok is False
    assert ws.read("input.txt").value == "old"

    succeeded = ws.write_atomic(
        "input.txt",
        "new",
        expected_hash=str(snapshot["content_hash"]),
    )
    assert succeeded.ok
    assert ws.read("input.txt").value == "new"
    assert ws.snapshot("input.txt")["content_hash"] != snapshot["content_hash"]


def test_workspace_write_bytes_and_root_confinement(tmp_path) -> None:
    ws = WorkspaceTool(tmp_path)
    result = ws.write_atomic("nested/data.bin", b"\x00\xff")
    assert result.ok
    assert ws.read_bytes("nested/data.bin") == b"\x00\xff"
    with pytest.raises(PermissionError):
        ws.snapshot("../outside")
