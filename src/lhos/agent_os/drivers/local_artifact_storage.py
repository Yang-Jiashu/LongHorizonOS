"""Versioned local artifact storage driver.

Content-addressable storage (CAS) on local filesystem.
- Content is stored by hash: `<root>/cas/<hash[:2]>/<hash>`
- Staged content goes to `<root>/staging/<transaction_id>` then is linked into CAS on commit
- Crash recovery: orphaned staging files are cleaned up on init

Design constraints:
- No symlinks (TOCTOU safety)
- No `os.rename` across devices (use shutil.move fallback)
- Atomic commit: write-to-temp + fsync + atomic rename
- Content deduplication by SHA-256 hash
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Any

from lhos.agent_os.artifacts.models import (
    StagedArtifact,
    StorageCommitResult,
    StorageTransactionStatus,
)

# Default chunk size for reading/writing (64KB)
_CHUNK_SIZE = 64 * 1024

# SHA-256 hash of empty content
EMPTY_HASH = hashlib.sha256(b"").hexdigest()


class LocalArtifactStorageDriver:
    """Content-addressable local filesystem storage for artifacts.

    Directory layout under ``root``:
    ::
        root/
        ├── cas/                  # content-addressable store
        │   ├── ab/               # first 2 hex chars of hash
        │   │   └── abcdef...     # full hash
        │   └── ...
        └── staging/              # in-progress writes
            └── <transaction_id>
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self._cas_dir = self.root / "cas"
        self._staging_dir = self.root / "staging"

        # Create directories
        self._cas_dir.mkdir(parents=True, exist_ok=True)
        self._staging_dir.mkdir(parents=True, exist_ok=True)

        # Track active transactions (for crash recovery inspection)
        self._active_txns: set[str] = set()

        # Clean up orphaned staging files from previous crash
        self._cleanup_orphans()

    # ── Stage ─────────────────────────────────────────────────────────────

    def stage(
        self,
        transaction_id: str,
        content: bytes | str,
    ) -> StagedArtifact:
        """Stage content for a transaction.

        Writes to a temp file, computes hash, returns metadata.
        Does NOT commit — content is in staging/ and not visible to readers.
        """
        content_bytes = content.encode("utf-8") if isinstance(content, str) else content

        staging_path = self._staging_dir / transaction_id
        staging_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to staging file
        with open(staging_path, "wb") as f:
            f.write(content_bytes)
            f.flush()
            os.fsync(f.fileno())

        content_hash = hashlib.sha256(content_bytes).hexdigest()
        size_bytes = len(content_bytes)

        self._active_txns.add(transaction_id)

        return StagedArtifact(
            transaction_id=transaction_id,
            content_ref=content_hash,
            content_hash=content_hash,
            size_bytes=size_bytes,
        )

    def stage_from_file(
        self,
        transaction_id: str,
        source_path: str | Path,
    ) -> StagedArtifact:
        """Stage content from an existing file (copy, not move)."""
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        staging_path = self._staging_dir / transaction_id
        staging_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to staging
        shutil.copy2(source, staging_path)

        # Compute hash
        content_hash = self._hash_file(staging_path)
        size_bytes = staging_path.stat().st_size

        self._active_txns.add(transaction_id)

        return StagedArtifact(
            transaction_id=transaction_id,
            content_ref=content_hash,
            content_hash=content_hash,
            size_bytes=size_bytes,
        )

    # ── Commit ────────────────────────────────────────────────────────────

    def commit(self, transaction_id: str) -> StorageCommitResult:
        """Commit staged content to CAS.

        Moves the staging file into the CAS directory.
        If content already exists (same hash), the staging file is removed.
        This operation is idempotent.
        """
        staging_path = self._staging_dir / transaction_id
        if not staging_path.exists():
            # Already committed or never staged — check if we have a record
            return StorageCommitResult(
                transaction_id=transaction_id,
                content_ref="",
                committed=False,
            )

        # Compute hash from staging file
        content_hash = self._hash_file(staging_path)
        cas_path = self._cas_path_for_hash(content_hash)

        if cas_path.exists():
            # Content already in CAS — just remove staging
            staging_path.unlink()
        else:
            # Move to CAS atomically
            cas_path.parent.mkdir(parents=True, exist_ok=True)
            # Use os.rename (atomic on same filesystem)
            try:
                os.rename(staging_path, cas_path)
            except OSError:
                # Cross-device fallback: copy + delete
                shutil.copy2(staging_path, cas_path)
                staging_path.unlink()

        self._active_txns.discard(transaction_id)

        return StorageCommitResult(
            transaction_id=transaction_id,
            content_ref=content_hash,
            committed=True,
        )

    # ── Abort ─────────────────────────────────────────────────────────────

    def abort(self, transaction_id: str) -> bool:
        """Abort a transaction by removing its staging file."""
        staging_path = self._staging_dir / transaction_id
        if staging_path.exists():
            staging_path.unlink()
            self._active_txns.discard(transaction_id)
            return True
        return False

    # ── Read ──────────────────────────────────────────────────────────────

    def read(self, content_ref: str) -> bytes:
        """Read content by its content_ref (hash)."""
        cas_path = self._cas_path_for_hash(content_ref)
        if not cas_path.exists():
            raise FileNotFoundError(f"Content not found: {content_ref}")
        with open(cas_path, "rb") as f:
            return f.read()

    def read_stream(self, content_ref: str) -> Any:
        """Return a file-like object for streaming content."""
        cas_path = self._cas_path_for_hash(content_ref)
        if not cas_path.exists():
            raise FileNotFoundError(f"Content not found: {content_ref}")
        return open(cas_path, "rb")

    def exists(self, content_ref: str) -> bool:
        """Check if content exists in CAS."""
        return self._cas_path_for_hash(content_ref).exists()

    def size(self, content_ref: str) -> int:
        """Get size of content by ref."""
        cas_path = self._cas_path_for_hash(content_ref)
        if not cas_path.exists():
            raise FileNotFoundError(f"Content not found: {content_ref}")
        return cas_path.stat().st_size

    # ── Inspect ───────────────────────────────────────────────────────────

    def inspect_transaction(self, transaction_id: str) -> StorageTransactionStatus:
        """Inspect the status of a storage transaction."""
        staging_path = self._staging_dir / transaction_id

        if staging_path.exists():
            content_hash = self._hash_file(staging_path)
            return StorageTransactionStatus(
                transaction_id=transaction_id,
                status="staged",
                content_ref=content_hash,
                content_hash=content_hash,
            )

        # Not in staging — check if it was ever active
        return StorageTransactionStatus(
            transaction_id=transaction_id,
            status="unknown",
        )

    # ── Delete ────────────────────────────────────────────────────────────

    def delete_content(self, content_ref: str) -> bool:
        """Delete content from CAS.

        Returns True if deleted, False if not found.
        WARNING: This does not check for references — caller must ensure no
        artifacts reference this content.
        """
        cas_path = self._cas_path_for_hash(content_ref)
        if cas_path.exists():
            cas_path.unlink()
            return True
        return False

    # ── Stats ─────────────────────────────────────────────────────────────

    def total_size(self) -> int:
        """Total size of all content in CAS."""
        total = 0
        for hash_dir in self._cas_dir.iterdir():
            if hash_dir.is_dir():
                for f in hash_dir.iterdir():
                    if f.is_file():
                        total += f.stat().st_size
        return total

    def content_count(self) -> int:
        """Number of unique content blobs in CAS."""
        count = 0
        for hash_dir in self._cas_dir.iterdir():
            if hash_dir.is_dir():
                count += sum(1 for _ in hash_dir.iterdir() if _.is_file())
        return count

    # ── Recovery ──────────────────────────────────────────────────────────

    def recover(self, known_transaction_ids: set[str]) -> dict[str, str]:
        """Recover from crash.

        For each staging file:
        - If transaction_id is in known set: keep it (will be committed/aborted later)
        - If not in known set: delete (orphaned)

        Returns dict of {transaction_id: status} for surviving staged files.
        """
        results: dict[str, str] = {}
        for f in self._staging_dir.iterdir():
            if f.is_file():
                txn_id = f.name
                if txn_id in known_transaction_ids:
                    results[txn_id] = "staged"
                else:
                    # Orphaned — clean up
                    f.unlink()
        return results

    def list_orphaned_staging(self) -> list[str]:
        """List transaction IDs with orphaned staging files."""
        return [f.name for f in self._staging_dir.iterdir() if f.is_file()]

    # ── Private Helpers ───────────────────────────────────────────────────

    def _cas_path_for_hash(self, content_hash: str) -> Path:
        """Get the CAS path for a content hash."""
        prefix = content_hash[:2]
        return self._cas_dir / prefix / content_hash

    @staticmethod
    def _hash_file(path: Path) -> str:
        """Compute SHA-256 hash of a file."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(_CHUNK_SIZE)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _cleanup_orphans(self) -> None:
        """Remove orphaned staging files on init.

        On first init, we don't know which transactions are valid,
        so we leave staging files in place. They will be cleaned up
        by explicit recover() calls.
        """
        pass  # Intentionally no-op — recovery is explicit

    # ── Driver Protocol ───────────────────────────────────────────────────

    @property
    def device_type(self) -> str:
        return "storage/local_artifact"

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> Any:
        """Dispatch storage operations through the driver protocol."""
        from lhos.agent_os.drivers.base import DriverResult

        try:
            if operation == "stage":
                staged = self.stage(arguments["transaction_id"], arguments["content"])
                return DriverResult(
                    status="completed",
                    output={
                        "content_ref": staged.content_ref,
                        "content_hash": staged.content_hash,
                        "size_bytes": staged.size_bytes,
                    },
                )
            elif operation == "commit":
                commit_res = self.commit(arguments["transaction_id"])
                return DriverResult(
                    status="completed",
                    output={
                        "content_ref": commit_res.content_ref,
                        "committed": commit_res.committed,
                    },
                )
            elif operation == "abort":
                self.abort(arguments["transaction_id"])
                return DriverResult(status="completed")
            elif operation == "read":
                data = self.read(arguments["content_ref"])
                return DriverResult(
                    status="completed",
                    output={"data": data.decode("utf-8") if isinstance(data, bytes) else data},
                )
            else:
                return DriverResult(
                    status="failed",
                    error={"reason": f"unknown operation: {operation}"},
                )
        except Exception as e:
            return DriverResult(
                status="failed",
                error={"reason": str(e)},
            )

    async def inspect(self, action_id: str) -> Any:
        from lhos.agent_os.drivers.base import DriverInspect

        return DriverInspect(status="unknown")

    def reset(self) -> None:
        """Reset driver state — does NOT delete CAS content."""
        self._active_txns.clear()
        # Clean up staging
        for f in self._staging_dir.iterdir():
            if f.is_file():
                f.unlink()
