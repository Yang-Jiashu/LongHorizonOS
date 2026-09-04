"""LongHorizonOS E2 — WorkspaceTool (root-scoped filesystem access).

Provides read/write/list/stat operations scoped under a single `root`.  It is
Capability-governed and side-effect-conscious: a write records an "ArtifactVersion
registration request" the SDK turns into a real ArtifactVersion (so the physical
world and the Artifact FS authority stay consistent).  It never mutates the
semantic graph.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any

from .base import ToolResult


class WorkspaceTool:
    """Read/write under a root; all paths are resolved and confined to root."""

    def __init__(self, root: str | Path, *, capability: str = "filesystem") -> None:
        self.root = Path(root).resolve()
        self.capability = capability
        if not self.root.exists():
            self.root.mkdir(parents=True, exist_ok=True)

    @property
    def name(self) -> str:
        return f"workspace({self.root})"

    def _resolve(self, rel: str) -> Path:
        p = (self.root / rel).resolve()
        if p != self.root and not p.is_relative_to(self.root):
            raise PermissionError(f"path escapes workspace root: {rel}")
        return p

    def resolve(self, rel: str) -> Path:
        """Resolve a relative path while enforcing this workspace's root."""

        return self._resolve(rel)

    def read(self, rel: str) -> ToolResult:
        try:
            p = self._resolve(rel)
            return ToolResult(
                ok=True, value=p.read_text(encoding="utf-8"), kind="workspace", action_id=""
            )
        except Exception as e:
            return ToolResult(ok=False, error=str(e), kind="workspace")

    def write(self, rel: str, content: str | bytes) -> ToolResult:
        """Atomically write a file under root; parent dirs are created.

        The old implementation wrote directly to the destination.  A process
        crash or concurrent reader could therefore observe a truncated file
        while the SDK had not yet registered its ArtifactVersion.  We write a
        temporary file in the same directory, fsync it, and replace the target
        in one rename operation.  ``write_atomic`` exposes the optional CAS
        guard for callers that already hold an observation hash.
        """

        return self.write_atomic(rel, content)

    def write_atomic(
        self,
        rel: str,
        content: str | bytes,
        *,
        expected_hash: str | None = None,
    ) -> ToolResult:
        """Atomically write ``content`` and optionally enforce a hash CAS.

        If ``expected_hash`` is supplied, the existing file must still have
        that exact SHA-256 digest.  A mismatch is rejected without touching the
        destination, providing a small fail-closed guard against lost updates
        between observation and mutation.
        """

        try:
            p = self._resolve(rel)
            p.parent.mkdir(parents=True, exist_ok=True)
            payload = content.encode("utf-8") if isinstance(content, str) else bytes(content)
            if expected_hash is not None:
                if not p.exists():
                    raise ValueError(f"workspace CAS failed: {rel!r} does not exist")
                current_hash = hashlib.sha256(p.read_bytes()).hexdigest()
                if current_hash != expected_hash:
                    raise ValueError(
                        f"workspace CAS failed for {rel!r}: expected {expected_hash}, "
                        f"found {current_hash}"
                    )
            fd, temp_name = tempfile.mkstemp(prefix=f".{p.name}.lhos-", dir=str(p.parent))
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, p)
            except BaseException:
                with suppress(OSError):
                    os.unlink(temp_name)
                raise
            return ToolResult(ok=True, value=str(p), kind="workspace", action_id="")
        except Exception as e:
            return ToolResult(ok=False, error=str(e), kind="workspace")

    def list(self, rel: str = ".") -> ToolResult:
        try:
            p = self._resolve(rel)
            return ToolResult(
                ok=True,
                value=sorted(str(x.relative_to(self.root)) for x in p.rglob("*") if x.is_file()),
                kind="workspace",
            )
        except Exception as e:
            return ToolResult(ok=False, error=str(e), kind="workspace")

    def stat(self, rel: str) -> ToolResult:
        try:
            p = self._resolve(rel)
            s = p.stat()
            return ToolResult(
                ok=True, value={"size": s.st_size, "mtime": s.st_mtime}, kind="workspace"
            )
        except Exception as e:
            return ToolResult(ok=False, error=str(e), kind="workspace")

    def content_hash(self, rel: str) -> str:
        """Deterministic content hash of a workspace file (version identity)."""
        p = self._resolve(rel)
        return hashlib.sha256(p.read_bytes()).hexdigest()

    def read_bytes(self, rel: str) -> bytes:
        """Read raw bytes through the root-scoped authority."""

        return self._resolve(rel).read_bytes()

    def byte_content(self, rel: str) -> str:
        return self._resolve(rel).read_text(encoding="utf-8")

    def snapshot(self, rel: str) -> dict[str, Any]:
        """Return a compact content snapshot suitable for CAS/observation.

        The snapshot intentionally contains no mutable file handle.  Callers
        can persist its digest and later use ``write_atomic(expected_hash=...)``
        to reject a mutation based on a stale observation.
        """

        payload = self.read_bytes(rel)
        return {
            "artifact_id": rel,
            "canonical_uri": f"vpg://{rel}",
            "content_hash": hashlib.sha256(payload).hexdigest(),
            "size": len(payload),
        }
