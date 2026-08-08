"""LongHorizonOS E2 — WorkspaceTool (root-scoped filesystem access).

Provides read/write/list/stat operations scoped under a single `root`.  It is
Capability-governed and side-effect-conscious: a write records an "ArtifactVersion
registration request" the SDK turns into a real ArtifactVersion (so the physical
world and the Artifact FS authority stay consistent).  It never mutates the
semantic graph.
"""

from __future__ import annotations

from pathlib import Path

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

    def read(self, rel: str) -> ToolResult:
        try:
            p = self._resolve(rel)
            return ToolResult(ok=True, value=p.read_text(), kind="workspace", action_id="")
        except Exception as e:
            return ToolResult(ok=False, error=str(e), kind="workspace")

    def write(self, rel: str, content: str) -> ToolResult:
        """Write a file under root; parent dirs are created."""
        try:
            p = self._resolve(rel)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
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
        import hashlib

        p = self._resolve(rel)
        return hashlib.sha256(p.read_bytes()).hexdigest()

    def byte_content(self, rel: str) -> str:
        return self._resolve(rel).read_text()
