"""Filesystem tool: read / write / append / exists / list within a workspace.

All paths are confined to the workspace directory.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from lhos.domain.errors import ToolExecutionError
from lhos.ports.tools import ToolMetadata, ToolRequest, ToolResult


def _now() -> datetime:
    return datetime.now().astimezone()


class FilesystemTool:
    name = "filesystem"

    def _resolve(self, workspace_dir: str, rel_path: str) -> Path:
        root = Path(workspace_dir).resolve()
        path = (root / rel_path).resolve()
        if not str(path).startswith(str(root)):
            raise ToolExecutionError(f"path escapes workspace: {rel_path!r}")
        return path

    def execute(self, request: ToolRequest, workspace_dir: str) -> ToolResult:
        args = request.arguments
        op = args.get("op")
        started = _now()
        Path(workspace_dir).mkdir(parents=True, exist_ok=True)
        if op == "write":
            path = self._resolve(workspace_dir, args["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(args.get("content", ""), encoding="utf-8")
            result = ToolResult(
                success=True,
                stdout=f"wrote {path}",
                environment_delta={"file_written": args["path"]},
                started_at=started,
                finished_at=_now(),
            )
        elif op == "append":
            path = self._resolve(workspace_dir, args["path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                fh.write(args.get("content", ""))
            result = ToolResult(
                success=True,
                stdout=f"appended {path}",
                environment_delta={"file_written": args["path"]},
                started_at=started,
                finished_at=_now(),
            )
        elif op == "read":
            path = self._resolve(workspace_dir, args["path"])
            if not path.exists():
                raise ToolExecutionError(f"file not found: {args['path']!r}")
            result = ToolResult(
                success=True,
                stdout=path.read_text(encoding="utf-8"),
                started_at=started,
                finished_at=_now(),
            )
        elif op == "exists":
            path = self._resolve(workspace_dir, args["path"])
            result = ToolResult(
                success=path.exists(),
                stdout=str(path.exists()),
                started_at=started,
                finished_at=_now(),
            )
        elif op == "list":
            path = self._resolve(workspace_dir, args.get("path", "."))
            entries = sorted(p.name for p in path.iterdir()) if path.is_dir() else []
            result = ToolResult(
                success=True,
                stdout="\n".join(entries),
                started_at=started,
                finished_at=_now(),
            )
        else:
            raise ToolExecutionError(f"unknown filesystem op: {op!r}")
        return result


FILESYSTEM_METADATA = ToolMetadata(
    name="filesystem",
    side_effect_level="local_write",
    retry_safe=False,
    default_timeout_seconds=30,
    supports_idempotency=True,
)
