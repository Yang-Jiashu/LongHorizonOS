"""LongHorizonOS E2 — CommandVerifier (real tool-to-Evidence path).

Turns a deterministic shell command into a `VerificationOutcome` (from
`lhos.sdk.verification`) by running it through a ShellTool.  Crucially, the
outcome alone does NOT mark a Task VERIFIED — the E1 evidence guardian converts a
PASS into a real Evidence + exact ArtifactVersion and VPG derives VERIFIED.
A shell exit 0 therefore only *supports* a Verification, never directly sets
semantic state (VPG-G3).
"""

from __future__ import annotations

from lhos.sdk.verification import VerificationOutcome

from .tools.shell import ShellTool


class CommandVerifier:
    """Verifier that runs a shell command and yields a VerificationOutcome.

    ``artifact_id``/``version``/`content` describe the ArtifactVersion the
    verification binds (the E1 evidence guardian will register it).
    """

    def __init__(
        self,
        command: str | list[str],
        *,
        artifact_id: str,
        version: int,
        content: str | None = None,
        shell: ShellTool | None = None,
        cwd=None,
        workspace: object | None = None,
    ) -> None:
        self.command = command
        self.artifact_id = artifact_id
        self.version = version
        self.content = content
        self.shell = shell or ShellTool()
        self.cwd = cwd
        self.workspace = workspace  # optional WorkspaceTool for artifact ingestion

    def __call__(self) -> VerificationOutcome:
        # If a WorkspaceTool is provided and a file with artifact_id exists, ingest
        # its current bytes as the verification content (real file -> artifact).
        content = self.content
        if self.workspace is not None and content is None:
            reader = getattr(self.workspace, "byte_content", None)
            if reader is not None:
                import contextlib

                with contextlib.suppress(Exception):
                    content = reader(self.artifact_id)
        r = self.shell.run(self.command, cwd=self.cwd)
        passed = r.ok
        return VerificationOutcome(
            passed=passed,
            artifact_id=self.artifact_id,
            version=self.version,
            content=content
            if content is not None
            else (r.value.get("stdout", "") if isinstance(r.value, dict) else str(r.value)),
            evidence_note=f"command: {self.command} exit={r.value.get('exit_code') if isinstance(r.value, dict) else '?'}",
            details={"tool_result": r.value, "kind": "command"},
        )
