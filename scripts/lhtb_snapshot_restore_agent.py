"""Restore an externally captured LHTB workspace before one verifier pass.

This agent is only used for post-hoc checkpoint scoring.  It is deliberately
not part of either experimental arm.  The primary DSH/Harbor harness writes no
checkpoint hooks and receives no changed prompts, time slices, or feedback.
"""

from __future__ import annotations

import hashlib
import re
import shlex
from pathlib import PurePosixPath
from typing import Any

from harbor.agents.base import BaseAgent
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _absolute_container_path(value: str, *, label: str) -> PurePosixPath:
    raw = str(value).strip()
    path = PurePosixPath(raw)
    if not raw or not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{label} must be an absolute normalized POSIX path")
    return path


class LHTBSnapshotRestoreAgent(BaseAgent):
    """Replace ``/app`` with a verified snapshot archive and then stop."""

    SUPPORTS_ATIF = False
    SUPPORTS_WINDOWS = False

    def __init__(
        self,
        *args: Any,
        archive_path: str = "/opt/lhtb-checkpoint/workspace.tar",
        workspace: str = "/app",
        archive_sha256: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.archive_path = _absolute_container_path(
            archive_path,
            label="archive_path",
        )
        self.workspace = _absolute_container_path(workspace, label="workspace")
        normalized_hash = str(archive_sha256).strip().lower()
        if _SHA256.fullmatch(normalized_hash) is None:
            raise ValueError("archive_sha256 must be a lowercase SHA-256 digest")
        self.archive_sha256 = normalized_hash

    @staticmethod
    def name() -> str:
        return "lhtb-external-checkpoint-restore"

    def version(self) -> str:
        return "1"

    async def setup(self, environment: BaseEnvironment) -> None:
        del environment

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        del instruction
        archive = shlex.quote(self.archive_path.as_posix())
        workspace = shlex.quote(self.workspace.as_posix())
        expected = shlex.quote(self.archive_sha256)
        command = (
            "set -eu; "
            f"test -f {archive}; "
            f"observed=$(sha256sum {archive} | awk '{{print $1}}'); "
            f'test "$observed" = {expected}; '
            f"test -d {workspace}; "
            f"find {workspace} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +; "
            f"tar --extract --file {archive} --directory {workspace} "
            "--preserve-permissions --numeric-owner --xattrs --acls"
        )
        result = await environment.exec(
            command=command,
            cwd="/",
            env=None,
            user="root",
        )
        if result.return_code != 0:
            detail = (result.stderr or result.stdout or "").strip()[-1000:]
            raise RuntimeError(
                f"checkpoint workspace restore failed: {detail or result.return_code}"
            )
        context.metadata = {
            "schema_version": "lhos-lhtb-snapshot-restore.v1",
            "termination_reason": "snapshot_restored_for_final_verifier",
            "workspace": self.workspace.as_posix(),
            "archive_sha256": self.archive_sha256,
        }

    async def resume_after_verifier_rejection(
        self,
        user_prompt: str,
        context: AgentContext,
    ) -> None:
        # Harbor validates this method's presence in same-conversation mode.
        # ``run`` uses a non-completion termination reason, so the primary
        # agent loop exits and Harbor invokes exactly one final verifier; this
        # method must never be reached.
        del user_prompt, context
        raise RuntimeError("checkpoint scoring must not resume the restore agent")


def archive_sha256(path: str) -> str:
    """Small public helper used by isolated integration probes."""

    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
