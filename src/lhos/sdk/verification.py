"""LongHorizonOS Public SDK — verification + evidence guardian (E1).

A verifier is a developer-facing callback that says "what counts as done".
The **evidence guardian** turns a PASS outcome into real Core Evidence (a VPG
patch: verification + evidence + verifies/produces edges + a committed source
action + an exact ArtifactVersion binding).  VPG then DERIVES VERIFIED; the SDK
never sets validity directly.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from lhos.subprocess_policy import run_command


@dataclass
class VerificationOutcome:
    """Result of a developer-facing verifier."""

    passed: bool
    artifact_id: str
    version: int
    content: str | None = None
    evidence_note: str = ""
    details: dict[str, Any] = field(default_factory=dict)


class Verifier(Protocol):
    def __call__(self) -> VerificationOutcome: ...


def callback_verifier(fn: Callable[[], VerificationOutcome]) -> Verifier:
    """Wrap an arbitrary callable returning a VerificationOutcome."""
    return fn  # type: ignore[return-value]


def command_verifier(
    command: str | Sequence[str],
    *,
    artifact_id: str = "artifact",
    version: int = 1,
    cwd: str | Path | None = None,
    trusted: bool = False,
    allow_shell: bool = False,
) -> Verifier:
    """Verify a controlled local command without raw subprocess fallback."""

    def _run() -> VerificationOutcome:
        proc = run_command(
            command,
            cwd=cwd,
            trusted=trusted,
            allow_shell=allow_shell,
        )
        return VerificationOutcome(
            passed=proc.returncode == 0,
            artifact_id=artifact_id,
            version=version,
            content=(proc.stdout or "") + (proc.stderr or ""),
            evidence_note=f"command: {command}",
        )

    return _run


def scripted_executor(
    *, artifact_id: str = "artifact", version: int = 1, content: str = "ok"
) -> Verifier:
    """Deterministic no-API-key verifier/executor for demos & tests."""

    def _run() -> VerificationOutcome:
        return VerificationOutcome(
            passed=True,
            artifact_id=artifact_id,
            version=version,
            content=content,
            evidence_note="scripted",
        )

    return _run
