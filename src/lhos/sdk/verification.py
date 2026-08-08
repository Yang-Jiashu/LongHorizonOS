"""LongHorizonOS Public SDK — verification + evidence guardian (E1).

A verifier is a developer-facing callback that says "what counts as done".
The **evidence guardian** turns a PASS outcome into real Core Evidence (a VPG
patch: verification + evidence + verifies/produces edges + a committed source
action + an exact ArtifactVersion binding).  VPG then DERIVES VERIFIED; the SDK
never sets validity directly.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol


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


def command_verifier(command: str, *, artifact_id: str = "artifact", version: int = 1) -> Verifier:
    """Verify that a shell command runs with exit 0 (deterministic)."""
    import subprocess

    def _run() -> VerificationOutcome:
        proc = subprocess.run(command, shell=True, capture_output=True, text=True)
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
