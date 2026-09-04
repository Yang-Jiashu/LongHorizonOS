from __future__ import annotations

from lhos.runtimes.verified_progress.sdk import _has_superseded_artifact


class _UnavailableArtifactFacts:
    def latest(self, artifact_id: str) -> int:
        raise RuntimeError(f"artifact authority unavailable for {artifact_id}")


def test_unavailable_artifact_authority_invalidates_verified_proof():
    pinned = {("vpg://artifact-a", 1)}
    assert _has_superseded_artifact(pinned, _UnavailableArtifactFacts()) is True
