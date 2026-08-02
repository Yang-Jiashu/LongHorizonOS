"""Verifier registry (spec section 14.2). llm_judge is always the last resort."""

from lhos.domain.errors import VerificationError
from lhos.ports.verifier import Verifier


class VerifierRegistry:
    def __init__(self) -> None:
        self._verifiers: dict[str, Verifier] = {}

    def register(self, verifier: Verifier) -> None:
        self._verifiers[verifier.verifier_type] = verifier

    def get(self, verifier_type: str) -> Verifier:
        if verifier_type not in self._verifiers:
            raise VerificationError(f"unknown verifier type {verifier_type!r}")
        return self._verifiers[verifier_type]

    def names(self) -> list[str]:
        return sorted(self._verifiers)


def build_default_registry(allow_llm_judge: bool = False, llm=None) -> VerifierRegistry:  # noqa: ANN001
    from lhos.verification.command_verifier import CommandVerifier, ExitCodeVerifier
    from lhos.verification.composite_verifier import (
        CompositeAndVerifier,
        CompositeOrVerifier,
        ManualVerifier,
    )
    from lhos.verification.file_verifier import (
        ArtifactExistsVerifier,
        FileChangedVerifier,
        FileContainsVerifier,
        FileExistsVerifier,
    )
    from lhos.verification.json_verifier import JsonSchemaVerifier
    from lhos.verification.llm_verifier import LlmJudgeVerifier

    registry = VerifierRegistry()
    registry.register(CommandVerifier())
    registry.register(ExitCodeVerifier())
    registry.register(FileExistsVerifier())
    registry.register(FileChangedVerifier())
    registry.register(FileContainsVerifier())
    registry.register(ArtifactExistsVerifier())
    registry.register(JsonSchemaVerifier())
    registry.register(CompositeAndVerifier(registry))
    registry.register(CompositeOrVerifier(registry))
    registry.register(ManualVerifier())
    registry.register(LlmJudgeVerifier(allow_llm_judge=allow_llm_judge, llm=llm))
    return registry
