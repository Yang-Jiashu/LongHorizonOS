"""LongHorizonOS error hierarchy."""


class LhosError(Exception):
    """Base error for all LongHorizonOS failures."""


class InvalidStateTransition(LhosError):
    """A node state transition is not allowed by the state machine."""


class EvidenceRequiredError(LhosError):
    """A transition to VERIFIED was attempted without evidence."""


class VersionConflictError(LhosError):
    """expected_version does not match the current node version."""


class PatchValidationError(LhosError):
    """A graph patch failed validation; nothing was applied."""


class CycleError(PatchValidationError):
    """An edge would introduce a cycle in the active DEPENDS_ON subgraph."""


class NodeNotFoundError(LhosError):
    """Referenced node does not exist."""


class EdgeNotFoundError(LhosError):
    """Referenced edge does not exist."""


class EvidenceNotFoundError(LhosError):
    """Referenced evidence does not exist."""


class RunNotFoundError(LhosError):
    """Referenced run does not exist."""


class LeaseError(LhosError):
    """Lease acquisition or release failed."""


class BudgetExhaustedError(LhosError):
    """A budget limit was reached."""


class VerificationError(LhosError):
    """A verifier could not be executed."""


class LlmJudgeDisabledError(VerificationError):
    """llm_judge verifier was requested but is disabled by config."""


class ToolError(LhosError):
    """Base error for tool runtime failures."""


class ToolNotAllowedError(ToolError):
    """Tool side-effect level is not allowed in the MVP."""


class IdempotencyKeyRequiredError(ToolError):
    """A side-effecting tool call was attempted without an idempotency key."""


class ToolExecutionError(ToolError):
    """The tool itself failed while executing."""


class SimulatedCrashError(LhosError):
    """Raised by the FakeWorker to simulate a runtime crash mid-run (tests only)."""


class StructuredOutputError(LhosError):
    """LLM output could not be parsed into the required schema."""
