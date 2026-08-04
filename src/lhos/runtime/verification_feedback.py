"""Structured verification failure feedback (Milestone 2.2 Step 3).

When verification fails, this module generates structured feedback that
includes the specific failure details (failed checks, stderr, affected
artifacts) so the worker can take corrective action on retry.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class VerificationFailureFeedback(BaseModel):
    """Structured feedback from a verification failure.

    This is passed to the worker's context on retry so the worker knows
    exactly what went wrong and what to fix.
    """

    verifier_type: str
    failure_code: str
    summary: str

    command: str | None = None
    exit_code: int | None = None
    failed_checks: list[str] = Field(default_factory=list)
    relevant_stdout: str | None = None
    relevant_stderr: str | None = None

    affected_artifacts: list[str] = Field(default_factory=list)
    suggested_scope: list[str] = Field(default_factory=list)

    retryable: bool = True
    rollback_recommended: bool = False

    def to_context_string(self) -> str:
        """Render as a concise string for the worker context.

        Keeps the feedback short — only the actionable parts.
        """
        parts = [f"Verification failed: {self.summary}"]
        if self.failed_checks:
            parts.append(f"Failed checks: {', '.join(self.failed_checks)}")
        if self.command:
            parts.append(f"Command: {self.command}")
        if self.exit_code is not None:
            parts.append(f"Exit code: {self.exit_code}")
        if self.relevant_stderr:
            # Truncate stderr to 500 chars to avoid context bloat.
            stderr = self.relevant_stderr.strip()
            if len(stderr) > 500:
                stderr = stderr[:500] + "...[truncated]"
            parts.append(f"stderr: {stderr}")
        if self.relevant_stdout:
            stdout = self.relevant_stdout.strip()
            if len(stdout) > 500:
                stdout = stdout[:500] + "...[truncated]"
            parts.append(f"stdout: {stdout}")
        if self.affected_artifacts:
            parts.append(f"Affected artifacts: {', '.join(self.affected_artifacts)}")
        if not self.retryable:
            parts.append("This failure is NOT retryable.")
        return "\n".join(parts)


def build_feedback_from_verification(
    verifier_type: str,
    summary: str,
    spec_params: dict | None = None,
    evidence: list | None = None,
) -> VerificationFailureFeedback:
    """Build structured feedback from a verification failure.

    Parameters
    ----------
    verifier_type : str
        The type of verifier that failed (file_exists, command, etc.)
    summary : str
        The failure summary from the verifier.
    spec_params : dict | None
        The verification spec parameters.
    evidence : list | None
        Evidence records from the verification attempt.
    """
    spec_params = spec_params or {}
    evidence = evidence or []

    # Determine failure code based on verifier type and summary.
    failure_code = "verification_failed"
    if "no path" in summary.lower():
        failure_code = "missing_path_parameter"
    elif "command not found" in summary.lower():
        failure_code = "command_not_found"
    elif "exit" in summary.lower() and "expected" in summary.lower():
        failure_code = "command_exit_code_mismatch"
    elif "not found" in summary.lower():
        failure_code = "file_not_found"
    elif "missing" in summary.lower():
        failure_code = "file_missing"

    # Extract command and exit code from evidence metadata.
    command = spec_params.get("command")
    exit_code = None
    relevant_stdout = None
    relevant_stderr = None
    for ev in evidence:
        if isinstance(ev, dict):
            meta = ev.get("metadata", {})
            if "exit_code" in meta:
                exit_code = meta.get("exit_code")
            if "stdout_tail" in meta:
                relevant_stdout = meta.get("stdout_tail")
            if "stderr_tail" in meta:
                relevant_stderr = meta.get("stderr_tail")

    # Determine affected artifacts.
    affected = []
    path = spec_params.get("path") or spec_params.get("artifact_name")
    if path:
        affected.append(path)

    # Determine if retryable.
    retryable = failure_code not in {"missing_path_parameter"}
    # missing_path_parameter is a spec bug — the worker can't fix it,
    # but a local repair/reconciler might.

    rollback_recommended = failure_code in {"command_exit_code_mismatch"}

    return VerificationFailureFeedback(
        verifier_type=verifier_type,
        failure_code=failure_code,
        summary=summary,
        command=command,
        exit_code=exit_code,
        failed_checks=[summary] if summary else [],
        relevant_stdout=relevant_stdout,
        relevant_stderr=relevant_stderr,
        affected_artifacts=affected,
        retryable=retryable,
        rollback_recommended=rollback_recommended,
    )
