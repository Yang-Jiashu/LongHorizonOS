"""Bounded local repair (Milestone 2.2 Step 5).

When the same node fails verification repeatedly, this module:
1. Computes a failure signature to detect repeated failures.
2. On the 2nd identical failure, marks it as repeated and instructs the
   worker to avoid the same action.
3. On max attempts, triggers the Semantic Reconciler for local graph repair
   (split node, add diagnostic sub-nodes, modify verification proposal).
4. Produces specific failure codes instead of vague "run_stuck".
"""

from __future__ import annotations

import hashlib
from typing import Any

from pydantic import BaseModel

# Failure codes (Step 5).
FAILURE_NODE_ATTEMPTS_EXHAUSTED = "node_attempts_exhausted"
FAILURE_VERIFICATION_SPEC_INVALID = "verification_spec_invalid"
FAILURE_REPEATED_VERIFICATION_FAILURE = "repeated_verification_failure"
FAILURE_NODE_LOCAL_BUDGET_EXHAUSTED = "node_local_budget_exhausted"
FAILURE_PARSE_FAILURE_EXHAUSTED = "parse_failure_exhausted"
FAILURE_RUN_STUCK = "run_stuck"


class FailureSignature(BaseModel):
    """A deterministic signature for a verification failure.

    Used to detect when the same failure occurs repeatedly.
    """

    verifier_type: str
    failure_code: str
    error_category: str
    affected_artifact_hash: str | None = None

    @property
    def signature(self) -> str:
        material = f"{self.verifier_type}:{self.failure_code}:{self.error_category}:{self.affected_artifact_hash or ''}"
        return hashlib.sha256(material.encode()).hexdigest()[:16]


class LocalRepairDecision(BaseModel):
    """Decision about what local repair action to take."""

    action: str = "retry"  # retry | reconciler | fail
    failure_code: str = ""
    repeated_failure: bool = False
    feedback_message: str = ""
    reconciler_input: dict[str, Any] | None = None


class LocalRepairManager:
    """Manages bounded local repair for failed nodes.

    Flow:
    - First verification failure: pass structured feedback to worker for retry.
    - Second identical failure (same signature): mark as repeated, warn worker.
    - Max attempts: trigger reconciler or produce explicit failure code.
    """

    def __init__(self) -> None:
        self._failure_signatures: dict[str, list[FailureSignature]] = {}

    def record_failure(
        self,
        node_id: str,
        verifier_type: str,
        failure_code: str,
        error_category: str,
        affected_artifact_hash: str | None = None,
    ) -> LocalRepairDecision:
        """Record a verification failure and decide what to do next.

        Parameters
        ----------
        node_id : str
            The node that failed.
        verifier_type : str
            Type of verifier (file_exists, command, etc.)
        failure_code : str
            Structured failure code from VerificationFailureFeedback.
        error_category : str
            Category of error (e.g. "missing_path", "exit_code_mismatch").
        affected_artifact_hash : str | None
            Hash of the affected artifact, if any.

        Returns
        -------
        LocalRepairDecision
            What action to take next.
        """
        sig = FailureSignature(
            verifier_type=verifier_type,
            failure_code=failure_code,
            error_category=error_category,
            affected_artifact_hash=affected_artifact_hash,
        )

        if node_id not in self._failure_signatures:
            self._failure_signatures[node_id] = []

        signatures = self._failure_signatures[node_id]
        signatures.append(sig)

        # Check if this is a repeated failure (same signature as last time).
        repeated = len(signatures) >= 2 and signatures[-1].signature == signatures[-2].signature

        if repeated:
            return LocalRepairDecision(
                action="retry",
                failure_code=FAILURE_REPEATED_VERIFICATION_FAILURE,
                repeated_failure=True,
                feedback_message=(
                    "Last execution still resulted in the same failure. "
                    "Do NOT repeat the exact same tool call or modification. "
                    "Try a fundamentally different approach."
                ),
            )

        # First failure — just retry with feedback.
        return LocalRepairDecision(
            action="retry",
            failure_code=failure_code,
            repeated_failure=False,
            feedback_message="Verification failed. Review the failure details and fix the issue.",
        )

    def should_trigger_reconciler(
        self,
        node_id: str,
        attempt_count: int,
        max_attempts: int,
    ) -> bool:
        """Check if the reconciler should be triggered.

        The reconciler is triggered when:
        - The node has failed verification at least twice with the same signature.
        - The node has not yet exhausted all attempts.
        """
        signatures = self._failure_signatures.get(node_id, [])
        if len(signatures) < 2:
            return False
        # Check if the last two failures have the same signature.
        return signatures[-1].signature == signatures[-2].signature and attempt_count < max_attempts

    def get_terminal_failure_code(
        self,
        node_id: str,
        attempt_count: int,
        max_attempts: int,
    ) -> str:
        """Get the specific failure code for a node that can no longer retry.

        Returns a specific code instead of the vague "run_stuck".
        """
        signatures = self._failure_signatures.get(node_id, [])

        if attempt_count >= max_attempts:
            if (
                signatures
                and len(signatures) >= 2
                and signatures[-1].signature == signatures[-2].signature
            ):
                return FAILURE_REPEATED_VERIFICATION_FAILURE
            return FAILURE_NODE_ATTEMPTS_EXHAUSTED

        return FAILURE_NODE_ATTEMPTS_EXHAUSTED

    def build_reconciler_input(
        self,
        node_id: str,
        node_specification: str,
        direct_dependencies: list[dict[str, Any]],
        relevant_artifacts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Build the input for the Semantic Reconciler.

        Only includes: current failed node, direct dependencies, relevant
        artifacts, last two failures. Prohibits rebuilding the entire graph.
        """
        signatures = self._failure_signatures.get(node_id, [])
        last_two = [
            {
                "verifier_type": s.verifier_type,
                "failure_code": s.failure_code,
                "error_category": s.error_category,
                "affected_artifact_hash": s.affected_artifact_hash,
            }
            for s in signatures[-2:]
        ]

        return {
            "failed_node": {
                "node_id": node_id,
                "specification": node_specification,
            },
            "direct_dependencies": direct_dependencies,
            "relevant_artifacts": relevant_artifacts,
            "last_two_failures": last_two,
            "allowed_actions": [
                "split_current_node",
                "add_diagnostic_sub_nodes",
                "modify_verification_proposal",
                "mark_upstream_artifact_stale",
            ],
            "prohibited_actions": [
                "rebuild_entire_graph",
                "modify_unrelated_branches",
                "directly_set_verified",
                "delete_failure_evidence",
            ],
        }

    def clear(self, node_id: str) -> None:
        """Clear failure tracking for a node."""
        self._failure_signatures.pop(node_id, None)
