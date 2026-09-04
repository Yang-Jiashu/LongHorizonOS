"""Integration tests for Milestone 2.2: verification failure → feedback → retry flow.

Tests the complete flow:
1. Node fails verification → structured feedback is generated.
2. Context compiler includes the feedback in the next retry's context.
3. Local repair manager tracks failure signatures and detects repeats.
4. Repair guidance is injected into the worker context.
5. Node budget exhaustion triggers specific failure code.
"""

from __future__ import annotations

from lhos.domain.enums import NodeState
from lhos.domain.events import ActorType, EventType, RuntimeEvent
from lhos.runtime.context_compiler import ContextCompiler, ContextRequest
from lhos.runtime.local_repair import (
    FAILURE_REPEATED_VERIFICATION_FAILURE,
    LocalRepairManager,
)
from lhos.runtime.node_budget import NodeBudgetConfig, NodeExecutionBudget
from lhos.runtime.verification_feedback import build_feedback_from_verification
from tests.conftest import make_node


class TestVerificationFailureToContextFlow:
    """Test that verification failure feedback flows into the next retry context."""

    def test_verification_failed_event_produces_structured_feedback(
        self, graph_store, event_store, run_id
    ):
        """A VERIFICATION_FAILED event should produce structured feedback in context."""
        # Seed a node with a verification spec.
        node = make_node(
            "n1",
            run_id=run_id,
            state=NodeState.READY,
            verification_spec={
                "verifier_type": "file_exists",
                "parameters": {"path": "output.txt"},
            },
        )
        graph_store.add_node(node)

        # Emit a VERIFICATION_FAILED event (as the verification gate would).
        event_store.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.VERIFICATION_FAILED,
                actor_type=ActorType.VERIFIER,
                actor_id="n1",
                payload={
                    "node_id": "n1",
                    "summary": "file_exists: file not found: output.txt",
                    "spec": {
                        "verifier_type": "file_exists",
                        "parameters": {"path": "output.txt"},
                    },
                    "evidence": [],
                },
            )
        )

        # Compile context for the node.
        compiler = ContextCompiler(graph_store, event_store)
        packet = compiler.compile(
            ContextRequest(
                run_id=run_id,
                node_id="n1",
                max_tokens=4000,
                include_last_failures=2,
            )
        )

        # The previous_failures should contain structured feedback.
        assert len(packet.previous_failures) > 0
        failure_text = packet.previous_failures[0]
        assert "file not found" in failure_text
        assert "output.txt" in failure_text

    def test_repair_feedback_injected_from_node_metadata(self, graph_store, event_store, run_id):
        """Repair feedback stored in node.metadata should appear in context."""
        node = make_node(
            "n1",
            run_id=run_id,
            state=NodeState.FAILED,
            verification_spec={
                "verifier_type": "file_exists",
                "parameters": {"artifact_name": "design.md"},
            },
            metadata={
                "repair_feedback": "Do NOT repeat the same action. Try a different approach.",
                "failure_code": "missing_path_parameter",
                "retryable": False,
            },
        )
        graph_store.add_node(node)

        compiler = ContextCompiler(graph_store, event_store)
        packet = compiler.compile(
            ContextRequest(
                run_id=run_id,
                node_id="n1",
                max_tokens=4000,
            )
        )

        # Repair guidance should be in the context.
        repair_lines = [f for f in packet.previous_failures if "REPAIR GUIDANCE" in f]
        assert len(repair_lines) == 1
        assert "Do NOT repeat" in repair_lines[0]

        # Failure code should be in the context.
        code_lines = [f for f in packet.previous_failures if "FAILURE CODE" in f]
        assert len(code_lines) == 1
        assert "missing_path_parameter" in code_lines[0]
        assert "NOT retryable" in code_lines[0]

    def test_multiple_failures_included_in_order(self, graph_store, event_store, run_id):
        """Multiple verification failures should be included in sequence order."""
        node = make_node(
            "n1",
            run_id=run_id,
            state=NodeState.FAILED,
            verification_spec={
                "verifier_type": "file_exists",
                "parameters": {"path": "output.txt"},
            },
        )
        graph_store.add_node(node)

        # First failure: file not found.
        event_store.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.VERIFICATION_FAILED,
                actor_type=ActorType.VERIFIER,
                actor_id="n1",
                payload={
                    "node_id": "n1",
                    "summary": "file_exists: file not found: output.txt",
                    "spec": {"verifier_type": "file_exists", "parameters": {"path": "output.txt"}},
                    "evidence": [],
                },
            )
        )

        # Second failure: still not found.
        event_store.append(
            RuntimeEvent(
                run_id=run_id,
                event_type=EventType.VERIFICATION_FAILED,
                actor_type=ActorType.VERIFIER,
                actor_id="n1",
                payload={
                    "node_id": "n1",
                    "summary": "file_exists: file not found: output.txt",
                    "spec": {"verifier_type": "file_exists", "parameters": {"path": "output.txt"}},
                    "evidence": [],
                },
            )
        )

        compiler = ContextCompiler(graph_store, event_store)
        packet = compiler.compile(
            ContextRequest(
                run_id=run_id,
                node_id="n1",
                max_tokens=4000,
                include_last_failures=2,
            )
        )

        # Should have 2 failure entries.
        assert len(packet.previous_failures) >= 2


class TestLocalRepairIntegration:
    """Test local repair manager integration with failure tracking."""

    def test_repeated_verification_failure_flow(self):
        """Simulate the n3 scenario: same failure repeated → repeated flag."""
        mgr = LocalRepairManager()

        # First failure.
        d1 = mgr.record_failure(
            node_id="n3",
            verifier_type="file_exists",
            failure_code="missing_path_parameter",
            error_category="missing_path_parameter",
        )
        assert not d1.repeated_failure

        # Second identical failure.
        d2 = mgr.record_failure(
            node_id="n3",
            verifier_type="file_exists",
            failure_code="missing_path_parameter",
            error_category="missing_path_parameter",
        )
        assert d2.repeated_failure
        assert d2.failure_code == FAILURE_REPEATED_VERIFICATION_FAILURE

        # Third attempt still fails → terminal.
        code = mgr.get_terminal_failure_code("n3", attempt_count=3, max_attempts=3)
        assert code == FAILURE_REPEATED_VERIFICATION_FAILURE

    def test_reconciler_triggered_on_repeated_failure(self):
        """Reconciler should be triggered after 2 identical failures with attempts left."""
        mgr = LocalRepairManager()
        for _ in range(2):
            mgr.record_failure(
                node_id="n1",
                verifier_type="file_exists",
                failure_code="missing_path_parameter",
                error_category="missing_path_parameter",
            )
        assert mgr.should_trigger_reconciler("n1", attempt_count=2, max_attempts=3)

        # Build reconciler input.
        reconciler_input = mgr.build_reconciler_input(
            node_id="n1",
            node_specification="Design config loader",
            direct_dependencies=[],
            relevant_artifacts=[],
        )
        assert reconciler_input["failed_node"]["node_id"] == "n1"
        assert len(reconciler_input["last_two_failures"]) == 2
        assert "modify_verification_proposal" in reconciler_input["allowed_actions"]

    def test_different_failure_codes_not_repeated(self):
        """Different failure codes should not trigger repeated failure detection."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        d2 = mgr.record_failure(
            node_id="n1",
            verifier_type="command",
            failure_code="command_exit_code_mismatch",
            error_category="command_exit_code_mismatch",
        )
        assert not d2.repeated_failure


class TestNodeBudgetIntegration:
    """Test per-node budget enforcement."""

    def test_budget_exhaustion_triggers_failure(self):
        """When per-node budget is exhausted, the correct failure should be produced."""
        config = NodeBudgetConfig(max_model_calls=3, max_tool_calls=100, max_total_tokens=100000)
        budget = NodeExecutionBudget(config=config)
        budget.start_node("n1")

        # Make 3 model calls to exhaust the budget.
        for _ in range(3):
            budget.record_model_call("n1", 100, 50)

        assert budget.is_exhausted("n1")
        reason = budget.get_exhaustion_reason("n1")
        assert "model_calls" in reason
        assert "3" in reason

    def test_budget_clear_after_success(self):
        """Budget should be cleared after a node succeeds."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_model_call("n1", 100, 50)
        budget.record_tool_call("n1")

        # Simulate success.
        budget.clear("n1")
        assert budget.get_state("n1") is None
        assert not budget.is_exhausted("n1")

    def test_budget_report_for_diagnostics(self):
        """Budget report should be comprehensive for diagnostics."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_model_call("n1", 1000, 500)
        budget.record_tool_call("n1")
        budget.record_tool_call("n1")
        budget.record_progress_delta("n1", 5)

        report = budget.get_report("n1")
        assert report["model_calls"] == 1
        assert report["tool_calls"] == 2
        assert report["total_tokens"] == 1500
        assert report["external_progress_delta"] == 5
        assert report["is_exhausted"] is False


class TestStructuredFeedbackClassification:
    """Test that build_feedback_from_verification correctly classifies the n3 scenario."""

    def test_n3_scenario_missing_path_parameter(self):
        """The exact n3 root cause should be classified as missing_path_parameter."""
        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary="file_exists: no path",
            spec_params={"artifact_name": "config_loader_design.md"},
            evidence=[],
        )
        assert feedback.failure_code == "missing_path_parameter"
        assert feedback.retryable is False
        assert "config_loader_design.md" in feedback.affected_artifacts

        # The context string should be actionable.
        ctx = feedback.to_context_string()
        assert "no path" in ctx
        assert "NOT retryable" in ctx

    def test_command_failure_with_evidence(self):
        """Command failure with evidence should extract exit code and stderr."""
        feedback = build_feedback_from_verification(
            verifier_type="command",
            summary="exit code 1, expected 0",
            spec_params={"command": "pytest -xvs tests/"},
            evidence=[
                {
                    "evidence_type": "command_output",
                    "metadata": {
                        "exit_code": 1,
                        "stdout_tail": "1 passed",
                        "stderr_tail": "1 failed: test_config",
                    },
                }
            ],
        )
        assert feedback.failure_code == "command_exit_code_mismatch"
        assert feedback.exit_code == 1
        assert feedback.command == "pytest -xvs tests/"
        assert "1 failed: test_config" in feedback.relevant_stderr
        assert "1 passed" in feedback.relevant_stdout

    def test_non_retryable_spec_bug(self):
        """Spec bugs (missing_path_parameter) should be non-retryable."""
        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary="file_exists: no path",
            spec_params={"artifact_name": "missing.txt"},
            evidence=[],
        )
        assert not feedback.retryable
