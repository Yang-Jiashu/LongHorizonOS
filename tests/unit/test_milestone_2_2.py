"""Unit tests for Milestone 2.2: Stuck-Node Recovery and Pilot Readiness.

Covers:
- Step 3: Structured Verification Failure Feedback
- Step 4: Retry state machine (separated attempt counting)
- Step 5: Bounded Local Repair (failure signatures, reconciler triggering)
- Step 6: Duplicate Work Detection
- Step 7: Per-node Worker Loop Budget
"""

from __future__ import annotations

from lhos.domain.enums import NodeKind
from lhos.domain.models import GraphNode
from lhos.runtime.duplicate_work import (
    DuplicateWorkDetector,
)
from lhos.runtime.local_repair import (
    FAILURE_NODE_ATTEMPTS_EXHAUSTED,
    FAILURE_REPEATED_VERIFICATION_FAILURE,
    FailureSignature,
    LocalRepairManager,
)
from lhos.runtime.node_budget import (
    NodeBudgetConfig,
    NodeExecutionBudget,
)
from lhos.runtime.verification_feedback import (
    VerificationFailureFeedback,
    build_feedback_from_verification,
)

# ---------------------------------------------------------------------------
# Step 3: Structured Verification Failure Feedback
# ---------------------------------------------------------------------------


class TestVerificationFailureFeedback:
    """Test VerificationFailureFeedback model and build_feedback_from_verification."""

    def test_file_exists_no_path_feedback(self):
        """The n3 root cause: 'no path' should be classified as missing_path_parameter."""
        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary="file_exists: no path",
            spec_params={"artifact_name": "config_loader_design.md"},
            evidence=[],
        )
        assert feedback.failure_code == "missing_path_parameter"
        assert feedback.retryable is False
        assert "config_loader_design.md" in feedback.affected_artifacts

    def test_command_exit_code_mismatch(self):
        """Exit code mismatch should be classified correctly."""
        feedback = build_feedback_from_verification(
            verifier_type="command",
            summary="exit code 1, expected 0",
            spec_params={"command": "pytest tests/"},
            evidence=[
                {"metadata": {"exit_code": 1, "stderr_tail": "AssertionError"}},
            ],
        )
        assert feedback.failure_code == "command_exit_code_mismatch"
        assert feedback.exit_code == 1
        assert feedback.command == "pytest tests/"
        assert "AssertionError" in feedback.relevant_stderr
        assert feedback.rollback_recommended is True

    def test_file_not_found(self):
        """File not found should be classified correctly."""
        feedback = build_feedback_from_verification(
            verifier_type="file_exists",
            summary="file not found: output.txt",
            spec_params={"path": "output.txt"},
            evidence=[],
        )
        assert feedback.failure_code == "file_not_found"
        assert feedback.retryable is True
        assert "output.txt" in feedback.affected_artifacts

    def test_to_context_string_truncates_stderr(self):
        """stderr should be truncated to 500 chars in context string."""
        long_stderr = "x" * 1000
        feedback = VerificationFailureFeedback(
            verifier_type="command",
            failure_code="command_exit_code_mismatch",
            summary="command failed",
            relevant_stderr=long_stderr,
        )
        result = feedback.to_context_string()
        assert "...[truncated]" in result
        assert len(result) < len(long_stderr)

    def test_to_context_string_includes_all_fields(self):
        """Context string should include all available fields."""
        feedback = VerificationFailureFeedback(
            verifier_type="command",
            failure_code="command_exit_code_mismatch",
            summary="tests failed",
            command="pytest tests/",
            exit_code=1,
            failed_checks=["test_parser", "test_config"],
            relevant_stdout="OK",
            relevant_stderr="FAIL",
            affected_artifacts=["output.txt"],
            retryable=True,
        )
        result = feedback.to_context_string()
        assert "tests failed" in result
        assert "test_parser" in result
        assert "pytest tests/" in result
        assert "Exit code: 1" in result
        assert "FAIL" in result
        assert "OK" in result
        assert "output.txt" in result

    def test_non_retryable_in_context_string(self):
        """Non-retryable flag should appear in context string."""
        feedback = VerificationFailureFeedback(
            verifier_type="file_exists",
            failure_code="missing_path_parameter",
            summary="no path",
            retryable=False,
        )
        result = feedback.to_context_string()
        assert "NOT retryable" in result

    def test_generic_verification_failed(self):
        """Unknown failure should get generic code."""
        feedback = build_feedback_from_verification(
            verifier_type="custom",
            summary="something unexpected happened",
            spec_params={},
            evidence=[],
        )
        assert feedback.failure_code == "verification_failed"
        assert feedback.retryable is True


# ---------------------------------------------------------------------------
# Step 5: Bounded Local Repair
# ---------------------------------------------------------------------------


class TestLocalRepairManager:
    """Test LocalRepairManager failure signature tracking and reconciler triggering."""

    def test_first_failure_returns_retry(self):
        """First failure should return a retry decision."""
        mgr = LocalRepairManager()
        decision = mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        assert decision.action == "retry"
        assert decision.repeated_failure is False
        assert "Verification failed" in decision.feedback_message

    def test_repeated_failure_detected(self):
        """Same failure signature twice should be detected as repeated."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="missing_path_parameter",
            error_category="missing_path_parameter",
        )
        decision = mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="missing_path_parameter",
            error_category="missing_path_parameter",
        )
        assert decision.repeated_failure is True
        assert decision.failure_code == FAILURE_REPEATED_VERIFICATION_FAILURE
        assert "Do NOT repeat" in decision.feedback_message

    def test_different_failures_not_repeated(self):
        """Different failure signatures should not be flagged as repeated."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        decision = mgr.record_failure(
            node_id="n1",
            verifier_type="command",
            failure_code="command_exit_code_mismatch",
            error_category="command_exit_code_mismatch",
        )
        assert decision.repeated_failure is False

    def test_should_trigger_reconciler_after_two_identical(self):
        """Reconciler should be triggered after 2 identical failures with attempts remaining."""
        mgr = LocalRepairManager()
        for _ in range(2):
            mgr.record_failure(
                node_id="n1",
                verifier_type="file_exists",
                failure_code="missing_path_parameter",
                error_category="missing_path_parameter",
            )
        assert mgr.should_trigger_reconciler("n1", attempt_count=2, max_attempts=3)

    def test_should_not_trigger_reconciler_first_failure(self):
        """Reconciler should NOT be triggered after only 1 failure."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        assert not mgr.should_trigger_reconciler("n1", attempt_count=1, max_attempts=3)

    def test_should_not_trigger_reconciler_max_attempts(self):
        """Reconciler should NOT be triggered when max attempts exhausted."""
        mgr = LocalRepairManager()
        for _ in range(2):
            mgr.record_failure(
                node_id="n1",
                verifier_type="file_exists",
                failure_code="file_not_found",
                error_category="file_not_found",
            )
        assert not mgr.should_trigger_reconciler("n1", attempt_count=3, max_attempts=3)

    def test_terminal_failure_code_attempts_exhausted(self):
        """When attempts are exhausted, should return node_attempts_exhausted."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        code = mgr.get_terminal_failure_code("n1", attempt_count=3, max_attempts=3)
        assert code == FAILURE_NODE_ATTEMPTS_EXHAUSTED

    def test_terminal_failure_code_repeated(self):
        """When repeated failures exhaust attempts, should return repeated_verification_failure."""
        mgr = LocalRepairManager()
        for _ in range(3):
            mgr.record_failure(
                node_id="n1",
                verifier_type="file_exists",
                failure_code="missing_path_parameter",
                error_category="missing_path_parameter",
            )
        code = mgr.get_terminal_failure_code("n1", attempt_count=3, max_attempts=3)
        assert code == FAILURE_REPEATED_VERIFICATION_FAILURE

    def test_build_reconciler_input(self):
        """Reconciler input should include failed node, deps, artifacts, and last 2 failures."""
        mgr = LocalRepairManager()
        for _ in range(2):
            mgr.record_failure(
                node_id="n1",
                verifier_type="file_exists",
                failure_code="missing_path_parameter",
                error_category="missing_path_parameter",
                affected_artifact_hash="abc123",
            )
        reconciler_input = mgr.build_reconciler_input(
            node_id="n1",
            node_specification="Design config loader",
            direct_dependencies=[{"node_id": "n0", "title": "Review tests"}],
            relevant_artifacts=[{"path": "config_loader_design.md"}],
        )
        assert reconciler_input["failed_node"]["node_id"] == "n1"
        assert len(reconciler_input["last_two_failures"]) == 2
        assert "split_current_node" in reconciler_input["allowed_actions"]
        assert "rebuild_entire_graph" in reconciler_input["prohibited_actions"]

    def test_clear_resets_tracking(self):
        """Clear should remove all failure tracking for a node."""
        mgr = LocalRepairManager()
        mgr.record_failure(
            node_id="n1",
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        mgr.clear("n1")
        assert not mgr.should_trigger_reconciler("n1", 1, 3)

    def test_failure_signature_deterministic(self):
        """Same inputs should produce same signature."""
        sig1 = FailureSignature(
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
            affected_artifact_hash="abc",
        )
        sig2 = FailureSignature(
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
            affected_artifact_hash="abc",
        )
        assert sig1.signature == sig2.signature

    def test_failure_signature_different_for_different_inputs(self):
        """Different inputs should produce different signatures."""
        sig1 = FailureSignature(
            verifier_type="file_exists",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        sig2 = FailureSignature(
            verifier_type="command",
            failure_code="file_not_found",
            error_category="file_not_found",
        )
        assert sig1.signature != sig2.signature


# ---------------------------------------------------------------------------
# Step 6: Duplicate Work Detection
# ---------------------------------------------------------------------------


class TestDuplicateWorkDetector:
    """Test DuplicateWorkDetector for redundant tool call detection."""

    def test_first_call_not_duplicate(self):
        """First call should not be flagged as duplicate."""
        detector = DuplicateWorkDetector()
        result = detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "read", "path": "file.txt"},
        )
        assert result.duplicate is False
        assert result.blocked is False

    def test_second_call_warns(self):
        """Second identical call should warn."""
        detector = DuplicateWorkDetector()
        args = {"op": "read", "path": "file.txt"}
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        result = detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        assert result.duplicate is True
        assert result.blocked is False
        assert result.feedback is not None

    def test_third_call_blocked(self):
        """Third identical call should be blocked."""
        detector = DuplicateWorkDetector()
        args = {"op": "read", "path": "file.txt"}
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        result = detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        assert result.blocked is True
        assert "BLOCKED" in result.feedback

    def test_different_arguments_not_duplicate(self):
        """Different arguments should not be flagged as duplicate."""
        detector = DuplicateWorkDetector()
        detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "read", "path": "file1.txt"},
        )
        result = detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "read", "path": "file2.txt"},
        )
        assert result.duplicate is False

    def test_no_op_write_detected(self):
        """Writing the same content should be detected as no-op."""
        detector = DuplicateWorkDetector()
        content = "hello world"
        detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "write", "path": "out.txt", "content": content},
        )
        result = detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "write", "path": "out.txt", "content": content},
        )
        assert result.duplicate is True
        assert "no-op" in result.feedback.lower()

    def test_repeated_failing_command_blocked(self):
        """Repeated failing commands should be blocked after 2 failures."""
        detector = DuplicateWorkDetector()
        cmd = "pytest tests/"
        for _i in range(2):
            result = detector.check_and_record(
                node_id="n1",
                tool_name="shell",
                arguments={"command": cmd},
                result_success=False,
            )
        # Third failure should be blocked
        result = detector.check_and_record(
            node_id="n1",
            tool_name="shell",
            arguments={"command": cmd},
            result_success=False,
        )
        assert result.blocked is True
        assert "BLOCKED" in result.feedback

    def test_metrics_tracked(self):
        """Metrics should be properly tracked."""
        detector = DuplicateWorkDetector()
        args = {"op": "read", "path": "file.txt"}
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        assert detector.metrics.duplicate_tool_call_count >= 1

    def test_get_feedback_for_worker(self):
        """Feedback summary should be generated when duplicates exist."""
        detector = DuplicateWorkDetector()
        args = {"op": "read", "path": "file.txt"}
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        detector.check_and_record(node_id="n1", tool_name="filesystem", arguments=args)
        feedback = detector.get_feedback_for_worker()
        assert feedback is not None
        assert "Duplicate work" in feedback

    def test_get_feedback_none_when_no_duplicates(self):
        """Feedback should be None when no duplicates."""
        detector = DuplicateWorkDetector()
        detector.check_and_record(
            node_id="n1",
            tool_name="filesystem",
            arguments={"op": "read", "path": "file.txt"},
        )
        assert detector.get_feedback_for_worker() is None


# ---------------------------------------------------------------------------
# Step 7: Per-Node Worker Loop Budget
# ---------------------------------------------------------------------------


class TestNodeExecutionBudget:
    """Test NodeExecutionBudget for per-node resource tracking."""

    def test_start_node_creates_state(self):
        """start_node should create a tracking state."""
        budget = NodeExecutionBudget()
        state = budget.start_node("n1")
        assert state is not None
        assert state.node_id == "n1"
        assert state.model_calls == 0
        assert state.started_at is not None

    def test_record_model_call(self):
        """record_model_call should increment counters."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_model_call("n1", 1000, 500)
        state = budget.get_state("n1")
        assert state.model_calls == 1
        assert state.input_tokens == 1000
        assert state.output_tokens == 500
        assert state.total_tokens == 1500

    def test_record_tool_call(self):
        """record_tool_call should increment counter."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_tool_call("n1")
        budget.record_tool_call("n1")
        state = budget.get_state("n1")
        assert state.tool_calls == 2

    def test_not_exhausted_initially(self):
        """Budget should not be exhausted initially."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        assert not budget.is_exhausted("n1")

    def test_exhausted_by_model_calls(self):
        """Budget should be exhausted when max_model_calls reached."""
        config = NodeBudgetConfig(max_model_calls=3, max_tool_calls=100, max_total_tokens=100000)
        budget = NodeExecutionBudget(config=config)
        budget.start_node("n1")
        for _ in range(3):
            budget.record_model_call("n1", 100, 50)
        assert budget.is_exhausted("n1")
        reason = budget.get_exhaustion_reason("n1")
        assert "model_calls" in reason

    def test_exhausted_by_tool_calls(self):
        """Budget should be exhausted when max_tool_calls reached."""
        config = NodeBudgetConfig(max_model_calls=100, max_tool_calls=5, max_total_tokens=100000)
        budget = NodeExecutionBudget(config=config)
        budget.start_node("n1")
        for _ in range(5):
            budget.record_tool_call("n1")
        assert budget.is_exhausted("n1")
        reason = budget.get_exhaustion_reason("n1")
        assert "tool_calls" in reason

    def test_exhausted_by_tokens(self):
        """Budget should be exhausted when max_total_tokens reached."""
        config = NodeBudgetConfig(max_model_calls=100, max_tool_calls=100, max_total_tokens=1000)
        budget = NodeExecutionBudget(config=config)
        budget.start_node("n1")
        budget.record_model_call("n1", 600, 500)
        assert budget.is_exhausted("n1")
        reason = budget.get_exhaustion_reason("n1")
        assert "tokens" in reason

    def test_clear_removes_tracking(self):
        """clear should remove budget tracking for a node."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_model_call("n1", 100, 50)
        budget.clear("n1")
        assert budget.get_state("n1") is None
        assert not budget.is_exhausted("n1")

    def test_get_report(self):
        """get_report should return a comprehensive report."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_model_call("n1", 1000, 500)
        budget.record_tool_call("n1")
        report = budget.get_report("n1")
        assert report["node_id"] == "n1"
        assert report["model_calls"] == 1
        assert report["tool_calls"] == 1
        assert report["total_tokens"] == 1500
        assert report["is_exhausted"] is False

    def test_get_report_untracked(self):
        """get_report for untracked node should indicate not tracked."""
        budget = NodeExecutionBudget()
        report = budget.get_report("nonexistent")
        assert report["tracked"] is False

    def test_record_progress_delta(self):
        """record_progress_delta should increment the delta."""
        budget = NodeExecutionBudget()
        budget.start_node("n1")
        budget.record_progress_delta("n1", 5)
        budget.record_progress_delta("n1", 3)
        state = budget.get_state("n1")
        assert state.external_progress_delta == 8

    def test_untracked_node_not_exhausted(self):
        """is_exhausted for untracked node should return False."""
        budget = NodeExecutionBudget()
        assert not budget.is_exhausted("nonexistent")

    def test_untracked_node_no_reason(self):
        """get_exhaustion_reason for untracked node should return None."""
        budget = NodeExecutionBudget()
        assert budget.get_exhaustion_reason("nonexistent") is None


# ---------------------------------------------------------------------------
# Step 4: Retry State Machine (separated attempt counting)
# ---------------------------------------------------------------------------


class TestRetryStateMachine:
    """Test that GraphNode has separated attempt counters."""

    def test_node_has_separated_counters(self):
        """GraphNode should have attempt_count, verification_attempts, parse_attempts."""
        node = GraphNode(
            id="n1",
            run_id="test-run",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test spec",
        )
        assert node.attempt_count == 0
        assert node.verification_attempts == 0
        assert node.parse_attempts == 0
        assert node.tool_attempts == 0
        assert node.max_attempts == 3

    def test_counters_increment_independently(self):
        """Each counter should increment independently."""
        node = GraphNode(
            id="n1",
            run_id="test-run",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test spec",
        )
        node.attempt_count += 1
        node.verification_attempts += 1
        assert node.attempt_count == 1
        assert node.parse_attempts == 0

        node.attempt_count += 1
        node.parse_attempts += 1
        assert node.attempt_count == 2
        assert node.verification_attempts == 1
        assert node.parse_attempts == 1

    def test_parse_failure_counts_as_attempt(self):
        """Parse failures count as execution attempts (Milestone 2.3 fix).

        Previously, parse failures decremented attempt_count to avoid
        counting toward max_attempts. This caused UNIQUE constraint
        violations when the node was retried with the same attempt_number.
        Now parse failures keep attempt_count incremented and track
        parse-specific failures via parse_attempts.
        """
        node = GraphNode(
            id="n1",
            run_id="test-run",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test spec",
            max_attempts=3,
        )
        # Controller increments at start.
        node.attempt_count += 1
        # Parse failure: attempt_count stays incremented (no decrement).
        node.parse_attempts += 1
        assert node.attempt_count == 1
        assert node.parse_attempts == 1
        # Node can still retry (1 < 3).
        assert node.attempt_count < node.max_attempts

    def test_verification_failure_increments_verification_attempts(self):
        """Verification failures should increment verification_attempts separately."""
        node = GraphNode(
            id="n1",
            run_id="test-run",
            kind=NodeKind.SUBTASK,
            title="test",
            specification="test spec",
            max_attempts=3,
        )
        node.attempt_count += 1
        node.verification_attempts += 1
        assert node.attempt_count == 1
        assert node.verification_attempts == 1
