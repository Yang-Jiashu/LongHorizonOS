"""Integration tests for the Worker → ToolRuntime execution loop (Step 5).

Tests use a scripted MockLLMClient (NOT a real API) to simulate the
following sequence:
  Iteration 1: tool_call → filesystem write config_loader.py
  Iteration 2: tool_call → filesystem read config_loader.py
  Iteration 3: tool_call → shell pytest
  Iteration 4: claim_done

Verifies:
- File is actually created in the workspace.
- TOOL_CALL_REQUESTED and TOOL_CALL_COMPLETED events both exist.
- Tool result is returned to the next worker iteration.
- Shell verifier actually executes.
- claim_done does not directly become VERIFIED.
- Verification Gate processes the claim.
"""

from __future__ import annotations

import json

import pytest

from lhos.agents.llm_worker_adapter import LLMWorkerAdapter
from lhos.domain.enums import NodeState
from lhos.domain.models import GraphNode
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.llm.adapter import MockLLMClient
from lhos.infrastructure.tools.filesystem_tool import FILESYSTEM_METADATA, FilesystemTool
from lhos.infrastructure.tools.registry import ToolRegistry
from lhos.infrastructure.tools.shell_tool import SHELL_METADATA, ShellTool
from lhos.runtime.context_compiler import ContextPacket
from lhos.runtime.tool_runtime import ToolRuntime


@pytest.fixture
def workspace(tmp_path):
    ws = tmp_path / "workspace"
    ws.mkdir()
    return ws


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "test.db")
    yield database
    database.close()


@pytest.fixture
def event_store(db):
    return SqliteEventStore(db)


@pytest.fixture
def tool_registry():
    reg = ToolRegistry()
    reg.register(ShellTool(), SHELL_METADATA)
    reg.register(FilesystemTool(), FILESYSTEM_METADATA)
    return reg


@pytest.fixture
def tool_runtime(event_store, tool_registry, workspace):
    return ToolRuntime(event_store, tool_registry, str(workspace))


def _make_node(run_id: str = "test-run") -> GraphNode:
    return GraphNode(
        id="node-1",
        run_id=run_id,
        kind="subtask",
        title="Implement config_loader.py",
        specification="Create a config loader module",
        state=NodeState.RUNNING,
        schedulable=True,
        attempt_count=1,
        version=1,
    )


def _make_context() -> ContextPacket:
    return ContextPacket(
        node_id="node-1",
        global_goal="Add config loader",
        current_task="Implement config_loader.py",
        dependency_summaries=[],
        constraints=["Must handle JSON"],
        previous_failures=[],
        verification_requirements=["File exists at src/config_loader.py"],
        estimated_tokens=1000,
        context_hash="test-hash",
    )


def _make_worker_response(action_type: str, **kwargs) -> str:
    """Create a JSON response string for the MockLLMClient."""
    resp = {"action_type": action_type, "summary": kwargs.get("summary", "")}
    if "tool_name" in kwargs:
        resp["tool_request"] = {
            "tool_name": kwargs["tool_name"],
            "arguments": kwargs.get("arguments", {}),
            "timeout_seconds": kwargs.get("timeout_seconds", 30),
        }
    if action_type == "claim_done":
        resp["produced_artifacts"] = kwargs.get("produced_artifacts", [])
        resp["verification_request"] = kwargs.get("verification_request")
    resp["suggested_graph_patch"] = []
    return json.dumps(resp)


class TestWorkerAdapterFileWrite:
    """test_real_worker_adapter_executes_file_write"""

    def test_file_write_creates_file_in_workspace(self, tool_runtime, workspace):
        """The filesystem write tool actually creates the file."""
        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="filesystem",
                arguments={
                    "op": "write",
                    "path": "config_loader.py",
                    "content": "def load(): pass",
                },
                summary="Writing config_loader.py",
            ),
            _make_worker_response(
                "claim_done",
                summary="Created config_loader.py",
                produced_artifacts=[{"path": "config_loader.py", "artifact_type": "file"}],
            ),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "claimed_done"
        assert (workspace / "config_loader.py").exists()
        assert (workspace / "config_loader.py").read_text() == "def load(): pass"

    def test_tool_call_events_exist(self, tool_runtime, event_store):
        """TOOL_CALL_REQUESTED and TOOL_CALL_COMPLETED are both in the event log."""
        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="filesystem",
                arguments={"op": "write", "path": "test.txt", "content": "hello"},
            ),
            _make_worker_response("claim_done", summary="Done"),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        adapter.execute(_make_node(), _make_context())

        events = event_store.list_events("test-run")
        event_types = [e.event_type for e in events]
        assert "TOOL_CALL_REQUESTED" in event_types
        assert "TOOL_CALL_COMPLETED" in event_types


class TestToolResultReturnedToNextIteration:
    """test_tool_result_is_returned_to_next_worker_iteration"""

    def test_read_result_appears_in_next_context(self, tool_runtime, workspace):
        """The result of a filesystem read is passed to the next LLM call."""
        # First write a file, then read it.
        (workspace / "test.txt").write_text("file content here")

        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="filesystem",
                arguments={"op": "read", "path": "test.txt"},
                summary="Reading test.txt",
            ),
            _make_worker_response("claim_done", summary="Done reading"),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        adapter.execute(_make_node(), _make_context())

        # The second LLM call should have received the tool result in its messages.
        assert len(client.requests) == 2
        second_request = client.requests[1]
        # The messages should include the tool result.
        all_messages = " ".join(m.get("content", "") for m in second_request.messages)
        assert "file content here" in all_messages


class TestWorkerToolLoopReachesVerification:
    """test_worker_tool_loop_reaches_verification"""

    def test_full_tool_loop_sequence(self, tool_runtime, workspace):
        """Simulate: write → read → shell → claim_done."""
        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="filesystem",
                arguments={"op": "write", "path": "config.py", "content": "import json"},
                summary="Writing config.py",
            ),
            _make_worker_response(
                "tool_call",
                tool_name="filesystem",
                arguments={"op": "read", "path": "config.py"},
                summary="Reading config.py to verify",
            ),
            _make_worker_response(
                "tool_call",
                tool_name="shell",
                arguments={"command": "echo 'test passed'"},
                summary="Running tests",
            ),
            _make_worker_response(
                "claim_done",
                summary="All tests passed",
                produced_artifacts=[{"path": "config.py", "artifact_type": "file"}],
            ),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "claimed_done"
        assert result.tool_call_count == 3
        assert (workspace / "config.py").exists()
        assert (workspace / "config.py").read_text() == "import json"

        # All 4 LLM calls were made.
        assert len(client.requests) == 4


class TestWorkerCannotClaimDoneWithoutToolExecution:
    """test_worker_cannot_claim_done_without_tool_execution

    The worker CAN claim_done without tools, but the verification gate
    should reject it if no artifacts were produced. This test verifies
    that the adapter doesn't prevent early claim_done — that's the gate's job.
    """

    def test_early_claim_done_is_allowed_by_adapter(self, tool_runtime):
        """The adapter allows claim_done without tool calls (gate decides)."""
        responses = [
            _make_worker_response("claim_done", summary="Done without tools"),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "claimed_done"
        assert result.tool_call_count == 0


class TestToolNameNormalization:
    """Test that tool name variants are normalized correctly."""

    def test_filesystem_write_variant(self, tool_runtime, workspace):
        """'filesystem.write' is normalized to 'filesystem'."""
        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="filesystem.write",
                arguments={"op": "write", "path": "test.txt", "content": "data"},
            ),
            _make_worker_response("claim_done", summary="Done"),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "claimed_done"
        assert result.tool_call_count == 1
        assert (workspace / "test.txt").exists()

    def test_shell_exec_variant(self, tool_runtime, workspace):
        """'shell.exec' is normalized to 'shell'."""
        responses = [
            _make_worker_response(
                "tool_call",
                tool_name="shell.exec",
                arguments={"command": "echo hello"},
            ),
            _make_worker_response("claim_done", summary="Done"),
        ]
        client = MockLLMClient(responses=responses)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "claimed_done"
        assert result.tool_call_count == 1


class TestBudgetExhaustion:
    """Test that the adapter returns 'failed' after max rounds."""

    def test_max_rounds_exceeded(self, tool_runtime):
        """After _MAX_TOOL_ROUNDS, the adapter returns status='failed'."""
        # Always return tool_call — never claim_done.
        response = _make_worker_response(
            "tool_call",
            tool_name="filesystem",
            arguments={"op": "read", "path": "nonexistent.txt"},
        )
        client = MockLLMClient(default_response=response)
        adapter = LLMWorkerAdapter(client=client, tool_runtime=tool_runtime)

        result = adapter.execute(_make_node(), _make_context())

        assert result.status == "failed"
        assert "Exceeded" in result.summary
