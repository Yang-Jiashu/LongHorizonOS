"""Phase 7 crash injection (spec 26.2): kill the runtime at each crash point,
resume on the same database with a fresh controller, and verify:
- the run continues to completion;
- verified work is never repeated;
- environment and graph stay consistent;
- the idempotency-key path replays recorded tool results instead of
  re-executing them.

Crash points covered:
1. before node execution (crash_before_execution);
2. during tool execution (simulate_crash inside the tool);
3. after tool completion, before claim events (crash_after_tool_calls —
   the idempotency path);
4. before verification (crash_before_verification — CLAIMED_DONE recovery);
5. after the verified commit (crash_after_verified).
"""

from __future__ import annotations

import pytest

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.errors import SimulatedCrashError
from lhos.domain.events import EventType


def _node(temp_id, script_extra=None):
    script = {
        "summary": f"{temp_id} done",
        "produced_artifacts": [{"path": f"{temp_id}.txt", "content": temp_id}],
    }
    if script_extra:
        script.update(script_extra)
    return {
        "temp_id": temp_id,
        "kind": "subtask",
        "title": temp_id,
        "specification": f"produce {temp_id}.txt",
        "schedulable": True,
        "progress_weight": 1.0,
        "verification_spec": {"type": "file_exists", "path": f"{temp_id}.txt"},
        "metadata": {"script": script},
    }


def _chain(**script_overrides):
    return {
        "goal": "crash injection test",
        "nodes": [
            _node("n1", script_overrides.get("n1")),
            _node("n2", script_overrides.get("n2")),
            _node("n3", script_overrides.get("n3")),
        ],
        "edges": [
            {"source": "n2", "target": "n1", "kind": "depends_on"},
            {"source": "n3", "target": "n2", "kind": "depends_on"},
        ],
    }


RUN = "run-crash"


def _first_process(tmp_path, spec, fake_script=None):
    stack = RuntimeStack(
        tmp_path / "lhos.db", tmp_path / "ws", config={}, fake_tool_script=fake_script
    )
    stack.graph_store.create_run(RUN, spec["goal"], {})
    stack.initial_builder.build(RUN, spec)
    with pytest.raises(SimulatedCrashError):
        stack.controller.run(RUN)
    return stack


def _second_process(tmp_path, fake_script=None):
    stack = RuntimeStack(
        tmp_path / "lhos.db", tmp_path / "ws", config={}, fake_tool_script=fake_script
    )
    run = stack.controller.resume(RUN)
    return stack, run


def _starts(stack, node_suffix):
    return [
        e
        for e in stack.event_store.list_events(RUN)
        if e.event_type == EventType.EXECUTION_STARTED
        and e.payload.get("node_id") == f"{RUN}:{node_suffix}"
    ]


def _resume_payload(stack):
    resumed = [
        e for e in stack.event_store.list_events(RUN) if e.event_type == EventType.RUN_RESUMED
    ]
    assert len(resumed) == 1
    return resumed[0].payload["recovery"]


# ----------------------------------------------------------- 1. before exec
def test_crash_before_node_execution(tmp_path):
    spec = _chain(n2={"crash_before_execution": True})
    stack1 = _first_process(tmp_path, spec)
    assert stack1.graph_store.get_node(f"{RUN}:n1").state == NodeState.VERIFIED
    # n2 never started executing.
    assert stack1.graph_store.get_node(f"{RUN}:n2").state == NodeState.READY
    assert _starts(stack1, "n2") == []
    stack1.close()

    stack2, run = _second_process(tmp_path)
    try:
        assert run.status == "completed"
        n2 = stack2.graph_store.get_node(f"{RUN}:n2")
        assert n2.state == NodeState.VERIFIED
        assert n2.attempt_count == 1
        assert len(_starts(stack2, "n2")) == 1
    finally:
        stack2.close()


# ------------------------------------------------------- 2. mid-tool crash
def test_crash_during_tool_execution(tmp_path):
    spec = _chain(
        n2={
            "tool_calls": [{"tool_name": "fake", "arguments": {"simulate_crash": True}}],
            "attempts": {"2": {"tool_calls": [{"tool_name": "fake", "arguments": {}}]}},
        }
    )
    stack1 = _first_process(tmp_path, spec, fake_script=[{"stdout": "ok"}])
    # REQUESTED was written, no terminal event for that call.
    requested = [
        e
        for e in stack1.event_store.list_events(RUN)
        if e.event_type == EventType.TOOL_CALL_REQUESTED and e.payload.get("node_id") == f"{RUN}:n2"
    ]
    assert len(requested) == 1
    key = requested[0].idempotency_key
    assert stack1.event_store.find_by_idempotency(RUN, f"{key}:completed") is None
    stack1.close()

    stack2, run = _second_process(tmp_path, fake_script=[{"stdout": "ok"}])
    try:
        assert run.status == "completed"
        recovery = _resume_payload(stack2)
        assert recovery["incomplete_tool_calls"] == 1
        assert recovery["recovered_running_nodes"] == 1
        # Attempt 2 used different arguments → new key → tool really executed.
        fake = stack2.tool_registry.get("fake")
        assert len(fake.calls) == 1
        assert stack2.graph_store.get_node(f"{RUN}:n2").state == NodeState.VERIFIED
    finally:
        stack2.close()


# ---------------------------- 3. post-tool, pre-event crash (idempotency)
def test_crash_after_tool_completion_before_claim_uses_idempotency(tmp_path):
    spec = _chain(
        n2={
            "tool_calls": [{"tool_name": "fake", "arguments": {"q": "1"}}],
            "crash_after_tool_calls": 1,
        }
    )
    stack1 = _first_process(tmp_path, spec, fake_script=[{"stdout": "recorded-result"}])
    completed = [
        e
        for e in stack1.event_store.list_events(RUN)
        if e.event_type == EventType.TOOL_CALL_COMPLETED and e.payload.get("node_id") == f"{RUN}:n2"
    ]
    assert len(completed) == 1
    stack1.close()

    stack2, run = _second_process(tmp_path, fake_script=[{"stdout": "recorded-result"}])
    try:
        assert run.status == "completed"
        # The idempotency-key path replayed the recorded result: the tool was
        # NOT executed again in the second process.
        fake2 = stack2.tool_registry.get("fake")
        assert len(fake2.calls) == 0
        # Still exactly one COMPLETED event for that key in the whole log.
        completed2 = [
            e
            for e in stack2.event_store.list_events(RUN)
            if e.event_type == EventType.TOOL_CALL_COMPLETED
            and e.payload.get("node_id") == f"{RUN}:n2"
            and e.payload.get("tool_name") == "fake"
        ]
        assert len(completed2) == 1
        n2 = stack2.graph_store.get_node(f"{RUN}:n2")
        assert n2.state == NodeState.VERIFIED
        assert n2.attempt_count == 2
    finally:
        stack2.close()


# --------------------------------------- 4. crash before verification
def test_crash_before_verification_recovers_claimed_node(tmp_path):
    spec = _chain(n2={"crash_before_verification": True})
    stack1 = _first_process(tmp_path, spec)
    # The claim was persisted; verification never ran.
    assert stack1.graph_store.get_node(f"{RUN}:n2").state == NodeState.CLAIMED_DONE
    claims = [
        e
        for e in stack1.event_store.list_events(RUN)
        if e.event_type == EventType.CLAIM_SUBMITTED and e.payload.get("node_id") == f"{RUN}:n2"
    ]
    assert len(claims) == 1
    stack1.close()

    stack2, run = _second_process(tmp_path)
    try:
        assert run.status == "completed"
        recovery = _resume_payload(stack2)
        assert recovery["recovered_claimed_nodes"] == 1
        n2 = stack2.graph_store.get_node(f"{RUN}:n2")
        assert n2.state == NodeState.VERIFIED
        assert n2.attempt_count == 2
        # And it went through the full claim -> verify flow again.
        passed = [
            e
            for e in stack2.event_store.list_events(RUN)
            if e.event_type == EventType.VERIFICATION_PASSED
            and e.payload.get("node_id") == f"{RUN}:n2"
        ]
        assert len(passed) == 1
    finally:
        stack2.close()


# ------------------------------------------ 5. crash after the commit
def test_crash_after_verified_commit_does_not_repeat_work(tmp_path):
    spec = _chain(n2={"crash_after_verified": True})
    stack1 = _first_process(tmp_path, spec)
    assert stack1.graph_store.get_node(f"{RUN}:n2").state == NodeState.VERIFIED
    stack1.close()

    stack2, run = _second_process(tmp_path)
    try:
        assert run.status == "completed"
        n1 = stack2.graph_store.get_node(f"{RUN}:n1")
        n2 = stack2.graph_store.get_node(f"{RUN}:n2")
        n3 = stack2.graph_store.get_node(f"{RUN}:n3")
        assert n1.attempt_count == 1
        assert n2.attempt_count == 1  # verified before the crash: not repeated
        assert n3.state == NodeState.VERIFIED
        assert len(_starts(stack2, "n2")) == 1
        # Environment consistency: every produced file exists.
        for name in ("n1.txt", "n2.txt", "n3.txt"):
            assert (tmp_path / "ws" / name).exists()
    finally:
        stack2.close()
