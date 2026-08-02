"""E2E: tiny repository task (spec 26.3 / 31) through the full runtime stack:

inspect repo -> implement parser -> run tests -> update docs, with the parser
check failing once before succeeding (fail_times: 1). README has no code
dependency and is verified independently.
"""

from __future__ import annotations

import json
from pathlib import Path

from lhos.bootstrap import RuntimeStack
from lhos.domain.enums import NodeState
from lhos.domain.events import EventType

FIXTURE = Path(__file__).parent / "fixtures" / "tiny_repo_task.json"


def test_tiny_repository_task_end_to_end(tmp_path):
    spec = json.loads(FIXTURE.read_text(encoding="utf-8"))
    stack = RuntimeStack(
        tmp_path / "lhos.db",
        tmp_path / "repo",
        config={"runtime": {"lease_seconds": 60}},
    )
    try:
        run_id = "run-e2e"
        stack.graph_store.create_run(run_id, spec["goal"], {})
        stack.initial_builder.build(run_id, spec)
        run = stack.controller.run(run_id)

        assert run.status == "completed"
        nodes = {n.id: n for n in stack.graph_store.list_nodes(run_id)}
        assert all(n.state == NodeState.VERIFIED for n in nodes.values())

        # The parser node failed once, retried, and only the parser node did.
        parser = nodes[f"{run_id}:parser"]
        assert parser.attempt_count == 2
        assert nodes[f"{run_id}:docs"].attempt_count == 1

        # The failure left a verification-failure record, then a pass.
        events = stack.event_store.list_events(run_id)
        failed = [e for e in events if e.event_type == EventType.VERIFICATION_FAILED]
        passed = [e for e in events if e.event_type == EventType.VERIFICATION_PASSED]
        assert len(failed) == 1
        assert failed[0].payload["node_id"] == f"{run_id}:parser"
        assert len(passed) == 4

        # Every verified node has evidence (invariant 2).
        evidence = stack.graph_store.list_evidence(run_id)
        evidenced_nodes = {e.metadata.get("node_id") for e in evidence}
        assert evidenced_nodes == set(nodes)

        # Costs are fully traceable.
        executions = stack.graph_store.list_executions(run_id)
        assert len(executions) == 5  # 4 nodes + 1 parser retry
        assert all(e.status in {"verified", "verification_failed"} for e in executions)

        # The graph can be rebuilt from the event log.
        from lhos.cli.replay import _projection_hash
        from lhos.graph.projection import rebuild_projection

        before = _projection_hash(stack.db, run_id)
        rebuild_projection(stack.db, run_id)
        assert _projection_hash(stack.db, run_id) == before
    finally:
        stack.close()
