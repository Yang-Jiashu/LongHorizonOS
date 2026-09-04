#!/usr/bin/env python
"""Debug script for e2e test failure."""

import json
import tempfile
from pathlib import Path

from lhos.bootstrap import RuntimeStack
from lhos.domain.events import EventType

spec = json.loads(Path("tests/e2e/fixtures/tiny_repo_task.json").read_text())
tmp = Path(tempfile.mkdtemp())
stack = RuntimeStack(tmp / "lhos.db", tmp / "repo", config={"runtime": {"lease_seconds": 60}})
run_id = "run-e2e"
stack.graph_store.create_run(run_id, spec["goal"], {})
stack.initial_builder.build(run_id, spec)
run = stack.controller.run(run_id)
nodes = stack.graph_store.list_nodes(run_id)
for n in nodes:
    print(f"Node {n.id}: state={n.state}, attempts={n.attempt_count}, max={n.max_attempts}")

events = stack.event_store.list_events(run_id)
for e in events:
    if e.event_type in (
        EventType.VERIFICATION_FAILED,
        EventType.VERIFICATION_PASSED,
        EventType.EXECUTION_STARTED,
        EventType.EXECUTION_FINISHED,
        EventType.NODE_STATE_CHANGED,
        EventType.RUN_FAILED,
    ):
        print(f"  {e.event_type}: node={e.actor_id} payload={json.dumps(e.payload)[:300]}")

# Check workspace files
import os

print("\nWorkspace files:")
for f in os.listdir(tmp / "repo"):
    print(f"  {f}: {Path(tmp / 'repo' / f).read_text()[:200]}")
stack.close()
