"""`lhos inspect` and `lhos graph` (spec section 20)."""

from __future__ import annotations

import json

from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore
from lhos.infrastructure.telemetry.metrics_collector import MetricsCollector


def _stores(db_path: str):
    db = Database(db_path)
    events = SqliteEventStore(db)
    store = SqliteGraphStore(db, events)
    return db, events, store


def cmd_inspect(args) -> int:
    db, events, store = _stores(args.db)
    try:
        metrics = MetricsCollector(store, events).collect(args.run_id)
        print(f"run:            {metrics['run_id']}")
        print(f"goal:           {metrics['goal']}")
        print(f"status:         {metrics['status']}")
        print(
            f"verified progress: {metrics['verified_progress']} / "
            f"{metrics['total_progress']} ({metrics['progress_ratio']:.1%})"
        )
        print(f"ready nodes:    {metrics['ready_nodes']}")
        print(f"running nodes:  {metrics['running_nodes']}")
        print(f"waiting nodes:  {metrics['waiting_nodes']}")
        print(f"failed nodes:   {metrics['failed_nodes']}")
        print(f"verified nodes: {metrics['verified_nodes']}")
        print(
            f"token usage:    {metrics['total_tokens']} "
            f"(in {metrics['input_tokens']} / out {metrics['output_tokens']}, "
            f"{metrics['model_calls']} model calls)"
        )
        print(f"tool calls:     {metrics['tool_calls']}")
        print(f"wall time:      {metrics['wall_time_seconds']}s")
        print(
            f"graph overhead: {metrics['graph_maintenance_events']} graph events "
            f"/ {metrics['total_events']} total events"
        )
        return 0
    finally:
        db.close()


def cmd_graph(args) -> int:
    db, _events, store = _stores(args.db)
    try:
        graph = store.load_graph(args.run_id)
        dump = {
            "run_id": args.run_id,
            "nodes": [
                n.model_dump(mode="json") for n in sorted(graph.nodes.values(), key=lambda n: n.id)
            ],
            "edges": [e.model_dump(mode="json") for e in sorted(graph.edges, key=lambda e: e.id)],
        }
        print(json.dumps(dump, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        db.close()
