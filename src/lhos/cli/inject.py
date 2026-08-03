"""`lhos inject`: push an external environment event into a run (spec 15).

Example:
    lhos inject --db artifacts/lhos.db --run-id run-1 \
        --type artifact_updated \
        --payload '{"node_id": "run-1:art", "new_hash": "v2"}'
"""

from __future__ import annotations

import json

from lhos.domain.events import ActorType, RuntimeEvent
from lhos.graph.reconciler import DeterministicReconciler
from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore


def cmd_inject(args) -> int:
    payload = json.loads(args.payload) if args.payload else {}
    db = Database(args.db)
    try:
        events = SqliteEventStore(db)
        store = SqliteGraphStore(db, events)
        store.get_run(args.run_id)  # validates the run exists
        event = events.append(
            RuntimeEvent(
                run_id=args.run_id,
                event_type=args.type.upper(),
                actor_type=ActorType.CLI,
                payload=payload,
            )
        )
        reconciler = DeterministicReconciler(store)
        handled = reconciler.reconcile_event(args.run_id, event)
        print(
            f"injected {args.type} as event #{event.sequence} "
            f"(reconciled deterministically: {handled})"
        )
        return 0
    finally:
        db.close()
