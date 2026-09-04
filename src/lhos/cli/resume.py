"""`lhos resume` (spec sections 16.3, 20)."""

from __future__ import annotations

from lhos.bootstrap import RuntimeStack
from lhos.config import load_config
from lhos.domain.errors import SimulatedCrashError


def cmd_resume(args) -> int:
    from lhos.infrastructure.db.connection import Database
    from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore
    from lhos.infrastructure.db.sqlite_graph_store import SqliteGraphStore

    config = load_config(args.config)
    # Recover the original workspace from the run config unless overridden.
    probe_db = Database(args.db)
    probe_store = SqliteGraphStore(probe_db, SqliteEventStore(probe_db))
    run = probe_store.get_run(args.run_id)
    workspace = args.workspace or run.config.get("workspace_dir")
    scheduler_type = run.config.get("scheduler")
    probe_db.close()
    if not workspace:
        raise ValueError("workspace unknown; pass --workspace")

    stack = RuntimeStack(
        db_path=args.db,
        workspace_dir=workspace,
        config=config,
        scheduler_type=scheduler_type,
    )
    try:
        try:
            resumed = stack.controller.resume(args.run_id)
        except SimulatedCrashError as exc:
            print(f"RUN INTERRUPTED AGAIN (simulated crash): {exc}")
            return 3
        print(f"run {args.run_id} resumed; status: {resumed.status}")
        return 0 if resumed.status == "completed" else 2
    finally:
        stack.close()
