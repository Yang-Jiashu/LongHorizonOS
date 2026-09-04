"""`lhos init` and `lhos run` (spec section 20)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from lhos.bootstrap import RuntimeStack
from lhos.config import load_config
from lhos.domain.errors import SimulatedCrashError
from lhos.infrastructure.db.connection import Database


def cmd_init(args) -> int:
    db = Database(args.db)
    db.close()
    print(f"initialized database at {args.db}")
    return 0


def cmd_run(args) -> int:
    config = load_config(args.config)
    spec = json.loads(Path(args.graph_file).read_text(encoding="utf-8"))
    goal = args.goal or spec.get("goal") or "complete the task graph"
    run_id = args.run_id or f"run-{uuid4().hex[:8]}"

    stack = RuntimeStack(
        db_path=args.db,
        workspace_dir=args.workspace,
        config=config,
        scheduler_type=args.scheduler,
    )
    try:
        run_config = {
            "workspace_dir": str(args.workspace),
            "scheduler": args.scheduler or config.get("scheduler", {}).get("type", "fifo"),
            "config_file": args.config,
            "graph_file": str(args.graph_file),
        }
        stack.graph_store.create_run(run_id, goal, run_config)
        id_map = stack.initial_builder.build(run_id, spec)
        print(f"run {run_id}: built initial graph with {len(id_map)} nodes")
        try:
            run = stack.controller.run(run_id)
        except SimulatedCrashError as exc:
            print(f"RUN INTERRUPTED (simulated crash): {exc}")
            print(f"resume with: lhos resume --db {args.db} --run-id {run_id}")
            return 3
        print(f"run {run_id} finished with status: {run.status}")
        return 0 if run.status == "completed" else 2
    finally:
        stack.close()
