"""`lhos replay`: delete the materialized graph and rebuild it from the event
log, then verify the rebuild is identical (spec 26.2 event replay)."""

from __future__ import annotations

import hashlib
import json

from lhos.graph.projection import rebuild_projection
from lhos.infrastructure.db.connection import Database


def _projection_hash(db: Database, run_id: str) -> str:
    material: dict[str, list] = {"nodes": [], "edges": [], "evidence": []}
    for table, key in (("nodes", "nodes"), ("edges", "edges"), ("evidence", "evidence")):
        rows = db.conn.execute(
            f"SELECT * FROM {table} WHERE run_id = ? ORDER BY id", (run_id,)
        ).fetchall()
        material[key] = [dict(r) for r in rows]
    canonical = json.dumps(material, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def cmd_replay(args) -> int:  # noqa: ANN001 - argparse.Namespace
    db = Database(args.db)
    try:
        before = _projection_hash(db, args.run_id)
        counts = rebuild_projection(db, args.run_id)
        after = _projection_hash(db, args.run_id)
        print(f"rebuilt projection from events: {counts}")
        print(f"projection hash before: {before}")
        print(f"projection hash after:  {after}")
        if before == after:
            print("replay OK: rebuilt projection is identical")
            return 0
        print("replay MISMATCH: rebuilt projection differs")
        return 1
    finally:
        db.close()
