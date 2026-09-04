"""Export a run's event trace as JSON (spec 24: complete run trace)."""

import json
import sys

from lhos.infrastructure.db.connection import Database
from lhos.infrastructure.db.sqlite_event_store import SqliteEventStore

if __name__ == "__main__":
    db_path, run_id = sys.argv[1], sys.argv[2]
    db = Database(db_path)
    events = SqliteEventStore(db).list_events(run_id)
    print(json.dumps([e.model_dump(mode="json") for e in events], indent=2, default=str))
