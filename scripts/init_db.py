"""Initialize a LongHorizonOS SQLite database (equivalent to `lhos init`)."""

import sys

from lhos.infrastructure.db.connection import Database

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "artifacts/lhos.db"
    Database(path).close()
    print(f"initialized database at {path}")
