"""Convenience wrapper: run the controlled benchmark without the CLI ceremony.

Usage: python scripts/run_controlled.py [extra lhos benchmark args...]
Example: python scripts/run_controlled.py --seeds 1,2,3 --size medium
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from lhos.cli.main import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main(["benchmark", "--suite", "controlled", *sys.argv[1:]]))
