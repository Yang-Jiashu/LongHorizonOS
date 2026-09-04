"""Sanity test: every flagship demo must import and run to completion
with exit code 0."""

from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
DEMOS = sorted((REPO / "examples" / "multi_agent").glob("*.py"))
DEMOS = [d for d in DEMOS if d.name != "__init__.py"]
assert DEMOS, "no demo modules collected"


@pytest.mark.parametrize("path", DEMOS, ids=[d.stem for d in DEMOS])
def test_demo_runs(path: Path, monkeypatch) -> None:
    """Run each demo module as __main__ and ensure SystemExit(0)."""
    import sys

    monkeypatch.setattr(sys, "argv", [str(path)])
    # Each demo exits via sys.exit — a non-zero code is a demo bug.
    with pytest.raises(SystemExit) as exc:
        # Execute via exec to simulate `python -m module` without hitting
        # the real subprocess layer — fast and deterministic.
        src = path.read_text(encoding="utf-8")
        ns: dict = {"__name__": "__main__", "__file__": str(path)}
        exec(compile(src, str(path), "exec"), ns)
    # The test framework wraps runpy/exec returns; SystemExit(0) is success.
    # If the demo finished without exit, treat as success.
    if exc.value.code is not None:
        assert exc.value.code == 0, f"{path.name} exited with {exc.value.code}"
