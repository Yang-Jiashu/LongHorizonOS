.PHONY: install test test-stress lint smoke

install:
	python -m pip install -e .[dev]

test:
	python -m pytest tests/ -x -q -m "not slow"

test-stress:
	python -m pytest tests/runtimes/verified_progress -q -m slow

lint:
	@command -v ruff >/dev/null 2>&1 && ruff check src tests || echo "ruff not installed; skipping lint"

smoke:
	lhos demo recovery-repair --json
