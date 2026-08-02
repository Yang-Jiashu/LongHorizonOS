.PHONY: install test lint smoke

install:
	python -m pip install -e .[dev]

test:
	python -m pytest tests/ -x -q

lint:
	@command -v ruff >/dev/null 2>&1 && ruff check src tests || echo "ruff not installed; skipping lint"

smoke:
	lhos init --db artifacts/lhos.db
	lhos run --db artifacts/lhos.db --graph-file tasks/example_task.json --workspace artifacts/smoke_workspace --config configs/development.yaml --scheduler fifo
