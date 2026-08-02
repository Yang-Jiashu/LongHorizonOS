"""JSONL tracer: mirrors every appended event into artifacts/traces/<run>.jsonl."""

from __future__ import annotations

import json
from pathlib import Path

from lhos.domain.events import RuntimeEvent


class JsonlTracer:
    def __init__(self, trace_directory: str):
        self._dir = Path(trace_directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    def record_event(self, event: RuntimeEvent) -> None:
        path = self._dir / f"{event.run_id}.jsonl"
        with path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(event.model_dump(mode="json"), sort_keys=True) + "\n"
            )
