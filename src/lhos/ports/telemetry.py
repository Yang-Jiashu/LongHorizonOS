"""Telemetry port."""

from typing import Protocol

from lhos.domain.events import RuntimeEvent


class Tracer(Protocol):
    def record_event(self, event: RuntimeEvent) -> None: ...
