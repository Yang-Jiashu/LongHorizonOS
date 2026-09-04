"""Context VM SDK facade.

High-level API for agent processes to load, read, inspect, pin, unpin,
evict, snapshot, restore, close, and list context working sets.
"""

from __future__ import annotations

from typing import Any

from lhos.agent_os.context.models import (
    ContextHandle,
    ContextManifest,
    ContextSnapshot,
    LoadedContext,
)
from lhos.agent_os.context.service import ContextService


class ContextSDK:
    """Convenience facade wrapping ContextService."""

    def __init__(self, service: ContextService) -> None:
        self._service = service

    # ── Context Lifecycle ─────────────────────────────────────────────────

    def load(
        self,
        *,
        pid: str,
        manifest: ContextManifest,
        idempotency_key: str = "",
    ) -> tuple[ContextHandle, LoadedContext]:
        return self._service.load(
            manifest=manifest,
            caller_pid=pid,
            idempotency_key=idempotency_key,
        )

    def read(self, *, pid: str, handle_id: str) -> LoadedContext:
        return self._service.read(pid=pid, handle_id=handle_id)

    def inspect(self, *, pid: str, handle_id: str) -> dict[str, Any]:
        return self._service.inspect(pid=pid, handle_id=handle_id)

    def pin(self, *, pid: str, handle_id: str, page_ids: list[str]) -> list[str]:
        return self._service.pin(pid=pid, handle_id=handle_id, page_ids=page_ids)

    def unpin(self, *, pid: str, handle_id: str, page_ids: list[str]) -> list[str]:
        return self._service.unpin(pid=pid, handle_id=handle_id, page_ids=page_ids)

    def evict(
        self,
        *,
        pid: str,
        working_set_id: str,
        target_tokens: int,
    ) -> dict[str, Any]:
        return self._service.evict(
            pid=pid,
            working_set_id=working_set_id,
            target_tokens=target_tokens,
        )

    def snapshot(
        self,
        *,
        pid: str,
        context_id: str,
        idempotency_key: str = "",
    ) -> ContextSnapshot:
        return self._service.snapshot(
            pid=pid,
            context_id=context_id,
            idempotency_key=idempotency_key,
        )

    def restore_snapshot(
        self,
        *,
        pid: str,
        snapshot_id: str,
        idempotency_key: str = "",
    ) -> tuple[ContextHandle, LoadedContext]:
        return self._service.restore_snapshot(
            pid=pid,
            snapshot_id=snapshot_id,
            idempotency_key=idempotency_key,
        )

    def close(
        self,
        *,
        pid: str,
        handle_id: str,
        idempotency_key: str = "",
    ) -> bool:
        return self._service.close(
            pid=pid,
            handle_id=handle_id,
            idempotency_key=idempotency_key,
        )

    def list_working_sets(self, *, pid: str) -> list[dict[str, Any]]:
        wss = self._service.list_working_sets(pid=pid)
        return [
            {
                "working_set_id": ws.working_set_id,
                "pid": ws.pid,
                "manifest_id": ws.manifest_id,
                "manifest_hash": ws.manifest_hash,
                "state": ws.state,
                "tokens_used": ws.tokens_used,
                "bytes_used": ws.bytes_used,
                "selected_pages": len(ws.selected_page_ids),
                "omitted_pages": len(ws.omitted_page_ids),
            }
            for ws in wss
        ]

    def cleanup_process(self, pid: str) -> dict[str, int]:
        return self._service.cleanup_process(pid)
