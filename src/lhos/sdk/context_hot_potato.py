"""Context Hot-Potato: context hot-migration prototype (P0).

Provides data structures and a manager for migrating accumulated agent context
from a preempted/failed worker to a replacement worker without cold reload.

This is the P0 prototype: data structures + create/apply/estimate interfaces
with unit tests.  Integration into run_async (preempt/repair/refill paths)
is P1.
"""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ContextDelta:
    """Mutable context increment between two executions.

    Only the *delta* is migrated; immutable (L0) and verified (L1) context
    is passed by content-hash reference and rehydrated from the VPG/Facts
    store by the target worker.
    """

    new_messages: tuple[dict[str, Any], ...] = ()
    new_tool_calls: tuple[dict[str, Any], ...] = ()
    modified_file_refs: tuple[str, ...] = ()
    scratch_pad: str = ""
    token_estimate: int = 0

    def __post_init__(self) -> None:
        if self.token_estimate == 0:
            self.token_estimate = self._estimate_tokens()

    def _estimate_tokens(self) -> int:
        """Rough token estimate: ~4 chars/token for text, 100/tool-call."""
        text = self.scratch_pad
        for msg in self.new_messages:
            text += str(msg.get("content", ""))
        text_tokens = len(text) // 4
        tool_tokens = len(self.new_tool_calls) * 100
        file_tokens = len(self.modified_file_refs) * 50
        return text_tokens + tool_tokens + file_tokens

    def is_empty(self) -> bool:
        return not (
            self.new_messages
            or self.new_tool_calls
            or self.modified_file_refs
            or self.scratch_pad
        )


@dataclass
class ContextMigrationTicket:
    """A hot-potato ticket carrying context from one worker to another.

    L0 (immutable) and L1 (verified) context are referenced by content hash
    so the target can rehydrate from the authoritative store.  L2 (mutable)
    is carried inline as a ContextDelta.
    """

    ticket_id: str = field(default_factory=lambda: f"hpt-{uuid.uuid4().hex[:12]}")
    source_claim_id: str = ""
    target_task_id: str = ""
    graph_id: str = ""
    graph_version: int = 0
    l0_refs: tuple[str, ...] = ()        # content-hash references
    l1_refs: tuple[str, ...] = ()        # content-hash references
    l2_delta: ContextDelta = field(default_factory=ContextDelta)
    created_at: float = field(default_factory=time.time)
    ttl_seconds: float = 300.0

    def is_expired(self, now: float | None = None) -> bool:
        if now is None:
            now = time.time()
        return (now - self.created_at) > self.ttl_seconds

    def fingerprint(self) -> str:
        """Deterministic fingerprint for idempotency checks."""
        payload = (
            f"{self.source_claim_id}|{self.target_task_id}|{self.graph_id}"
            f"|{self.graph_version}|{','.join(self.l0_refs)}"
            f"|{','.join(self.l1_refs)}|{self.l2_delta.token_estimate}"
        )
        return hashlib.sha256(payload.encode()).hexdigest()[:16]


class ContextHotPotato:
    """Manager for creating, applying, and auditing context migration tickets.

    P0: in-memory store, no integration with run_async.  The manager tracks
    created tickets and can estimate token savings vs cold start.
    """

    def __init__(self) -> None:
        self._tickets: dict[str, ContextMigrationTicket] = {}
        self._applied: list[dict[str, Any]] = []

    def create_ticket(
        self,
        *,
        source_claim_id: str,
        target_task_id: str,
        graph_id: str,
        graph_version: int,
        l0_refs: tuple[str, ...] = (),
        l1_refs: tuple[str, ...] = (),
        l2_delta: ContextDelta | None = None,
        ttl_seconds: float = 300.0,
    ) -> ContextMigrationTicket:
        """Create a migration ticket from a source worker context.

        The source worker's accumulated context is split into L0/L1 references
        and an L2 delta.  Only the delta is carried inline.
        """
        ticket = ContextMigrationTicket(
            source_claim_id=source_claim_id,
            target_task_id=target_task_id,
            graph_id=graph_id,
            graph_version=graph_version,
            l0_refs=l0_refs,
            l1_refs=l1_refs,
            l2_delta=l2_delta or ContextDelta(),
            ttl_seconds=ttl_seconds,
        )
        self._tickets[ticket.ticket_id] = ticket
        return ticket

    def apply_ticket(
        self,
        ticket: ContextMigrationTicket,
        *,
        current_context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Apply a ticket to a target worker, returning merged context.

        In P0 this is a pure merge: L0/L1 refs are marked as 'rehydrated'
        (the actual rehydration from VPG/Facts is the caller's responsibility),
        and the L2 delta is merged into the current context.

        Returns a dict with keys: l0_refs, l1_refs, messages, tool_calls,
        modified_files, scratch_pad.
        """
        if ticket.is_expired():
            raise ValueError(f"ticket {ticket.ticket_id} expired")

        ctx = current_context or {}
        merged = {
            "l0_refs": list(ticket.l0_refs),
            "l1_refs": list(ticket.l1_refs),
            "messages": list(ctx.get("messages", [])) + list(ticket.l2_delta.new_messages),
            "tool_calls": list(ctx.get("tool_calls", [])) + list(ticket.l2_delta.new_tool_calls),
            "modified_files": list(ticket.l2_delta.modified_file_refs),
            "scratch_pad": ctx.get("scratch_pad", "") + ticket.l2_delta.scratch_pad,
            "migrated_from": ticket.source_claim_id,
            "ticket_id": ticket.ticket_id,
        }
        self._applied.append({
            "ticket_id": ticket.ticket_id,
            "target_task_id": ticket.target_task_id,
            "delta_tokens": ticket.l2_delta.token_estimate,
            "at": time.time(),
        })
        return merged

    def estimate_savings(self, ticket: ContextMigrationTicket, cold_start_tokens: int) -> int:
        """Estimate tokens saved by hot migration vs cold start.

        cold_start_tokens: total tokens the target would need to reload
        everything from scratch.  Savings = cold_start - delta_tokens,
        because only the L2 delta needs to be sent inline.
        """
        delta = ticket.l2_delta.token_estimate
        return max(0, cold_start_tokens - delta)

    def get_ticket(self, ticket_id: str) -> ContextMigrationTicket | None:
        return self._tickets.get(ticket_id)

    @property
    def tickets_created(self) -> int:
        return len(self._tickets)

    @property
    def tickets_applied(self) -> int:
        return len(self._applied)
