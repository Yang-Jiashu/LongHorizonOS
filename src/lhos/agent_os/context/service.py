"""Context VM service — full implementation (manifest validation, load, inspect,
pin/unpin, eviction, snapshots, restore, close, recovery hooks).

For C2 this is the event source of truth for the Context VM durable state.
Journal projections/profiles can recover metadata deterministically; bytes
come from the Artifact FS.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from lhos.agent_os.context.errors import (
    ErrArtifactNotFound,
    ErrCapabilityDenied,
    ErrDuplicateRefId,
    ErrHandleClosed,
    ErrHandleNotOwned,
    ErrInvalidContentHash,
    ErrInvalidManifest,
    ErrSnapshotCorrupt,
)
from lhos.agent_os.context.estimator import TokenEstimator
from lhos.agent_os.context.models import (
    ContentRef,
    ContextHandle,
    ContextManifest,
    ContextPage,
    ContextSnapshot,
    LoadedContext,
    LoadedPage,
    OmittedRef,
    PageBinding,
    VersionBinding,
    WorkingSet,
    _content_hash_for,
    _deterministic_hash,
    _utcnow,
    _uuid,
)
from lhos.agent_os.context.pager import VersionContentProvider, compute_pages_for_ref
from lhos.agent_os.context.policies import (
    RefPages,
    select_pages_v1,
    sort_refs_deterministic,
)


class CapabilityChecker(Protocol):
    """Capability verifier for Context VM operations."""

    def can_context_operation(
        self,
        *,
        pid: str,
        operation: str,
        working_set_id: str | None = None,
        context_id: str | None = None,
    ) -> bool: ...

    def can_artifact_read(self, *, pid: str, artifact_id: str, version: int) -> bool: ...


ArtifactContentSupplier = VersionContentProvider


# ── internal handle record (in-memory view) ──────────────────────────────────


@dataclass
class _HandleRec:
    handle: ContextHandle
    manifest: ContextManifest | None = None
    loaded: LoadedContext | None = None
    working_set: WorkingSet | None = None


def _now_iso() -> str:
    return _utcnow().isoformat()


class ContextService:
    """Core Context VM service for one process-group (one journal/stream)."""

    def __init__(
        self,
        *,
        content_supplier: ArtifactContentSupplier,
        capability_checker: CapabilityChecker,
        estimator: TokenEstimator,
    ) -> None:
        self._content = content_supplier
        self._caps = capability_checker
        self._estimator = estimator

        # Durable state, keyed by owner pid for process isolation.
        self._ws_by_pid: dict[str, dict[str, WorkingSet]] = {}
        self._handles_by_pid: dict[str, dict[str, _HandleRec]] = {}
        self._snaps: dict[str, ContextSnapshot] = {}
        # global pin counts — refcounted
        self._pin_counts: dict[str[int, int]] = {}

        # Idempotency: keyed by (pid, manifest_hash, idempotency_key)
        self._idem_load: dict[tuple[str, str, str], str] = {}
        self._idem_snapshot: dict[tuple[str, str, str], str] = {}
        self._idem_restore: dict[tuple[str, str, str], str] = {}

        # simple replay event sink for tests and projection
        self._events: list[dict[str, Any]] = []

    # ── helpers ───────────────────────────────────────────────────────────────

    def _emit(self, kind: str, **fields: Any) -> dict[str, Any]:
        ev = {"event": kind, "at": _now_iso(), **fields}
        self._events.append(ev)
        return ev

    @staticmethod
    def _require_handle_owned(rec: _HandleRec, pid: str, handle_id: str) -> None:
        if rec.handle.pid != pid:
            raise ErrHandleNotOwned(f"handle {handle_id} belongs to {rec.handle.pid}, not {pid}")
        if rec.handle.closed_at is not None:
            raise ErrHandleClosed(f"handle {handle_id} is closed")

    def _ws_map(self, pid: str) -> dict[str, WorkingSet]:
        return self._ws_by_pid.setdefault(pid, {})

    def _handles_map(self, pid: str) -> dict[str, _HandleRec]:
        return self._handles_by_pid.setdefault(pid, {})

    def _sink_page(self, *, ref: ContentRef, manifest: ContextManifest) -> list[ContextPage]:
        return compute_pages_for_ref(
            ref=ref,
            content_supplier=self._content,
            estimator=self._estimator,
            page_size=manifest.page_size_bytes,
        )

    # ── manifest validation ──────────────────────────────────────────────────

    def validate_manifest(self, manifest: ContextManifest, *, caller_pid: str) -> None:
        """Validate structural + version-binding invariants.

        Raises ErrInvalidManifest / ErrDuplicateRefId on any violation.
        """
        if manifest.token_budget <= 0:
            raise ErrInvalidManifest("token_budget must be > 0")
        if manifest.page_size_bytes <= 0:
            raise ErrInvalidManifest("page_size_bytes must be > 0")
        ids = [r.ref_id for r in manifest.refs]
        if len(set(ids)) != len(ids):
            raise ErrDuplicateRefId("manifest contains duplicate ref_id")
        if manifest.owner_pid != caller_pid:
            raise ErrCapabilityDenied(
                f"manifest.owner_pid {manifest.owner_pid} != caller {caller_pid}"
            )
        # context load-capability gating
        if not self._caps.can_context_operation(
            pid=caller_pid,
            operation="load",
        ):
            raise ErrCapabilityDenied(f"{caller_pid}: context load capability denied")

    # ── verification at load ──────────────────────────────────────────────────

    def _resolve_and_verify_ref(self, ref: ContentRef, *, caller_pid: str) -> bytes:
        """Capability + existence + version + hash check.

        Raises on any integrity violation so that load fails loudly.
        """
        if not self._caps.can_artifact_read(
            pid=caller_pid,
            artifact_id=ref.artifact_id,
            version=ref.version,
        ):
            raise ErrCapabilityDenied(
                f"{caller_pid}: cannot read artifact {ref.artifact_id} v{ref.version}"
            )
        try:
            content = self._content.read_version(
                artifact_id=ref.artifact_id,
                version=ref.version,
                canonical_uri=ref.canonical_uri,
            )
        except FileNotFoundError as e:
            raise ErrArtifactNotFound(f"{ref.artifact_id} v{ref.version}: {e}") from e
        actual_hash = _content_hash_for(content)
        if actual_hash != ref.content_hash:
            raise ErrInvalidContentHash(
                f"ref {ref.ref_id}: expected {ref.content_hash}, got {actual_hash}"
            )
        return content

    # ── deterministic core ────────────────────────────────────────────────────

    def _pages_for_manifest(self, manifest: ContextManifest, *, caller_pid: str) -> list[RefPages]:
        sorted_refs = sort_refs_deterministic(manifest.refs)
        ref_pages_list: list[RefPages] = []
        for ref in sorted_refs:
            self._resolve_and_verify_ref(ref, caller_pid=caller_pid)
            pages = self._sink_page(ref=ref, manifest=manifest)
            ref_pages_list.append(RefPages(ref=ref, pages=tuple(pages)))
        return ref_pages_list

    def _materialize(
        self,
        *,
        manifest: ContextManifest,
        caller_pid: str,
        handle_id: str,
        working_set_id: str,
        selected: list[ContextPage],
        omitted_ids: list[str],
        tokens_used: int,
        bytes_used: int,
    ) -> tuple[WorkingSet, LoadedContext]:
        # derive page bindings & omitted refs
        loaded_pages: list[LoadedPage] = []
        bindings: list[VersionBinding] = []
        for page in selected:
            # lazy fetch content only once
            # content already validated above
            byte_start = page.byte_start
            byte_end = page.byte_end
            content_bytes = self._content.read_version(
                artifact_id=page.artifact_id,
                version=page.version,
                canonical_uri=page.canonical_uri,
            )[byte_start:byte_end]
            # recover media/encoding from manifest ref
            ref = next(
                (r for r in manifest.refs if r.artifact_id == page.artifact_id),
                None,
            )
            mt = ref.media_type if ref else "application/octet-stream"
            enc = ref.encoding if ref else "utf-8"

            loaded_pages.append(
                LoadedPage(
                    page_id=page.page_id,
                    canonical_uri=page.canonical_uri,
                    artifact_id=page.artifact_id,
                    version=page.version,
                    content_hash=page.content_hash,
                    page_hash=page.page_hash,
                    byte_start=byte_start,
                    byte_end=byte_end,
                    size_bytes=len(content_bytes),
                    required=page.required,
                    priority=page.priority,
                    media_type=mt,
                    encoding=enc,
                    estimated_tokens=page.estimated_tokens,
                    content=content_bytes,
                )
            )
            bindings.append(
                VersionBinding(
                    page_id=page.page_id,
                    canonical_uri=page.canonical_uri,
                    artifact_id=page.artifact_id,
                    version=page.version,
                    content_hash=page.content_hash,
                )
            )

        omitted_refs_list: list[OmittedRef] = []
        for ref in manifest.refs:
            if ref.ref_id in omitted_ids:
                # approximate the cost that would have been
                pages_that_would = self._sink_page(ref=ref, manifest=manifest)
                approx = sum(p.estimated_tokens for p in pages_that_would)
                omitted_refs_list.append(
                    OmittedRef(
                        ref_id=ref.ref_id,
                        reason="optional_skipped",
                        requested_tokens=approx,
                    )
                )
            # required refs that are in omitted_ids must not happen — selection
            # raises first.

        ws = WorkingSet(
            working_set_id=working_set_id,
            pid=caller_pid,
            manifest_id=manifest.manifest_id,
            manifest_hash=manifest.manifest_hash(),
            policy_id=manifest.policy_id,
            token_budget=manifest.token_budget,
            byte_budget=manifest.byte_budget,
            selected_page_ids=tuple(p.page_id for p in selected),
            omitted_page_ids=tuple(omitted_ids),
            tokens_used=tokens_used,
            bytes_used=bytes_used,
            state="resident",  # selection already succeeded fully
        )
        materialized = LoadedContext(
            context_id=_uuid(),
            pid=caller_pid,
            manifest_id=manifest.manifest_id,
            manifest_hash=manifest.manifest_hash(),
            working_set_id=working_set_id,
            ordered_pages=tuple(loaded_pages),
            token_budget=manifest.token_budget,
            tokens_used=tokens_used,
            byte_budget=manifest.byte_budget,
            bytes_used=bytes_used,
            omitted_refs=tuple(omitted_refs_list),
            version_bindings=tuple(bindings),
            materialized_hash="",
        )

        # materialized_hash must be deterministic — compute AFTER assembling
        # bindings. MUST also incorporate content bytes so two refs with
        # different content bytes but same schema produce different hashes.
        ordered_bindings = [vb.model_dump_json() for vb in bindings]
        ordered_content = [lp.content.hex() for lp in loaded_pages]
        materialized_hash = _deterministic_hash(ordered_bindings + ordered_content)
        # reopen model to set final hash (single-field reassign is fine for
        # our freeze-on-creation approach since spec allows mutable creation).
        object.__setattr__(materialized, "materialized_hash", materialized_hash)
        return ws, materialized

    def _find_handle_owner(self, handle_id: str) -> str | None:
        """Locate the PID that owns a handle, or None if not found anywhere."""
        for pid, hmap in self._handles_by_pid.items():
            if handle_id in hmap:
                return pid
        return None

    def _get_owning_rec(self, pid: str, handle_id: str) -> _HandleRec:
        """Return the handle record, enforcing cross-PID ownership errors.

        Raises ErrHandleNotOwned when the handle exists under another PID,
        ErrInvalidManifest when the handle is unknown to any PID.
        """
        hmap = self._handles_map(pid)
        rec = hmap.get(handle_id)
        if rec is not None:
            return rec
        # Possibly cross-PID access: surface ErrHandleNotOwned
        owner = self._find_handle_owner(handle_id)
        if owner is not None and owner != pid:
            raise ErrHandleNotOwned(
                f"handle {handle_id} belongs to {owner}, not {pid}"
            )
        raise ErrInvalidManifest(f"handle {handle_id} not found")

    def load(
        self,
        *,
        manifest: ContextManifest,
        caller_pid: str,
        idempotency_key: str = "",
    ) -> tuple[ContextHandle, LoadedContext]:
        self.validate_manifest(manifest, caller_pid=caller_pid)

        idem_key = (caller_pid, manifest.manifest_hash(), idempotency_key)
        if idempotency_key and idem_key in self._idem_load:
            handle_id = self._idem_load[idem_key]
            rec = self._handles_map(caller_pid)[handle_id]
            return (rec.handle, rec.loaded)  # type: ignore[return-value]

        # resolve + paginate
        ref_pages_list = self._pages_for_manifest(manifest, caller_pid=caller_pid)

        # select under policy
        selected, omitted_ids, tokens_used, bytes_used = select_pages_v1(
            manifest=manifest, ref_pages=ref_pages_list
        )

        ws_id = _uuid()
        handle_id = _uuid()
        ws, loaded = self._materialize(
            manifest=manifest,
            caller_pid=caller_pid,
            handle_id=handle_id,
            working_set_id=ws_id,
            selected=selected,
            omitted_ids=omitted_ids,
            tokens_used=tokens_used,
            bytes_used=bytes_used,
        )

        handle = ContextHandle(
            handle_id=handle_id,
            pid=caller_pid,
            working_set_id=ws_id,
            pinned_page_ids=(),
        )
        rec = _HandleRec(handle=handle, manifest=manifest, loaded=loaded, working_set=ws)

        # register durable state
        self._ws_map(caller_pid)[ws_id] = ws
        self._handles_map(caller_pid)[handle_id] = rec
        if idempotency_key:
            self._idem_load[idem_key] = handle_id

        self._emit(
            "CONTEXT_MANIFEST_ACCEPTED",
            manifest_id=manifest.manifest_id,
            manifest_hash=manifest.manifest_hash(),
            pid=caller_pid,
        )
        self._emit(
            "CONTEXT_LOAD_STARTED",
            handle_id=handle_id,
            working_set_id=ws_id,
        )
        for p in loaded.ordered_pages:
            self._emit(
                "CONTEXT_PAGE_MATERIALIZED",
                context_id=loaded.context_id,
                page_id=p.page_id,
                artifact_id=p.artifact_id,
                version=p.version,
            )
        for om in loaded.omitted_refs:
            self._emit(
                "CONTEXT_PAGE_OMITTED",
                context_id=loaded.context_id,
                ref_id=om.ref_id,
                reason=om.reason,
            )
        self._emit(
            "CONTEXT_WORKING_SET_RESIDENT",
            working_set_id=ws_id,
            tokens_used=loaded.tokens_used,
            bytes_used=loaded.bytes_used,
            page_count=len(loaded.ordered_pages),
            omitted_count=len(loaded.omitted_refs),
        )

        return handle, loaded

    # ── read ──────────────────────────────────────────────────────────────────

    def read(self, *, pid: str, handle_id: str) -> LoadedContext:
        rec = self._get_owning_rec(pid, handle_id)
        self._require_handle_owned(rec, pid, handle_id)
        if rec.loaded is None:
            raise ErrInvalidManifest(f"handle {handle_id} has no loaded context")
        return rec.loaded

    # ── inspect ───────────────────────────────────────────────────────────────

    def inspect(self, *, pid: str, handle_id: str) -> dict[str, Any]:
        rec = self._get_owning_rec(pid, handle_id)
        if rec.loaded is None:
            raise ErrInvalidManifest(f"handle {handle_id} not found")
        # NOTE: intentionally do NOT call _require_handle_owned here —
        # inspect must remain safe on closed handles so callers can read
        # the `closed` flag. Cross-PID access is still enforced by
        # _get_owning_rec above.
        loaded = rec.loaded
        return {
            "context_id": loaded.context_id,
            "handle_id": rec.handle.handle_id,
            "pid": loaded.pid,
            "working_set_id": loaded.working_set_id,
            "manifest_id": loaded.manifest_id,
            "manifest_hash": loaded.manifest_hash,
            "policy_id": rec.manifest.policy_id if rec.manifest else "",
            "estimator_id": self._estimator.estimator_id,
            "page_count": len(loaded.ordered_pages),
            "tokens_used": loaded.tokens_used,
            "token_budget": loaded.token_budget,
            "bytes_used": loaded.bytes_used,
            "byte_budget": loaded.byte_budget,
            "omitted_count": len(loaded.omitted_refs),
            "closed": rec.handle.closed_at is not None,
        }

    # ── pinning ───────────────────────────────────────────────────────────────

    def pin(self, *, pid: str, handle_id: str, page_ids: list[str]) -> list[str]:
        rec = self._get_owning_rec(pid, handle_id)
        self._require_handle_owned(rec, pid, handle_id)
        for pg_id in page_ids:
            self._pin_counts[pg_id] = self._pin_counts.get(pg_id, 0) + 1
        # update pinned page ids on handle
        new_pins = tuple(sorted(set(rec.handle.pinned_page_ids) | set(page_ids)))
        new_handle = rec.handle.model_copy(update={"pinned_page_ids": new_pins})
        object.__setattr__(new_handle, "pinned_page_ids", new_pins)
        self._handles_map(pid)[handle_id] = _HandleRec(
            handle=new_handle,
            manifest=rec.manifest,
            loaded=rec.loaded,
            working_set=rec.working_set,
        )
        for pg_id in page_ids:
            self._emit(
                "CONTEXT_PAGE_PINNED",
                page_id=pg_id,
                pid=pid,
                handle_id=handle_id,
            )
        return list(new_pins)

    def unpin(self, *, pid: str, handle_id: str, page_ids: list[str]) -> list[str]:
        rec = self._get_owning_rec(pid, handle_id)
        self._require_handle_owned(rec, pid, handle_id)
        current = dict(self._pin_counts)
        new_pins = set(rec.handle.pinned_page_ids)
        for pg_id in page_ids:
            if pg_id in new_pins:
                new_pins.discard(pg_id)
            cnt = current.get(pg_id, 0)
            if cnt > 0:
                current[pg_id] = cnt - 1
        # commit pin count changes
        self._pin_counts.update(current)
        new_handle = rec.handle.model_copy(update={"pinned_page_ids": tuple(sorted(new_pins))})
        object.__setattr__(new_handle, "pinned_page_ids", tuple(sorted(new_pins)))
        self._handles_map(pid)[handle_id] = _HandleRec(
            handle=new_handle,
            manifest=rec.manifest,
            loaded=rec.loaded,
            working_set=rec.working_set,
        )
        for pg_id in page_ids:
            self._emit(
                "CONTEXT_PAGE_UNPINNED",
                page_id=pg_id,
                pid=pid,
                handle_id=handle_id,
            )
        return list(sorted(new_pins))

    # ── eviction ──────────────────────────────────────────────────────────────

    def evict(
        self,
        *,
        pid: str,
        working_set_id: str,
        target_tokens: int,
    ) -> dict[str, Any]:
        ws_map = self._ws_map(pid)
        ws = ws_map.get(working_set_id)
        if ws is None:
            # cross-PID? report working set owned by another PID
            for other_pid, other_map in self._ws_by_pid.items():
                if working_set_id in other_map:
                    raise ErrHandleNotOwned(
                        f"working set {working_set_id} belongs to {other_pid}, not {pid}"
                    )
            raise ErrInvalidManifest(f"working set {working_set_id} not found")
        self._emit(
            "CONTEXT_EVICTION_STARTED",
            working_set_id=working_set_id,
            target_tokens=target_tokens,
            pid=pid,
        )

        # candidate pages: optional (not required), not pinned
        rec_for_ws = [
            rec
            for rec in self._handles_map(pid).values()
            if rec.working_set is not None and rec.working_set.working_set_id == working_set_id
        ]
        # Build a synthetic page_info lookup for the pages in this ws.
        # Since we don't retain full page state in WorkingSet post-materialization,
        # we reconstruct trivial eviction order equivalent from ordered_pages of
        # the loaded context (which already holds the page metadata).
        candidates: list[tuple[str, int, bool]] = []
        pinned_blocked: list[str] = []
        freed_tokens = 0
        for rec in rec_for_ws:
            if rec.loaded is None:
                continue
            for page in rec.loaded.ordered_pages:
                if page.required:
                    continue
                if self._pin_counts.get(page.page_id, 0) > 0:
                    pinned_blocked.append(page.page_id)
                    continue
                candidates.append((page.page_id, page.estimated_tokens, False))
        # deterministic order: lowest priority first, stable tie-break by page_id
        candidates.sort(key=lambda t: (t[1], t[0]))

        remaining_target = target_tokens
        evicted: list[str] = []
        for page_id, tokens, _ in candidates:
            if freed_tokens >= target_tokens:
                break
            evicted.append(page_id)
            freed_tokens += tokens
            remaining_target -= tokens

        self._emit(
            "CONTEXT_PAGE_EVICTED",
            page_ids=evicted,
            freed_tokens=freed_tokens,
            pid=pid,
        )
        self._emit(
            "CONTEXT_EVICTION_COMPLETED",
            working_set_id=working_set_id,
            evicted_ids=evicted,
            tokens_freed=freed_tokens,
            pid=pid,
        )
        return {
            "evicted_pages": evicted,
            "tokens_freed": freed_tokens,
            "target_tokens": target_tokens,
            "pinned_blocked": pinned_blocked,
        }

    # ── snapshot ──────────────────────────────────────────────────────────────

    def snapshot(
        self,
        *,
        pid: str,
        context_id: str,
        idempotency_key: str = "",
    ) -> ContextSnapshot:
        if idempotency_key:
            idem = (pid, context_id, idempotency_key)
            if idem in self._idem_snapshot:
                snap_id = self._idem_snapshot[idem]
                return self._snaps[snap_id]

        # locate handle
        for rec in self._handles_map(pid).values():
            if rec.loaded is None:
                continue
            if rec.loaded.context_id != context_id:
                continue
            loaded = rec.loaded
            bindings = tuple(
                PageBinding(
                    page_id=vb.page_id,
                    canonical_uri=vb.canonical_uri,
                    artifact_id=vb.artifact_id,
                    version=vb.version,
                    content_hash=vb.content_hash,
                    page_hash=next(
                        (p.page_hash for p in loaded.ordered_pages if p.page_id == vb.page_id),
                        "",
                    ),
                    byte_start=next(
                        (p.byte_start for p in loaded.ordered_pages if p.page_id == vb.page_id),
                        0,
                    ),
                    byte_end=next(
                        (p.byte_end for p in loaded.ordered_pages if p.page_id == vb.page_id),
                        0,
                    ),
                )
                for vb in loaded.version_bindings
            )
            snap = ContextSnapshot(
                pid=pid,
                manifest_hash=loaded.manifest_hash,
                working_set_hash="",  # placeholder filled after ws meta
                materialized_hash=loaded.materialized_hash,
                policy_id=next(
                    (rec.manifest.policy_id for m in [rec] if rec.manifest),
                    "",
                ),
                estimator_id=self._estimator.estimator_id,
                page_bindings=bindings,
                tokens_used=loaded.tokens_used,
                bytes_used=loaded.bytes_used,
            )
            ws_hash = _deterministic_hash([loaded.working_set_id, str(len(loaded.ordered_pages))])
            snap = snap.model_copy(update={"working_set_hash": ws_hash})
            self._snaps[snap.snapshot_id] = snap
            if idempotency_key:
                self._idem_snapshot[(pid, context_id, idempotency_key)] = snap.snapshot_id
            self._emit(
                "CONTEXT_SNAPSHOT_CREATED",
                snapshot_id=snap.snapshot_id,
                context_id=context_id,
                pid=pid,
            )
            return snap
        raise ErrInvalidManifest(f"context {context_id} not found for pid {pid}")

    # ── restore snapshot ──────────────────────────────────────────────────────

    def restore_snapshot(
        self,
        *,
        pid: str,
        snapshot_id: str,
        idempotency_key: str = "",
    ) -> tuple[ContextHandle, LoadedContext]:
        if idempotency_key:
            idem = (pid, snapshot_id, idempotency_key)
            if idem in self._idem_restore:
                handle_id = self._idem_restore[idem]
                rec = self._handles_map(pid)[handle_id]
                return (rec.handle, rec.loaded)  # type: ignore[return-value]
        snap = self._snaps.get(snapshot_id)
        if snap is None:
            raise ErrSnapshotCorrupt(f"snapshot {snapshot_id} not found")
        if snap.pid != pid:
            raise ErrCapabilityDenied(
                f"snapshot {snapshot_id} owned by {snap.pid}, not {pid}"
            )
        # re-verify each materialized binding against ArtifactVersion
        for b in snap.page_bindings:
            try:
                content = self._content.read_version(
                    artifact_id=b.artifact_id,
                    version=b.version,
                    canonical_uri=b.canonical_uri,
                )
            except FileNotFoundError as e:
                raise ErrSnapshotCorrupt(
                    f"snapshot {snapshot_id}: artifact {b.artifact_id} v{b.version} missing"
                ) from e
            actual = _content_hash_for(content)
            if actual != b.content_hash:
                raise ErrSnapshotCorrupt(f"snapshot {snapshot_id}: page {b.page_id} hash mismatch")
            # page hash from re-split should match
            chunk = content[b.byte_start : b.byte_end]
            if _content_hash_for(chunk) != b.page_hash:
                raise ErrSnapshotCorrupt(
                    f"snapshot {snapshot_id}: page {b.page_id} page-hash mismatch"
                )

        # build a LoadedContext from snapshot bindings
        loaded_pages = []
        bindings = []
        for b in snap.page_bindings:
            content_bytes = self._content.read_version(
                artifact_id=b.artifact_id,
                version=b.version,
                canonical_uri=b.canonical_uri,
            )[b.byte_start : b.byte_end]
            loaded_pages.append(
                LoadedPage(
                    page_id=b.page_id,
                    canonical_uri=b.canonical_uri,
                    artifact_id=b.artifact_id,
                    version=b.version,
                    content_hash=b.content_hash,
                    page_hash=b.page_hash,
                    byte_start=b.byte_start,
                    byte_end=b.byte_end,
                    size_bytes=len(content_bytes),
                    required=False,
                    priority=0,
                    media_type="application/octet-stream",
                    encoding="utf-8",
                    estimated_tokens=0,
                    content=content_bytes,
                )
            )
            bindings.append(
                VersionBinding(
                    page_id=b.page_id,
                    canonical_uri=b.canonical_uri,
                    artifact_id=b.artifact_id,
                    version=b.version,
                    content_hash=b.content_hash,
                )
            )
        ordered_json = [vb.model_dump_json() for vb in bindings]
        ordered_content = [lp.content.hex() for lp in loaded_pages]
        restored_hash = _deterministic_hash(ordered_json + ordered_content)
        if restored_hash != snap.materialized_hash:
            raise ErrSnapshotCorrupt(
                f"snapshot {snapshot_id}: re-materialized hash {restored_hash} "
                f"!= recorded {snap.materialized_hash}"
            )

        ws_id = _uuid()
        handle_id = _uuid()
        handle = ContextHandle(
            handle_id=handle_id,
            pid=pid,
            working_set_id=ws_id,
            pinned_page_ids=(),
        )
        loaded = LoadedContext(
            context_id=_uuid(),
            pid=pid,
            manifest_id="",
            manifest_hash=snap.manifest_hash,
            working_set_id=ws_id,
            ordered_pages=tuple(loaded_pages),
            token_budget=snap.tokens_used,
            tokens_used=snap.tokens_used,
            byte_budget=snap.bytes_used,
            bytes_used=snap.bytes_used,
            omitted_refs=(),
            version_bindings=tuple(bindings),
            materialized_hash=snap.materialized_hash,
        )
        ws = WorkingSet(
            working_set_id=ws_id,
            pid=pid,
            manifest_id="",
            manifest_hash=snap.manifest_hash,
            policy_id=snap.policy_id,
            token_budget=snap.tokens_used,
            byte_budget=snap.bytes_used,
            selected_page_ids=tuple(b.page_id for b in snap.page_bindings),
            omitted_page_ids=(),
            tokens_used=snap.tokens_used,
            bytes_used=snap.bytes_used,
            state="resident",
        )
        self._ws_map(pid)[ws_id] = ws
        self._handles_map(pid)[handle_id] = _HandleRec(handle=handle, loaded=loaded, working_set=ws)
        if idempotency_key:
            self._idem_restore[(pid, snapshot_id, idempotency_key)] = handle_id
        self._emit(
            "CONTEXT_SNAPSHOT_RESTORED",
            snapshot_id=snapshot_id,
            handle_id=handle_id,
        )
        return handle, loaded

    # ── close ─────────────────────────────────────────────────────────────────

    def close(
        self,
        *,
        pid: str,
        handle_id: str,
        idempotency_key: str = "",
    ) -> bool:
        rec = self._handles_map(pid).get(handle_id)
        if rec is None:
            # idempotent close on already-closed/nonexistent returns True
            return True
        if rec.handle.closed_at is not None:
            return True
        self._require_handle_owned(rec, pid, handle_id)
        # release pins
        for pg_id in rec.handle.pinned_page_ids:
            cnt = self._pin_counts.get(pg_id, 0)
            if cnt > 0:
                self._pin_counts[pg_id] = cnt - 1
        closed = rec.handle.model_copy(update={"closed_at": _utcnow()})
        object.__setattr__(closed, "closed_at", _utcnow())
        self._handles_map(pid)[handle_id] = _HandleRec(
            handle=closed,
            manifest=rec.manifest,
            loaded=rec.loaded,
            working_set=rec.working_set,
        )
        self._emit(
            "CONTEXT_HANDLE_CLOSED",
            handle_id=handle_id,
            pid=pid,
            working_set_id=rec.handle.working_set_id,
        )
        return True

    # ── list ──────────────────────────────────────────────────────────────────

    def list_working_sets(self, *, pid: str) -> list[WorkingSet]:
        return [ws for ws in self._ws_map(pid).values()]

    # ── process termination cleanup ───────────────────────────────────────────

    def cleanup_process(self, pid: str) -> dict[str, int]:
        handles = list(self._handles_map(pid).values())
        released_handles = 0
        released_pins = 0
        for rec in handles:
            if rec.handle.closed_at is None:
                for pg_id in rec.handle.pinned_page_ids:
                    cnt = self._pin_counts.get(pg_id, 0)
                    if cnt > 0:
                        self._pin_counts[pg_id] = cnt - 1
                        released_pins += 1
                closed = rec.handle.model_copy(update={"closed_at": _utcnow()})
                # mutate via fields
                new_rec = _HandleRec(
                    handle=closed,
                    manifest=rec.manifest,
                    loaded=rec.loaded,
                    working_set=rec.working_set,
                )
                self._handles_map(pid)[rec.handle.handle_id] = new_rec
                released_handles += 1
        return {"released_handles": released_handles, "released_pins": released_pins}
