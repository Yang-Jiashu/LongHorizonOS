"""Bounded automatic Context refresh for stale cognition.

This module is intentionally a small, pure control-plane bridge.  It turns a
failed read-set freshness check into a deterministic ``REBASE`` or
``FULL_RELOAD`` decision that a caller-owned execution loop may use for a
*fresh* Attempt.  It does not mutate VPG, Claims, Leases, Harness sessions, or
the ContextService.

The planner only trusts identities that are explicit in the stale
``AgentSnapshot`` and the task's ``ContextManifest``.  Missing versions,
hashes, hidden reads, or a changed resource that is not represented by the
manifest fail closed.
"""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from lhos.agent_os.context.models import ContextManifest, LoadedContext
from lhos.runtimes.multi_agent.models import AgentSnapshot, ResourceBinding

from .context_delta import (
    ContextGraphChange,
    ContextGraphDelta,
    ContextRebaseAction,
    RebasePlan,
    plan_context_rebase,
)

AUTOMATIC_REBASE_SCHEMA_VERSION: Final[Literal["automatic-rebase.v1"]] = "automatic-rebase.v1"
AUTOMATIC_REBASE_POLICY_ID: Final[str] = "fresh-attempt-context-rebase.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class AutomaticRebaseStatus(StrEnum):
    """Whether a fresh-attempt repair decision is usable."""

    PLANNED = "planned"
    BLOCKED = "blocked"


class AutomaticRebaseRef(_FrozenModel):
    """One version/hash refresh for an explicit ContextManifest ref."""

    ref_id: str = Field(min_length=1)
    canonical_uri: str = ""
    artifact_id: str = Field(min_length=1)
    old_version: StrictInt = Field(ge=1)
    new_version: StrictInt = Field(ge=1)
    old_content_hash: str = Field(min_length=1)
    new_content_hash: str = Field(min_length=1)

    @field_validator(
        "ref_id",
        "canonical_uri",
        "artifact_id",
        "old_content_hash",
        "new_content_hash",
        mode="before",
    )
    @classmethod
    def _trim(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("old_content_hash", "new_content_hash")
    @classmethod
    def _lower_hash(cls, value: str) -> str:
        return value.lower()


class AutomaticRebaseDecision(_FrozenModel):
    """Auditable decision consumed by the bounded ``run_async`` retry."""

    schema_version: Literal["automatic-rebase.v1"] = AUTOMATIC_REBASE_SCHEMA_VERSION
    policy_id: str = AUTOMATIC_REBASE_POLICY_ID
    status: AutomaticRebaseStatus
    graph_id: str = Field(min_length=1)
    graph_version: StrictInt = Field(ge=0)
    task_id: str = Field(min_length=1)
    source_claim_id: str = ""
    source_attempt_id: str = ""
    source_context_snapshot_id: str = ""
    action: ContextRebaseAction
    plan: RebasePlan | None = None
    changed_refs: tuple[AutomaticRebaseRef, ...] = ()
    delta_ref_ids: tuple[str, ...] = ()
    preserve_ref_ids: tuple[str, ...] = ()
    reload_ref_ids: tuple[str, ...] = ()
    replacement_manifest_id: str = ""
    replacement_manifest_hash: str = ""
    reason: str = ""
    decision_hash: str = Field(min_length=64, max_length=64)

    @property
    def redispatchable(self) -> bool:
        return (
            self.status is AutomaticRebaseStatus.PLANNED
            and self.action
            in {
                ContextRebaseAction.REBASE,
                ContextRebaseAction.FULL_RELOAD,
            }
            and self.plan is not None
            and bool(self.replacement_manifest_id)
        )

    @property
    def blocked(self) -> bool:
        return not self.redispatchable

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class AutomaticContextDelta(_FrozenModel):
    """Bounded projection of the changed pages exposed to a fresh callback."""

    schema_version: Literal["automatic-rebase.v1"] = AUTOMATIC_REBASE_SCHEMA_VERSION
    decision_hash: str = Field(min_length=64, max_length=64)
    action: ContextRebaseAction
    source_context_snapshot_id: str = ""
    ref_ids: tuple[str, ...] = ()
    artifact_ids: tuple[str, ...] = ()
    page_ids: tuple[str, ...] = ()
    tokens_used: StrictInt = Field(ge=0)
    bytes_used: StrictInt = Field(ge=0)

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _hash_payload(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _artifact_id(binding: Any) -> str:
    value = str(getattr(binding, "artifact_id", "") or "").strip()
    if value:
        return value.removeprefix("vpg://")
    # Context/provenance adapters may carry only a canonical VPG URI.  Derive
    # an artifact identity only for that authoritative scheme; arbitrary
    # external URIs remain fail-closed rather than being guessed.
    uri = str(getattr(binding, "resource_uri", "") or "").strip()
    if uri.startswith("vpg://"):
        return uri.removeprefix("vpg://").strip("/")
    return ""


def _uri(binding: Any) -> str:
    return str(
        getattr(binding, "resource_uri", "") or getattr(binding, "canonical_uri", "") or ""
    ).strip()


def _hash_for(facts: Any, artifact_id: str, uri: str, version: int) -> str | None:
    reader = getattr(facts, "read_hash", None)
    if not callable(reader):
        return None
    for candidate in (uri, f"vpg://{artifact_id}", artifact_id):
        if not candidate:
            continue
        try:
            value = reader("automatic-rebase", candidate, version)
        except Exception:
            value = None
        if value:
            return str(value).strip().lower()
    return None


def _blocked(
    *,
    graph_id: str,
    graph_version: int,
    task_id: str,
    source_claim_id: str,
    source_attempt_id: str,
    source_context_snapshot_id: str,
    reason: str,
    action: ContextRebaseAction = ContextRebaseAction.BLOCKED,
    plan: RebasePlan | None = None,
    changed_refs: tuple[AutomaticRebaseRef, ...] = (),
) -> AutomaticRebaseDecision:
    payload = {
        "schema_version": AUTOMATIC_REBASE_SCHEMA_VERSION,
        "policy_id": AUTOMATIC_REBASE_POLICY_ID,
        "status": AutomaticRebaseStatus.BLOCKED.value,
        "graph_id": graph_id,
        "graph_version": graph_version,
        "task_id": task_id,
        "source_claim_id": source_claim_id,
        "source_attempt_id": source_attempt_id,
        "source_context_snapshot_id": source_context_snapshot_id,
        "action": action.value,
        "reason": reason,
        "changed_refs": [item.model_dump(mode="json") for item in changed_refs],
        "plan_hash": "" if plan is None else plan.plan_hash,
    }
    return AutomaticRebaseDecision(
        status=AutomaticRebaseStatus.BLOCKED,
        graph_id=graph_id,
        graph_version=graph_version,
        task_id=task_id,
        source_claim_id=source_claim_id,
        source_attempt_id=source_attempt_id,
        source_context_snapshot_id=source_context_snapshot_id,
        action=action,
        plan=plan,
        changed_refs=changed_refs,
        reason=reason,
        decision_hash=_hash_payload(payload),
    )


def plan_automatic_rebase(
    *,
    task_id: str,
    graph_id: str,
    graph_version: int,
    agent_snapshot: AgentSnapshot | None,
    context_manifest: ContextManifest | None,
    facts: Any,
    claim_id: str = "",
    attempt_id: str = "",
    reason: str = "",
) -> tuple[AutomaticRebaseDecision, ContextManifest | None]:
    """Plan a deterministic fresh-attempt Context refresh.

    The returned manifest is a template.  ``AgentOS`` binds its owner PID to
    the replacement Attempt immediately before Context VM materialization.
    ``None`` means the decision is blocked and no retry may be dispatched.
    """

    gid = str(graph_id).strip()
    tid = str(task_id).strip()
    if not gid or not tid:
        raise ValueError("graph_id and task_id must be non-empty")
    if isinstance(graph_version, bool) or not isinstance(graph_version, int) or graph_version < 0:
        raise ValueError("graph_version must be a non-negative integer")
    if agent_snapshot is None:
        return (
            _blocked(
                graph_id=gid,
                graph_version=graph_version,
                task_id=tid,
                source_claim_id=str(claim_id),
                source_attempt_id=str(attempt_id),
                source_context_snapshot_id="",
                reason="stale cognition has no durable AgentSnapshot",
            ),
            None,
        )
    snapshot_id = str(
        getattr(getattr(agent_snapshot, "context_identity", None), "snapshot_id", "") or ""
    )
    if context_manifest is None:
        return (
            _blocked(
                graph_id=gid,
                graph_version=graph_version,
                task_id=tid,
                source_claim_id=str(claim_id),
                source_attempt_id=str(attempt_id),
                source_context_snapshot_id=snapshot_id,
                reason="automatic rebase requires an explicit ContextManifest",
            ),
            None,
        )

    refs_by_artifact: dict[str, list[Any]] = {}
    refs_by_uri: dict[str, list[Any]] = {}
    for ref in context_manifest.refs:
        refs_by_artifact.setdefault(str(ref.artifact_id).strip(), []).append(ref)
        refs_by_uri.setdefault(str(ref.canonical_uri).strip(), []).append(ref)

    # First establish that every observed stale read is both identifiable and
    # represented by the explicit manifest.  A hidden/manual read cannot be
    # repaired safely by refreshing the manifest.
    stale_bindings: list[ResourceBinding] = []
    for binding in agent_snapshot.read_set:
        if not bool(getattr(binding, "known", True)):
            return (
                _blocked(
                    graph_id=gid,
                    graph_version=graph_version,
                    task_id=tid,
                    source_claim_id=str(claim_id),
                    source_attempt_id=str(attempt_id),
                    source_context_snapshot_id=snapshot_id,
                    reason="automatic rebase blocked by an unknown read binding",
                ),
                None,
            )
        artifact = _artifact_id(binding)
        uri = _uri(binding)
        version = getattr(binding, "version", None)
        content_hash = str(getattr(binding, "content_hash", "") or "").strip().lower()
        if not artifact or not isinstance(version, int) or version < 1 or not content_hash:
            return (
                _blocked(
                    graph_id=gid,
                    graph_version=graph_version,
                    task_id=tid,
                    source_claim_id=str(claim_id),
                    source_attempt_id=str(attempt_id),
                    source_context_snapshot_id=snapshot_id,
                    reason="automatic rebase blocked by an unversioned read binding",
                ),
                None,
            )
        latest = getattr(facts, "latest", lambda _value: None)(artifact)
        current_hash = None if latest is None else _hash_for(facts, artifact, uri, int(latest))
        if latest is None or current_hash is None:
            return (
                _blocked(
                    graph_id=gid,
                    graph_version=graph_version,
                    task_id=tid,
                    source_claim_id=str(claim_id),
                    source_attempt_id=str(attempt_id),
                    source_context_snapshot_id=snapshot_id,
                    reason=f"automatic rebase cannot prove current identity for {artifact}",
                ),
                None,
            )
        if int(latest) != version or current_hash != content_hash:
            matches = refs_by_artifact.get(artifact, ()) or refs_by_uri.get(uri, ())
            if len(matches) != 1:
                return (
                    _blocked(
                        graph_id=gid,
                        graph_version=graph_version,
                        task_id=tid,
                        source_claim_id=str(claim_id),
                        source_attempt_id=str(attempt_id),
                        source_context_snapshot_id=snapshot_id,
                        reason=(
                            f"stale read {artifact} is not represented by exactly "
                            "one ContextManifest ref"
                        ),
                    ),
                    None,
                )
            stale_bindings.append(binding)

    # Refresh every manifest ref whose authoritative version/hash changed.
    # This keeps the replacement context self-consistent even when an optional
    # ref was not selected into the old working set.
    updates: dict[str, AutomaticRebaseRef] = {}
    for ref in context_manifest.refs:
        artifact = str(ref.artifact_id).strip()
        latest = getattr(facts, "latest", lambda _value: None)(artifact)
        if latest is None:
            return (
                _blocked(
                    graph_id=gid,
                    graph_version=graph_version,
                    task_id=tid,
                    source_claim_id=str(claim_id),
                    source_attempt_id=str(attempt_id),
                    source_context_snapshot_id=snapshot_id,
                    reason=f"automatic rebase cannot find manifest artifact {artifact}",
                ),
                None,
            )
        current_hash = _hash_for(
            facts,
            artifact,
            str(ref.canonical_uri),
            int(latest),
        )
        if current_hash is None:
            return (
                _blocked(
                    graph_id=gid,
                    graph_version=graph_version,
                    task_id=tid,
                    source_claim_id=str(claim_id),
                    source_attempt_id=str(attempt_id),
                    source_context_snapshot_id=snapshot_id,
                    reason=f"automatic rebase cannot hash manifest artifact {artifact}",
                ),
                None,
            )
        if int(latest) != int(ref.version) or current_hash != str(ref.content_hash).lower():
            updates[ref.ref_id] = AutomaticRebaseRef(
                ref_id=ref.ref_id,
                canonical_uri=ref.canonical_uri,
                artifact_id=artifact,
                old_version=int(ref.version),
                new_version=int(latest),
                old_content_hash=str(ref.content_hash).lower(),
                new_content_hash=current_hash,
            )

    # A stale commit with no manifest refresh is evidence of a race or an
    # unsupported read path.  Do not silently retry the same cognition.
    if not updates:
        return (
            _blocked(
                graph_id=gid,
                graph_version=graph_version,
                task_id=tid,
                source_claim_id=str(claim_id),
                source_attempt_id=str(attempt_id),
                source_context_snapshot_id=snapshot_id,
                reason=reason or "stale cognition has no refreshable manifest delta",
            ),
            None,
        )

    # Canonicalize the changed-ref projection before hashing the decision.
    # ContextManifest callers may provide refs in a different tuple order,
    # but that order is not semantic identity.  Without this normalization,
    # equivalent planner inputs would receive different decision hashes and
    # replacement manifest ids, defeating idempotent audit/replay.
    ordered_updates = tuple(
        sorted(
            updates.values(),
            key=lambda item: (
                item.ref_id,
                item.artifact_id,
                item.canonical_uri,
                item.old_version,
                item.new_version,
                item.old_content_hash,
                item.new_content_hash,
            ),
        )
    )
    changes = tuple(
        ContextGraphChange(
            ref_id=item.ref_id,
            canonical_uri=item.canonical_uri,
            artifact_id=item.artifact_id,
            old_version=item.old_version,
            new_version=item.new_version,
            old_content_hash=item.old_content_hash,
            new_content_hash=item.new_content_hash,
        )
        for item in ordered_updates
    )
    delta = ContextGraphDelta(
        graph_id=gid,
        changes=changes,
        coverage="complete",
        reason="automatic facts refresh after stale cognition",
    )
    plan = plan_context_rebase(
        graph_version,
        graph_version,
        old_context_manifest=context_manifest,
        graph_delta=delta,
    )
    if plan.blocked:
        return (
            _blocked(
                graph_id=gid,
                graph_version=graph_version,
                task_id=tid,
                source_claim_id=str(claim_id),
                source_attempt_id=str(attempt_id),
                source_context_snapshot_id=snapshot_id,
                reason=plan.reason or "Context planner blocked automatic rebase",
                plan=plan,
                changed_refs=ordered_updates,
            ),
            None,
        )

    payload = {
        "schema_version": AUTOMATIC_REBASE_SCHEMA_VERSION,
        "policy_id": AUTOMATIC_REBASE_POLICY_ID,
        "graph_id": gid,
        "graph_version": graph_version,
        "task_id": tid,
        "source_claim_id": str(claim_id),
        "source_attempt_id": str(attempt_id),
        "source_context_snapshot_id": snapshot_id,
        "action": plan.action.value,
        "plan_hash": plan.plan_hash,
        "changed_refs": [item.model_dump(mode="json") for item in ordered_updates],
    }
    decision_hash = _hash_payload(payload)
    replacement_id = f"{context_manifest.manifest_id}:auto:{decision_hash[:16]}"
    replacement_refs = tuple(
        ref.model_copy(
            update={
                "version": updates[ref.ref_id].new_version,
                "content_hash": updates[ref.ref_id].new_content_hash,
            }
        )
        if ref.ref_id in updates
        else ref
        for ref in context_manifest.refs
    )
    replacement = context_manifest.model_copy(
        update={
            "manifest_id": replacement_id,
            "refs": replacement_refs,
            "metadata": {
                **dict(context_manifest.metadata),
                "automatic_rebase": {
                    "decision_hash": decision_hash,
                    "action": plan.action.value,
                    "source_context_snapshot_id": snapshot_id,
                },
            },
        }
    )
    decision = AutomaticRebaseDecision(
        status=AutomaticRebaseStatus.PLANNED,
        graph_id=gid,
        graph_version=graph_version,
        task_id=tid,
        source_claim_id=str(claim_id),
        source_attempt_id=str(attempt_id),
        source_context_snapshot_id=snapshot_id,
        action=plan.action,
        plan=plan,
        changed_refs=ordered_updates,
        delta_ref_ids=tuple(plan.reload_ref_ids),
        preserve_ref_ids=tuple(plan.preserve_ref_ids),
        reload_ref_ids=tuple(plan.reload_ref_ids),
        replacement_manifest_id=replacement_id,
        replacement_manifest_hash=replacement.manifest_hash(),
        reason=plan.reason,
        decision_hash=decision_hash,
    )
    return decision, replacement


def delta_view_for_loaded_context(
    loaded: LoadedContext,
    decision: AutomaticRebaseDecision,
) -> AutomaticContextDelta:
    """Project changed pages without serializing their content into the audit."""

    changed_artifacts = {item.artifact_id for item in decision.changed_refs}
    changed_refs = {item.ref_id for item in decision.changed_refs}
    pages = tuple(
        page
        for page in loaded.ordered_pages
        if page.artifact_id in changed_artifacts or page.canonical_uri in changed_refs
    )
    return AutomaticContextDelta(
        decision_hash=decision.decision_hash,
        action=decision.action,
        source_context_snapshot_id=decision.source_context_snapshot_id,
        ref_ids=tuple(item.ref_id for item in decision.changed_refs),
        artifact_ids=tuple(sorted(changed_artifacts)),
        page_ids=tuple(page.page_id for page in pages),
        tokens_used=sum(int(page.estimated_tokens) for page in pages),
        bytes_used=sum(int(page.size_bytes) for page in pages),
    )


__all__ = [
    "AUTOMATIC_REBASE_POLICY_ID",
    "AUTOMATIC_REBASE_SCHEMA_VERSION",
    "AutomaticContextDelta",
    "AutomaticRebaseDecision",
    "AutomaticRebaseRef",
    "AutomaticRebaseStatus",
    "delta_view_for_loaded_context",
    "plan_automatic_rebase",
]
