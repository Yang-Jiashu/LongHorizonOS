"""Bounded Context VM delta/rebase planning.

This module is deliberately a *pure control-plane primitive*.  It compares a
version-pinned Context VM working set with an explicitly supplied graph delta
and returns an immutable plan describing what can be retained and what must be
reloaded.  It does not inspect the world, discover hidden dependencies,
materialize pages, mutate a ContextService, or interrupt an Agent.

The boundary is intentionally conservative:

* known bindings are matched only by explicit ref/URI/artifact identities;
* an unknown graph delta or an unknown binding cannot receive a validity
  guarantee;
* a graph-version change with no declared affected identity is a no-op for
  exact bindings (the caller has supplied no evidence that those refs changed).

This is the bounded ``Graph Delta -> Context Planner`` seam described in the
Mind-VLA design notes.  A future runtime may feed its output to a Context VM
adapter, but the SDK execution path is not changed here.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Final, Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
    model_validator,
)

from lhos.agent_os.context.models import ContentRef, ContextManifest

CONTEXT_DELTA_SCHEMA_VERSION: Final[Literal["context-delta.v1"]] = "context-delta.v1"
CONTEXT_REBASE_POLICY_ID: Final[str] = "explicit-context-rebase.v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ContextBindingRef(_FrozenModel):
    """A small immutable identity for one context/read binding.

    ``ref_id`` is the stable manifest identity when one exists.  Bindings
    projected from a provenance ``ResourceBinding`` may not have a manifest
    id; in that case the planner derives one deterministically from URI or
    artifact identity.
    """

    ref_id: str = Field(min_length=1)
    canonical_uri: str = ""
    artifact_id: str | None = None
    version: StrictInt | None = Field(default=None, ge=0)
    content_hash: str | None = None
    required: StrictBool = False
    known: StrictBool = True

    @field_validator("ref_id", "canonical_uri", mode="before")
    @classmethod
    def _text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("artifact_id", "content_hash", mode="before")
    @classmethod
    def _optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("content_hash")
    @classmethod
    def _lower_hash(cls, value: str | None) -> str | None:
        return value.lower() if value else None

    @field_validator("version")
    @classmethod
    def _real_version(cls, value: int | None) -> int | None:
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise TypeError("binding version must be an integer")
        return value

    @property
    def identity_key(self) -> tuple[str, str, str, int | None, str]:
        return (
            self.ref_id,
            self.canonical_uri,
            self.artifact_id or "",
            self.version,
            self.content_hash or "",
        )

    @property
    def resource_keys(self) -> tuple[str, ...]:
        """Exact keys accepted by the explicit graph-delta matcher."""

        values = [self.canonical_uri]
        if self.artifact_id:
            values.append(self.artifact_id)
        if self.version is not None and self.artifact_id:
            values.append(f"{self.artifact_id}@{self.version}")
        return tuple(sorted({value for value in values if value}))

    @classmethod
    def from_any(cls, value: Any, *, index: int = 0) -> ContextBindingRef:
        """Coerce a ContentRef/VersionBinding/ResourceBinding-like value."""

        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                raise ValueError("context binding string must be non-empty")
            return cls(ref_id=normalized, canonical_uri=normalized)
        if isinstance(value, ContentRef):
            return cls(
                ref_id=value.ref_id,
                canonical_uri=value.canonical_uri,
                artifact_id=value.artifact_id,
                version=value.version,
                content_hash=value.content_hash,
                required=value.required,
                known=True,
            )
        if isinstance(value, Mapping):
            data = dict(value)
        else:
            data = {
                "ref_id": getattr(value, "ref_id", None),
                "canonical_uri": getattr(
                    value,
                    "canonical_uri",
                    getattr(value, "resource_uri", None),
                ),
                "artifact_id": getattr(value, "artifact_id", None),
                "version": getattr(value, "version", None),
                "content_hash": getattr(value, "content_hash", None),
                "required": getattr(value, "required", False),
                "known": getattr(value, "known", True),
            }
        data.setdefault("canonical_uri", data.get("resource_uri", data.get("uri", "")))
        data.setdefault("artifact_id", data.get("artifact"))
        data.setdefault("content_hash", data.get("hash"))
        data.setdefault("version", data.get("artifact_version"))
        data.setdefault("required", data.get("is_required", False))
        data.setdefault("known", data.get("authoritative", True))
        for alias in (
            "resource_uri",
            "uri",
            "artifact",
            "hash",
            "artifact_version",
            "is_required",
            "authoritative",
        ):
            data.pop(alias, None)
        ref_id = str(data.get("ref_id") or "").strip()
        if not ref_id:
            ref_id = str(data.get("canonical_uri") or data.get("artifact_id") or "").strip()
        if not ref_id:
            ref_id = f"binding-{index}"
        data["ref_id"] = ref_id
        return cls.model_validate(data)


class ContextGraphChange(_FrozenModel):
    """One explicitly observed graph/resource change.

    ``old_*`` and ``new_*`` are optional because callers may only know that a
    resource changed.  If a ``new_*`` identity is supplied and the old context
    already carries that exact identity, the binding is considered current and
    is retained.  This makes repeated planning idempotent.
    """

    ref_id: str | None = None
    canonical_uri: str | None = None
    resource_uri: str | None = None
    artifact_id: str | None = None
    old_version: StrictInt | None = Field(default=None, ge=0)
    new_version: StrictInt | None = Field(default=None, ge=0)
    old_content_hash: str | None = None
    new_content_hash: str | None = None
    known: StrictBool = True

    @field_validator(
        "ref_id",
        "canonical_uri",
        "resource_uri",
        "artifact_id",
        "old_content_hash",
        "new_content_hash",
        mode="before",
    )
    @classmethod
    def _optional_text(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("old_content_hash", "new_content_hash")
    @classmethod
    def _lower_hash(cls, value: str | None) -> str | None:
        return value.lower() if value else None

    @property
    def uri(self) -> str | None:
        return self.canonical_uri or self.resource_uri

    @property
    def resource_keys(self) -> tuple[str, ...]:
        values = [self.uri, self.artifact_id]
        if self.artifact_id and self.new_version is not None:
            values.append(f"{self.artifact_id}@{self.new_version}")
        if self.artifact_id and self.old_version is not None:
            values.append(f"{self.artifact_id}@{self.old_version}")
        return tuple(sorted({value for value in values if value}))

    @model_validator(mode="after")
    def _has_target_identity(self) -> ContextGraphChange:
        if not any((self.ref_id, self.uri, self.artifact_id)):
            raise ValueError("graph change requires a ref, resource URI, or artifact identity")
        if (
            self.old_version is not None
            and self.new_version is not None
            and self.new_version < self.old_version
        ):
            raise ValueError("graph change new_version cannot be older than old_version")
        return self

    @classmethod
    def from_any(cls, value: Any) -> ContextGraphChange:
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data = dict(value)
        else:
            data = {
                name: getattr(value, name, None)
                for name in (
                    "ref_id",
                    "canonical_uri",
                    "resource_uri",
                    "artifact_id",
                    "old_version",
                    "new_version",
                    "old_content_hash",
                    "new_content_hash",
                    "known",
                )
            }
        # Friendly aliases used by watcher/provider integrations.
        aliases = {
            "uri": "canonical_uri",
            "resource": "resource_uri",
            "version": "new_version",
            "content_hash": "new_content_hash",
            "old_hash": "old_content_hash",
            "new_hash": "new_content_hash",
        }
        for source, target in aliases.items():
            if target not in data and source in data:
                data[target] = data[source]
            data.pop(source, None)
        return cls.model_validate(data)


class ContextGraphDelta(_FrozenModel):
    """Explicit, bounded graph delta consumed by the context planner."""

    graph_id: str | None = None
    changed_ref_ids: tuple[str, ...] = ()
    changed_resource_keys: tuple[str, ...] = ()
    changed_artifact_ids: tuple[str, ...] = ()
    changes: tuple[ContextGraphChange, ...] = ()
    # ``known=False`` means the producer observed a graph change but cannot
    # enumerate its affected identities.  The planner then invalidates every
    # old binding rather than making an unsafe reuse claim.
    known: StrictBool = True
    coverage: Literal["complete", "partial", "unknown"] = "partial"
    reason: str = ""

    @field_validator(
        "changed_ref_ids",
        "changed_resource_keys",
        "changed_artifact_ids",
        mode="before",
    )
    @classmethod
    def _normalize_ids(cls, value: Any) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            value = (value,)
        return tuple(sorted({str(item).strip() for item in value if str(item).strip()}))

    @field_validator("changes", mode="before")
    @classmethod
    def _normalize_changes(cls, value: Any) -> tuple[ContextGraphChange, ...]:
        if value is None:
            return ()
        if isinstance(value, (Mapping, ContextGraphChange)):
            value = (value,)
        changes = {ContextGraphChange.from_any(item) for item in value}
        return tuple(
            sorted(
                changes,
                key=lambda item: (
                    item.ref_id or "",
                    item.uri or "",
                    item.artifact_id or "",
                    -1 if item.old_version is None else item.old_version,
                    -1 if item.new_version is None else item.new_version,
                    item.old_content_hash or "",
                    item.new_content_hash or "",
                    item.known,
                ),
            )
        )

    @classmethod
    def from_any(cls, value: Any) -> ContextGraphDelta:
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data = dict(value)
            if "changes" not in data:
                raw = data.get("changed_refs", data.get("changed_bindings"))
                if raw is not None:
                    data["changes"] = raw
            data.pop("changed_refs", None)
            data.pop("changed_bindings", None)
            if "changed_resource_keys" not in data:
                data["changed_resource_keys"] = data.get(
                    "changed_resources",
                    data.get("resource_keys", ()),
                )
            data.pop("changed_resources", None)
            data.pop("resource_keys", None)
            if "changed_artifact_ids" not in data:
                data["changed_artifact_ids"] = data.get("changed_artifacts", ())
            data.pop("changed_artifacts", None)
            if "coverage" not in data and data.get("unknown") is True:
                data["coverage"] = "unknown"
                data["known"] = False
            data.pop("unknown", None)
            return cls.model_validate(data)
        if isinstance(value, (str, bytes)):
            normalized = value.decode() if isinstance(value, bytes) else value
            return cls(changed_resource_keys=(normalized,))
        items = tuple(value)
        if all(isinstance(item, str) for item in items):
            return cls(changed_resource_keys=items)
        return cls(changes=items)

    @property
    def effective_ref_ids(self) -> frozenset[str]:
        return frozenset(self.changed_ref_ids) | frozenset(
            change.ref_id for change in self.changes if change.ref_id
        )

    @property
    def effective_resource_keys(self) -> frozenset[str]:
        keys = set(self.changed_resource_keys)
        for change in self.changes:
            keys.update(change.resource_keys)
        return frozenset(keys)

    @property
    def effective_artifact_ids(self) -> frozenset[str]:
        return frozenset(self.changed_artifact_ids) | frozenset(
            change.artifact_id for change in self.changes if change.artifact_id
        )


# Friendly short name for callers that already use ``GraphDelta``.
GraphDelta = ContextGraphDelta


class ContextDelta(_FrozenModel):
    """Immutable classification of one old context against a graph delta."""

    schema_version: Literal["context-delta.v1"] = CONTEXT_DELTA_SCHEMA_VERSION
    old_graph_version: StrictInt = Field(ge=0)
    new_graph_version: StrictInt = Field(ge=0)
    old_bindings: tuple[ContextBindingRef, ...] = ()
    affected_bindings: tuple[ContextBindingRef, ...] = ()
    still_valid_bindings: tuple[ContextBindingRef, ...] = ()
    unknown_bindings: tuple[ContextBindingRef, ...] = ()
    required_bindings: tuple[ContextBindingRef, ...] = ()
    required_affected_bindings: tuple[ContextBindingRef, ...] = ()
    reasons: tuple[str, ...] = ()
    delta_hash: str = Field(min_length=64, max_length=64)

    @property
    def affected_ref_ids(self) -> tuple[str, ...]:
        return tuple(item.ref_id for item in self.affected_bindings)

    @property
    def still_valid_ref_ids(self) -> tuple[str, ...]:
        return tuple(item.ref_id for item in self.still_valid_bindings)

    @property
    def unknown_ref_ids(self) -> tuple[str, ...]:
        return tuple(item.ref_id for item in self.unknown_bindings)

    @property
    def required_ref_ids(self) -> tuple[str, ...]:
        return tuple(item.ref_id for item in self.required_bindings)

    @property
    def required_affected_ref_ids(self) -> tuple[str, ...]:
        return tuple(item.ref_id for item in self.required_affected_bindings)

    @property
    def is_noop(self) -> bool:
        return not self.affected_bindings

    def as_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.model_dump(mode="json"))


class ContextRebaseAction(StrEnum):
    """Action recommendation for a context adapter."""

    REUSE = "reuse"
    REBASE = "rebase"
    FULL_RELOAD = "full_reload"
    BLOCKED = "blocked"


class RebasePlan(_FrozenModel):
    """Immutable action plan derived from :class:`ContextDelta`."""

    schema_version: Literal["context-delta.v1"] = CONTEXT_DELTA_SCHEMA_VERSION
    policy_id: str = CONTEXT_REBASE_POLICY_ID
    old_graph_version: StrictInt = Field(ge=0)
    new_graph_version: StrictInt = Field(ge=0)
    action: ContextRebaseAction
    preserve_ref_ids: tuple[str, ...] = ()
    reload_ref_ids: tuple[str, ...] = ()
    required_ref_ids: tuple[str, ...] = ()
    blocked_ref_ids: tuple[str, ...] = ()
    context_delta: ContextDelta
    reason: str = ""
    plan_hash: str = Field(min_length=64, max_length=64)

    @property
    def affected_ref_ids(self) -> tuple[str, ...]:
        return self.context_delta.affected_ref_ids

    @property
    def still_valid_ref_ids(self) -> tuple[str, ...]:
        return self.context_delta.still_valid_ref_ids

    @property
    def blocked(self) -> bool:
        return self.action is ContextRebaseAction.BLOCKED

    def as_dict(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.model_dump(mode="json"))


def _coerce_bindings(
    values: Any,
    *,
    required_ids: frozenset[str] = frozenset(),
) -> tuple[ContextBindingRef, ...]:
    if values is None:
        return ()
    if isinstance(values, ContextManifest):
        values = values.refs
    elif isinstance(values, ContextBindingRef):
        values = (values,)
    elif isinstance(values, Mapping):
        # A mapping of ref_id -> binding is a convenient caller form.
        if "refs" in values:
            values = values["refs"]
        elif any(
            key in values for key in ("canonical_uri", "resource_uri", "artifact_id", "version")
        ):
            values = (values,)
        else:
            values = tuple(
                dict(item, ref_id=ref_id) if isinstance(item, Mapping) else item
                for ref_id, item in values.items()
            )
    if isinstance(values, (str, bytes)):
        values = (values.decode() if isinstance(values, bytes) else values,)
    result = [ContextBindingRef.from_any(value, index=index) for index, value in enumerate(values)]
    # An explicit required set upgrades the corresponding old binding without
    # changing the caller's model.
    if required_ids:
        result = [
            item.model_copy(update={"required": True}) if item.ref_id in required_ids else item
            for item in result
        ]
    by_id: dict[str, ContextBindingRef] = {}
    for item in result:
        previous = by_id.get(item.ref_id)
        if previous is not None and previous.identity_key != item.identity_key:
            raise ValueError(f"duplicate context binding id with different identity: {item.ref_id}")
        by_id[item.ref_id] = item
    return tuple(by_id[ref_id] for ref_id in sorted(by_id))


def _coerce_required_ids(values: Any) -> frozenset[str]:
    """Normalize required-ref input without requiring full binding metadata."""

    if values is None:
        return frozenset()
    if isinstance(values, str):
        return frozenset({values.strip()}) if values.strip() else frozenset()
    if isinstance(values, Mapping):
        raw: Any = (values["ref_id"],) if "ref_id" in values else tuple(values.keys())
    else:
        raw = values
    result: set[str] = set()
    for item in raw:
        if isinstance(item, str):
            normalized = item.strip()
        else:
            normalized = str(getattr(item, "ref_id", item)).strip()
        if normalized:
            result.add(normalized)
    return frozenset(result)


def _same_new_identity(binding: ContextBindingRef, change: ContextGraphChange) -> bool:
    if change.new_version is not None and binding.version != change.new_version:
        return False
    if change.new_content_hash is not None and binding.content_hash != change.new_content_hash:
        return False
    return change.new_version is not None or change.new_content_hash is not None


def _change_matches(binding: ContextBindingRef, change: ContextGraphChange) -> bool:
    if change.ref_id and change.ref_id == binding.ref_id:
        return True
    if change.uri and change.uri == binding.canonical_uri:
        return True
    if change.artifact_id and change.artifact_id == binding.artifact_id:
        # If a precise new identity is present, an already-rebased binding is
        # current and should not be invalidated again.
        return not _same_new_identity(binding, change)
    return False


def _binding_is_affected(
    binding: ContextBindingRef,
    delta: ContextGraphDelta,
) -> tuple[bool, bool, str]:
    """Return (affected, unknown, reason) for one binding."""

    if not binding.known:
        return True, True, "binding_identity_unknown"
    if not delta.known or delta.coverage == "unknown":
        return True, True, "graph_delta_unknown"
    matching_changes = tuple(change for change in delta.changes if _change_matches(binding, change))
    if matching_changes:
        if any(not change.known for change in matching_changes):
            return True, True, "matched_change_identity_unknown"
        if all(_same_new_identity(binding, change) for change in matching_changes):
            return False, False, "already_rebased"
        return True, False, "explicit_graph_change"
    if binding.ref_id in delta.changed_ref_ids:
        return True, False, "explicit_ref_changed"
    if set(binding.resource_keys) & set(delta.changed_resource_keys):
        return True, False, "explicit_resource_changed"
    if binding.artifact_id and binding.artifact_id in delta.changed_artifact_ids:
        return True, False, "explicit_artifact_changed"
    return False, False, "unchanged"


def _hash_payload(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def plan_context_rebase(
    old_graph_version: int,
    new_graph_version: int,
    old_bindings: Any = None,
    graph_delta: Any = None,
    *,
    old_context_manifest: ContextManifest | None = None,
    old_read_bindings: Any = None,
    required_refs: Any = None,
) -> RebasePlan:
    """Plan a bounded context reuse/rebase operation.

    ``old_bindings`` may be a ``ContextManifest``, ``ContentRef`` sequence,
    ``VersionBinding``/``ResourceBinding``-like sequence, or a mapping.  The
    delta must explicitly identify changed refs/resources unless marked
    unknown.  No ContextService or graph store is consulted.
    """

    if isinstance(old_graph_version, bool) or not isinstance(old_graph_version, int):
        raise TypeError("old_graph_version must be an integer")
    if isinstance(new_graph_version, bool) or not isinstance(new_graph_version, int):
        raise TypeError("new_graph_version must be an integer")
    if old_graph_version < 0 or new_graph_version < 0:
        raise ValueError("graph versions must be non-negative")
    if new_graph_version < old_graph_version:
        raise ValueError("new_graph_version cannot be older than old_graph_version")
    sources = [
        value
        for value in (old_bindings, old_context_manifest, old_read_bindings)
        if value is not None
    ]
    if len(sources) > 1:
        raise ValueError(
            "provide only one of old_bindings, old_context_manifest, old_read_bindings"
        )
    source = sources[0] if sources else ()
    required_ids = _coerce_required_ids(required_refs)
    bindings = _coerce_bindings(source, required_ids=required_ids)
    delta = ContextGraphDelta.from_any(graph_delta)

    affected: list[ContextBindingRef] = []
    valid: list[ContextBindingRef] = []
    unknown: list[ContextBindingRef] = []
    reasons: set[str] = set()
    for binding in bindings:
        is_affected, is_unknown, reason = _binding_is_affected(binding, delta)
        if is_affected:
            affected.append(binding)
            reasons.add(reason)
            if is_unknown:
                unknown.append(binding)
        else:
            valid.append(binding)

    required = tuple(item for item in bindings if item.required)
    required_affected = tuple(item for item in affected if item.required)
    delta_payload = {
        "schema_version": CONTEXT_DELTA_SCHEMA_VERSION,
        "old_graph_version": old_graph_version,
        "new_graph_version": new_graph_version,
        "old_bindings": [item.model_dump(mode="json") for item in bindings],
        "affected": [item.ref_id for item in affected],
        "valid": [item.ref_id for item in valid],
        "unknown": [item.ref_id for item in unknown],
        "delta": delta.model_dump(mode="json"),
    }
    context_delta = ContextDelta(
        old_graph_version=old_graph_version,
        new_graph_version=new_graph_version,
        old_bindings=bindings,
        affected_bindings=tuple(affected),
        still_valid_bindings=tuple(valid),
        unknown_bindings=tuple(unknown),
        required_bindings=required,
        required_affected_bindings=required_affected,
        reasons=tuple(sorted(reasons)),
        delta_hash=_hash_payload(delta_payload),
    )

    preserve_ids = tuple(item.ref_id for item in valid)
    reload_ids = tuple(item.ref_id for item in affected if item not in unknown)
    blocked_ids = tuple(item.ref_id for item in unknown)
    if unknown:
        action = ContextRebaseAction.BLOCKED
        reason = "authoritative identity is unavailable for one or more bindings"
    elif not affected:
        action = ContextRebaseAction.REUSE
        reason = "all explicit context bindings remain valid"
    elif bindings and not valid:
        action = ContextRebaseAction.FULL_RELOAD
        reason = "every old context binding is affected"
    elif required_affected:
        action = ContextRebaseAction.REBASE
        reason = "reload affected required bindings before continuing"
    else:
        action = ContextRebaseAction.REBASE
        reason = "reload affected bindings and retain the valid working set"
    plan_payload = {
        "schema_version": CONTEXT_DELTA_SCHEMA_VERSION,
        "policy_id": CONTEXT_REBASE_POLICY_ID,
        "old_graph_version": old_graph_version,
        "new_graph_version": new_graph_version,
        "action": action.value,
        "preserve_ref_ids": preserve_ids,
        "reload_ref_ids": reload_ids,
        "required_ref_ids": tuple(item.ref_id for item in required),
        "blocked_ref_ids": blocked_ids,
        "delta_hash": context_delta.delta_hash,
    }
    return RebasePlan(
        old_graph_version=old_graph_version,
        new_graph_version=new_graph_version,
        action=action,
        preserve_ref_ids=preserve_ids,
        reload_ref_ids=reload_ids,
        required_ref_ids=tuple(item.ref_id for item in required),
        blocked_ref_ids=blocked_ids,
        context_delta=context_delta,
        reason=reason,
        plan_hash=_hash_payload(plan_payload),
    )


def build_context_delta(*args: Any, **kwargs: Any) -> ContextDelta:
    """Return only the immutable classification from a rebase plan."""

    return plan_context_rebase(*args, **kwargs).context_delta


def plan_context_delta(*args: Any, **kwargs: Any) -> ContextDelta:
    """Compatibility alias for :func:`build_context_delta`."""

    return build_context_delta(*args, **kwargs)


__all__ = [
    "CONTEXT_DELTA_SCHEMA_VERSION",
    "CONTEXT_REBASE_POLICY_ID",
    "ContextBindingRef",
    "ContextDelta",
    "ContextGraphChange",
    "ContextGraphDelta",
    "ContextRebaseAction",
    "GraphDelta",
    "RebasePlan",
    "build_context_delta",
    "plan_context_delta",
    "plan_context_rebase",
]
