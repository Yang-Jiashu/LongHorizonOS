"""Executor-facing provenance recorder and ``ExecutionContext`` facade.

Besides recording reads, the execution context exposes a deliberately small
side-effect boundary.  A context may *describe* an effect with
``declare_effect`` and may *execute* it only through an injected
``ActionGateway``.  This is intentionally independent from the Kernel
implementation: the SDK/Kernel composition root can provide a gateway while
the provenance package enforces the contract and records the write-set.

The boundary is opt-in for compatibility (``secure_mode=False``), but once a
context is created in secure mode an undeclared effect or a gateway response
which cannot be proven is rejected fail-closed.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from .models import ProvenanceEvent, ProvenanceOperation
from .store import InMemoryProvenanceStore, ProvenanceStore


class EffectBoundaryError(RuntimeError):
    """A side effect crossed the execution boundary without a valid contract."""


class EffectSubmissionError(RuntimeError):
    """An ActionGateway could not return a trustworthy effect receipt."""


class EffectClass(StrEnum):
    """Portable side-effect classes understood by the provenance boundary.

    ``non_reversible`` is the spelling used by the Kernel model; it is accepted
    by :func:`normalize_effect_class` and normalized to ``irreversible`` here.
    Keeping this enum in the dependency-light provenance package avoids an
    import cycle from provenance into the Kernel.
    """

    PURE = "pure"
    IDEMPOTENT = "idempotent"
    COMPENSATABLE = "compensatable"
    IRREVERSIBLE = "irreversible"
    UNKNOWN = "unknown"


def normalize_effect_class(value: EffectClass | str) -> str:
    """Normalize an effect class without silently accepting arbitrary values."""

    normalized = str(value).strip().lower().replace("-", "_")
    aliases = {
        "non_reversible": EffectClass.IRREVERSIBLE.value,
        "nonreversible": EffectClass.IRREVERSIBLE.value,
        "reversible": EffectClass.COMPENSATABLE.value,
    }
    normalized = aliases.get(normalized, normalized)
    try:
        return EffectClass(normalized).value
    except ValueError as exc:
        raise ValueError(
            "side_effect_class must be one of "
            "'pure', 'idempotent', 'compensatable', 'irreversible', or 'unknown'"
        ) from exc


@dataclass(frozen=True, slots=True)
class EffectDeclaration:
    """The immutable effect contract accepted by an execution context."""

    effect_id: str
    side_effect_class: str = EffectClass.UNKNOWN.value
    resource_uri: str = ""
    idempotency_key: str | None = None
    operation: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        effect_id = str(self.effect_id).strip()
        if not effect_id:
            raise ValueError("effect_id must be non-empty")
        object.__setattr__(self, "effect_id", effect_id)
        object.__setattr__(
            self,
            "side_effect_class",
            normalize_effect_class(self.side_effect_class),
        )
        object.__setattr__(self, "resource_uri", str(self.resource_uri).strip())
        if self.idempotency_key is not None:
            key = str(self.idempotency_key).strip()
            object.__setattr__(self, "idempotency_key", key or None)
        object.__setattr__(self, "operation", str(self.operation).strip())
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True, slots=True)
class EffectRequest:
    """A gateway submission carrying the exact execution identity."""

    effect_id: str
    operation: str
    arguments: Mapping[str, Any] = field(default_factory=dict)
    declaration: EffectDeclaration | None = None
    graph_id: str = ""
    task_id: str = ""
    claim_id: str = ""
    attempt_id: str = ""
    semantic_epoch: int = 0
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        effect_id = str(self.effect_id).strip()
        operation = str(self.operation).strip()
        if not effect_id:
            raise ValueError("effect_id must be non-empty")
        if not operation:
            raise ValueError("effect operation must be non-empty")
        if self.declaration is not None and self.declaration.effect_id != effect_id:
            raise ValueError("effect declaration id does not match request effect_id")
        if self.semantic_epoch < 0:
            raise ValueError("semantic_epoch must be >= 0")
        object.__setattr__(self, "effect_id", effect_id)
        object.__setattr__(self, "operation", operation)
        object.__setattr__(self, "arguments", dict(self.arguments))
        object.__setattr__(self, "metadata", dict(self.metadata))


@dataclass(frozen=True, slots=True)
class EffectReceipt:
    """A normalized gateway acknowledgement.

    ``uncertain`` is intentionally a first-class status: a gateway that
    applied an effect but lost the acknowledgement must not be treated as a
    successful, retryable completion.
    """

    effect_id: str
    status: str
    action_id: str | None = None
    output: Any = None
    error: str | None = None
    idempotency_key: str | None = None
    fencing_token: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        effect_id = str(self.effect_id).strip()
        if not effect_id:
            raise ValueError("receipt effect_id must be non-empty")
        status = str(self.status).strip().lower().replace("-", "_")
        aliases = {"success": "completed", "done": "completed", "failed": "rejected"}
        status = aliases.get(status, status)
        if status not in {"completed", "uncertain", "rejected"}:
            raise ValueError("receipt status must be completed, uncertain, or rejected")
        object.__setattr__(self, "effect_id", effect_id)
        object.__setattr__(self, "status", status)
        if self.action_id is not None:
            object.__setattr__(self, "action_id", str(self.action_id).strip() or None)
        if self.idempotency_key is not None:
            object.__setattr__(
                self,
                "idempotency_key",
                str(self.idempotency_key).strip() or None,
            )
        if self.fencing_token is not None:
            try:
                token = int(self.fencing_token)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("receipt fencing_token must be an integer") from exc
            if token < 0:
                raise ValueError("receipt fencing_token must be >= 0")
            object.__setattr__(self, "fencing_token", token)
        object.__setattr__(self, "metadata", dict(self.metadata))


@runtime_checkable
class ActionGateway(Protocol):
    """Minimal dependency-injection contract for mediated side effects."""

    def submit(self, request: EffectRequest) -> EffectReceipt | Mapping[str, Any] | Any:
        """Submit one effect and return a receipt or a receipt-like object."""


def _normalize_receipt(
    value: EffectReceipt | Mapping[str, Any] | Any,
    *,
    request: EffectRequest,
) -> EffectReceipt:
    """Normalize a gateway result and reject ambiguous acknowledgements."""

    if isinstance(value, EffectReceipt):
        receipt = value
    else:
        if isinstance(value, Mapping):
            raw = dict(value)
        else:
            raw = {
                name: getattr(value, name)
                for name in (
                    "effect_id",
                    "status",
                    "action_id",
                    "output",
                    "result",
                    "error",
                    "idempotency_key",
                    "fencing_token",
                    "metadata",
                )
                if hasattr(value, name)
            }
        if "status" not in raw:
            # Kernel ActionControlBlock-like objects expose ``state`` rather
            # than ``status``.  Only map known terminal states; every other
            # state is ambiguous and must fail closed.
            state = raw.get("state", getattr(value, "state", None))
            state_value = getattr(state, "value", state)
            if state_value in {"committed", "completed"}:
                raw["status"] = "completed"
            elif state_value in {"uncertain"}:
                raw["status"] = "uncertain"
            elif state_value in {"failed", "cancelled", "rejected"}:
                raw["status"] = "rejected"
        raw.setdefault("effect_id", request.effect_id)
        if "output" not in raw and "result" in raw:
            raw["output"] = raw["result"]
        try:
            receipt = EffectReceipt(**raw)
        except (TypeError, ValueError) as exc:
            raise EffectSubmissionError(
                f"ActionGateway returned malformed receipt for effect {request.effect_id!r}: {exc}"
            ) from exc
    if receipt.effect_id != request.effect_id:
        raise EffectSubmissionError(
            f"ActionGateway receipt effect_id {receipt.effect_id!r} does not match "
            f"request {request.effect_id!r}"
        )
    return receipt


class ProvenanceRecorder:
    """Record observations for one graph/task/attempt execution.

    The recorder is intentionally explicit.  It cannot observe arbitrary
    Python reads automatically; callers must use ``read``, ``record_tool``,
    ``observe_external`` and the other helpers at tool boundaries.
    """

    def __init__(
        self,
        graph_id: str,
        *,
        task_id: str = "",
        attempt_id: str = "",
        semantic_epoch: int = 0,
        store: ProvenanceStore | None = None,
        source: str = "runtime",
        secure_mode: bool = False,
        action_gateway: ActionGateway | Any | None = None,
    ) -> None:
        self.graph_id = str(graph_id)
        self.task_id = str(task_id)
        self.attempt_id = str(attempt_id)
        self.semantic_epoch = int(semantic_epoch)
        self.source = str(source)
        self.store = store or InMemoryProvenanceStore()
        self._events: list[ProvenanceEvent] = []
        self._context_snapshot_metadata: dict[str, Any] = {}
        # Mediated adapters may register bounded commit-time validators on
        # this execution context. The provenance package keeps them opaque to
        # avoid importing integration modules or claiming a universal I/O
        # interception boundary.
        self._workspace_provenance_gateways: tuple[Any, ...] = ()
        self.secure_mode = bool(secure_mode)
        self.action_gateway = action_gateway
        self._effect_declarations: dict[str, EffectDeclaration] = {}
        self._effect_receipts: dict[str, EffectReceipt] = {}

    @property
    def events(self) -> tuple[ProvenanceEvent, ...]:
        """Events captured by this recorder (committed or pending)."""

        return tuple(self._events)

    @property
    def effect_declarations(self) -> tuple[EffectDeclaration, ...]:
        """Immutable view of declarations made in this execution."""

        return tuple(self._effect_declarations.values())

    @property
    def effect_receipts(self) -> tuple[EffectReceipt, ...]:
        """Receipts accepted from the configured gateway."""

        return tuple(self._effect_receipts.values())

    def bind_context_snapshot(
        self,
        *,
        snapshot_id: str,
        manifest_id: str,
        manifest_hash: str,
        working_set_hash: str,
        materialized_hash: str,
        handle: Any = None,
        loaded_context: Any = None,
        snapshot: Any = None,
        record_bindings: bool = True,
    ) -> None:
        """Bind the immutable Context VM identity used by this execution.

        A Context VM snapshot is not merely bookkeeping: its materialized
        pages are inputs that the executor was actually given.  When a
        snapshot object exposes ``page_bindings`` (the public Context VM
        contract), those exact version/content bindings are therefore folded
        into the execution read-set by default.  This closes the common gap
        where a ``context_v1`` callback receives a file through Context VM but
        forgets to call ``ctx.read(...)`` manually.

        ``record_bindings=False`` is retained for adapters that intentionally
        want to account for the snapshot as metadata only.  It is never
        implied by secure mode; strict provenance should normally keep the
        default so omitted/changed pages fail closed.
        """

        binding = {
            "snapshot_id": str(snapshot_id),
            "manifest_id": str(manifest_id),
            "manifest_hash": str(manifest_hash),
            "working_set_hash": str(working_set_hash),
            "materialized_hash": str(materialized_hash),
        }
        if not all(binding.values()):
            raise ValueError("context snapshot identity fields must be non-empty")
        if self._context_snapshot_metadata and self._context_snapshot_metadata != binding:
            raise ValueError("execution context is already bound to a different context snapshot")
        self._context_snapshot_metadata = binding
        self.context_snapshot_id = binding["snapshot_id"]
        self.context_manifest_id = binding["manifest_id"]
        self.context_manifest_hash = binding["manifest_hash"]
        self.context_working_set_hash = binding["working_set_hash"]
        self.context_materialized_hash = binding["materialized_hash"]
        self.context_handle = handle
        self.loaded_context = loaded_context
        self.context_snapshot = snapshot
        if record_bindings and snapshot is not None:
            # A snapshot may be a lightweight test double.  Treat a missing
            # page_bindings attribute as an empty set rather than guessing
            # dependencies from unrelated fields.
            page_bindings = getattr(snapshot, "page_bindings", ()) or ()
            for binding in page_bindings:
                canonical_uri = str(
                    getattr(binding, "canonical_uri", "")
                    or f"vpg://{getattr(binding, 'artifact_id', '')}"
                ).strip()
                artifact_id = str(getattr(binding, "artifact_id", "")).strip() or None
                version = getattr(binding, "version", None)
                content_hash = str(getattr(binding, "content_hash", "")).strip() or None
                page_id = str(getattr(binding, "page_id", "")).strip()
                # Do not manufacture an identifiable read from a malformed
                # binding.  Recording it as unknown preserves fail-closed
                # strict coverage and makes the defect auditable.
                if not canonical_uri or not artifact_id or version is None or not content_hash:
                    self.observe_unknown(
                        op=ProvenanceOperation.READ,
                        resource_hint=canonical_uri or artifact_id or "context://unknown",
                        source="context_vm",
                        page_id=page_id,
                        reason="malformed_context_page_binding",
                    )
                    continue
                self.record(
                    ProvenanceOperation.READ,
                    resource_uri=canonical_uri,
                    artifact_id=artifact_id,
                    version=int(version),
                    content_hash=content_hash,
                    source="context_vm",
                    metadata={
                        "context_snapshot_id": self.context_snapshot_id,
                        "page_id": page_id,
                        "byte_start": getattr(binding, "byte_start", None),
                        "byte_end": getattr(binding, "byte_end", None),
                    },
                )

    def declare_effect(
        self,
        effect_id: str,
        *,
        side_effect_class: EffectClass | str = EffectClass.UNKNOWN,
        resource_uri: str = "",
        idempotency_key: str | None = None,
        operation: str = "",
        **metadata: Any,
    ) -> EffectDeclaration:
        """Declare one external effect before submitting it.

        Declarations are immutable per ``effect_id``.  Re-declaring the same
        ID with a different contract is rejected, preventing a worker from
        downgrading an irreversible effect after dispatch.
        """

        declaration = EffectDeclaration(
            effect_id=effect_id,
            side_effect_class=side_effect_class,
            resource_uri=resource_uri,
            idempotency_key=idempotency_key,
            operation=operation,
            metadata=metadata,
        )
        existing = self._effect_declarations.get(declaration.effect_id)
        if existing is not None and existing != declaration:
            raise EffectBoundaryError(
                f"effect {declaration.effect_id!r} was already declared with a different contract"
            )
        self._effect_declarations[declaration.effect_id] = declaration
        return declaration

    def _resolve_gateway(self) -> Any:
        gateway = self.action_gateway
        if gateway is None:
            raise EffectBoundaryError(
                "secure execution requires an ActionGateway; "
                "use ctx.submit_effect(...) instead of direct side effects"
            )
        submit = getattr(gateway, "submit", None)
        if callable(submit):
            return submit
        submit_effect = getattr(gateway, "submit_effect", None)
        if callable(submit_effect):
            return submit_effect
        if callable(gateway):
            return gateway
        raise EffectBoundaryError("configured action_gateway has no callable submit method")

    def _gateway_submit(self, request: EffectRequest) -> Any:
        submit = self._resolve_gateway()
        try:
            signature = inspect.signature(submit)
        except (TypeError, ValueError):
            return submit(request)

        # Preferred protocol is submit(request).  A small compatibility path
        # supports gateways exposing submit_effect(**kwargs) without masking
        # errors raised *inside* a callback.
        try:
            signature.bind(request)
        except TypeError:
            try:
                signature.bind(
                    request=request,
                    context=self,
                    effect_id=request.effect_id,
                    operation=request.operation,
                    arguments=dict(request.arguments),
                    declaration=request.declaration,
                )
            except TypeError as exc:
                raise EffectBoundaryError(
                    "ActionGateway must accept submit(request) or "
                    "submit_effect(request=..., context=..., ...)"
                ) from exc
            return submit(
                request=request,
                context=self,
                effect_id=request.effect_id,
                operation=request.operation,
                arguments=dict(request.arguments),
                declaration=request.declaration,
            )
        return submit(request)

    def _record_uncertain_effect(
        self,
        *,
        effect_id: str,
        declaration: EffectDeclaration,
        reason: str,
        error: str,
        metadata: Mapping[str, Any],
    ) -> None:
        """Persist a fail-closed write when an effect acknowledgement is untrusted."""

        self.record(
            ProvenanceOperation.WRITE,
            resource_uri=declaration.resource_uri or f"effect:{effect_id}",
            action_id=None,
            known=False,
            idempotency_key=declaration.idempotency_key,
            metadata={
                **dict(metadata),
                "effect_id": effect_id,
                "side_effect_class": declaration.side_effect_class,
                "status": "uncertain",
                "reason": reason,
                "error": error,
            },
        )

    def submit_effect(
        self,
        effect_id: str,
        operation: str,
        *,
        arguments: Mapping[str, Any] | None = None,
        declaration: EffectDeclaration | None = None,
        **metadata: Any,
    ) -> EffectReceipt:
        """Submit a declared effect through the ActionGateway.

        In secure mode all effects must have a declaration and all
        non-``pure`` effects must carry an idempotency key.  In compatibility
        mode an undeclared effect is recorded as ``UNKNOWN`` but is still sent
        through the gateway if one exists.
        """

        resolved_id = str(effect_id).strip()
        if not resolved_id:
            raise EffectBoundaryError("effect_id must be non-empty")
        declared = declaration or self._effect_declarations.get(resolved_id)
        if declared is None:
            if self.secure_mode:
                raise EffectBoundaryError(
                    f"effect {resolved_id!r} was not declared before submission"
                )
            declared = self.declare_effect(
                resolved_id,
                side_effect_class=EffectClass.UNKNOWN,
                operation=operation,
                **metadata,
            )
        elif declared.effect_id != resolved_id:
            raise EffectBoundaryError("effect declaration id does not match effect_id")
        else:
            # A caller may pass an explicit declaration object.  Register it
            # through the same immutable map so subsequent submissions cannot
            # silently replace the contract.
            self.declare_effect(
                resolved_id,
                side_effect_class=declared.side_effect_class,
                resource_uri=declared.resource_uri,
                idempotency_key=declared.idempotency_key,
                operation=declared.operation,
                **dict(declared.metadata),
            )
        if declared.operation and declared.operation != str(operation).strip():
            raise EffectBoundaryError(
                f"effect {resolved_id!r} operation does not match its declaration"
            )
        if (
            self.secure_mode
            and declared.side_effect_class != EffectClass.PURE.value
            and not declared.idempotency_key
        ):
            raise EffectBoundaryError(
                f"secure effect {resolved_id!r} requires an idempotency_key "
                "for non-pure side effects"
            )
        request = EffectRequest(
            effect_id=resolved_id,
            operation=operation,
            arguments=arguments or {},
            declaration=declared,
            graph_id=self.graph_id,
            task_id=self.task_id,
            claim_id=str(getattr(self, "claim_id", "")),
            attempt_id=self.attempt_id,
            semantic_epoch=self.semantic_epoch,
            metadata=metadata,
        )
        try:
            raw_receipt = self._gateway_submit(request)
        except EffectBoundaryError:
            raise
        except Exception as exc:
            # A gateway exception does not prove that an effect did not occur.
            # Record uncertainty and surface a typed error; callers must not
            # blindly retry irreversible/unknown effects.
            self._record_uncertain_effect(
                effect_id=resolved_id,
                declaration=declared,
                reason="gateway_submission_failed",
                error=str(exc),
                metadata=metadata,
            )
            raise EffectSubmissionError(
                f"ActionGateway submission failed for effect {resolved_id!r}: {exc}"
            ) from exc
        try:
            receipt = _normalize_receipt(raw_receipt, request=request)
            if declared.idempotency_key and receipt.idempotency_key not in {
                None,
                declared.idempotency_key,
            }:
                raise EffectSubmissionError(
                    f"ActionGateway receipt idempotency key does not match effect {resolved_id!r}"
                )
            if (
                self.secure_mode
                and declared.side_effect_class != EffectClass.PURE.value
                and receipt.idempotency_key != declared.idempotency_key
            ):
                raise EffectSubmissionError(
                    f"secure effect {resolved_id!r} completed without binding the "
                    "declared idempotency_key"
                )
            if (
                self.secure_mode
                and receipt.status == "completed"
                and declared.side_effect_class != EffectClass.PURE.value
                and not receipt.action_id
            ):
                raise EffectSubmissionError(
                    f"secure effect {resolved_id!r} completed without an action_id"
                )
        except EffectSubmissionError as exc:
            # The gateway returned, so the external effect may already have
            # happened. A malformed or identity-mismatched acknowledgement is
            # uncertainty, not absence of a write.
            self._record_uncertain_effect(
                effect_id=resolved_id,
                declaration=declared,
                reason="untrusted_gateway_receipt",
                error=str(exc),
                metadata=metadata,
            )
            raise
        self._effect_receipts[resolved_id] = receipt
        self.record(
            ProvenanceOperation.WRITE,
            resource_uri=declared.resource_uri or f"effect:{resolved_id}",
            action_id=receipt.action_id,
            known=receipt.status == "completed",
            idempotency_key=declared.idempotency_key,
            metadata={
                **metadata,
                "effect_id": resolved_id,
                "side_effect_class": declared.side_effect_class,
                "status": receipt.status,
                "fencing_token": receipt.fencing_token,
            },
        )
        if self.secure_mode and receipt.status != "completed":
            raise EffectSubmissionError(
                f"secure effect {resolved_id!r} is {receipt.status}; "
                "semantic verification must wait for reconciliation"
            )
        return receipt

    def submit_tool(
        self,
        tool_name: str,
        operation: str = "invoke",
        *,
        effect_id: str | None = None,
        arguments: Mapping[str, Any] | None = None,
        side_effect_class: EffectClass | str = EffectClass.UNKNOWN,
        resource_uri: str = "",
        idempotency_key: str | None = None,
        **metadata: Any,
    ) -> EffectReceipt:
        """Convenience wrapper for a mediated tool invocation."""

        resolved_id = effect_id or f"tool:{tool_name}"
        declaration = self.declare_effect(
            resolved_id,
            side_effect_class=side_effect_class,
            resource_uri=resource_uri or f"tool:{tool_name}",
            idempotency_key=idempotency_key,
            operation=operation,
            tool=tool_name,
        )
        return self.submit_effect(
            resolved_id,
            operation,
            arguments=arguments,
            declaration=declaration,
            tool=tool_name,
            **metadata,
        )

    def record(
        self,
        op: ProvenanceOperation | str,
        *,
        resource_uri: str = "",
        artifact_id: str | None = None,
        version: int | None = None,
        content_hash: str | None = None,
        action_id: str | None = None,
        source: str | None = None,
        confidence: float = 1.0,
        known: bool = True,
        metadata: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> ProvenanceEvent:
        """Capture one observation and append it to the configured store."""

        operation = (
            op if isinstance(op, ProvenanceOperation) else ProvenanceOperation(str(op).lower())
        )
        event_metadata = dict(metadata or {})
        if self._context_snapshot_metadata:
            event_metadata["context_snapshot"] = dict(self._context_snapshot_metadata)
        event = ProvenanceEvent(
            graph_id=self.graph_id,
            task_id=self.task_id,
            attempt_id=self.attempt_id,
            semantic_epoch=self.semantic_epoch,
            op=operation,
            resource_uri=resource_uri,
            artifact_id=artifact_id,
            version=version,
            content_hash=content_hash,
            action_id=action_id,
            source=source or self.source,
            confidence=confidence,
            known=known,
            metadata=event_metadata,
            idempotency_key=idempotency_key,
        )
        persisted = self.store.append(event)
        self._events.append(persisted)
        return persisted

    def read(
        self,
        resource_uri: str,
        *,
        artifact_id: str | None = None,
        version: int | None = None,
        content_hash: str | None = None,
        **metadata: Any,
    ) -> ProvenanceEvent:
        return self.record(
            ProvenanceOperation.READ,
            resource_uri=resource_uri,
            artifact_id=artifact_id,
            version=version,
            content_hash=content_hash,
            metadata=metadata,
        )

    def write(
        self,
        resource_uri: str,
        *,
        artifact_id: str | None = None,
        version: int | None = None,
        content_hash: str | None = None,
        **metadata: Any,
    ) -> ProvenanceEvent:
        return self.record(
            ProvenanceOperation.WRITE,
            resource_uri=resource_uri,
            artifact_id=artifact_id,
            version=version,
            content_hash=content_hash,
            metadata=metadata,
        )

    def record_tool(
        self,
        tool_name: str,
        *,
        action_id: str | None = None,
        resource_uri: str = "",
        known: bool = True,
        **metadata: Any,
    ) -> ProvenanceEvent:
        uri = resource_uri or f"tool:{tool_name}"
        return self.record(
            ProvenanceOperation.TOOL,
            resource_uri=uri,
            action_id=action_id,
            known=known,
            metadata={"tool": tool_name, **metadata},
        )

    def record_network(
        self,
        resource_uri: str,
        *,
        content_hash: str | None = None,
        version: int | None = None,
        known: bool = True,
        **metadata: Any,
    ) -> ProvenanceEvent:
        return self.record(
            ProvenanceOperation.NETWORK,
            resource_uri=resource_uri,
            content_hash=content_hash,
            version=version,
            known=known,
            metadata=metadata,
        )

    def record_model(
        self,
        model_name: str,
        *,
        model_hash: str | None = None,
        prompt_hash: str | None = None,
        schema_hash: str | None = None,
        **metadata: Any,
    ) -> ProvenanceEvent:
        return self.record(
            ProvenanceOperation.MODEL,
            resource_uri=f"model:{model_name}",
            content_hash=model_hash,
            metadata={
                "model": model_name,
                **({"prompt_hash": prompt_hash} if prompt_hash else {}),
                **({"schema_hash": schema_hash} if schema_hash else {}),
                **metadata,
            },
        )

    def observe_external(
        self,
        resource_uri: str | None = None,
        *,
        content_hash: str | None = None,
        version: int | None = None,
        known: bool = True,
        **metadata: Any,
    ) -> ProvenanceEvent:
        """Record an externally observed fact or an explicitly unknown read."""

        return self.record(
            ProvenanceOperation.EXTERNAL,
            resource_uri=resource_uri or "",
            content_hash=content_hash,
            version=version,
            known=known,
            metadata=metadata,
        )

    def observe_unknown(
        self,
        *,
        op: ProvenanceOperation | str = ProvenanceOperation.EXTERNAL,
        resource_hint: str = "",
        **metadata: Any,
    ) -> ProvenanceEvent:
        """Fail-closed marker for a hidden/unknown input dependency."""

        return self.record(
            op,
            resource_uri=resource_hint,
            known=False,
            metadata={"unknown": True, **metadata},
        )

    def record_many(self, events: Iterable[ProvenanceEvent]) -> tuple[ProvenanceEvent, ...]:
        """Persist pre-built events, useful for adapter integrations."""

        persisted = self.store.append_many(events)
        self._events.extend(persisted)
        return persisted

    def flush(self) -> tuple[ProvenanceEvent, ...]:
        """Return the recorder's captured events.

        Stores append eagerly; ``flush`` is provided as a neutral lifecycle
        hook for executors that use buffered instrumentation.
        """

        return self.events

    def __enter__(self) -> ProvenanceRecorder:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.flush()


# Naming requested by the v0.2 contract.  It is a subclass rather than a
# simple alias so type checkers and API docs show the execution-facing name.
class ExecutionContext(ProvenanceRecorder):
    """Public execution context carrying provenance instrumentation."""

    # SDK adapters may attach ownership metadata to the context without
    # putting Kernel-specific fields into the standalone ProvenanceEvent wire
    # schema.  These are intentionally plain, mutable integration attributes.
    claim_id: str = ""
    graph_version: int = 0
    agent_id: str = ""
    process_id: str = ""
    lease_id: str | None = None
    lease_fencing_token: int | None = None
    executor_api: str = "context_v1"
    context_snapshot_id: str = ""
    context_manifest_id: str = ""
    context_manifest_hash: str = ""
    context_working_set_hash: str = ""
    context_materialized_hash: str = ""
    context_handle: Any = None
    loaded_context: Any = None
    context_snapshot: Any = None
    # Fresh-attempt Context rebase metadata is attached by the SDK after the
    # normal ownership and Context VM fences have passed.  These attributes
    # are advisory integration data only; they never authorize Evidence.
    automatic_rebase_manifest: Any = None
    automatic_rebase_decision: Any = None
    automatic_rebase_action: str = ""
    automatic_rebase_source_attempt_id: str = ""
    automatic_rebase_delta_ref_ids: tuple[str, ...] = ()
    automatic_rebase_delta: Any = None
    # Cooperative interrupt capability is attached by an execution runtime.
    # The provenance package deliberately treats it as an opaque object to
    # avoid importing the multi-agent worker runtime (and creating a cycle).
    cancellation_token: Any = None

    def bind_cancellation_token(self, token: Any | None) -> None:
        """Bind the cooperative interrupt token for this execution attempt.

        A context can be bound at most once.  Rebinding to the same token is
        idempotent; replacing an active token would make the executor's
        cognition/ownership identity ambiguous and is therefore rejected.
        """

        current = getattr(self, "cancellation_token", None)
        if current is not None and current is not token:
            raise ValueError("execution context is already bound to another cancellation token")
        self.cancellation_token = token

    @property
    def interrupt_requested(self) -> bool:
        """Whether an interrupt has been requested, without acknowledging it."""

        token = getattr(self, "cancellation_token", None)
        return bool(token is not None and getattr(token, "request_pending", False))

    @property
    def interrupt_observed(self) -> bool:
        """Whether the executor explicitly observed the interrupt token."""

        token = getattr(self, "cancellation_token", None)
        return bool(token is not None and getattr(token, "observed", False))

    def observe_interrupt(self) -> bool:
        """Observe a pending interrupt and return whether one was pending."""

        token = getattr(self, "cancellation_token", None)
        if token is None:
            return False
        # ``is_cancelled`` is the public worker-token observation operation;
        # unlike a raw flag read it advances the durable observation boundary.
        return bool(getattr(token, "is_cancelled", False))

    def raise_if_interrupted(self) -> None:
        """Raise the runtime's cooperative interrupt exception when pending."""

        token = getattr(self, "cancellation_token", None)
        if token is not None:
            token.raise_if_cancelled()


__all__ = ["ExecutionContext", "ProvenanceRecorder"]
