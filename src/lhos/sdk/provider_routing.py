"""Opt-in provider-backed execution routing.

The regular SDK path intentionally has no provider registry and therefore
keeps the historical ``Agent.executor``/``Task.verify`` callbacks unchanged.
When a task explicitly opts in with ``metadata["compute_routing"][
"provider_routing"] = {"enabled": True}``, :class:`ComputeProviderRegistry`
can replace the executor, verifier, and/or Context VM view for that one
scheduled attempt.

This module is deliberately an execution adapter, not a scheduler.  Routing
is performed *after* the Scheduler has created a Claim and the Kernel lease
has been acquired.  Providers receive the exact task/context identity and
cannot create claims, leases, or semantic Evidence.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .compute_routing import (
    CandidateTaskMetadata,
    ComputeRoutingDecision,
    ComputeRoutingPolicy,
)
from .errors import ConfigurationError


class ProviderRoutingError(ConfigurationError):
    """An explicit provider route is malformed or unavailable."""


@dataclass(frozen=True, slots=True)
class ProviderRoute:
    """Resolved providers for one exact scheduled attempt."""

    decision: ComputeRoutingDecision
    model_key: str | None = None
    model_provider: Any | None = None
    verifier_key: str | None = None
    verifier_provider: Any | None = None
    context_adapter_key: str | None = None
    context_adapter: Any | None = None

    @property
    def enabled(self) -> bool:
        return any(
            provider is not None
            for provider in (
                self.model_provider,
                self.verifier_provider,
                self.context_adapter,
            )
        )


class ComputeProviderRegistry:
    """Small explicit registry used by the opt-in SDK execution path.

    Provider hooks use these contracts:

    * model provider: ``execute(task_id, context, base_executor)``;
    * verifier provider: ``verify(task_id, context, executor_outcome,
      base_verifier)``;
    * context adapter: ``adapt(task_id, context)``.

    A callable object may implement the same method directly.  Async hooks are
    supported by :meth:`execute_async`/:meth:`verify_async`; synchronous
    ``AgentOS.run`` rejects an awaitable as it does for ordinary callbacks.
    """

    def __init__(self) -> None:
        self._models: dict[str, Any] = {}
        self._verifiers: dict[str, Any] = {}
        self._contexts: dict[str, Any] = {}

    @staticmethod
    def _key(value: Any) -> str:
        key = str(value).strip().lower()
        if not key:
            raise ValueError("provider key must be non-empty")
        return key

    def register_model(self, key: str, provider: Any) -> ComputeProviderRegistry:
        return self._register(self._models, key, provider, "model", hook_name="execute")

    def register_verifier(self, key: str, provider: Any) -> ComputeProviderRegistry:
        return self._register(self._verifiers, key, provider, "verifier", hook_name="verify")

    def register_context_adapter(self, key: str, adapter: Any) -> ComputeProviderRegistry:
        return self._register(
            self._contexts,
            key,
            adapter,
            "context adapter",
            hook_name="adapt",
        )

    def _register(
        self,
        target: dict[str, Any],
        key: str,
        value: Any,
        kind: str,
        *,
        hook_name: str,
    ) -> ComputeProviderRegistry:
        normalized = self._key(key)
        if value is None or (not callable(value) and not callable(getattr(value, hook_name, None))):
            raise TypeError(f"{kind} provider must be callable or implement {hook_name}()")
        if normalized in target:
            raise ValueError(f"{kind} provider {normalized!r} is already registered")
        target[normalized] = value
        return self

    def model(self, key: str) -> Any | None:
        return self._models.get(self._key(key))

    def verifier(self, key: str) -> Any | None:
        return self._verifiers.get(self._key(key))

    def context_adapter(self, key: str) -> Any | None:
        return self._contexts.get(self._key(key))

    def route(
        self,
        decision: ComputeRoutingDecision,
        metadata: Mapping[str, Any] | None = None,
    ) -> ProviderRoute | None:
        """Resolve an explicit task route, or ``None`` when opt-in is absent."""

        raw = {} if metadata is None else metadata.get("provider_routing", metadata)
        if not isinstance(raw, Mapping) or not bool(raw.get("enabled", False)):
            return None
        model_raw = raw.get("model", decision.model_tier.value)
        verifier_raw = raw.get("verifier", decision.verification_strength.value)
        if not isinstance(model_raw, str) or not isinstance(verifier_raw, str):
            raise ProviderRoutingError("provider route model/verifier keys must be strings")
        model_key = self._key(model_raw)
        verifier_key = self._key(verifier_raw)
        context_raw = raw.get("context_adapter")
        context_key = None if context_raw in (None, "") else self._key(context_raw)
        model_provider = self.model(model_key)
        verifier_provider = self.verifier(verifier_key)
        context_adapter = None if context_key is None else self.context_adapter(context_key)
        missing = [
            name
            for name, value in (
                ("model", model_provider),
                ("verifier", verifier_provider),
            )
            if value is None
        ]
        if context_key is not None and context_adapter is None:
            missing.append("context_adapter")
        if missing:
            raise ProviderRoutingError(
                "enabled provider route has no registered provider: "
                + ", ".join(sorted(set(missing)))
            )
        return ProviderRoute(
            decision=decision,
            model_key=model_key if model_provider is not None else None,
            model_provider=model_provider,
            verifier_key=verifier_key if verifier_provider is not None else None,
            verifier_provider=verifier_provider,
            context_adapter_key=context_key,
            context_adapter=context_adapter,
        )

    def route_task(
        self,
        state: Any,
        task_id: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ProviderRoute | None:
        """Build a policy decision and resolve an explicit task route."""

        raw = {} if metadata is None else metadata.get("compute_routing", {})
        if not isinstance(raw, Mapping):
            return None
        provider_raw = raw.get("provider_routing", {})
        if not isinstance(provider_raw, Mapping) or not bool(provider_raw.get("enabled", False)):
            return None
        decision = build_routing_decision(state, task_id, raw)
        return self.route(decision, provider_raw)

    @staticmethod
    def _hook(provider: Any, primary: str) -> Any:
        hook = getattr(provider, primary, None)
        if callable(hook):
            return hook
        if callable(provider):
            return provider
        raise ProviderRoutingError(f"provider does not implement {primary}()")

    @staticmethod
    def _invoke(hook: Any, args: tuple[Any, ...]) -> Any:
        try:
            signature = inspect.signature(hook)
        except (TypeError, ValueError):
            return hook(*args)
        # Keep one documented contract while allowing keyword-only providers.
        try:
            signature.bind(*args)
        except TypeError as exc:
            raise ProviderRoutingError(
                f"provider hook {getattr(hook, '__name__', type(hook).__name__)!r} "
                "must accept the documented arguments"
            ) from exc
        return hook(*args)

    @staticmethod
    def _sync_result(value: Any, *, role: str) -> Any:
        if inspect.isawaitable(value):
            close = getattr(value, "close", None)
            if callable(close):
                close()
            raise ProviderRoutingError(
                f"{role} provider returned an awaitable in synchronous AgentOS.run; "
                "use AgentOS.run_async(...)"
            )
        return value

    def adapt_context(self, route: ProviderRoute, task_id: str, context: Any) -> Any:
        if route.context_adapter is None:
            return context
        adapted = self._sync_result(
            self._invoke(
                self._hook(route.context_adapter, "adapt"),
                (task_id, context),
            ),
            role="context adapter",
        )
        # ``None`` means "no adaptation"; every other object (including an
        # intentionally false-y ContextView) is authoritative and must be
        # passed through unchanged.
        return context if adapted is None else adapted

    def execute_sync(
        self,
        route: ProviderRoute,
        task_id: str,
        context: Any,
        base_executor: Any,
    ) -> Any:
        if route.model_provider is None:
            if base_executor is None:
                return None
            return self._sync_result(base_executor(task_id), role="model")
        return self._sync_result(
            self._invoke(
                self._hook(route.model_provider, "execute"),
                (task_id, context, base_executor),
            ),
            role="model",
        )

    async def execute_async(
        self,
        route: ProviderRoute,
        task_id: str,
        context: Any,
        base_executor: Any,
    ) -> Any:
        if route.model_provider is None:
            return None
        result = self._invoke(
            self._hook(route.model_provider, "execute"),
            (task_id, context, base_executor),
        )
        return await result if inspect.isawaitable(result) else result

    def verify_sync(
        self,
        route: ProviderRoute,
        task_id: str,
        context: Any,
        executor_outcome: Any,
        base_verifier: Any,
    ) -> Any:
        if route.verifier_provider is None:
            return base_verifier() if base_verifier is not None else executor_outcome
        return self._sync_result(
            self._invoke(
                self._hook(route.verifier_provider, "verify"),
                (task_id, context, executor_outcome, base_verifier),
            ),
            role="verifier",
        )

    async def verify_async(
        self,
        route: ProviderRoute,
        task_id: str,
        context: Any,
        executor_outcome: Any,
        base_verifier: Any,
    ) -> Any:
        if route.verifier_provider is None:
            return executor_outcome
        result = self._invoke(
            self._hook(route.verifier_provider, "verify"),
            (task_id, context, executor_outcome, base_verifier),
        )
        return await result if inspect.isawaitable(result) else result


def build_routing_decision(
    state: Any,
    task_id: str,
    metadata: Mapping[str, Any],
) -> ComputeRoutingDecision:
    """Build a policy decision from explicit task metadata only."""

    allowed = frozenset(CandidateTaskMetadata.model_fields)
    payload = {
        key: metadata[key] for key in sorted(metadata) if key in allowed and key != "task_id"
    }
    payload["task_id"] = task_id
    return ComputeRoutingPolicy().route(
        state,
        CandidateTaskMetadata.model_validate(payload),
    )


__all__ = [
    "ComputeProviderRegistry",
    "ProviderRoute",
    "ProviderRoutingError",
    "build_routing_decision",
]
