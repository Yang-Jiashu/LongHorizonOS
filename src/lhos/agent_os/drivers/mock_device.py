"""Mock Device Driver — simulates tool execution with side-effect tracking.

Supports:
- PURE action
- IDEMPOTENT action
- NON_REVERSIBLE action
- side effect journal (independent effect store)
- inspect request status
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from lhos.agent_os.drivers.base import DriverInspect, DriverResult


class MockDeviceDriver:
    """Mock device driver with independent effect store."""

    device_type = "tool/mock"

    def __init__(self) -> None:
        # action_id → behavior config
        self._behaviors: dict[str, dict[str, Any]] = {}
        # Independent effect store: action_id → effect record
        # This simulates: "side effect happened but kernel completion event not yet persisted"
        self._effect_store: dict[str, dict[str, Any]] = {}
        self._dispatched: dict[str, bool] = {}
        self._idempotency_keys: dict[str, str] = {}
        self._default_behavior: str = "pure_success"

    def set_default_behavior(self, behavior: str) -> None:
        self._default_behavior = behavior

    def configure(
        self,
        action_id: str,
        behavior: Literal[
            "pure_success",
            "idempotent_success",
            "non_reversible_success",
            "delayed_success",
            "deterministic_failure",
            "crash_after_effect",
        ],
        delay: float = 0.0,
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        self._behaviors[action_id] = {
            "behavior": behavior,
            "delay": delay,
            "output": output or {"result": "ok"},
            "error": error,
            "idempotency_key": idempotency_key,
        }

    async def dispatch(
        self,
        action_id: str,
        operation: str,
        arguments: dict[str, Any],
    ) -> DriverResult:
        config = self._behaviors.get(
            action_id,
            {
                "behavior": self._default_behavior,
                "delay": 0.0,
                "output": {"result": "ok"},
                "error": None,
                "idempotency_key": None,
            },
        )
        behavior = config["behavior"]
        delay = config.get("delay", 0.0)

        self._dispatched[action_id] = True

        if behavior == "pure_success":
            return DriverResult(status="completed", output=config["output"])

        if behavior == "idempotent_success":
            key = config.get("idempotency_key")
            if key and key in self._idempotency_keys:
                # Already executed with this key
                prev_action = self._idempotency_keys[key]
                return DriverResult(
                    status="completed",
                    output={
                        "result": "ok",
                        "deduplicated": True,
                        "original_action_id": prev_action,
                    },
                )
            if key:
                self._idempotency_keys[key] = action_id
            # Record effect
            self._effect_store[action_id] = {"operation": operation, "arguments": arguments}
            return DriverResult(status="completed", output=config["output"])

        if behavior == "non_reversible_success":
            # Record effect
            self._effect_store[action_id] = {"operation": operation, "arguments": arguments}
            return DriverResult(status="completed", output=config["output"])

        if behavior == "delayed_success":
            if delay:
                await asyncio.sleep(delay)
            # Record effect
            self._effect_store[action_id] = {"operation": operation, "arguments": arguments}
            return DriverResult(status="completed", output=config["output"])

        if behavior == "deterministic_failure":
            return DriverResult(
                status="failed",
                error=config["error"] or {"reason": "deterministic_failure"},
            )

        if behavior == "crash_after_effect":
            # Record side effect
            self._effect_store[action_id] = {"operation": operation, "arguments": arguments}
            # Return unknown to simulate crash
            return DriverResult(
                status="unknown",
                error={"reason": "crash_after_effect"},
                side_effect_recorded=True,
            )

        return DriverResult(status="completed", output=config["output"])

    async def inspect(self, action_id: str) -> DriverInspect:
        if action_id in self._effect_store:
            return DriverInspect(
                status="completed",
                output={"result": "effect_verified"},
            )
        if action_id in self._dispatched:
            return DriverInspect(status="running")
        return DriverInspect(status="unknown")

    def has_effect(self, action_id: str) -> bool:
        return action_id in self._effect_store

    def get_effect(self, action_id: str) -> dict[str, Any] | None:
        return self._effect_store.get(action_id)

    def reset(self) -> None:
        self._behaviors.clear()
        self._effect_store.clear()
        self._dispatched.clear()
        self._idempotency_keys.clear()
