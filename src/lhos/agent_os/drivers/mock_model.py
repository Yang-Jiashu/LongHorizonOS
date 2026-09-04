"""Mock Model Driver — simulates LLM calls with scripted responses.

Supports:
- immediate success
- delayed success
- deterministic failure
- timeout
- crash-after-effect
- inspect returns completed/running/unknown
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from lhos.agent_os.drivers.base import DriverInspect, DriverResult


class MockModelDriver:
    """Mock model driver with configurable behavior per action."""

    device_type = "model/mock"

    def __init__(self) -> None:
        # action_id → behavior config
        self._behaviors: dict[str, dict[str, Any]] = {}
        # action_id → effect store (simulates side effects already happened)
        self._effects: dict[str, dict[str, Any]] = {}
        # action_id → dispatch state
        self._dispatched: dict[str, bool] = {}
        self._default_behavior: str = "immediate_success"

    def configure(
        self,
        action_id: str,
        behavior: Literal[
            "immediate_success",
            "delayed_success",
            "deterministic_failure",
            "timeout",
            "crash_after_effect",
        ],
        delay: float = 0.0,
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        self._behaviors[action_id] = {
            "behavior": behavior,
            "delay": delay,
            "output": output or {"response": "mock_output"},
            "error": error,
        }

    def set_default_behavior(self, behavior: str) -> None:
        self._default_behavior = behavior

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
                "output": {"response": "mock_output"},
                "error": None,
            },
        )
        behavior = config["behavior"]
        delay = config.get("delay", 0.0)

        self._dispatched[action_id] = True

        if behavior == "immediate_success":
            return DriverResult(
                status="completed",
                output=config["output"],
            )

        if behavior == "delayed_success":
            if delay:
                await asyncio.sleep(delay)
            return DriverResult(
                status="completed",
                output=config["output"],
            )

        if behavior == "deterministic_failure":
            return DriverResult(
                status="failed",
                error=config["error"] or {"reason": "deterministic_failure"},
            )

        if behavior == "timeout":
            if delay:
                await asyncio.sleep(delay)
            return DriverResult(
                status="failed",
                error={"reason": "timeout"},
            )

        if behavior == "crash_after_effect":
            # Record the side effect
            self._effects[action_id] = {"operation": operation, "arguments": arguments}
            # Return "unknown" to simulate crash before completion
            return DriverResult(
                status="unknown",
                output={},
                error={"reason": "crash_after_effect"},
                side_effect_recorded=True,
            )

        return DriverResult(status="completed", output=config["output"])

    async def inspect(self, action_id: str) -> DriverInspect:
        # Check if effect was recorded
        if action_id in self._effects:
            # Effect happened — we know it completed
            return DriverInspect(
                status="completed",
                output={"response": "effect_verified"},
            )
        if action_id in self._dispatched:
            return DriverInspect(status="running")
        return DriverInspect(status="unknown")

    def has_effect(self, action_id: str) -> bool:
        return action_id in self._effects

    def reset(self) -> None:
        self._behaviors.clear()
        self._effects.clear()
        self._dispatched.clear()
