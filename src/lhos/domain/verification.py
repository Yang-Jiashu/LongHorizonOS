"""Verification spec and result models (spec section 14)."""

from typing import Any

from pydantic import BaseModel, Field


class VerificationSpec(BaseModel):
    """Spec section 14.3."""

    verifier_type: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 120
    required: bool = True

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "VerificationSpec":
        """Accept both the 14.3 shape (``verifier_type``/``parameters``) and the
        compact 8.1 planner shape (``type`` plus inline parameters)."""
        if "verifier_type" in raw:
            return cls.model_validate(raw)
        if "type" in raw:
            params = {k: v for k, v in raw.items() if k not in {"type", "timeout_seconds", "required"}}
            return cls(
                verifier_type=raw["type"],
                parameters=params,
                timeout_seconds=raw.get("timeout_seconds", 120),
                required=raw.get("required", True),
            )
        raise ValueError(f"unrecognized verification spec: {raw!r}")


class VerificationResult(BaseModel):
    """Outcome of a single verifier execution."""

    passed: bool
    summary: str = ""
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    pending: bool = False  # manual verification: node must wait for a human
