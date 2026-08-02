"""json_schema verifier with a minimal deterministic schema subset.

Supports: type (object/array/string/number/integer/boolean/null),
properties, required, items. Enough for artifact validation without pulling in
a full JSON Schema dependency.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext

_TYPE_CHECKS = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
}


def _validate(value: Any, schema: dict[str, Any], path: str, errors: list[str]) -> None:
    expected = schema.get("type")
    if expected and expected in _TYPE_CHECKS and not _TYPE_CHECKS[expected](value):
        errors.append(f"{path}: expected {expected}, got {type(value).__name__}")
        return
    if expected == "object":
        for req in schema.get("required", []):
            if req not in value:
                errors.append(f"{path}.{req}: required property missing")
        for key, subschema in schema.get("properties", {}).items():
            if key in value:
                _validate(value[key], subschema, f"{path}.{key}", errors)
    elif expected == "array" and "items" in schema:
        for i, item in enumerate(value):
            _validate(item, schema["items"], f"{path}[{i}]", errors)


class JsonSchemaVerifier:
    verifier_type = "json_schema"

    def verify(self, node: GraphNode, spec: VerificationSpec,
               context: VerificationContext) -> VerificationResult:
        rel = spec.parameters.get("path")
        schema = spec.parameters.get("schema")
        if not rel or not schema:
            return VerificationResult(
                passed=False, summary="json_schema: need path and schema"
            )
        file_path = (Path(context.workspace_dir).resolve() / rel).resolve()
        if not file_path.exists():
            return VerificationResult(passed=False, summary=f"json file missing: {rel}")
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return VerificationResult(passed=False, summary=f"invalid JSON: {exc}")
        errors: list[str] = []
        _validate(data, schema, "$", errors)
        passed = not errors
        evidence = []
        if passed:
            evidence.append(
                {
                    "evidence_type": "json_artifact",
                    "uri": str(file_path),
                    "summary": f"{rel} satisfies schema",
                    "metadata": {"path": rel},
                }
            )
        return VerificationResult(
            passed=passed,
            summary=f"json_schema({rel}): {'ok' if passed else '; '.join(errors)}",
            evidence=evidence,
        )
