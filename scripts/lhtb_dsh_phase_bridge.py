"""A small, fail-closed telemetry bridge for DeepSeek Harness rc.8.

The bridge has two deliberately separate halves:

* :func:`build_phase_bridge_js` renders an agent-scoped Cordis plugin.  The
  plugin observes the public ``session/event`` and lifecycle events and emits
  only a typed, allow-listed NDJSON record.  It never serializes a message,
  tool arguments, model output, or an environment value.
* The Python helpers parse and validate that NDJSON after it crosses the
  process boundary.  Binding and invocation identity are checked again here,
  and the per-bridge sequence plus durable session sequence are checked for
  regressions.

This module is intentionally telemetry-only.  It does not poll a file and it
does not expose a command channel that could mutate a running Harness.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, TextIO

BRIDGE_SCHEMA_VERSION = "lhos-dsh-phase-bridge.v1"
"""Stable schema identifier for the bridge records."""

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z")
_INVOCATION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_EVENT_TYPE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_./:-]{0,127}\Z")
_STATUS = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")

_BINDING_KEYS = frozenset(
    {"claim_id", "attempt_id", "semantic_epoch", "lease_fencing_token"}
)
_USAGE_KEYS = frozenset(
    {
        "uncached_input_tokens",
        "output_tokens",
        "reasoning_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "verification_tokens",
        "model_calls",
        "tool_calls",
        "wall_time_ms",
        "monetary_microusd",
    }
)
_RECORD_KEYS = frozenset(
    {
        "schema_version",
        "record_type",
        "bridge_seq",
        "invocation_id",
        "binding",
        "session_id",
        "source_seq",
        "event_type",
        "turn",
        "step",
        "usage",
        "status",
        "tool_name",
        "tool_name_sha256",
        "compaction_id",
        "durability",
    }
)

# These names are rejected recursively by the Python boundary.  The runtime
# writer never emits them, but rejecting them here prevents a future JS edit or
# an untrusted producer from smuggling model text into a supposedly safe log.
_FORBIDDEN_KEYS = frozenset(
    {
        "content",
        "message",
        "arguments",
        "raw_output",
        "rawoutput",
        "prompt",
        "response",
        "credential",
        "api_key",
        "apikey",
        "secret",
        "password",
        "token",
    }
)


class BridgeValidationError(ValueError):
    """Raised when a bridge record violates the boundary contract."""


def _string(value: Any, *, name: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str):
        raise BridgeValidationError(f"{name} must be a string")
    value = value.strip()
    if not value or pattern.fullmatch(value) is None:
        raise BridgeValidationError(f"invalid {name}")
    return value


def _nonnegative_int(value: Any, *, name: str, allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    # bool is an int subclass but is not a valid wire integer.
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BridgeValidationError(f"{name} must be a non-negative integer")
    return value


@dataclass(frozen=True, slots=True)
class BridgeBinding:
    """The scheduler-owned identity attached to every bridge record."""

    claim_id: str
    attempt_id: str
    semantic_epoch: int
    lease_fencing_token: int | None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "claim_id",
            _string(self.claim_id, name="claim_id", pattern=_IDENTIFIER),
        )
        object.__setattr__(
            self,
            "attempt_id",
            _string(self.attempt_id, name="attempt_id", pattern=_IDENTIFIER),
        )
        object.__setattr__(
            self,
            "semantic_epoch",
            _nonnegative_int(self.semantic_epoch, name="semantic_epoch"),
        )
        object.__setattr__(
            self,
            "lease_fencing_token",
            _nonnegative_int(
                self.lease_fencing_token,
                name="lease_fencing_token",
                allow_none=True,
            ),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BridgeBinding":
        if not isinstance(value, Mapping):
            raise BridgeValidationError("binding must be an object")
        unknown = set(value) - _BINDING_KEYS
        missing = _BINDING_KEYS - set(value)
        if unknown:
            raise BridgeValidationError(f"unknown binding fields: {sorted(unknown)}")
        if missing:
            raise BridgeValidationError(f"missing binding fields: {sorted(missing)}")
        return cls(
            claim_id=value["claim_id"],
            attempt_id=value["attempt_id"],
            semantic_epoch=value["semantic_epoch"],
            lease_fencing_token=value["lease_fencing_token"],
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "attempt_id": self.attempt_id,
            "semantic_epoch": self.semantic_epoch,
            "lease_fencing_token": self.lease_fencing_token,
        }


def _normalise_usage(value: Mapping[str, Any] | None) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise BridgeValidationError("usage must be an object")
    unknown = set(value) - _USAGE_KEYS
    if unknown:
        raise BridgeValidationError(f"unknown usage fields: {sorted(unknown)}")
    result: dict[str, int] = {}
    for key, raw in value.items():
        parsed = _nonnegative_int(raw, name=f"usage.{key}")
        assert parsed is not None
        result[key] = parsed
    return dict(sorted(result.items()))


@dataclass(frozen=True, slots=True)
class BridgeRecord:
    """One sanitized, typed NDJSON record."""

    record_type: str
    bridge_seq: int
    invocation_id: str
    binding: BridgeBinding
    session_id: str
    source_seq: int | None = None
    event_type: str | None = None
    turn: int | None = None
    step: int | None = None
    usage: Mapping[str, int] = field(default_factory=dict)
    status: str | None = None
    tool_name: str | None = None
    tool_name_sha256: str | None = None
    compaction_id: str | None = None
    durability: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "record_type",
            _string(self.record_type, name="record_type", pattern=_STATUS),
        )
        object.__setattr__(
            self,
            "bridge_seq",
            _nonnegative_int(self.bridge_seq, name="bridge_seq"),
        )
        object.__setattr__(
            self,
            "invocation_id",
            _string(self.invocation_id, name="invocation_id", pattern=_INVOCATION_ID),
        )
        object.__setattr__(
            self,
            "session_id",
            _string(self.session_id, name="session_id", pattern=_IDENTIFIER),
        )
        object.__setattr__(
            self,
            "source_seq",
            _nonnegative_int(self.source_seq, name="source_seq", allow_none=True),
        )
        object.__setattr__(
            self,
            "turn",
            _nonnegative_int(self.turn, name="turn", allow_none=True),
        )
        object.__setattr__(
            self,
            "step",
            _nonnegative_int(self.step, name="step", allow_none=True),
        )
        object.__setattr__(self, "usage", _normalise_usage(self.usage))
        for name, pattern in (
            ("event_type", _EVENT_TYPE),
            ("status", _STATUS),
            ("durability", _STATUS),
            ("compaction_id", _IDENTIFIER),
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _string(value, name=name, pattern=pattern))
        if self.tool_name is not None:
            object.__setattr__(
                self,
                "tool_name",
                _string(self.tool_name, name="tool_name", pattern=_IDENTIFIER),
            )
        if self.tool_name_sha256 is not None:
            digest = _string(
                self.tool_name_sha256,
                name="tool_name_sha256",
                pattern=re.compile(r"[0-9a-f]{64}\Z"),
            )
            object.__setattr__(self, "tool_name_sha256", digest)
        if self.tool_name is not None and self.tool_name_sha256 is not None:
            raise BridgeValidationError("tool_name and tool_name_sha256 are mutually exclusive")
        if self.record_type == "session_event" and self.event_type is None:
            raise BridgeValidationError("session_event requires event_type")
        if self.record_type == "session_event" and self.source_seq is None:
            raise BridgeValidationError("session_event requires source_seq")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "BridgeRecord":
        if not isinstance(value, Mapping):
            raise BridgeValidationError("bridge record must be an object")
        _reject_forbidden_keys(value)
        unknown = set(value) - _RECORD_KEYS
        if unknown:
            raise BridgeValidationError(f"unknown record fields: {sorted(unknown)}")
        required = {"schema_version", "record_type", "bridge_seq", "invocation_id", "binding", "session_id"}
        missing = required - set(value)
        if missing:
            raise BridgeValidationError(f"missing record fields: {sorted(missing)}")
        if value["schema_version"] != BRIDGE_SCHEMA_VERSION:
            raise BridgeValidationError("unsupported bridge schema version")
        binding = BridgeBinding.from_mapping(value["binding"])
        return cls(
            record_type=value["record_type"],
            bridge_seq=value["bridge_seq"],
            invocation_id=value["invocation_id"],
            binding=binding,
            session_id=value["session_id"],
            source_seq=value.get("source_seq"),
            event_type=value.get("event_type"),
            turn=value.get("turn"),
            step=value.get("step"),
            usage=value.get("usage") or {},
            status=value.get("status"),
            tool_name=value.get("tool_name"),
            tool_name_sha256=value.get("tool_name_sha256"),
            compaction_id=value.get("compaction_id"),
            durability=value.get("durability"),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": BRIDGE_SCHEMA_VERSION,
            "record_type": self.record_type,
            "bridge_seq": self.bridge_seq,
            "invocation_id": self.invocation_id,
            "binding": self.binding.as_dict(),
            "session_id": self.session_id,
            "source_seq": self.source_seq,
            "event_type": self.event_type,
            "turn": self.turn,
            "step": self.step,
            "usage": dict(self.usage),
            "status": self.status,
            "tool_name": self.tool_name,
            "tool_name_sha256": self.tool_name_sha256,
            "compaction_id": self.compaction_id,
            "durability": self.durability,
        }


def _reject_forbidden_keys(value: Any, *, path: str = "record") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).replace("-", "_").lower()
            if normalized in _FORBIDDEN_KEYS:
                raise BridgeValidationError(f"forbidden field at {path}.{key}")
            _reject_forbidden_keys(child, path=f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _reject_forbidden_keys(child, path=f"{path}[{index}]")


def validate_phase_bridge_records(
    records: Iterable[BridgeRecord],
    *,
    expected_binding: BridgeBinding | None = None,
    expected_invocation_id: str | None = None,
    expected_session_id: str | None = None,
) -> tuple[BridgeRecord, ...]:
    """Validate identity and sequence continuity, returning an immutable tuple.

    ``bridge_seq`` must start at zero and increase by one.  Durable
    ``source_seq`` values are checked strictly monotonically per session; gaps
    are allowed because the bridge intentionally filters some event types.
    """

    normalized = tuple(records)
    invocation = (
        None
        if expected_invocation_id is None
        else _string(expected_invocation_id, name="expected_invocation_id", pattern=_INVOCATION_ID)
    )
    source_by_session: dict[str, int] = {}
    for index, record in enumerate(normalized):
        if not isinstance(record, BridgeRecord):
            raise BridgeValidationError("records must contain BridgeRecord values")
        if record.bridge_seq != index:
            raise BridgeValidationError(
                f"bridge sequence discontinuity at index {index}: {record.bridge_seq}"
            )
        if invocation is not None and record.invocation_id != invocation:
            raise BridgeValidationError("invocation_id does not match expected invocation")
        if expected_binding is not None and record.binding != expected_binding:
            raise BridgeValidationError("record binding does not match expected binding")
        if expected_session_id is not None and record.session_id != expected_session_id:
            raise BridgeValidationError("session_id does not match expected session")
        if record.record_type == "session_event":
            previous = source_by_session.get(record.session_id)
            assert record.source_seq is not None
            if previous is not None and record.source_seq <= previous:
                raise BridgeValidationError(
                    f"durable source sequence regressed for {record.session_id}"
                )
            source_by_session[record.session_id] = record.source_seq
    return normalized


def parse_phase_bridge_ndjson(
    payload: str | Iterable[str],
    *,
    expected_binding: BridgeBinding | None = None,
    expected_invocation_id: str | None = None,
    expected_session_id: str | None = None,
    allow_blank_lines: bool = True,
    max_line_bytes: int = 1_048_576,
) -> tuple[BridgeRecord, ...]:
    """Parse and validate bridge NDJSON from text or a line iterator."""

    if max_line_bytes < 1:
        raise ValueError("max_line_bytes must be positive")
    lines = payload.splitlines() if isinstance(payload, str) else payload
    records: list[BridgeRecord] = []
    for line_number, line in enumerate(lines, start=1):
        if not isinstance(line, str):
            raise BridgeValidationError(f"line {line_number} is not text")
        if not line.strip():
            if allow_blank_lines:
                continue
            raise BridgeValidationError(f"blank line at {line_number}")
        if len(line.encode("utf-8")) > max_line_bytes:
            raise BridgeValidationError(f"line {line_number} exceeds max_line_bytes")
        try:
            decoded = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BridgeValidationError(f"invalid JSON at line {line_number}") from exc
        records.append(BridgeRecord.from_mapping(decoded))
    return validate_phase_bridge_records(
        records,
        expected_binding=expected_binding,
        expected_invocation_id=expected_invocation_id,
        expected_session_id=expected_session_id,
    )


def summarize_phase_bridge(records: Iterable[BridgeRecord]) -> dict[str, Any]:
    """Return deterministic, aggregate-only telemetry suitable for reports."""

    validated = validate_phase_bridge_records(records)
    event_counts = Counter(
        record.event_type for record in validated if record.event_type is not None
    )
    record_type_counts = Counter(record.record_type for record in validated)
    usage_totals: Counter[str] = Counter()
    for record in validated:
        usage_totals.update(record.usage)
    source_values = [
        record.source_seq
        for record in validated
        if record.record_type == "session_event" and record.source_seq is not None
    ]
    return {
        "schema_version": BRIDGE_SCHEMA_VERSION,
        "record_count": len(validated),
        "invocation_ids": sorted({record.invocation_id for record in validated}),
        "session_ids": sorted({record.session_id for record in validated}),
        "record_type_counts": dict(sorted(record_type_counts.items())),
        "event_type_counts": dict(sorted(event_counts.items())),
        "tool_call_count": event_counts.get("tool/call", 0),
        "compaction_count": record_type_counts.get("compaction", 0),
        "durability_checkpoint_count": record_type_counts.get(
            "durability_checkpoint", 0
        ),
        "usage_totals": dict(sorted(usage_totals.items())),
        "source_seq_first": min(source_values) if source_values else None,
        "source_seq_last": max(source_values) if source_values else None,
    }


def _js_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _tool_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def build_phase_bridge_js(
    binding: BridgeBinding,
    invocation_id: str,
    *,
    session_id: str | None = None,
    tool_name_allowlist: Iterable[str] = (),
    output: str = "stderr",
    output_path: str | Path | None = None,
) -> str:
    """Render an rc.8 Cordis plugin that emits sanitized phase telemetry.

    ``output`` is ``stderr`` by default so the bridge cannot corrupt the
    headless runner's assistant stdout.  ``stdout`` is useful for a dedicated
    pipe, and ``file`` appends to ``output_path`` inside the container.
    """

    if not isinstance(binding, BridgeBinding):
        raise TypeError("binding must be BridgeBinding")
    invocation = _string(invocation_id, name="invocation_id", pattern=_INVOCATION_ID)
    selected_session = (
        None
        if session_id is None
        else _string(session_id, name="session_id", pattern=_IDENTIFIER)
    )
    if output not in {"stdout", "stderr", "file"}:
        raise ValueError("output must be stdout, stderr, or file")
    if output == "file" and output_path is None:
        raise ValueError("output_path is required when output='file'")
    if output != "file" and output_path is not None:
        raise ValueError("output_path is only valid when output='file'")
    path_literal = None if output_path is None else str(output_path)
    if path_literal is not None and (not path_literal or "\x00" in path_literal):
        raise ValueError("output_path must be a non-empty path without NUL")
    names: list[str] = []
    for name in tool_name_allowlist:
        names.append(_string(name, name="tool_name_allowlist entry", pattern=_IDENTIFIER))
    names = sorted(set(names))
    binding_json = _js_json(binding.as_dict())
    invocation_json = _js_json(invocation)
    session_json = _js_json(selected_session)
    names_json = _js_json(names)
    output_path_json = _js_json(path_literal)
    stream_expr = "process.stdout" if output == "stdout" else "process.stderr"
    return f'''/* Generated by LongHorizonOS; telemetry only, no control channel. */
import {{ appendFileSync }} from "node:fs";
import {{ createHash }} from "node:crypto";

export const name = "lhos-dsh-phase-bridge";
export const inject = ["agents"];

const SCHEMA_VERSION = {BRIDGE_SCHEMA_VERSION!r};
const INVOCATION_ID = {invocation_json};
const BINDING = Object.freeze({binding_json});
const FILTER_SESSION_ID = {session_json};
const TOOL_ALLOWLIST = new Set({names_json});
const OUTPUT_MODE = {output!r};
const OUTPUT_PATH = {output_path_json};
let bridgeSeq = 0;
const attached = new Map();

const USAGE_ALIASES = Object.freeze({{
  uncached_input_tokens: ["uncached_input_tokens", "uncachedInputTokens", "inputTokens"],
  output_tokens: ["output_tokens", "outputTokens"],
  reasoning_tokens: ["reasoning_tokens", "reasoningTokens"],
  cache_read_tokens: ["cache_read_tokens", "cacheReadTokens"],
  cache_write_tokens: ["cache_write_tokens", "cacheWriteTokens"],
  verification_tokens: ["verification_tokens", "verificationTokens"],
  model_calls: ["model_calls", "modelCalls"],
  tool_calls: ["tool_calls", "toolCalls"],
  wall_time_ms: ["wall_time_ms", "wallTimeMs"],
  monetary_microusd: ["monetary_microusd", "monetaryMicrousd"],
}});

function safeText(value, max = 256) {{
  return typeof value === "string" && value.length > 0 && value.length <= max
    ? value
    : undefined;
}}

function safeInteger(value) {{
  return Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}}

function usageOf(data) {{
  const source = data && typeof data === "object" && data.usage && typeof data.usage === "object"
    ? data.usage
    : undefined;
  if (!source) return {{}};
  const result = {{}};
  for (const [target, aliases] of Object.entries(USAGE_ALIASES)) {{
    for (const alias of aliases) {{
      const value = safeInteger(source[alias]);
      if (value !== undefined) {{ result[target] = value; break; }}
    }}
  }}
  return result;
}}

function toolHash(name) {{
  return createHash("sha256").update(name, "utf8").digest("hex");
}}

function writer() {{
  if (OUTPUT_MODE === "file") return (record) => appendFileSync(OUTPUT_PATH, JSON.stringify(record) + "\\n", {{ encoding: "utf8", flag: "a" }});
  const stream = {stream_expr};
  return (record) => stream.write(JSON.stringify(record) + "\\n");
}}
const write = writer();

function emit(recordType, session, event, extra = {{}}) {{
  const id = safeText(session && (session.id ?? session.header?.id));
  if (!id || (FILTER_SESSION_ID !== null && id !== FILTER_SESSION_ID)) return;
  const data = event && event.data && typeof event.data === "object" ? event.data : {{}};
  const record = {{
    schema_version: SCHEMA_VERSION,
    record_type: recordType,
    bridge_seq: bridgeSeq++,
    invocation_id: INVOCATION_ID,
    binding: BINDING,
    session_id: id,
    source_seq: safeInteger(event && event.seq) ?? null,
    event_type: safeText(event && event.type, 128) ?? null,
    turn: safeInteger(data.turn) ?? null,
    step: safeInteger(data.step) ?? null,
    usage: usageOf(data),
    status: null,
    tool_name: null,
    tool_name_sha256: null,
    compaction_id: null,
    durability: null,
    ...extra,
  }};
  write(record);
}}

function handleSessionEvent(session, event) {{
  if (!event || typeof event.type !== "string") return;
  const type = event.type;
  const data = event.data && typeof event.data === "object" ? event.data : {{}};
  if (type.startsWith("compaction/")) {{
    emit("compaction", session, event, {{
      status: safeText(type.slice("compaction/".length), 128) ?? "unknown",
      compaction_id: safeText(data.compactionId ?? data.id),
    }});
    return;
  }}
  if (type === "tool/call") {{
    const name = safeText(data.name, 256);
    emit("session_event", session, event, name === undefined ? {{}} : TOOL_ALLOWLIST.has(name)
      ? {{ tool_name: name }}
      : {{ tool_name_sha256: toolHash(name) }});
    return;
  }}
  emit("session_event", session, event);
}}

function handleFlush(session) {{
  const seq = safeInteger(session && session.seq);
  emit("durability_checkpoint", session, {{
    seq: seq === undefined ? undefined : Math.max(0, seq - 1),
    type: "session/flush",
    data: {{}},
  }}, {{
    source_seq: seq === undefined ? null : Math.max(0, seq - 1),
    event_type: "session/flush",
    durability: "session_flush",
    status: "flushed",
  }});
}}

function handleAgentStatus(agent, status) {{
  const session = agent && agent.session;
  emit("agent_status", session, {{ type: "agent/status", data: {{}} }}, {{ status: safeText(status, 128) ?? "unknown" }});
}}

function attach(agent) {{
  if (!agent || !agent.ctx || attached.has(agent)) return;
  if (FILTER_SESSION_ID !== null && String(agent.id) !== FILTER_SESSION_ID) return;
  const disposers = [];
  disposers.push(agent.ctx.on("session/event", handleSessionEvent));
  disposers.push(agent.ctx.on("session/flush", handleFlush));
  disposers.push(agent.ctx.on("agent/status", (payload) => handleAgentStatus(agent, payload?.status)));
  disposers.push(agent.ctx.on("agent/session-start", (payload) => emit("agent_lifecycle", agent.session,
    {{ type: "agent/session-start", data: {{}} }}, {{ status: safeText(payload?.source, 128) ?? "started" }})));
  disposers.push(agent.ctx.on("agent/error", (payload) => emit("agent_lifecycle", agent.session,
    {{ type: "agent/error", data: {{ turn: payload?.turn, step: payload?.step }} }}, {{ status: "error" }})));
  attached.set(agent, disposers);
}}

function detach(agent) {{
  const disposers = attached.get(agent);
  if (!disposers) return;
  attached.delete(agent);
  for (const dispose of disposers) {{ try {{ dispose?.(); }} catch {{ /* teardown is best effort */ }} }}
}}

export function apply(ctx) {{
  for (const agent of (ctx.agents?.list?.() ?? [])) attach(agent);
  const onCreated = ctx.on("agent/created", ({{ agent }}) => attach(agent));
  const onDisposed = ctx.on("agent/disposed", ({{ agent }}) => {{
    emit("agent_lifecycle", agent.session, {{ type: "agent/disposed", data: {{}} }}, {{ status: "disposed" }});
    detach(agent);
  }});
  return () => {{
    try {{ onCreated?.(); }} catch {{}}
    try {{ onDisposed?.(); }} catch {{}}
    for (const agent of [...attached.keys()]) detach(agent);
  }};
}}
'''


# Friendly aliases for callers that name the output after its Cordis role.
build_cordis_phase_bridge = build_phase_bridge_js
render_phase_bridge = build_phase_bridge_js


def _cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="NDJSON telemetry file")
    parser.add_argument("--claim-id", required=True)
    parser.add_argument("--attempt-id", required=True)
    parser.add_argument("--semantic-epoch", required=True, type=int)
    parser.add_argument("--lease-fencing-token", type=int)
    parser.add_argument("--invocation-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _cli_parser().parse_args(argv)
    binding = BridgeBinding(
        claim_id=args.claim_id,
        attempt_id=args.attempt_id,
        semantic_epoch=args.semantic_epoch,
        lease_fencing_token=args.lease_fencing_token,
    )
    records = parse_phase_bridge_ndjson(
        args.input.read_text(encoding="utf-8"),
        expected_binding=binding,
        expected_invocation_id=args.invocation_id,
    )
    json.dump(summarize_phase_bridge(records), sys.stdout, ensure_ascii=True, sort_keys=True)
    sys.stdout.write("\n")
    return 0


__all__ = [
    "BRIDGE_SCHEMA_VERSION",
    "BridgeBinding",
    "BridgeRecord",
    "BridgeValidationError",
    "build_cordis_phase_bridge",
    "build_phase_bridge_js",
    "main",
    "parse_phase_bridge_ndjson",
    "render_phase_bridge",
    "summarize_phase_bridge",
    "validate_phase_bridge_records",
]


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI.
    raise SystemExit(main())
