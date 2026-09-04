"""Harbor custom agent for running a mounted Linux DeepSeek Harness in LHTB.

The adapter keeps Harbor in charge of the task container and hidden verifier.
DSH runs inside that same container with ``cwd=/app`` so its native filesystem
and shell tools observe the real benchmark environment.

Experiment arms are deliberately different:

* ``baseline`` starts a fresh DSH home/session after every Harbor rejection.
* ``lhos`` requires ``HB_CONTINUE_MODE=same_conversation`` and normally
  resumes the exact persisted DSH session through ``ctx.agents.resume()``.
  A bounded semantic-context policy may instead checkpoint that cognition,
  start a fresh DSH home against the preserved workspace, and then resume the
  replacement session on later continuations.

Stock DSH rc.8 ``headless`` is one-shot and does not implement ``--resume``.
For the LHOS arm this module writes a small resume-aware Cordis runner into the
mounted agent log directory and applies it only to continuation invocations.
Each resumed generation is accepted only when the same JSONL file keeps the
same session header and gains durable events. Usage remains cumulative across
controlled session generations.

Credentials are resolved by Harbor from ``agent.env`` and transferred through
short-lived, mode-0600 files. Secret values never appear in Docker command
arguments, metadata, logs, or durable artifacts.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import shlex
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from uuid import uuid4

from harbor.agents.installed.base import BaseInstalledAgent, NonZeroAgentExitCodeError
from harbor.environments.base import BaseEnvironment
from harbor.models.agent.context import AgentContext

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from lhos.integrations.harness import DeepSeekTraceSummary, parse_deepseek_sessions
from lhos.integrations.harness.optimization import (
    HarnessContinuationAction,
    HarnessPhaseObservation,
    HarnessQualityProbe,
    SemanticContextPolicy,
    build_semantic_handoff,
)
from lhos.integrations.harness.protocol import HarnessUsage
from lhos.integrations.harness.optimization_v2 import SemanticContextPolicyV2

Arm = Literal["baseline", "lhos"]

_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_PROFILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SESSION_ID = re.compile(r"session-[A-Za-z0-9-]+\Z")
_SENSITIVE_ENV_NAME = re.compile(
    r"(?:API[_-]?KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|AUTH)",
    re.IGNORECASE,
)
_REMAINING_SECONDS = re.compile(
    r"(?:approximately\s+)?([0-9]+)\s+seconds?\s+(?:remain|remaining)",
    re.IGNORECASE,
)
_SCHEMA_VERSION = "lhos-lhtb-dsh-harbor-agent.v1"
_SEMANTIC_CONTROL_SCHEMA_VERSION = "lhos-dsh-semantic-control.v1"
_RESUME_RUNNER_VERSION = "lhos-dsh-resume-runner.v1"
_SLICE_EXIT_CODE = 197
_SLICE_OUTER_BUDGET_GUARD_SECONDS = 15
_FINAL_SHORT_SLICE_MAX_SECONDS = 10

# 漏洞C修复：one-shot / full_budget 任务在 lhos 臂下也强制使用的
# initial-run 控制时间片（秒）。与现有 sliced 任务口径一致，让语义决策
# 在会话内周期性介入，而不是让 initial run 一次跑满 max-tokens。
_INITIAL_CONTROL_SLICE_SECONDS = 75.0
# Live heartbeat: the full observability file is only refreshed at event
# boundaries (resume/verifier/invocation end), so a single long invocation is
# invisible to any outer controller for its whole duration. A periodic
# lightweight heartbeat closes that gap so an outer OS can observe real-time
# progress (events/tokens/model calls) of an in-flight invocation.
_HEARTBEAT_SCHEMA_VERSION = "lhos-dsh-heartbeat.v1"
_DEFAULT_HEARTBEAT_SECONDS = 10.0

_RESUME_RUNNER_TEMPLATE = r"""
import { readFileSync } from "node:fs";
import { installModelSelection } from "__MODULES_URL__/@deepseek-ai/dsh-agent/lib/index.js";
import { createUserMessage } from "__MODULES_URL__/@deepseek-ai/dsh-llm/lib/index.js";
import { SessionId } from "__MODULES_URL__/@deepseek-ai/dsh-session/lib/index.js";

export const name = "lhos-dsh-resume-runner";
export const inject = [
  "agentDefaultModel",
  "agents",
  "sessions",
  "sessionPersistence",
];

function summarize(events, firstSeq) {
  let started = false;
  let text = "";
  let reason;
  for (const event of events) {
    if (event.seq < firstSeq) continue;
    if (event.type === "turn/start") {
      started = true;
      continue;
    }
    if (!started) continue;
    if (event.type === "assistant/message") {
      const joined = event.data.message.content
        .filter((block) => block.type === "text")
        .map((block) => block.text)
        .join("");
      if (joined !== "") text = joined;
    }
    if (event.type === "turn/end") reason = event.data.reason;
  }
  return { text, reason };
}

async function run(ctx, io) {
  await ctx.get("loader")?.await();
  const agents = ctx.get("agents");
  const defaultModel = ctx.get("agentDefaultModel");
  const sessions = ctx.get("sessions");
  if (
    agents === undefined ||
    defaultModel === undefined ||
    sessions === undefined
  ) {
    throw new Error("LHOS resume runner dependencies are unavailable");
  }

  const rawSessionId = process.env.LHOS_DSH_RESUME_SESSION_ID ?? "";
  const taskFile = process.env.LHOS_DSH_RESUME_TASK_FILE ?? "";
  if (!/^session-[A-Za-z0-9-]+$/.test(rawSessionId)) {
    throw new Error("LHOS_DSH_RESUME_SESSION_ID is invalid");
  }
  if (taskFile === "") {
    throw new Error("LHOS_DSH_RESUME_TASK_FILE is required");
  }
  const task = readFileSync(taskFile, "utf8");
  if (task.trim() === "") {
    throw new Error("resume task is empty");
  }

  const selection = defaultModel.currentSelection();
  const agentOptions = {
    provider: selection.provider,
    model: selection.model,
  };
  const setup = (agentCtx) => {
    installModelSelection(agentCtx, {
      current: selection,
      assembled: undefined,
    });
  };
  const { agent } = await agents.resume({
    resumeSessionId: SessionId(rawSessionId),
    agentOptions,
    setup,
  });

  await agent.whenIdle();
  const firstSeq = agent.session.seq;
  agent.followup(
    createUserMessage({
      content: [{ type: "text", text: task }],
      source: { kind: "user" },
    }),
  );
  await agent.whenIdle();
  await sessions.flush(agent.session);
  const outcome = summarize(agent.session.events, firstSeq);
  io.stdout.write(outcome.text + "\n");
  if (outcome.reason?.kind === "error") {
    io.stderr.write(
      `dsh: ${outcome.reason.error.code}: ${outcome.reason.error.message}\n`,
    );
  }
  io.exit(outcome.reason?.kind === "completed" ? 0 : 1);
}

export function apply(ctx) {
  const exit = ctx.get("appExit");
  if (exit === undefined) {
    throw new Error("LHOS resume runner requires ctx.appExit");
  }
  const io = {
    stdout: process.stdout,
    stderr: process.stderr,
    exit,
  };
  run(ctx, io).catch((error) => {
    io.stderr.write(
      `dsh: ${error instanceof Error ? error.message : String(error)}\n`,
    );
    io.exit(1);
  });
}
""".strip()


def _sessions_fingerprint(root: Path) -> tuple[tuple[str, int, int], ...] | None:
    """Content fingerprint of the session JSONL tree for parse memoization.

    Session files are append-only, so (path, size, mtime_ns) fully determines
    what a re-parse would see.  Returns ``None`` (never cacheable) if the
    listing itself fails.
    """

    if not root.exists():
        return ()
    try:
        entries: list[tuple[str, int, int]] = []
        for path in _long_path(root).rglob("*.jsonl"):
            try:
                stat_result = path.stat()
            except OSError:
                return None
            entries.append(
                (str(path), stat_result.st_size, stat_result.st_mtime_ns)
            )
    except OSError:
        return None
    entries.sort()
    return tuple(entries)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _long_path(path: Path) -> Path:
    if os.name != "nt":
        return path
    resolved = str(path.resolve())
    return Path(resolved if resolved.startswith("\\\\?\\") else "\\\\?\\" + resolved)


def _validate_env_name(value: str) -> str:
    normalized = str(value).strip()
    if not _ENV_NAME.fullmatch(normalized):
        raise ValueError(f"invalid environment variable name: {value!r}")
    return normalized


def _validate_profile(value: str) -> str:
    normalized = str(value).strip()
    if not _PROFILE_NAME.fullmatch(normalized):
        raise ValueError(f"invalid DSH profile name: {value!r}")
    if normalized != "headless":
        raise ValueError("LHTB DSH agent currently requires the headless profile")
    return normalized


def _validate_container_path(
    value: str,
    *,
    label: str,
    under: PurePosixPath | None = None,
) -> PurePosixPath:
    raw = str(value).strip()
    if not raw.startswith("/") or "\x00" in raw or "\n" in raw or "\r" in raw:
        raise ValueError(f"{label} must be an absolute POSIX path")
    candidate = PurePosixPath(raw)
    if ".." in candidate.parts:
        raise ValueError(f"{label} must not contain '..'")
    if under is not None:
        try:
            candidate.relative_to(under)
        except ValueError as exc:
            raise ValueError(f"{label} must be under {under}") from exc
    return candidate


def _safe_relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except (OSError, ValueError):
        return path.name


def _session_header(path: Path) -> dict[str, Any]:
    with _long_path(path).open("r", encoding="utf-8") as stream:
        first = stream.readline()
    try:
        payload = json.loads(first)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"DSH session has an invalid first JSONL row: {path.name}") from exc
    if not isinstance(payload, dict) or payload.get("type") != "session":
        raise RuntimeError(f"DSH session is missing its first-row session header: {path.name}")
    return payload


def _harness_usage_payload(usage: HarnessUsage) -> dict[str, int]:
    return {
        "uncached_input_tokens": usage.uncached_input_tokens,
        "cache_read_tokens": usage.cache_read_tokens,
        "cache_write_tokens": usage.cache_write_tokens,
        "output_tokens": usage.output_tokens,
        "reasoning_tokens": usage.reasoning_tokens,
        "model_calls": usage.model_calls,
        "tool_calls": usage.tool_calls,
        "total_token_units": usage.total_token_units,
    }


_TEST_CMD_PATTERN = re.compile(
    r"(pytest|npm (run )?test|cargo test|go test|make\b|cmake --build|"
    r"gradle test|mvn test|./test|run_tests|python -m test)",
    re.IGNORECASE,
)


def _is_test_command(tool: Any) -> bool:
    """Heuristic: does this tool call look like running a test/build/verify?"""
    if tool.name not in {"bash", "shell", "exec", "run_command", "command", "Terminal"}:
        return False
    args = tool.arguments or {}
    command = str(
        args.get("command")
        or args.get("cmd")
        or args.get("input")
        or args.get("script")
        or ""
    )
    return bool(_TEST_CMD_PATTERN.search(command))


def _usage_payload(trace: DeepSeekTraceSummary) -> dict[str, int]:
    return _harness_usage_payload(trace.usage)


def _usage_delta(current: HarnessUsage, previous: HarnessUsage) -> HarnessUsage:
    """Return a fail-closed non-negative phase delta for a cumulative trace."""

    values: dict[str, int | None] = {}
    for field in (
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
    ):
        values[field] = max(0, int(getattr(current, field)) - int(getattr(previous, field)))
    if current.cpu_time_ms is None or previous.cpu_time_ms is None:
        values["cpu_time_ms"] = None
    else:
        values["cpu_time_ms"] = max(0, current.cpu_time_ms - previous.cpu_time_ms)
    return HarnessUsage(**values)


def _reason_kind(trace: DeepSeekTraceSummary) -> str:
    if not trace.turn_end_reasons:
        return ""
    latest = trace.turn_end_reasons[-1]
    if not isinstance(latest, dict):
        return ""
    return str(latest.get("kind", "") or "").strip().lower()


def _structured_provider_failure(
    trace: DeepSeekTraceSummary,
) -> dict[str, Any] | None:
    """Project a structured provider policy failure without prompt content."""

    if not trace.turn_end_reasons:
        return None
    latest = trace.turn_end_reasons[-1]
    if not isinstance(latest, dict) or str(latest.get("kind", "")).lower() != "error":
        return None
    error = latest.get("error") or latest.get("failure")
    if not isinstance(error, dict):
        return None
    message = str(error.get("message", "") or "")
    code = str(error.get("code", "") or "").strip()
    lowered = message.lower()
    status_match = re.search(r"\b([1-5][0-9]{2})\b", message)
    status_code = int(status_match.group(1)) if status_match else None
    if status_code != 451 and not any(
        token in lowered
        for token in (
            "censorship_blocked",
            "content policy",
            "content_policy",
            "moderation blocked",
        )
    ):
        return None
    safe_code = re.sub(r"[^A-Za-z0-9_.:-]+", "_", code)[:128] or None
    return {
        "outcome": "provider_censored",
        "failure_class": "content_policy",
        "status_code": status_code or 451,
        "provider_code": safe_code,
        "retryable": False,
        "source": "structured_turn_end",
        "message_sha256": hashlib.sha256(message.encode("utf-8")).hexdigest(),
    }


def _has_recoverable_max_tokens_checkpoint(
    trace: DeepSeekTraceSummary,
    *,
    previous_event_count: int = 0,
    previous_model_calls: int = 0,
) -> bool:
    """Require durable cognition and charged model work before continuing."""

    return bool(
        _reason_kind(trace) == "max-tokens"
        and trace.session_id
        and len(trace.session_files) == 1
        and trace.event_count > previous_event_count
        and trace.usage.model_calls > previous_model_calls
        and trace.usage.total_token_units > 0
    )


def _remaining_budget_seconds(instruction: str) -> int | None:
    matches = _REMAINING_SECONDS.findall(instruction)
    if not matches:
        return None
    return max(0, int(matches[-1]))


def _effective_slice_seconds(
    configured: float | None,
    instruction: str,
) -> float | None:
    """Keep a continuation slice inside Harbor's remaining outer budget."""

    if configured is None:
        return None
    lowered = instruction.lower()
    if (
        "verification failed" not in lowered
        and "submitted solution did not pass verification" not in lowered
    ):
        return configured
    remaining = _remaining_budget_seconds(instruction)
    if remaining is None:
        return configured
    # Harbor must still regain control, persist the trace, and run its
    # verifier before the outer Agent timeout. A 5-second guard was too small
    # in real Docker runs: 5/6 resumed 120-second cases were cancelled while
    # shutting down a nominal 52-second final slice. Keep a conservative
    # wrapper budget so the durable checkpoint can actually be observed.
    return min(
        configured,
        float(max(1, remaining - _SLICE_OUTER_BUDGET_GUARD_SECONDS)),
    )


def _build_semantic_policy(
    policy_version: int, config: dict[str, Any]
) -> Any:
    """Instantiate the configured continuation policy (v1 default, v2 opt-in).

    v1 knobs are translated onto the v2 field names; v2-only knobs
    (hard_cache_ratio, restart_rate_limit_phases, min_handoff_items, ...) take
    the v2 defaults unless the config explicitly carries them.  The v1
    ``max_restarts`` cap is deliberately NOT inherited: v2 replaces the
    absolute cap with rate-limited severe restarts plus a high backstop.
    """

    if policy_version == 1:
        # v2-only knobs (``v2_*`` keys) must not leak into the v1 constructor.
        return SemanticContextPolicy(
            **{key: value for key, value in config.items() if not key.startswith("v2_")}
        )
    if policy_version != 2:
        raise ValueError(f"unknown semantic_policy_version: {policy_version}")
    key_map = {
        "soft_cache_tokens_per_call": "cache_tokens_per_call_threshold",
        "min_phases_before_restart": "min_phases_before_restart",
        "cooldown_phases": "cooldown_phases",
        "payoff_rebloat_ratio": "restart_payoff_rebloat_ratio",
        "payoff_window_phases": "restart_payoff_window_phases",
        "max_handoff_items": "max_handoff_items",
        "max_handoff_chars": "max_handoff_chars",
        "recent_handoff_phases": "recent_handoff_phases",
    }
    v2_config = {
        v2_key: config[v1_key]
        for v2_key, v1_key in key_map.items()
        if v1_key in config
    }
    for v2_key in (
        "hard_cache_ratio",
        "restart_rate_limit_phases",
        "max_restarts",
        "min_handoff_items",
        "loop_repeat_streak",
        "loop_no_write_phases",
    ):
        if v2_key in config:
            v2_config[v2_key] = config[v2_key]
    # Second-round v2 knobs are carried under a ``v2_`` prefix so the shared
    # config dict can never collide with v1 field names.
    for v2_key in (
        "convergence_halt_enabled",
        "stop_loss_halt_enabled",
        "convergence_window_phases",
        "convergence_min_session_phases",
        "convergence_event_ratio",
        "stop_loss_cache_tokens",
        "stop_loss_min_session_phases",
        "max_converging_deferrals",
        "early_spin_phases",
        "early_spin_min_tool_calls",
        "early_spin_error_ratio",
        "slice_control_soft_ratio",
        "slice_relax_soft_ratio",
        "slice_upscale_factor",
        "slice_downscale_factor",
        "slice_min_seconds",
        "slice_max_seconds",
    ):
        config_key = f"v2_{v2_key}"
        if config_key in config:
            v2_config[v2_key] = config[config_key]
    return SemanticContextPolicyV2(**v2_config)


class LHTBDeepSeekHarnessAgent(BaseInstalledAgent):
    """Run DSH rc.8 inside a mounted LHTB Linux task container."""

    SUPPORTS_ATIF = False
    SUPPORTS_WINDOWS = False

    @staticmethod
    def name() -> str:
        return "lhos-deepseek-harness"

    def __init__(
        self,
        logs_dir: Path,
        model_name: str | None = None,
        *,
        arm: Arm = "baseline",
        bundle_root: str = "/opt/lhos-dsh",
        node_path: str = "/opt/lhos-dsh/bin/node",
        dsh_path: str = "/opt/lhos-dsh/node_modules/@deepseek-ai/dsh/lib/bin.js",
        patch_path: str = "/opt/lhos-dsh/patches/stepfun-3.7.cordis.patch.yml",
        credential_env: str = "STEPFUN_API_KEY",
        profile: str = "headless",
        workdir: str = "/app",
        expected_dsh_version: str = "0.1.0-rc.8",
        permission_mode: str = "workspace-write",
        tools_mode: str = "native",
        require_container_internet: bool = True,
        controlled_pair_mode: bool = False,
        time_slice_seconds: float | None = None,
        semantic_context_control: bool = True,
        semantic_context_min_phases_before_restart: int = 2,
        semantic_context_cache_tokens_per_call_threshold: int = 16_000,
        semantic_context_cache_growth_ratio_threshold: float = 1.75,
        semantic_context_max_consecutive_max_tokens: int = 2,
        semantic_context_cumulative_cache_read_tokens_threshold: int = 96_000,
        semantic_context_cumulative_cache_requires_max_tokens: int = 2,
        semantic_context_no_progress_phases: int = 2,
        semantic_context_no_progress_event_ratio_threshold: float = 0.10,
        semantic_context_quality_stall_phases: int = 3,
        semantic_context_quality_stall_error_ratio: float = 0.5,
        semantic_context_quality_stall_min_test_calls: int = 1,
        semantic_context_quality_stall_write_regression_phases: int = 2,
        semantic_context_cooldown_phases: int = 2,
        semantic_context_max_restarts: int = 6,
        semantic_context_restart_unproductive_restarts: int = 2,
        semantic_context_restart_payoff_window_phases: int = 3,
        semantic_context_restart_payoff_rebloat_ratio: float = 1.0,
        semantic_context_restart_payoff_failures: int = 2,
        semantic_handoff_max_items: int = 12,
        semantic_handoff_max_chars: int = 2_048,
        semantic_policy_version: int = 1,
        # v2-only knobs (ignored when semantic_policy_version=1).  Range
        # validation lives in SemanticContextPolicyV2.__post_init__.
        semantic_v2_convergence_halt_enabled: bool = False,
        semantic_v2_stop_loss_halt_enabled: bool = True,
        semantic_v2_convergence_window_phases: int = 4,
        semantic_v2_convergence_min_session_phases: int = 6,
        semantic_v2_convergence_event_ratio: float = 0.02,
        semantic_v2_stop_loss_cache_tokens: int = 8_000_000,
        semantic_v2_stop_loss_min_session_phases: int = 4,
        semantic_v2_max_converging_deferrals: int = 3,
        semantic_v2_early_spin_phases: int = 6,
        semantic_v2_early_spin_min_tool_calls: int = 10,
        semantic_v2_early_spin_error_ratio: float = 0.8,
        semantic_v2_adaptive_slice_enabled: bool = True,
        semantic_v2_slice_control_soft_ratio: float = 0.75,
        semantic_v2_slice_relax_soft_ratio: float = 0.5,
        semantic_v2_slice_upscale_factor: float = 2.0,
        # 1.0 = downscale off (watchdog interaction regression, see optimization_v2)
        semantic_v2_slice_downscale_factor: float = 1.0,
        semantic_v2_slice_min_seconds: float = 30.0,
        semantic_v2_slice_max_seconds: float = 240.0,
        extra_env: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        normalized_arm = str(arm).strip().lower()
        if normalized_arm not in {"baseline", "lhos"}:
            raise ValueError("arm must be 'baseline' or 'lhos'")
        self.arm: Arm = normalized_arm  # type: ignore[assignment]

        self.controlled_pair_mode = bool(controlled_pair_mode)
        continue_mode = os.environ.get("HB_CONTINUE_MODE", "").strip().lower()
        if self.controlled_pair_mode and continue_mode != "same_conversation":
            raise ValueError(
                "controlled_pair_mode requires HB_CONTINUE_MODE=same_conversation"
            )
        if self.arm == "lhos" and continue_mode != "same_conversation":
            raise ValueError(
                "LHOS arm requires HB_CONTINUE_MODE=same_conversation in the Harbor process"
            )
        if (
            self.arm == "baseline"
            and continue_mode == "same_conversation"
            and not self.controlled_pair_mode
        ):
            raise ValueError("baseline arm must not run with HB_CONTINUE_MODE=same_conversation")

        self.credential_env = _validate_env_name(credential_env)
        inherited = dict(extra_env or {})
        unexpected_sensitive = sorted(
            name
            for name in inherited
            if _SENSITIVE_ENV_NAME.search(name) and name != self.credential_env
        )
        if unexpected_sensitive:
            raise ValueError(
                "agent.env may contain only the configured provider credential; "
                f"unexpected sensitive variables: {unexpected_sensitive}"
            )
        credential_value = inherited.pop(self.credential_env, None)
        if inherited:
            raise ValueError(
                "agent.env may contain only the configured provider credential; "
                f"unexpected variables: {sorted(inherited)}"
            )
        if credential_value is not None:
            normalized_credential = str(credential_value)
            if not normalized_credential.strip():
                raise ValueError(f"{self.credential_env} must be non-empty")
            if any(char in normalized_credential for char in "\x00\r\n"):
                raise ValueError(f"{self.credential_env} must be a single line")
        self._credential_value = None if credential_value is None else str(credential_value)
        # Do not retain the secret in a local variable: Harbor's rich traceback
        # renderer may include locals when a trial times out.
        credential_value = None

        self.bundle_root = _validate_container_path(bundle_root, label="bundle_root")
        self.node_path = _validate_container_path(
            node_path,
            label="node_path",
            under=self.bundle_root,
        )
        self.dsh_path = _validate_container_path(
            dsh_path,
            label="dsh_path",
            under=self.bundle_root,
        )
        self.patch_path = _validate_container_path(
            patch_path,
            label="patch_path",
            under=self.bundle_root,
        )
        self.workdir = _validate_container_path(workdir, label="workdir")
        self.profile = _validate_profile(profile)

        self.expected_dsh_version = str(expected_dsh_version).strip()
        if not self.expected_dsh_version or any(
            char in self.expected_dsh_version for char in "\x00\r\n"
        ):
            raise ValueError("expected_dsh_version must be a non-empty single line")
        self.permission_mode = str(permission_mode).strip()
        self.tools_mode = str(tools_mode).strip()
        if self.permission_mode != "workspace-write":
            raise ValueError("LHTB DSH agent requires permission_mode='workspace-write'")
        if self.tools_mode != "native":
            raise ValueError("LHTB DSH agent requires tools_mode='native'")
        if time_slice_seconds is not None and float(time_slice_seconds) <= 0:
            raise ValueError("time_slice_seconds must be positive or None")
        self.time_slice_seconds = None if time_slice_seconds is None else float(time_slice_seconds)

        semantic_config = {
            "min_phases_before_restart": int(semantic_context_min_phases_before_restart),
            "cache_tokens_per_call_threshold": int(
                semantic_context_cache_tokens_per_call_threshold
            ),
            "cache_growth_ratio_threshold": float(
                semantic_context_cache_growth_ratio_threshold
            ),
            "max_consecutive_max_tokens": int(
                semantic_context_max_consecutive_max_tokens
            ),
            "cumulative_cache_read_tokens_threshold": int(
                semantic_context_cumulative_cache_read_tokens_threshold
            ),
            "cumulative_cache_requires_max_tokens": int(
                semantic_context_cumulative_cache_requires_max_tokens
            ),
            "no_progress_phases": int(semantic_context_no_progress_phases),
            "no_progress_event_ratio_threshold": float(
                semantic_context_no_progress_event_ratio_threshold
            ),
            "quality_stall_phases": int(semantic_context_quality_stall_phases),
            "quality_stall_error_ratio": float(
                semantic_context_quality_stall_error_ratio
            ),
            "quality_stall_min_test_calls": int(
                semantic_context_quality_stall_min_test_calls
            ),
            "quality_stall_write_regression_phases": int(
                semantic_context_quality_stall_write_regression_phases
            ),
            "cooldown_phases": int(semantic_context_cooldown_phases),
            "max_restarts": int(semantic_context_max_restarts),
            "restart_unproductive_restarts": int(
                semantic_context_restart_unproductive_restarts
            ),
            "restart_payoff_window_phases": int(
                semantic_context_restart_payoff_window_phases
            ),
            "restart_payoff_rebloat_ratio": float(
                semantic_context_restart_payoff_rebloat_ratio
            ),
            "restart_payoff_failures": int(
                semantic_context_restart_payoff_failures
            ),
            "max_handoff_items": int(semantic_handoff_max_items),
            "max_handoff_chars": int(semantic_handoff_max_chars),
            "v2_convergence_halt_enabled": bool(semantic_v2_convergence_halt_enabled),
            "v2_stop_loss_halt_enabled": bool(semantic_v2_stop_loss_halt_enabled),
            "v2_convergence_window_phases": int(semantic_v2_convergence_window_phases),
            "v2_convergence_min_session_phases": int(
                semantic_v2_convergence_min_session_phases
            ),
            "v2_convergence_event_ratio": float(semantic_v2_convergence_event_ratio),
            "v2_stop_loss_cache_tokens": int(semantic_v2_stop_loss_cache_tokens),
            "v2_stop_loss_min_session_phases": int(semantic_v2_stop_loss_min_session_phases),
            "v2_max_converging_deferrals": int(semantic_v2_max_converging_deferrals),
            "v2_early_spin_phases": int(semantic_v2_early_spin_phases),
            "v2_early_spin_min_tool_calls": int(semantic_v2_early_spin_min_tool_calls),
            "v2_early_spin_error_ratio": float(semantic_v2_early_spin_error_ratio),
            "v2_adaptive_slice_enabled": bool(semantic_v2_adaptive_slice_enabled),
            "v2_slice_control_soft_ratio": float(semantic_v2_slice_control_soft_ratio),
            "v2_slice_relax_soft_ratio": float(semantic_v2_slice_relax_soft_ratio),
            "v2_slice_upscale_factor": float(semantic_v2_slice_upscale_factor),
            "v2_slice_downscale_factor": float(semantic_v2_slice_downscale_factor),
            "v2_slice_min_seconds": float(semantic_v2_slice_min_seconds),
            "v2_slice_max_seconds": float(semantic_v2_slice_max_seconds),
        }
        if semantic_config["min_phases_before_restart"] < 1:
            raise ValueError("semantic_context_min_phases_before_restart must be positive")
        if semantic_config["cache_tokens_per_call_threshold"] < 1:
            raise ValueError(
                "semantic_context_cache_tokens_per_call_threshold must be positive"
            )
        if semantic_config["cache_growth_ratio_threshold"] <= 1.0:
            raise ValueError(
                "semantic_context_cache_growth_ratio_threshold must be greater than 1"
            )
        if semantic_config["max_consecutive_max_tokens"] < 1:
            raise ValueError(
                "semantic_context_max_consecutive_max_tokens must be positive"
            )
        if semantic_config["cumulative_cache_read_tokens_threshold"] < 1:
            raise ValueError(
                "semantic_context_cumulative_cache_read_tokens_threshold must be positive"
            )
        if semantic_config["cumulative_cache_requires_max_tokens"] < 1:
            raise ValueError(
                "semantic_context_cumulative_cache_requires_max_tokens must be positive"
            )
        if semantic_config["no_progress_phases"] < 2:
            raise ValueError(
                "semantic_context_no_progress_phases must be at least 2"
            )
        if not 0.0 <= semantic_config["no_progress_event_ratio_threshold"] <= 1.0:
            raise ValueError(
                "semantic_context_no_progress_event_ratio_threshold must be "
                "between zero and one"
            )
        if semantic_config["cooldown_phases"] < 0:
            raise ValueError("semantic_context_cooldown_phases must be non-negative")
        if semantic_config["max_restarts"] < 0:
            raise ValueError("semantic_context_max_restarts must be non-negative")
        if semantic_config["max_handoff_items"] < 1:
            raise ValueError("semantic_handoff_max_items must be positive")
        if semantic_config["max_handoff_chars"] < 256:
            raise ValueError("semantic_handoff_max_chars must be at least 256")

        self.semantic_context_control = self.arm == "lhos" and bool(
            semantic_context_control
        )
        self._semantic_config = semantic_config
        self._semantic_policy: SemanticContextPolicy | None = None
        self._original_instruction: str | None = None
        self._semantic_observations: list[HarnessPhaseObservation] = []
        self._semantic_decisions: list[dict[str, Any]] = []
        self._semantic_generation_usage: dict[int, HarnessUsage] = {}
        self._semantic_generation_tool_calls: dict[int, frozenset[str]] = {}
        self._semantic_generation_turn_end_counts: dict[int, int] = {}
        self._semantic_last_observation_ms = 0
        self._session_generation = 0
        self._session_generations: list[dict[str, Any]] = []
        self._controlled_restart_count = 0
        self._max_tokens_checkpoints = 0
        self._budget_tail_no_progress_checkpoints = 0
        self._slice_no_progress_failures = 0
        self._provider_censored_count = 0
        self._last_provider_failure: dict[str, Any] | None = None
        # Memoized parse of the append-only session JSONL tree, keyed by a
        # (path, size, mtime_ns) fingerprint.  The heartbeat polls every ~10s
        # while a single model call can run for minutes without appending a
        # single line; skipping unchanged re-parses is behavior-identical and
        # removes the dominant host-side parse cost on long runs.
        self._trace_cache: tuple[tuple[tuple[str, int, int], ...], DeepSeekTraceSummary] | None = None
        # Harness-side verifier outcomes are invisible to the agent-trace
        # quality probe (its test_calls only see agent-run commands).  Record
        # them here -- telemetry only, never fed into the restart policy.
        self._verifier_rejection_count = 0
        self._verifier_rejection_sha256: list[str] = []
        # Consecutive byte-identical verifier-rejection digests: the v2
        # policy's loop-escape signal (streak >= 3 = failure loop).
        self._verifier_repeat_streak = 0
        if int(semantic_policy_version) not in (1, 2):
            raise ValueError("semantic_policy_version must be 1 or 2")
        self._semantic_policy_version = int(semantic_policy_version)
        # Last-seen usage per session generation, summed into the heartbeat's
        # usage_cumulative block.  The per-generation trace resets on every
        # compacted restart; without a cumulative channel the outer token
        # budget/inert guards compare a single generation against the run
        # ceiling and never fire on multi-generation runs (observed: two runs
        # burned 45.9M/71.6M total while no generation approached the cap).
        self._heartbeat_generation_usage: dict[int, dict[str, int]] = {}
        self._last_budget_tail_no_progress = False

        self.require_container_internet = bool(require_container_internet)
        self._environment: BaseEnvironment | None = None
        self._invocation_count = 0
        self._resume_count = 0
        self._resume_session_id: str | None = None
        self._resume_session_file: Path | None = None
        self._resume_event_count = 0
        self._slice_preemptions = 0
        self._consecutive_slice_preemptions = 0
        self._policy_halt_count = 0
        self._last_slice_preempted = False
        self._last_effective_slice_seconds: float | None = None
        self._started_monotonic = time.monotonic()
        self._credential_owner_identity: tuple[int, int] | None = None

        super().__init__(
            logs_dir=logs_dir,
            model_name=model_name,
            version=self.expected_dsh_version,
            extra_env={},
            **kwargs,
        )

    @property
    def _agent_container_root(self) -> PurePosixPath:
        return PurePosixPath("/logs/agent")

    @property
    def _baseline_host_root(self) -> Path:
        return self.logs_dir / "dsh-runs"

    @property
    def _baseline_container_root(self) -> PurePosixPath:
        return self._agent_container_root / "dsh-runs"

    @property
    def _shared_host_home(self) -> Path:
        return self.logs_dir / "dsh-home"

    @property
    def _shared_container_home(self) -> PurePosixPath:
        return self._agent_container_root / "dsh-home"

    def _generation_host_home(self, generation: int) -> Path:
        if generation == 0:
            return self._shared_host_home
        return (
            self.logs_dir
            / "dsh-generations"
            / f"generation-{generation:04d}"
            / "dsh-home"
        )

    def _generation_container_home(self, generation: int) -> PurePosixPath:
        if generation == 0:
            return self._shared_container_home
        return (
            self._agent_container_root
            / "dsh-generations"
            / f"generation-{generation:04d}"
            / "dsh-home"
        )

    @property
    def _active_host_home(self) -> Path:
        return self._generation_host_home(self._session_generation)

    @property
    def _active_container_home(self) -> PurePosixPath:
        return self._generation_container_home(self._session_generation)

    @property
    def _observability_path(self) -> Path:
        return self.logs_dir / "dsh-observability.json"

    @property
    def _semantic_control_path(self) -> Path:
        return self.logs_dir / "dsh-semantic-control.json"

    @property
    def _credential_transport(self) -> str:
        return (
            "ephemeral-mode-0600-file"
            if self._credential_value is not None
            else "pre-injected-container-environment"
        )

    async def _stage_credential_file(
        self,
        environment: BaseEnvironment,
    ) -> PurePosixPath | None:
        if self._credential_value is None:
            return None

        descriptor, raw_path = tempfile.mkstemp(
            prefix="lhos-dsh-credential-",
            suffix=".tmp",
        )
        host_path = Path(raw_path)
        container_path = PurePosixPath(
            f"/tmp/.lhos-dsh-credential-{uuid4().hex}"
        )
        uploaded = False
        try:
            with os.fdopen(
                descriptor,
                "w",
                encoding="utf-8",
                newline="",
            ) as handle:
                handle.write(self._credential_value)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(host_path, 0o600)
            await environment.upload_file(
                host_path,
                container_path.as_posix(),
            )
            uploaded = True
            owner = self._credential_owner_identity
            if owner is None:
                identity = await environment.exec(
                    command="printf '%s:%s' \"$(id -u)\" \"$(id -g)\"",
                    cwd="/",
                    env=None,
                )
                match = re.fullmatch(
                    r"\s*([0-9]+):([0-9]+)\s*",
                    str(identity.stdout or ""),
                )
                if identity.return_code != 0 or match is None:
                    detail = (identity.stderr or identity.stdout or "").strip()[-1000:]
                    raise RuntimeError(
                        "cannot resolve the container Agent user for credential "
                        f"ownership: {detail or identity.return_code}"
                    )
                owner = (int(match.group(1)), int(match.group(2)))
                self._credential_owner_identity = owner
            uid, gid = owner
            result = await environment.exec(
                command=(
                    f"chown {uid}:{gid} -- {shlex.quote(container_path.as_posix())}; "
                    f"chmod 600 -- {shlex.quote(container_path.as_posix())}"
                ),
                user="root",
                cwd="/",
                env=None,
            )
            if result.return_code != 0:
                detail = (result.stderr or result.stdout or "").strip()[-1000:]
                raise RuntimeError(
                    "failed to protect staged provider credential: "
                    f"{detail or result.return_code}"
                )
            return container_path
        except BaseException:
            if uploaded:
                await self._remove_credential_file(
                    environment,
                    container_path,
                )
            raise
        finally:
            host_path.unlink(missing_ok=True)

    async def _remove_credential_file(
        self,
        environment: BaseEnvironment,
        path: PurePosixPath,
    ) -> None:
        try:
            await environment.exec(
                command=f"rm -f -- {shlex.quote(path.as_posix())}",
                user="root",
                cwd="/",
                env=None,
            )
        except BaseException:
            # The task container may already have stopped. The file lives in
            # the container's ephemeral filesystem and is never downloaded.
            return

    def _credential_shell_setup(
        self,
        credential_file: PurePosixPath | None,
    ) -> str:
        if credential_file is None:
            return ""
        quoted_path = shlex.quote(credential_file.as_posix())
        return (
            f'export {self.credential_env}="$(cat {quoted_path})"; '
            f"rm -f -- {quoted_path}; "
        )

    async def install(self, environment: BaseEnvironment) -> None:
        capabilities = getattr(environment, "capabilities", None)
        if not bool(getattr(capabilities, "mounted", False)):
            raise RuntimeError(
                "LHTB DSH agent requires Harbor mounted logs (local Docker environment)"
            )
        allow_internet = bool(
            getattr(getattr(environment, "task_env_config", None), "allow_internet", False)
        )
        if self.require_container_internet and not allow_internet:
            raise RuntimeError(
                "container-local DSH needs model-provider network access, but this task "
                "declares allow_internet=false; use a host/IPC tool bridge for an official run"
            )
        if self.time_slice_seconds is not None:
            timeout_check = (
                "command -v timeout >/dev/null 2>&1 || "
                '{ echo "GNU coreutils timeout is required for time slicing" >&2; exit 67; }; '
                "timeout --version >/dev/null 2>&1 || "
                '{ echo "timeout does not support GNU signal options" >&2; exit 67; }; '
            )
        else:
            timeout_check = ""

        node = shlex.quote(self.node_path.as_posix())
        dsh = shlex.quote(self.dsh_path.as_posix())
        patch = shlex.quote(self.patch_path.as_posix())
        workdir = shlex.quote(self.workdir.as_posix())
        expected = shlex.quote(self.expected_dsh_version)
        credential = self.credential_env
        credential_file = await self._stage_credential_file(environment)
        try:
            command = (
                "set -eu; umask 077; "
                f"{self._credential_shell_setup(credential_file)}"
                '[ "$(uname -s)" = "Linux" ] || '
                '{ echo "Linux container required" >&2; exit 64; }; '
                f'test -x {node} || '
                '{ echo "mounted Node executable missing" >&2; exit 66; }; '
                f'test -r {dsh} || '
                '{ echo "mounted DSH entrypoint missing" >&2; exit 66; }; '
                f'test -r {patch} || '
                '{ echo "mounted Cordis patch missing" >&2; exit 66; }; '
                f'test -d {workdir} || '
                '{ echo "LHTB workdir missing" >&2; exit 66; }; '
                f'test -n "${{{credential}:-}}" || '
                f'{{ echo "credential environment {credential} is not available" '
                ">&2; exit 78; }; "
                f"{timeout_check}"
                f'observed="$({node} {dsh} --version)"; '
                f'[ "$observed" = {expected} ] || '
                '{ echo "unexpected DSH version" >&2; exit 65; }'
            )
            result = await environment.exec(
                command=command,
                user="root",
                cwd="/",
                env=None,
            )
        finally:
            if credential_file is not None:
                await self._remove_credential_file(
                    environment,
                    credential_file,
                )
        if result.return_code != 0:
            detail = (result.stderr or result.stdout or "").strip()[-1000:]
            raise RuntimeError(f"mounted DSH preflight failed: {detail or result.return_code}")

    async def run(
        self,
        instruction: str,
        environment: BaseEnvironment,
        context: AgentContext,
    ) -> None:
        if self.arm == "lhos" and self._invocation_count:
            raise RuntimeError(
                "LHOS continuation must call resume_after_verifier_rejection(), "
                "not start another fresh DSH session"
            )
        self._environment = environment
        if self._original_instruction is None:
            self._original_instruction = instruction
        if self.arm == "lhos" and self.semantic_context_control:
            self._semantic_policy = _build_semantic_policy(
                self._semantic_policy_version, self._semantic_config
            )
            self._write_semantic_control()
        if (
            self.arm == "lhos"
            and self.semantic_context_control
            and self.time_slice_seconds is None
        ):
            # 漏洞C修复：one-shot / full_budget 任务（无 time_slice）的 initial run
            # 以前一次跑满 max-tokens、语义控制 0 介入。此处强制时间片循环 +
            # 会话内语义检查点；显式 time_slice 的任务仍保持"单片返回→verifier
            # 驱动 resume"流程不变。
            await self._run_initial_controlled(instruction, context)
        else:
            await self._execute(
                instruction=instruction,
                context=context,
                resume_session_id=None,
            )
        if self.arm == "lhos":
            self._lock_resume_identity(
                home=self._active_host_home,
                generation=self._session_generation,
            )
            self._refresh_context_control_metadata(context)

    async def _run_initial_controlled(
        self,
        instruction: str,
        context: AgentContext,
    ) -> None:
        """漏洞C修复：把 initial run 拆成多个时间片，每片之间执行语义决策。

        旧逻辑 initial run 只调用一次 ``_execute()``：
          - sliced 任务：跑完第一个 75s 片就结束，语义控制只在 resume 边界介入；
          - full_budget/one_shot 任务：一次跑满 max-tokens（50M+），decide() 全程 0 介入。
        新逻辑：即使任务未声明 time_slice（one-shot），lhos 臂也强制一个控制时间片，
        每片边界 observe + decide()：RESTART_COMPACTED → 新 session 继续；
        RESUME → 同 session 继续下一片；直到 completed / max_tokens / failed。
        """
        had_slice = self.time_slice_seconds is not None
        if not had_slice:
            self.time_slice_seconds = _INITIAL_CONTROL_SLICE_SECONDS
        base_control_slice = float(self.time_slice_seconds)
        adaptive_slice = bool(
            self._semantic_policy_version == 2
            and self._semantic_config.get("v2_adaptive_slice_enabled", True)
            and hasattr(self._semantic_policy, "recommended_slice_seconds")
        )
        resume_session_id: str | None = None
        current_instruction = instruction
        runtime_patch: PurePosixPath | None = None
        dsh_home_host: Path | None = None
        dsh_home_container: PurePosixPath | None = None
        try:
            while True:
                await self._execute(
                    instruction=current_instruction,
                    context=context,
                    resume_session_id=resume_session_id,
                    runtime_patch=runtime_patch,
                    dsh_home_host=dsh_home_host,
                    dsh_home_container=dsh_home_container,
                    continuation_action=(
                        HarnessContinuationAction.RESUME.value
                        if resume_session_id is not None
                        else None
                    ),
                )
                if not self._last_slice_preempted:
                    # completed / max_tokens_checkpoint 等：_execute() 已正常返回
                    break
                if resume_session_id is None:
                    # 首个片结束，先锁定 durable session 身份供后续 RESUME
                    self._lock_resume_identity(
                        home=self._active_host_home,
                        generation=self._session_generation,
                    )
                current_observation = self._observe_semantic_phase()
                decision = self._semantic_policy.decide(
                    tuple(self._semantic_observations),
                    current_observation,
                    original_instruction=self._original_instruction,
                )
                if decision.action == HarnessContinuationAction.TERMINATE:
                    # 策略判定任务已收敛/止损：结束切片循环，让 verifier 给
                    # 当前状态打分，不再继续烧预算。
                    self._policy_halt_count += 1
                    self._semantic_observations.append(current_observation)
                    self._record_semantic_decision(decision, current_observation)
                    self._write_semantic_control()
                    break
                self._semantic_observations.append(current_observation)
                self._record_semantic_decision(decision, current_observation)
                if decision.action == HarnessContinuationAction.RESTART_COMPACTED:
                    self._controlled_restart_count += 1
                    self._session_generation += 1
                    current_instruction = self._build_compacted_restart_instruction(
                        user_prompt=current_instruction,
                        current=current_observation,
                        decision=decision,
                    )
                    resume_session_id = None
                    runtime_patch = None
                    dsh_home_host = self._generation_host_home(
                        self._session_generation
                    )
                    dsh_home_container = self._generation_container_home(
                        self._session_generation
                    )
                    # 新代从基准片长重新开始，下一决策点再按观测适配。
                    self.time_slice_seconds = base_control_slice
                    continue
                # RESUME：同 session 继续下一片
                if adaptive_slice:
                    # P0：片长与策略共享状态——安静干净的 session 放大片长
                    # 摊薄 reload，逼近决策点时缩短片长保持控制粒度。
                    # 注意：current_observation 已 append 进 _semantic_observations，
                    # 这里传[:-1]避免 current 在 history 里出现两次。
                    self.time_slice_seconds = (
                        self._semantic_policy.recommended_slice_seconds(
                            tuple(self._semantic_observations[:-1]),
                            current_observation,
                            base_control_slice,
                        )
                    )
                resume_session_id = self._resume_session_id
                dsh_home_host = self._active_host_home
                dsh_home_container = self._active_container_home
                runtime_patch = self._write_resume_runner(
                    dsh_home_host,
                    dsh_home_container,
                )
                current_instruction = instruction
        finally:
            if not had_slice:
                self.time_slice_seconds = None

    async def resume_after_verifier_rejection(
        self,
        user_prompt: str,
        context: AgentContext,
    ) -> None:
        # Zero-cost telemetry: the harness verifier just rejected the
        # current state.  Verifier prompt bodies are PRIVATE and must never
        # land in durable artifacts, so persist only the count plus a content
        # hash per rejection (enough to tell repeats apart).  This is never
        # fed into the continuation policy.
        self._verifier_rejection_count += 1
        previous_digest = (
            self._verifier_rejection_sha256[-1]
            if self._verifier_rejection_sha256
            else None
        )
        digest = hashlib.sha256(user_prompt.encode("utf-8")).hexdigest()
        self._verifier_repeat_streak = (
            self._verifier_repeat_streak + 1 if digest == previous_digest else 1
        )
        self._verifier_rejection_sha256.append(digest)
        del self._verifier_rejection_sha256[:-3]
        if self._environment is None:
            raise RuntimeError("cannot resume before the initial DSH run")

        if self.arm == "baseline":
            if not self.controlled_pair_mode:
                raise RuntimeError(
                    "baseline resume requires controlled_pair_mode=True"
                )
            if self._original_instruction is None:
                raise RuntimeError("baseline initial instruction was not recorded")
            # Both arms use Harbor's same-conversation/binary verifier path. The
            # control arm deliberately starts a new DSH home, reconstructing the
            # original task prompt because it has no prior model conversation.
            await self._execute(
                instruction=(
                    self._original_instruction.rstrip()
                    + "\n\n"
                    + user_prompt.lstrip()
                ),
                context=context,
                resume_session_id=None,
                continuation_action="fresh_recomputation",
            )
            self._refresh_context_control_metadata(context)
            return

        if self._resume_session_id is None or self._resume_session_file is None:
            raise RuntimeError("initial DSH run did not publish a resumable session identity")

        decision = None
        current_observation = None
        if self.semantic_context_control:
            if self._semantic_policy is None or self._original_instruction is None:
                raise RuntimeError("semantic context policy was not initialized")
            current_observation = self._observe_semantic_phase()
            decision = self._semantic_policy.decide(
                tuple(self._semantic_observations),
                current_observation,
                original_instruction=self._original_instruction,
            )
            self._semantic_observations.append(current_observation)
            self._record_semantic_decision(decision, current_observation)

        if (
            decision is not None
            and decision.action == HarnessContinuationAction.TERMINATE
        ):
            # 策略 halt（收敛/止损）：拒绝继续投入，当前状态直接交给
            # verifier 记分。Harbor 侧表现为 agent 不再 resume。
            self._policy_halt_count += 1
            self._write_semantic_control()
            self._refresh_context_control_metadata(context)
            return

        if (
            decision is not None
            and decision.action == HarnessContinuationAction.RESTART_COMPACTED
        ):
            self._controlled_restart_count += 1
            self._session_generation += 1
            restart_instruction = self._build_compacted_restart_instruction(
                user_prompt=user_prompt,
                current=current_observation,
                decision=decision,
            )
            await self._execute(
                instruction=restart_instruction,
                context=context,
                resume_session_id=None,
                dsh_home_host=self._active_host_home,
                dsh_home_container=self._active_container_home,
                continuation_action=decision.action.value,
            )
            self._lock_resume_identity(
                home=self._active_host_home,
                generation=self._session_generation,
            )
            self._refresh_context_control_metadata(context)
            return

        runtime_patch = self._write_resume_runner(
            self._active_host_home,
            self._active_container_home,
        )
        await self._execute(
            instruction=user_prompt,
            context=context,
            resume_session_id=self._resume_session_id,
            runtime_patch=runtime_patch,
            dsh_home_host=self._active_host_home,
            dsh_home_container=self._active_container_home,
            continuation_action=(
                HarnessContinuationAction.RESUME.value
                if self.semantic_context_control
                else "resume"
            ),
        )
        self._verify_resumed_identity(
            allow_no_progress=self._last_budget_tail_no_progress,
        )
        self._refresh_context_control_metadata(context)

    def _observe_semantic_phase(self) -> HarnessPhaseObservation:
        trace = self._trace(self._active_host_home)
        generation = self._session_generation
        previous_usage = self._semantic_generation_usage.get(generation, HarnessUsage())
        delta = _usage_delta(trace.usage, previous_usage)
        self._semantic_generation_usage[generation] = trace.usage

        previous_tool_calls = self._semantic_generation_tool_calls.get(
            generation,
            frozenset(),
        )
        current_tool_calls: set[str] = set()
        phase_reads: set[str] = set()
        phase_writes: set[str] = set()
        phase_unknown_io = False
        phase_write_calls = 0
        phase_write_paths: set[str] = set()
        phase_test_calls = 0
        phase_error_calls = 0
        phase_tool_calls = 0
        for index, tool in enumerate(trace.tool_calls):
            identity = (
                str(tool.call_id).strip()
                or f"{tool.started_at_ms}:{tool.name}:{index}"
            )
            current_tool_calls.add(identity)
            if identity in previous_tool_calls:
                continue
            phase_tool_calls += 1
            if tool.is_error:
                phase_error_calls += 1
            if tool.write_set:
                phase_write_calls += 1
                phase_write_paths.update(tool.write_set)
            if _is_test_command(tool):
                phase_test_calls += 1
            phase_reads.update(tool.read_set)
            phase_writes.update(tool.write_set)
            phase_unknown_io = bool(phase_unknown_io or tool.unknown_io)
        self._semantic_generation_tool_calls[generation] = frozenset(
            current_tool_calls
        )
        write_repeat_ratio = (
            (phase_write_calls - len(phase_write_paths)) / phase_write_calls
            if phase_write_calls > 0
            else 0.0
        )
        error_ratio = (
            phase_error_calls / phase_tool_calls if phase_tool_calls > 0 else 0.0
        )
        quality_probe = HarnessQualityProbe(
            write_calls=phase_write_calls,
            distinct_writes=len(phase_write_paths),
            test_calls=phase_test_calls,
            error_calls=phase_error_calls,
            tool_calls=phase_tool_calls,
            write_repeat_ratio=round(write_repeat_ratio, 4),
            error_ratio=round(error_ratio, 4),
        )

        previous_turn_end_count = self._semantic_generation_turn_end_counts.get(
            generation,
            0,
        )
        new_turn_end_reasons = trace.turn_end_reasons[previous_turn_end_count:]
        self._semantic_generation_turn_end_counts[generation] = len(
            trace.turn_end_reasons
        )
        latest_turn_end = (
            new_turn_end_reasons[-1] if new_turn_end_reasons else {}
        )
        latest_turn_end_kind = str(latest_turn_end.get("kind", "") or "")

        elapsed_ms = round((time.monotonic() - self._started_monotonic) * 1000)
        phase_elapsed_ms = max(0, elapsed_ms - self._semantic_last_observation_ms)
        self._semantic_last_observation_ms = elapsed_ms
        return HarnessPhaseObservation(
            phase_index=self._invocation_count,
            usage=delta.model_copy(update={"wall_time_ms": phase_elapsed_ms}),
            event_count=trace.event_count,
            # Access sets are per-phase, not cumulative set differences.
            # Repeated edits to the same Artifact are semantic progress and
            # must remain visible to the policy and compact handoff.
            read_set=tuple(sorted(phase_reads)),
            write_set=tuple(sorted(phase_writes)),
            verifier_passed=False,
            max_tokens_checkpoint=latest_turn_end_kind == "max-tokens",
            harness_completed=latest_turn_end_kind == "completed",
            unknown_io=phase_unknown_io,
            session_id=trace.session_id,
            elapsed_ms=phase_elapsed_ms,
            quality_probe=quality_probe,
            verifier_rejection_count=self._verifier_rejection_count,
            verifier_rejection_repeat_streak=self._verifier_repeat_streak,
            slice_preempted=self._last_slice_preempted,
            consecutive_slice_preemptions=self._consecutive_slice_preemptions,
        )

    def _record_semantic_decision(
        self,
        decision: Any,
        observation: HarnessPhaseObservation,
    ) -> None:
        handoff_items = tuple(getattr(decision, "bounded_handoff_items", ()) or ())
        handoff_digest = hashlib.sha256(
            json.dumps(
                handoff_items,
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        reason = str(getattr(decision, "reason", "") or "").strip()
        reason_code = re.sub(r"[^A-Za-z0-9_.:-]+", "_", reason)[:160]
        record = {
            "phase_index": observation.phase_index,
            "session_generation": self._session_generation,
            "session_id": observation.session_id,
            "action": decision.action.value,
            "reason_code": reason_code,
            "context_score": float(getattr(decision, "context_score", 0.0)),
            "cache_tokens_per_call": float(
                getattr(decision, "cache_tokens_per_call", 0.0)
            ),
            "decision_hash": str(getattr(decision, "decision_hash", "") or ""),
            "usage_delta": _harness_usage_payload(observation.usage),
            "event_count": observation.event_count,
            "read_set_size": len(observation.read_set),
            "write_set_size": len(observation.write_set),
            "unknown_io": observation.unknown_io,
            "max_tokens_checkpoint": observation.max_tokens_checkpoint,
            "harness_completed": observation.harness_completed,
            "completed_without_verification": bool(
                getattr(decision, "completed_without_verification", False)
            ),
            "consecutive_max_tokens": int(
                getattr(decision, "consecutive_max_tokens", 0)
            ),
            "cumulative_session_cache_read_tokens": int(
                getattr(decision, "cumulative_session_cache_read_tokens", 0)
            ),
            "event_progress_ratio": getattr(
                decision,
                "event_progress_ratio",
                None,
            ),
            "guard_triggers": list(
                getattr(decision, "guard_triggers", ()) or ()
            ),
            "handoff_item_count": len(handoff_items),
            "handoff_sha256": handoff_digest,
            "quality_probe": {
                "write_calls": observation.quality_probe.write_calls,
                "distinct_writes": observation.quality_probe.distinct_writes,
                "test_calls": observation.quality_probe.test_calls,
                "error_calls": observation.quality_probe.error_calls,
                "tool_calls": observation.quality_probe.tool_calls,
                "write_repeat_ratio": observation.quality_probe.write_repeat_ratio,
                "error_ratio": observation.quality_probe.error_ratio,
            },
            "verifier_rejection_count": observation.verifier_rejection_count,
            "verifier_rejection_repeat_streak": (
                observation.verifier_rejection_repeat_streak
            ),
            "slice_preempted": observation.slice_preempted,
            "consecutive_slice_preemptions": (
                observation.consecutive_slice_preemptions
            ),
            "decided_at": _utc_now(),
        }
        self._semantic_decisions.append(record)
        self._write_semantic_control()

    def _semantic_guard_summary(self) -> tuple[dict[str, int], int]:
        trigger_counts: dict[str, int] = {}
        completed_not_verified = 0
        for decision in self._semantic_decisions:
            for trigger in decision.get("guard_triggers", []):
                name = str(trigger).strip()
                if name:
                    trigger_counts[name] = trigger_counts.get(name, 0) + 1
            if decision.get("completed_without_verification"):
                completed_not_verified += 1
        return dict(sorted(trigger_counts.items())), completed_not_verified

    def _build_compacted_restart_instruction(
        self,
        *,
        user_prompt: str,
        current: HarnessPhaseObservation | None,
        decision: Any,
    ) -> str:
        if self._original_instruction is None or current is None:
            raise RuntimeError("cannot build a compact handoff without semantic history")
        observations = (*self._semantic_observations[:-1], current)
        items = tuple(getattr(decision, "bounded_handoff_items", ()) or ())
        if not items:
            items = build_semantic_handoff(
                self._original_instruction,
                observations,
                max_items=int(self._semantic_config["max_handoff_items"]),
                max_chars=int(self._semantic_config["max_handoff_chars"]),
            )
        # The original task is authoritative input, not disposable cognition.
        # Keep it outside the bounded handoff budget: real LHTB instructions
        # exceed 2 KiB, and truncating them made compact restarts lose later
        # requirements. The bounded portion contains only the latest verifier
        # continuation plus artifact-oriented semantic state.
        original = self._original_instruction.strip()
        latest = user_prompt.strip()
        instruction_identity: list[str] = []
        artifact_items: list[str] = []
        for item in items:
            normalized = str(item).strip()
            if not normalized:
                continue
            if normalized.startswith("instruction_sha256:"):
                instruction_identity.append(normalized.split(";", 1)[0])
            else:
                artifact_items.append(normalized)
        limit = int(self._semantic_config["max_handoff_chars"])
        state_blocks: list[str] = []
        if latest:
            # Reserve at least half of the bounded state for whole Artifact
            # URIs. A verifier message may itself contain the original task.
            feedback_budget = max(1, limit // 2)
            state_blocks.append(
                "Latest verifier continuation:\n" + latest[:feedback_budget]
            )
        if artifact_items:
            state_blocks.append("Recent workspace artifacts:")
            for item in artifact_items:
                candidate = f"- {item}"
                used = sum(len(block) for block in state_blocks) + max(
                    0, len(state_blocks) - 1
                )
                if used + len(candidate) > limit:
                    continue
                state_blocks.append(candidate)
        if instruction_identity:
            for item in instruction_identity:
                candidate = f"- {item}"
                used = sum(len(block) for block in state_blocks) + max(
                    0, len(state_blocks) - 1
                )
                if used + len(candidate) <= limit:
                    state_blocks.append(candidate)
        bounded_state = "\n".join(state_blocks)
        return (
            "Continue the same task in the preserved /app workspace after a "
            "LongHorizonOS context compaction. Treat existing files and test "
            "outputs as the source of truth; inspect them before editing.\n\n"
            "Original task (authoritative):\n"
            f"{original}\n\n"
            "Bounded semantic handoff:\n"
            f"{bounded_state}"
        )

    def _write_semantic_control(self) -> None:
        if not self.semantic_context_control:
            return
        trigger_counts, completed_not_verified = self._semantic_guard_summary()
        payload = {
            "schema_version": _SEMANTIC_CONTROL_SCHEMA_VERSION,
            "enabled": True,
            "mode": "adaptive_context_control",
            "config": dict(self._semantic_config),
            "policy_version": self._semantic_policy_version,
            "session_generation": self._session_generation,
            "session_generation_count": len(self._session_generations),
            "controlled_restart_count": self._controlled_restart_count,
            "policy_halt_count": self._policy_halt_count,
            "max_tokens_checkpoints": self._max_tokens_checkpoints,
            "observation_count": len(self._semantic_observations),
            "decision_count": len(self._semantic_decisions),
            "guard_trigger_counts": trigger_counts,
            "completed_without_verification_count": completed_not_verified,
            "session_generations": list(self._session_generations),
            "decisions": list(self._semantic_decisions),
            "verifier_rejection_count": self._verifier_rejection_count,
            "verifier_rejection_sha256": list(self._verifier_rejection_sha256),
            "updated_at": _utc_now(),
        }
        _atomic_write_json(self._semantic_control_path, payload)

    def _refresh_context_control_metadata(self, context: AgentContext) -> None:
        metadata = getattr(context, "metadata", None)
        if not isinstance(metadata, dict) or self.arm != "lhos":
            return
        trigger_counts, completed_not_verified = self._semantic_guard_summary()
        metadata.update(
            {
                "dsh_session_id": self._resume_session_id,
                "dsh_session_reused": bool(self._resume_count),
                "dsh_resume_evidence_mode": (
                    "adaptive_context_control"
                    if self.semantic_context_control
                    else "durable_session_resume"
                ),
                "dsh_semantic_context_control": self.semantic_context_control,
                "dsh_session_generation": self._session_generation,
                "dsh_session_generation_count": len(self._session_generations),
                "dsh_controlled_restarts_cumulative": self._controlled_restart_count,
                "dsh_semantic_decisions_cumulative": len(self._semantic_decisions),
                "dsh_semantic_guard_trigger_counts": trigger_counts,
                "dsh_completed_without_verification_cumulative": (
                    completed_not_verified
                ),
                "dsh_max_tokens_checkpoints_cumulative": self._max_tokens_checkpoints,
                "dsh_policy_halts_cumulative": self._policy_halt_count,
            }
        )

    async def _heartbeat_loop(
        self,
        dsh_home_host: Path,
        interval_seconds: float = _DEFAULT_HEARTBEAT_SECONDS,
    ) -> None:
        """Publish a lightweight live heartbeat while an invocation runs.

        The full ``dsh-observability.json`` is only refreshed at event
        boundaries, leaving a single long invocation invisible for its whole
        duration (the root cause of the super-mario 120M-unit burn). This loop
        periodically refreshes a small ``dsh-heartbeat.json`` so an outer
        controller can observe real-time token/event/model-call progress of an
        in-flight invocation and stop the run before it spirals.
        """
        heartbeat_path = self.logs_dir / "dsh-heartbeat.json"
        while True:
            try:
                trace = self._trace(dsh_home_host)
                usage = _harness_usage_payload(trace.usage)
                generation_key = (
                    self._session_generation if self.arm == "lhos" else 0
                )
                self._heartbeat_generation_usage[generation_key] = usage
                usage_cumulative = {
                    key: sum(
                        generation_usage.get(key, 0)
                        for generation_usage in self._heartbeat_generation_usage.values()
                    )
                    for key in usage
                }
                heartbeat_tools = trace.tool_calls
                heartbeat_test_calls = sum(
                    1 for tool in heartbeat_tools if _is_test_command(tool)
                )
                heartbeat_error_calls = sum(
                    1 for tool in heartbeat_tools if tool.is_error
                )
                heartbeat_write_calls = sum(1 for tool in heartbeat_tools if tool.write_set)
                heartbeat_write_paths: set[str] = {
                    path for tool in heartbeat_tools for path in tool.write_set
                }
                payload = {
                    "schema_version": _HEARTBEAT_SCHEMA_VERSION,
                    "kind": "dsh-heartbeat",
                    "updated_at": _utc_now(),
                    "invocation_count": self._invocation_count,
                    "session_generation": (
                        self._session_generation if self.arm == "lhos" else None
                    ),
                    "session_id": (
                        self._resume_session_id if self.arm == "lhos" else None
                    ),
                    "semantic_context_control": self.semantic_context_control,
                    "semantic_decision_count": len(self._semantic_decisions),
                    "semantic_observation_count": len(self._semantic_observations),
                    "event_count": trace.event_count,
                    "session_file_count": len(trace.session_files),
                    "usage": usage,
                    # Run-total usage across session generations; "usage" above
                    # stays per-generation (resets on restart).  Budget guards
                    # must read this block.
                    "usage_cumulative": usage_cumulative,
                    # Zero-cost progressive-quality probe: cumulative tool
                    # behavior so the outer OS can sense a spinning agent
                    # (tests run but keep failing, no new artifacts) in real
                    # time even before any semantic boundary is reached.
                    "quality": {
                        "cumulative_tool_calls": len(heartbeat_tools),
                        "cumulative_test_calls": heartbeat_test_calls,
                        "cumulative_error_calls": heartbeat_error_calls,
                        "cumulative_write_calls": heartbeat_write_calls,
                        "cumulative_distinct_writes": len(heartbeat_write_paths),
                    },
                    # Harness verifier outcomes (the agent-trace probe above
                    # cannot see them): count + content hashes only, verifier
                    # prompt bodies are private and never persisted.
                    "verifier": {
                        "rejection_count": self._verifier_rejection_count,
                        "recent_rejection_sha256": list(
                            self._verifier_rejection_sha256
                        ),
                    },
                }
                _atomic_write_json(heartbeat_path, payload)
            except Exception:
                # A heartbeat must never take down the invocation it observes.
                pass
            await asyncio.sleep(max(1.0, float(interval_seconds)))

    async def _execute(
        self,
        *,
        instruction: str,
        context: AgentContext,
        resume_session_id: str | None,
        runtime_patch: PurePosixPath | None = None,
        dsh_home_host: Path | None = None,
        dsh_home_container: PurePosixPath | None = None,
        continuation_action: str | None = None,
    ) -> None:
        environment = self._environment
        if environment is None:
            raise RuntimeError("DSH environment is unavailable")
        if not instruction.strip():
            raise ValueError("DSH instruction must be non-empty")

        effective_slice_seconds = _effective_slice_seconds(
            self.time_slice_seconds,
            instruction,
        )
        remaining_budget_seconds = _remaining_budget_seconds(instruction)
        budget_tail_slice = bool(
            self.time_slice_seconds is not None
            and effective_slice_seconds is not None
            and remaining_budget_seconds is not None
            and effective_slice_seconds < self.time_slice_seconds
            and effective_slice_seconds <= _FINAL_SHORT_SLICE_MAX_SECONDS
        )
        self._last_budget_tail_no_progress = False
        self._last_slice_preempted = False
        self._last_effective_slice_seconds = effective_slice_seconds
        self._invocation_count += 1
        invocation = self._invocation_count
        is_resume = resume_session_id is not None
        if is_resume:
            self._resume_count += 1

        invocation_name = f"invocation-{invocation:04d}"
        invocation_host = self.logs_dir / "invocations" / invocation_name
        invocation_container = self._agent_container_root / "invocations" / invocation_name
        invocation_host.mkdir(parents=True, exist_ok=False)
        instruction_host = invocation_host / "instruction.txt"
        instruction_host.write_text(instruction, encoding="utf-8")
        instruction_container = invocation_container / "instruction.txt"

        if self.arm == "baseline":
            if dsh_home_host is not None or dsh_home_container is not None:
                raise RuntimeError("baseline DSH home cannot be overridden")
            dsh_home_host = self._baseline_host_root / invocation_name / "dsh-home"
            dsh_home_container = self._baseline_container_root / invocation_name / "dsh-home"
        else:
            dsh_home_host = dsh_home_host or self._active_host_home
            dsh_home_container = dsh_home_container or self._active_container_home
        dsh_home_host.mkdir(parents=True, exist_ok=True)
        trace_before = self._trace(dsh_home_host)
        stdout_container = invocation_container / "stdout.log"
        stderr_container = invocation_container / "stderr.log"
        intent_path = invocation_host / "invocation.json"
        started_at = _utc_now()
        intent: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "status": "running",
            "arm": self.arm,
            "controller": "longhorizonos" if self.arm == "lhos" else "none",
            "controlled_pair_mode": self.controlled_pair_mode,
            "harbor_continue_mode": (
                "same_conversation"
                if self.controlled_pair_mode
                else os.environ.get("HB_CONTINUE_MODE", "") or None
            ),
            "verifier_feedback_mode": os.environ.get(
                "HB_VERIFIER_FEEDBACK_MODE", "binary"
            ),
            "execution_location": "lhtb-container",
            "invocation": invocation,
            "resume": is_resume,
            "resume_session_id": resume_session_id,
            "continuation_action": continuation_action,
            "session_generation": self._session_generation if self.arm == "lhos" else None,
            "started_at": started_at,
            "prompt_sha256": hashlib.sha256(instruction.encode("utf-8")).hexdigest(),
            "model": self.model_name,
            "profile": self.profile,
            "workdir": self.workdir.as_posix(),
            "dsh_version": self.expected_dsh_version,
            "credential_env": self.credential_env,
            "credential_transport": self._credential_transport,
            "time_slice_seconds": self.time_slice_seconds,
            "effective_time_slice_seconds": effective_slice_seconds,
            "remaining_budget_seconds": remaining_budget_seconds,
            "budget_tail_slice": budget_tail_slice,
            "slice_preempted": False,
            "budget_tail_no_progress": False,
            "slice_no_progress_failure": False,
            "max_tokens_checkpoint": False,
            "provider_censored": False,
            "provider_failure": None,
        }
        _atomic_write_json(intent_path, intent)

        result = None
        status = "failed"
        failure_type: str | None = None
        slice_preempted = False
        budget_tail_no_progress = False
        slice_no_progress_failure = False
        max_tokens_checkpoint = False
        provider_censored: dict[str, Any] | None = None
        started = time.monotonic()
        credential_file: PurePosixPath | None = None
        heartbeat_task: asyncio.Task[Any] | None = None
        try:
            credential_file = await self._stage_credential_file(environment)
            command = self._build_command(
                instruction_file=instruction_container,
                dsh_home=dsh_home_container,
                stdout_file=stdout_container,
                stderr_file=stderr_container,
                resume_session_id=resume_session_id,
                runtime_patch=runtime_patch,
                time_slice_seconds=effective_slice_seconds,
                credential_file=credential_file,
            )
            # Publish live progress while the invocation runs so an outer
            # controller can observe (and, if needed, stop) it in real time.
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(dsh_home_host)
            )
            try:
                result = await environment.exec(
                    command=command,
                    cwd=self.workdir.as_posix(),
                    env=None,
                )
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await heartbeat_task
            observed_exec_seconds = max(0.0, time.monotonic() - started)
            trace_after_nonzero = (
                self._trace(dsh_home_host)
                if result.return_code not in {0, _SLICE_EXIT_CODE}
                else None
            )
            trace_after = trace_after_nonzero or self._trace(dsh_home_host)
            provider_censored = (
                _structured_provider_failure(trace_after)
                if result.return_code not in {0, _SLICE_EXIT_CODE}
                else None
            )
            durable_progress_after_sigkill = bool(
                trace_after_nonzero is not None
                and trace_after_nonzero.event_count > trace_before.event_count
                and trace_after_nonzero.usage.model_calls
                > trace_before.usage.model_calls
            )
            killed_after_slice_deadline = bool(
                effective_slice_seconds is not None
                and result.return_code == 137
                # GNU timeout returns 137 when --kill-after escalates TERM to
                # SIGKILL.  Only classify it as a controller checkpoint when
                # the process actually survived to the configured deadline;
                # an early 137 remains a real OOM/external-kill failure.
                and observed_exec_seconds
                >= max(0.0, float(effective_slice_seconds) - 1.0)
                and durable_progress_after_sigkill
            )
            slice_preempted = (
                effective_slice_seconds is not None
                and (
                    result.return_code == _SLICE_EXIT_CODE
                    or killed_after_slice_deadline
                )
            )
            no_durable_progress = bool(
                slice_preempted
                and trace_after.event_count <= trace_before.event_count
            )
            budget_tail_no_progress = bool(
                no_durable_progress and budget_tail_slice
            )
            slice_no_progress_failure = bool(
                no_durable_progress and not budget_tail_no_progress
            )
            # DSH rc.8 may exit non-zero after persisting a complete turn whose
            # structured reason is ``max-tokens``.  Treat that as a recoverable
            # Harness checkpoint for both experiment arms; making it LHOS-only
            # would change baseline semantics and invalidate the A/B comparison.
            if result.return_code != 0 and not slice_preempted:
                max_tokens_checkpoint = _has_recoverable_max_tokens_checkpoint(
                    trace_after_nonzero or self._trace(dsh_home_host),
                    previous_event_count=trace_before.event_count,
                    previous_model_calls=trace_before.usage.model_calls,
                )
            status = (
                "budget_tail_no_progress"
                if budget_tail_no_progress
                else "slice_no_progress_failure"
                if slice_no_progress_failure
                else "slice_preempted"
                if slice_preempted
                else "max_tokens_checkpoint"
                if max_tokens_checkpoint
                else "provider_censored"
                if provider_censored is not None
                else "completed"
                if result.return_code == 0
                else "failed"
            )
            if slice_preempted:
                self._slice_preemptions += 1
                self._consecutive_slice_preemptions += 1
            else:
                self._consecutive_slice_preemptions = 0
            self._last_slice_preempted = bool(slice_preempted)
            if budget_tail_no_progress:
                self._budget_tail_no_progress_checkpoints += 1
                self._last_budget_tail_no_progress = True
            if slice_no_progress_failure:
                self._slice_no_progress_failures += 1
            if max_tokens_checkpoint:
                self._max_tokens_checkpoints += 1
                self._write_semantic_control()
            if provider_censored is not None:
                self._provider_censored_count += 1
                self._last_provider_failure = dict(provider_censored)
            if slice_no_progress_failure:
                failure_type = "SliceNoProgress"
            elif provider_censored is not None:
                failure_type = "ProviderCensored"
        except asyncio.CancelledError:
            status = "cancelled"
            failure_type = "CancelledError"
            raise
        except Exception as exc:
            failure_type = type(exc).__name__
            raise
        finally:
            if credential_file is not None:
                await self._remove_credential_file(
                    environment,
                    credential_file,
                )
            elapsed_ms = round((time.monotonic() - started) * 1000)
            intent.update(
                {
                    "status": status,
                    "finished_at": _utc_now(),
                    "elapsed_ms": elapsed_ms,
                    "exit_code": None if result is None else int(result.return_code),
                    "failure_type": failure_type,
                    "remaining_budget_seconds": remaining_budget_seconds,
                    "budget_tail_slice": budget_tail_slice,
                    "slice_preempted": slice_preempted,
                    "budget_tail_no_progress": budget_tail_no_progress,
                    "slice_no_progress_failure": slice_no_progress_failure,
                    "slice_checkpoint_kind": (
                        "budget_tail_no_progress"
                        if budget_tail_no_progress
                        else "durable_progress"
                        if slice_preempted and not slice_no_progress_failure
                        else None
                    ),
                    "max_tokens_checkpoint": max_tokens_checkpoint,
                    "provider_censored": provider_censored is not None,
                    "provider_failure": provider_censored,
                    "time_slice_seconds": self.time_slice_seconds,
                    "effective_time_slice_seconds": effective_slice_seconds,
                }
            )
            _atomic_write_json(intent_path, intent)
            self._populate_context(
                context,
                current_home=dsh_home_host,
                current_is_cumulative=(self.arm == "lhos" or self.controlled_pair_mode),
                completed=result is not None and result.return_code == 0,
                slice_preempted=slice_preempted,
                remaining_budget_seconds=remaining_budget_seconds,
                budget_tail_slice=budget_tail_slice,
                budget_tail_no_progress=budget_tail_no_progress,
                slice_no_progress_failure=slice_no_progress_failure,
                max_tokens_checkpoint=max_tokens_checkpoint,
                provider_censored=provider_censored,
            )

        if result is None:
            raise RuntimeError("DSH execution returned no process result")
        if slice_preempted:
            if slice_no_progress_failure:
                detail = (
                    "slice ended without durable DSH progress outside the final "
                    "remaining-budget tail"
                )
                raise NonZeroAgentExitCodeError(detail)
            return
        if max_tokens_checkpoint:
            return
        if provider_censored is not None:
            raise NonZeroAgentExitCodeError(
                "DSH provider_censored by content policy "
                f"(HTTP {provider_censored['status_code']})"
            )
        if result.return_code != 0:
            detail = (result.stderr or result.stdout or "").strip()[-1000:]
            raise NonZeroAgentExitCodeError(
                f"DSH invocation failed with exit {result.return_code}: "
                f"{detail or 'inspect dsh-observability.json and invocation stderr.log'}"
            )

    def _build_command(
        self,
        *,
        instruction_file: PurePosixPath,
        dsh_home: PurePosixPath,
        stdout_file: PurePosixPath,
        stderr_file: PurePosixPath,
        resume_session_id: str | None,
        runtime_patch: PurePosixPath | None,
        time_slice_seconds: float | None = None,
        credential_file: PurePosixPath | None = None,
    ) -> str:
        node = shlex.quote(self.node_path.as_posix())
        dsh = shlex.quote(self.dsh_path.as_posix())
        patch = shlex.quote(self.patch_path.as_posix())
        profile = shlex.quote(self.profile)
        home = shlex.quote(dsh_home.as_posix())
        instruction = shlex.quote(instruction_file.as_posix())
        stdout = shlex.quote(stdout_file.as_posix())
        stderr = shlex.quote(stderr_file.as_posix())
        permission = shlex.quote(self.permission_mode)
        tools = shlex.quote(self.tools_mode)
        credential_setup = self._credential_shell_setup(credential_file)

        dsh_args = f"{node} {dsh} --profile {profile} --patch {patch}"
        if runtime_patch is not None:
            dsh_args += f" --patch {shlex.quote(runtime_patch.as_posix())}"
        resume_exports = ""
        if resume_session_id is not None:
            if not _SESSION_ID.fullmatch(resume_session_id):
                raise RuntimeError("refusing to execute an invalid DSH resume session id")
            resume_exports = (
                "export LHOS_DSH_RESUME_SESSION_ID="
                f"{shlex.quote(resume_session_id)}; "
                "export LHOS_DSH_RESUME_TASK_FILE="
                f"{instruction}; "
            )
        else:
            dsh_args += f' "$(cat {instruction})"'

        if time_slice_seconds is not None:
            seconds = f"{time_slice_seconds:.3f}".rstrip("0").rstrip(".")
            run_command = (
                "{ "
                f"timeout --foreground --signal=TERM --kill-after=5s "
                f"{seconds} {dsh_args} >{stdout} 2>{stderr}; rc=$?; "
                f'if [ "$rc" -eq 124 ]; then exit {_SLICE_EXIT_CODE}; fi; '
                'exit "$rc"; }'
            )
        else:
            run_command = f"{dsh_args} >{stdout} 2>{stderr}"
        return (
            "set -u; umask 077; "
            f"{credential_setup}"
            f"mkdir -p {home} {shlex.quote(stdout_file.parent.as_posix())}; "
            f"export DSH_HOME={home}; "
            f"export DSH_PERMISSION_MODE={permission}; "
            "export DSH_TELEMETRY_DISABLED=1; "
            f"export DSH_TOOLS_MODE={tools}; "
            f"{resume_exports}"
            f"{run_command}"
        )

    def _write_resume_runner(
        self,
        home_host: Path,
        home_container: PurePosixPath,
    ) -> PurePosixPath:
        """Materialize the env-driven Cordis resume overlay for one DSH home."""

        plugin_dir = home_host / "lhos-resume-runner"
        plugin_dir.mkdir(parents=True, exist_ok=True)
        modules_container = home_container / "profiles" / "node_modules"
        modules_url = "file://" + modules_container.as_posix()
        runner = _RESUME_RUNNER_TEMPLATE.replace("__MODULES_URL__", modules_url)
        (plugin_dir / "index.mjs").write_text(runner + "\n", encoding="utf-8")

        runner_container = home_container / "lhos-resume-runner" / "index.mjs"
        runner_url = "file://" + runner_container.as_posix()
        patch = (
            "# Generated by LongHorizonOS for verified DSH rc.8 session resume.\n"
            "- id: headless-startup\n"
            "  disabled: true\n"
            "- id: headless-runner\n"
            "  disabled: true\n"
            "- insert:\n"
            "    - id: lhos-headless-resume-runner\n"
            f"      name: '{runner_url}'\n"
            "      inject: [sessionPersistence]\n"
        )
        patch_path = home_host / "lhos-resume.patch.yml"
        patch_path.write_text(patch, encoding="utf-8")
        _atomic_write_json(
            home_host / "lhos-resume-runner.json",
            {
                "schema_version": _SCHEMA_VERSION,
                "runner_version": _RESUME_RUNNER_VERSION,
                "runner_sha256": hashlib.sha256(runner.encode("utf-8")).hexdigest(),
                "stock_headless_native_resume": False,
                "resume_api": "ctx.agents.resume",
            },
        )
        return home_container / "lhos-resume.patch.yml"

    def _trace(self, home: Path) -> DeepSeekTraceSummary:
        sessions_root = home / "sessions"
        fingerprint = _sessions_fingerprint(sessions_root)
        cached = self._trace_cache
        if (
            cached is not None
            and fingerprint is not None
            and cached[0] == fingerprint
        ):
            return cached[1]
        summary = parse_deepseek_sessions(
            sessions_root,
            workspace=Path(self.workdir.as_posix()),
        )
        if fingerprint is not None:
            self._trace_cache = (fingerprint, summary)
        return summary

    def _lock_resume_identity(
        self,
        *,
        home: Path,
        generation: int,
    ) -> None:
        trace = self._trace(home)
        if len(trace.session_files) != 1 or not trace.session_id:
            raise RuntimeError(
                "LHOS arm requires exactly one durable DSH session after the initial run"
            )
        if not _SESSION_ID.fullmatch(trace.session_id):
            raise RuntimeError("initial DSH session id is invalid")
        session_file = Path(trace.session_files[0])
        header = _session_header(session_file)
        if str(header.get("id", "")) != trace.session_id:
            raise RuntimeError("DSH parser/session-header identity mismatch")
        if str(header.get("cwd", "")) != self.workdir.as_posix():
            raise RuntimeError(
                "DSH session cwd does not match the LHTB workspace; refusing LHOS resume"
            )
        self._resume_session_id = trace.session_id
        self._resume_session_file = session_file
        self._resume_event_count = trace.event_count
        generation_record = {
            "generation": generation,
            "session_id": trace.session_id,
            "home": _safe_relative(home, self.logs_dir),
            "locked_after_invocation": self._invocation_count,
            "started_at": _utc_now(),
        }
        existing = next(
            (
                item
                for item in self._session_generations
                if int(item.get("generation", -1)) == generation
            ),
            None,
        )
        if existing is None:
            self._session_generations.append(generation_record)
        else:
            existing.update(generation_record)
        self._write_semantic_control()
        self._write_observability()

    def _verify_resumed_identity(
        self,
        *,
        allow_no_progress: bool = False,
    ) -> None:
        expected_id = self._resume_session_id
        expected_file = self._resume_session_file
        if expected_id is None or expected_file is None:
            raise RuntimeError("resume identity was not locked")
        trace = self._trace(self._active_host_home)
        if trace.session_id != expected_id:
            raise RuntimeError("DSH resume changed the durable session id")
        if len(trace.session_files) != 1:
            raise RuntimeError("DSH resume created an unexpected extra session JSONL")
        current_file = Path(trace.session_files[0])
        if str(_long_path(current_file)) != str(_long_path(expected_file)):
            raise RuntimeError("DSH resume wrote to a different session JSONL")
        header = _session_header(current_file)
        if str(header.get("id", "")) != expected_id:
            raise RuntimeError("resumed JSONL first-row session id changed")
        if trace.event_count < self._resume_event_count:
            raise RuntimeError("DSH resume durable event count moved backwards")
        if trace.event_count == self._resume_event_count and not allow_no_progress:
            raise RuntimeError("DSH resume produced no new durable session events")
        if trace.event_count > self._resume_event_count:
            self._resume_event_count = trace.event_count
        self._write_observability()

    def _aggregate_trace(self) -> DeepSeekTraceSummary:
        if self.arm == "baseline":
            root = self._baseline_host_root
        elif self.semantic_context_control:
            root = self.logs_dir
        else:
            root = self._shared_host_home
        return parse_deepseek_sessions(
            root,
            workspace=Path(self.workdir.as_posix()),
        )

    def _populate_context(
        self,
        context: AgentContext,
        *,
        current_home: Path,
        current_is_cumulative: bool,
        completed: bool,
        slice_preempted: bool,
        remaining_budget_seconds: int | None = None,
        budget_tail_slice: bool = False,
        budget_tail_no_progress: bool = False,
        slice_no_progress_failure: bool = False,
        max_tokens_checkpoint: bool = False,
        provider_censored: dict[str, Any] | None = None,
    ) -> None:
        current = self._trace(current_home)
        aggregate = self._aggregate_trace()
        current_usage = current.usage
        aggregate_usage = aggregate.usage
        trigger_counts, completed_not_verified = self._semantic_guard_summary()

        top_level = aggregate_usage if current_is_cumulative else current_usage
        context.n_input_tokens = (
            top_level.uncached_input_tokens
            + top_level.cache_read_tokens
            + top_level.cache_write_tokens
        )
        context.n_cache_tokens = top_level.cache_read_tokens + top_level.cache_write_tokens
        context.n_output_tokens = top_level.output_tokens
        context.cost_usd = (
            top_level.monetary_microusd / 1_000_000 if top_level.monetary_microusd else None
        )
        context.metadata = {
            "schema_version": _SCHEMA_VERSION,
            "experiment_arm": self.arm,
            "controller": "longhorizonos" if self.arm == "lhos" else "none",
            "controlled_pair_mode": self.controlled_pair_mode,
            "harbor_continue_mode": (
                "same_conversation"
                if self.controlled_pair_mode
                else os.environ.get("HB_CONTINUE_MODE", "") or None
            ),
            "verifier_feedback_mode": os.environ.get(
                "HB_VERIFIER_FEEDBACK_MODE", "binary"
            ),
            "execution_location": "lhtb-container",
            "workspace": self.workdir.as_posix(),
            "dsh_version": self.expected_dsh_version,
            "dsh_invocations_cumulative": self._invocation_count,
            "dsh_resumes_cumulative": self._resume_count,
            "dsh_session_id": self._resume_session_id if self.arm == "lhos" else None,
            "dsh_session_reused": bool(self._resume_count),
            "dsh_resume_evidence_mode": (
                "adaptive_context_control"
                if self.semantic_context_control
                else "durable_session_resume"
                if self.arm == "lhos"
                else None
            ),
            "dsh_semantic_context_control": self.semantic_context_control,
            "dsh_session_generation": (
                self._session_generation if self.arm == "lhos" else None
            ),
            "dsh_session_generation_count": (
                len(self._session_generations) if self.arm == "lhos" else 0
            ),
            "dsh_controlled_restarts_cumulative": self._controlled_restart_count,
            "dsh_semantic_decisions_cumulative": len(self._semantic_decisions),
            "dsh_semantic_guard_trigger_counts": trigger_counts,
            "dsh_completed_without_verification_cumulative": completed_not_verified,
            "dsh_max_tokens_checkpoints_cumulative": self._max_tokens_checkpoints,
            "dsh_usage_current": _usage_payload(current),
            "dsh_usage_cumulative": _usage_payload(aggregate),
            "dsh_model_calls_cumulative": aggregate_usage.model_calls,
            "dsh_tool_calls_cumulative": aggregate_usage.tool_calls,
            "dsh_token_units_cumulative": aggregate_usage.total_token_units,
            "dsh_retries_cumulative": aggregate.retries,
            "dsh_retry_delay_ms_cumulative": aggregate.retry_delay_ms,
            "dsh_event_count_cumulative": aggregate.event_count,
            "dsh_read_set_size_cumulative": len(aggregate.read_set),
            "dsh_write_set_size_cumulative": len(aggregate.write_set),
            "dsh_unknown_io": aggregate.unknown_io,
            "observability_artifact": self._observability_path.name,
            "n_episodes": len(aggregate.session_files),
            "time_slice_seconds": self.time_slice_seconds,
            "effective_time_slice_seconds": self._last_effective_slice_seconds,
            "remaining_budget_seconds": remaining_budget_seconds,
            "budget_tail_slice": budget_tail_slice,
            "slice_preempted": slice_preempted,
            "slice_preemptions_cumulative": self._slice_preemptions,
            "budget_tail_no_progress": budget_tail_no_progress,
            "budget_tail_no_progress_checkpoints_cumulative": (
                self._budget_tail_no_progress_checkpoints
            ),
            "slice_no_progress_failure": slice_no_progress_failure,
            "slice_no_progress_failures_cumulative": self._slice_no_progress_failures,
            "max_tokens_checkpoint": max_tokens_checkpoint,
            "provider_censored": provider_censored is not None,
            "provider_censored_cumulative": self._provider_censored_count,
            "provider_failure": provider_censored,
        }
        if completed:
            # Harbor's same-conversation loop only runs an interim verifier
            # after a confirmed successful agent turn. A zero exit alone is
            # not exposed as proof; it is recorded here alongside the DSH
            # durable trace and only on the successful process path.
            context.metadata["termination_reason"] = "confirmed_task_complete"
        elif slice_preempted and not slice_no_progress_failure:
            # Harbor's current same-conversation loop recognizes this marker
            # before running its interim verifier. The semantic reason remains
            # explicit so downstream reports do not mistake a controller
            # checkpoint for task success.
            context.metadata["termination_reason"] = "confirmed_task_complete"
            context.metadata["controller_termination_reason"] = (
                "budget_tail_no_progress_checkpoint"
                if budget_tail_no_progress
                else "time_slice_preempted"
            )
            context.metadata["slice_preempted"] = True
        elif max_tokens_checkpoint:
            context.metadata["termination_reason"] = "confirmed_task_complete"
            context.metadata["controller_termination_reason"] = "max_tokens_checkpoint"
            context.metadata["max_tokens_checkpoint"] = True
        self._write_observability(aggregate=aggregate)

    def _write_observability(
        self,
        *,
        aggregate: DeepSeekTraceSummary | None = None,
    ) -> None:
        trace = aggregate or self._aggregate_trace()
        trigger_counts, completed_not_verified = self._semantic_guard_summary()
        invocations: list[dict[str, Any]] = []
        invocation_root = self.logs_dir / "invocations"
        if invocation_root.exists():
            for path in sorted(invocation_root.glob("invocation-*/invocation.json")):
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                if isinstance(value, dict):
                    value["record"] = _safe_relative(path, self.logs_dir)
                    invocations.append(value)
        last_invocation = invocations[-1] if invocations else {}

        payload = {
            "schema_version": _SCHEMA_VERSION,
            "arm": self.arm,
            "controller": "longhorizonos" if self.arm == "lhos" else "none",
            "controlled_pair_mode": self.controlled_pair_mode,
            "harbor_continue_mode": (
                "same_conversation"
                if self.controlled_pair_mode
                else os.environ.get("HB_CONTINUE_MODE", "") or None
            ),
            "verifier_feedback_mode": os.environ.get(
                "HB_VERIFIER_FEEDBACK_MODE", "binary"
            ),
            "execution_location": "lhtb-container",
            "workspace": self.workdir.as_posix(),
            "model": self.model_name,
            "dsh_version": self.expected_dsh_version,
            "profile": self.profile,
            "credential_env": self.credential_env,
            "credential_transport": self._credential_transport,
            "stock_headless_native_resume": False,
            "resume_api": "ctx.agents.resume" if self.arm == "lhos" else None,
            "session_id": self._resume_session_id if self.arm == "lhos" else None,
            "session_reused": bool(self._resume_count),
            "resume_evidence_mode": (
                "adaptive_context_control"
                if self.semantic_context_control
                else "durable_session_resume"
                if self.arm == "lhos"
                else None
            ),
            "semantic_context_control": self.semantic_context_control,
            "semantic_control_artifact": (
                self._semantic_control_path.name
                if self.semantic_context_control
                else None
            ),
            "session_generation": (
                self._session_generation if self.arm == "lhos" else None
            ),
            "session_generation_count": (
                len(self._session_generations) if self.arm == "lhos" else 0
            ),
            "session_generations": (
                list(self._session_generations) if self.arm == "lhos" else []
            ),
            "controlled_restart_count": self._controlled_restart_count,
            "semantic_decision_count": len(self._semantic_decisions),
            "semantic_decisions": list(self._semantic_decisions),
            "semantic_guard_trigger_counts": trigger_counts,
            "completed_without_verification_count": completed_not_verified,
            "max_tokens_checkpoints": self._max_tokens_checkpoints,
            "time_slice_seconds": self.time_slice_seconds,
            "effective_time_slice_seconds": self._last_effective_slice_seconds,
            "last_invocation_status": last_invocation.get("status"),
            "last_slice_checkpoint_kind": last_invocation.get(
                "slice_checkpoint_kind"
            ),
            "remaining_budget_seconds": last_invocation.get(
                "remaining_budget_seconds"
            ),
            "budget_tail_slice": bool(last_invocation.get("budget_tail_slice")),
            "slice_preempted": any(bool(item.get("slice_preempted")) for item in invocations),
            "slice_preemptions_cumulative": self._slice_preemptions,
            "budget_tail_no_progress": any(
                bool(item.get("budget_tail_no_progress")) for item in invocations
            ),
            "budget_tail_no_progress_checkpoints": (
                self._budget_tail_no_progress_checkpoints
            ),
            "slice_no_progress_failure": any(
                bool(item.get("slice_no_progress_failure")) for item in invocations
            ),
            "slice_no_progress_failures": self._slice_no_progress_failures,
            "provider_censored": self._provider_censored_count > 0,
            "provider_censored_count": self._provider_censored_count,
            "provider_failure": self._last_provider_failure,
            "invocation_count": self._invocation_count,
            "resume_count": self._resume_count,
            "session_file_count": len(trace.session_files),
            "event_count": trace.event_count,
            "usage": _usage_payload(trace),
            "retries": trace.retries,
            "retry_delay_ms": trace.retry_delay_ms,
            "read_set_size": len(trace.read_set),
            "write_set_size": len(trace.write_set),
            "unknown_io": trace.unknown_io,
            "turn_end_reasons": trace.turn_end_reasons,
            "wall_elapsed_ms": round((time.monotonic() - self._started_monotonic) * 1000),
            "invocations": invocations,
            "updated_at": _utc_now(),
        }
        _atomic_write_json(self._observability_path, payload)

    def populate_context_post_run(self, context: AgentContext) -> None:
        """Recover partial usage after Harbor timeout/cancellation."""

        trace = self._aggregate_trace()
        usage = trace.usage
        trigger_counts, completed_not_verified = self._semantic_guard_summary()
        context.n_input_tokens = (
            usage.uncached_input_tokens + usage.cache_read_tokens + usage.cache_write_tokens
        )
        context.n_cache_tokens = usage.cache_read_tokens + usage.cache_write_tokens
        context.n_output_tokens = usage.output_tokens
        context.cost_usd = usage.monetary_microusd / 1_000_000 if usage.monetary_microusd else None
        context.metadata = {
            "schema_version": _SCHEMA_VERSION,
            "experiment_arm": self.arm,
            "controller": "longhorizonos" if self.arm == "lhos" else "none",
            "controlled_pair_mode": self.controlled_pair_mode,
            "harbor_continue_mode": (
                "same_conversation"
                if self.controlled_pair_mode
                else os.environ.get("HB_CONTINUE_MODE", "") or None
            ),
            "verifier_feedback_mode": os.environ.get(
                "HB_VERIFIER_FEEDBACK_MODE", "binary"
            ),
            "dsh_invocations_cumulative": self._invocation_count,
            "dsh_resumes_cumulative": self._resume_count,
            "dsh_session_id": self._resume_session_id if self.arm == "lhos" else None,
            "dsh_session_reused": bool(self._resume_count),
            "dsh_resume_evidence_mode": (
                "adaptive_context_control"
                if self.semantic_context_control
                else "durable_session_resume"
                if self.arm == "lhos"
                else None
            ),
            "dsh_semantic_context_control": self.semantic_context_control,
            "dsh_session_generation": (
                self._session_generation if self.arm == "lhos" else None
            ),
            "dsh_session_generation_count": (
                len(self._session_generations) if self.arm == "lhos" else 0
            ),
            "dsh_controlled_restarts_cumulative": self._controlled_restart_count,
            "dsh_semantic_decisions_cumulative": len(self._semantic_decisions),
            "dsh_semantic_guard_trigger_counts": trigger_counts,
            "dsh_completed_without_verification_cumulative": completed_not_verified,
            "dsh_max_tokens_checkpoints_cumulative": self._max_tokens_checkpoints,
            "dsh_usage_cumulative": _usage_payload(trace),
            "dsh_model_calls_cumulative": usage.model_calls,
            "dsh_tool_calls_cumulative": usage.tool_calls,
            "dsh_token_units_cumulative": usage.total_token_units,
            "dsh_retries_cumulative": trace.retries,
            "dsh_event_count_cumulative": trace.event_count,
            "dsh_unknown_io": trace.unknown_io,
            "observability_artifact": self._observability_path.name,
            "n_episodes": len(trace.session_files),
            "time_slice_seconds": self.time_slice_seconds,
            "effective_time_slice_seconds": self._last_effective_slice_seconds,
            "slice_preempted": False,
            "slice_preemptions_cumulative": self._slice_preemptions,
            "budget_tail_no_progress": self._last_budget_tail_no_progress,
            "budget_tail_no_progress_checkpoints_cumulative": (
                self._budget_tail_no_progress_checkpoints
            ),
            "slice_no_progress_failure": False,
            "slice_no_progress_failures_cumulative": self._slice_no_progress_failures,
            "provider_censored": self._provider_censored_count > 0,
            "provider_censored_cumulative": self._provider_censored_count,
            "provider_failure": self._last_provider_failure,
        }
        self._write_observability(aggregate=trace)


__all__ = ["LHTBDeepSeekHarnessAgent"]
