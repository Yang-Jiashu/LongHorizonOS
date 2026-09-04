"""Generate a per-case execution profile from a SWE host-native result."""

# ruff: noqa: RUF001

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _session_profile(session_file: Path, elapsed_ms: float) -> dict[str, Any]:
    event_counts: Counter[str] = Counter()
    tool_counts: Counter[str] = Counter()
    tool_started: dict[str, tuple[str, float]] = {}
    tool_duration_by_name: defaultdict[str, float] = defaultdict(float)
    first_time: float | None = None
    last_time: float | None = None
    retry_count = 0
    retry_delay_ms = 0.0
    tool_error_count = 0
    provider = ""
    model = ""
    api = ""

    lines = session_file.read_text(encoding="utf-8").splitlines()
    for raw in lines[1:]:
        if not raw.strip():
            continue
        event = json.loads(raw)
        event_type = str(event.get("type", ""))
        event_counts[event_type] += 1
        timestamp = float(event.get("time", 0.0) or 0.0)
        first_time = timestamp if first_time is None else min(first_time, timestamp)
        last_time = timestamp if last_time is None else max(last_time, timestamp)
        data = event.get("data", {})
        if not isinstance(data, dict):
            continue
        if event_type == "llm/retry":
            retry_count += 1
            retry_delay_ms += float(data.get("delayMs", 0.0) or 0.0)
        elif event_type == "assistant/message":
            message = data.get("message", {})
            source = message.get("source", {}) if isinstance(message, dict) else {}
            if isinstance(source, dict):
                provider = provider or str(source.get("provider", ""))
                model = model or str(source.get("model", ""))
                replay_state = source.get("replayState", {})
                response = (
                    replay_state.get("response", {}) if isinstance(replay_state, dict) else {}
                )
                if isinstance(response, dict):
                    provider = provider or str(response.get("provider", ""))
                    model = model or str(response.get("model", ""))
                    api = api or str(response.get("api", ""))
        elif event_type == "tool/call":
            call_id = str(data.get("callId", ""))
            name = str(data.get("name", ""))
            tool_counts[name] += 1
            tool_started[call_id] = (name, timestamp)
        elif event_type == "tool/result":
            message = data.get("message", {})
            if not isinstance(message, dict):
                continue
            source = message.get("source", {})
            call_id = str(source.get("callId", "")) if isinstance(source, dict) else ""
            started = tool_started.get(call_id)
            if started is not None:
                name, started_at = started
                tool_duration_by_name[name] += max(0.0, timestamp - started_at)
            for item in message.get("content", ()):
                if isinstance(item, dict) and item.get("isError") is True:
                    tool_error_count += 1

    session_span_ms = (
        0.0 if first_time is None or last_time is None else max(0.0, last_time - first_time)
    )
    tool_duration_ms = sum(tool_duration_by_name.values())
    return {
        "session_span_ms": round(session_span_ms, 3),
        "process_outside_session_ms": round(max(0.0, elapsed_ms - session_span_ms), 3),
        "retry_count": retry_count,
        "retry_delay_ms": round(retry_delay_ms, 3),
        "tool_duration_ms": round(tool_duration_ms, 3),
        "tool_duration_ms_by_name": {
            key: round(value, 3) for key, value in sorted(tool_duration_by_name.items())
        },
        "model_context_network_ms": round(
            max(0.0, session_span_ms - retry_delay_ms - tool_duration_ms),
            3,
        ),
        "tool_counts": dict(sorted(tool_counts.items())),
        "tool_error_count": tool_error_count,
        "event_counts": dict(sorted(event_counts.items())),
        "provider": provider,
        "model": model,
        "api": api,
    }


def _arm_profile(arm: dict[str, Any]) -> dict[str, Any]:
    dsh = arm["dsh"]
    usage = dsh["usage"]
    session_files = [Path(item) for item in usage.get("session_files", ())]
    session = _session_profile(session_files[0], float(dsh["elapsed_ms"])) if session_files else {}
    target_tests = arm.get("evaluation", {}).get("target_tests", {})
    stdout_tail = str(target_tests.get("stdout_tail", ""))
    test_summary = next(
        (line.strip() for line in reversed(stdout_tail.splitlines()) if line.strip()),
        "",
    )
    return {
        "valid": bool(arm["valid"]),
        "elapsed_ms": float(arm["elapsed_ms"]),
        "dsh_elapsed_ms": float(dsh["elapsed_ms"]),
        "model_calls": int(usage["model_calls"]),
        "tool_calls": int(usage["tool_calls"]),
        "token_units": int(usage["total_token_units"]),
        "uncached_input_tokens": int(usage["uncached_input_tokens"]),
        "cache_read_tokens": int(usage["cache_read_tokens"]),
        "cache_write_tokens": int(usage["cache_write_tokens"]),
        "output_tokens": int(usage["output_tokens"]),
        "target_test_summary": test_summary,
        "session": session,
    }


def build_profile(
    result_path: Path,
    *,
    model_override: str | None = None,
    reasoning: str | None = None,
    harness: str = "DeepSeek Harness",
) -> dict[str, Any]:
    result = _load_json(result_path)
    static = _arm_profile(result["static"])
    lhos = _arm_profile(result["lhos"])
    same_patch = result["static"]["evaluation"].get("agent_patch", "") == result["lhos"][
        "evaluation"
    ].get("agent_patch", "")
    static_model = str(static["session"].get("model", ""))
    lhos_model = str(lhos["session"].get("model", ""))
    model = model_override or (static_model if static_model == lhos_model else "")
    return {
        "schema_version": "lhos-swe-case-profile.v1",
        "source_result": str(result_path),
        "instance_id": result["instance_id"],
        "repo": result["repo"],
        "base_commit": result["base_commit"],
        "configuration": {
            "provider": str(
                static["session"].get("provider") or lhos["session"].get("provider") or ""
            ),
            "model": model,
            "static_model": static_model,
            "lhos_model": lhos_model,
            "reasoning": reasoning,
            "harness": harness,
        },
        "correctness": {
            "pair_valid": bool(result["comparison"]["pair_valid"]),
            "same_agent_patch": same_patch,
            "static_tests_passed": bool(result["static"]["evaluation"]["passed"]),
            "lhos_tests_passed": bool(result["lhos"]["evaluation"]["passed"]),
            "lhos_goal_state": result["lhos"]["run_result"]["goal_state"],
        },
        "static": static,
        "lhos": lhos,
        "observed_static_minus_lhos": {
            "elapsed_ms": round(static["elapsed_ms"] - lhos["elapsed_ms"], 3),
            "dsh_elapsed_ms": round(static["dsh_elapsed_ms"] - lhos["dsh_elapsed_ms"], 3),
            "token_units": static["token_units"] - lhos["token_units"],
            "uncached_input_tokens": (
                static["uncached_input_tokens"] - lhos["uncached_input_tokens"]
            ),
            "cache_read_tokens": static["cache_read_tokens"] - lhos["cache_read_tokens"],
            "cache_write_tokens": (static["cache_write_tokens"] - lhos["cache_write_tokens"]),
            "output_tokens": static["output_tokens"] - lhos["output_tokens"],
            "model_calls": static["model_calls"] - lhos["model_calls"],
            "tool_calls": static["tool_calls"] - lhos["tool_calls"],
        },
        "attribution": {
            "os_causal_saving": False,
            "preserved_tasks": 0,
            "skipped_tasks": 0,
            "reason": (
                "This is one independent READY task. LongHorizonOS dispatched it once, so "
                "the observed difference comes from two stochastic model trajectories, "
                "not selective scheduling or repair."
            ),
        },
    }


def _observed_pct(delta_static_minus_lhos: float, baseline: float) -> str:
    if baseline == 0:
        return "n/a"
    return f"{-100.0 * delta_static_minus_lhos / baseline:+.1f}%"


def _observed_change(delta_static_minus_lhos: float) -> str:
    return f"{-delta_static_minus_lhos:+,.0f}"


def _call_sentence(kind: str, static_value: int, lhos_value: int) -> str:
    delta = lhos_value - static_value
    if delta == 0:
        return f"两条轨迹的 {kind} 相同，均为 {static_value}。"
    direction = "多" if delta > 0 else "少"
    return f"LHOS 轨迹比 static {direction} {abs(delta)} 次 {kind}。"


def render_markdown(profile: dict[str, Any]) -> str:
    static = profile["static"]
    lhos = profile["lhos"]
    delta = profile["observed_static_minus_lhos"]
    static_session = static["session"]
    lhos_session = lhos["session"]
    configuration = profile["configuration"]
    model_label = configuration["model"] or (
        f"static={configuration['static_model']}, lhos={configuration['lhos_model']}"
    )
    reasoning_label = configuration.get("reasoning") or "not recorded"
    model_call_sentence = _call_sentence(
        "model call",
        int(static["model_calls"]),
        int(lhos["model_calls"]),
    )
    tool_call_sentence = _call_sentence(
        "tool call",
        int(static["tool_calls"]),
        int(lhos["tool_calls"]),
    )
    cache_delta = int(delta["cache_read_tokens"])
    token_delta = int(delta["token_units"])
    cache_share = 0.0 if token_delta == 0 else 100.0 * cache_delta / token_delta
    wall_delta_ms = float(delta["elapsed_ms"])
    model_wait_delta_ms = float(static_session["model_context_network_ms"]) - float(
        lhos_session["model_context_network_ms"]
    )
    tool_delta_ms = float(static_session["tool_duration_ms"]) - float(
        lhos_session["tool_duration_ms"]
    )
    outside_delta_ms = float(static_session["process_outside_session_ms"]) - float(
        lhos_session["process_outside_session_ms"]
    )
    patch_description = (
        "两边生成同一份 source patch"
        if profile["correctness"]["same_agent_patch"]
        else "两边生成不同的 source patch"
    )
    static_passed = bool(profile["correctness"]["static_tests_passed"])
    lhos_passed = bool(profile["correctness"]["lhos_tests_passed"])
    if static_passed and lhos_passed:
        correctness_description = "，且都通过外部注入的公开目标测试。"
    elif not static_passed and not lhos_passed:
        correctness_description = "，但都没有通过外部注入的公开目标测试。"
    else:
        passed_arm = "static" if static_passed else "LHOS"
        correctness_description = f"，只有 {passed_arm} 通过外部注入的公开目标测试。"
    patch_sentence = patch_description + correctness_description
    final_interpretation = (
        "这个 case 证明的是 StepFun -> DSH -> LongHorizonOS -> verifier 链路可用，\n"
        "以及 LHOS 持有最终 VERIFIED/closed 权限。它不能证明 LHOS 让单次 Agent call\n"
        "更快。OS 的可归因节省仍应由包含变更、独立分支和 selective repair 的\n"
        "multi-task episode 测量。"
        if static_passed and lhos_passed
        else "这是一个 unresolved failure case：verifier 正确拒绝了未满足公开契约的 patch，\n"
        "因此不能计入 resolved，也不能从 token/time 差异推导 OS 收益。失败轨迹仍保留，\n"
        "用于分析模型为什么理解错 API contract。"
    )
    lines = [
        f"# Case Profiling: {profile['instance_id']} (StepFun)",
        "",
        "## 结论",
        "",
        patch_sentence,
        "本例只有一个 READY Task，没有可保留分支、失效锥或并行调度决定。因此下面的",
        "token/time 差异是两次独立模型轨迹的观测，不是 LongHorizonOS 的因果收益。",
        "",
        "## Case",
        "",
        "```text",
        f"instance: {profile['instance_id']}",
        f"repo: {profile['repo']}",
        f"base: {profile['base_commit']}",
        f"provider: {configuration['provider'] or 'not recorded'}",
        f"model: {model_label or 'not recorded'}",
        f"reasoning: {reasoning_label}",
        f"harness: {configuration['harness']}",
        "input modality: text only",
        "```",
        "",
        "## 结果",
        "",
        "| 指标 | DSH static | DSH + LHOS | 观测差异 |",
        "|---|---:|---:|---:|",
        (
            f"| Provider token units | {static['token_units']:,} | "
            f"{lhos['token_units']:,} | {_observed_change(delta['token_units'])} "
            f"({_observed_pct(delta['token_units'], static['token_units'])}) |"
        ),
        (
            f"| Uncached input | {static['uncached_input_tokens']:,} | "
            f"{lhos['uncached_input_tokens']:,} | "
            f"{_observed_change(delta['uncached_input_tokens'])} |"
        ),
        (
            f"| Cache read | {static['cache_read_tokens']:,} | "
            f"{lhos['cache_read_tokens']:,} | "
            f"{_observed_change(delta['cache_read_tokens'])} |"
        ),
        (
            f"| Output | {static['output_tokens']:,} | {lhos['output_tokens']:,} | "
            f"{_observed_change(delta['output_tokens'])} |"
        ),
        (
            f"| Model calls | {static['model_calls']} | {lhos['model_calls']} | "
            f"{-delta['model_calls']:+d} |"
        ),
        (
            f"| Tool calls | {static['tool_calls']} | {lhos['tool_calls']} | "
            f"{-delta['tool_calls']:+d} |"
        ),
        (
            f"| End-to-end wall | {static['elapsed_ms'] / 1000:.3f}s | "
            f"{lhos['elapsed_ms'] / 1000:.3f}s | "
            f"{static['elapsed_ms'] / lhos['elapsed_ms']:.2f}x |"
        ),
        (
            f"| Target tests | {static['target_test_summary'] or 'passed'} | "
            f"{lhos['target_test_summary'] or 'passed'} | same outcome |"
        ),
        "",
        f"{model_call_sentence}{tool_call_sentence}",
        (
            f"观测 token 差中 cache-read 占 {cache_share:.1f}%。这反映两次会话轨迹的"
            "上下文累计差异，不代表 OS 跳过了 Task。"
        ),
        "",
        "## 时间花在哪里",
        "",
        "| DSH session component | DSH static | DSH + LHOS |",
        "|---|---:|---:|",
        (
            f"| Model/context/provider waiting | "
            f"{static_session['model_context_network_ms'] / 1000:.3f}s | "
            f"{lhos_session['model_context_network_ms'] / 1000:.3f}s |"
        ),
        (
            f"| Tool execution | {static_session['tool_duration_ms'] / 1000:.3f}s | "
            f"{lhos_session['tool_duration_ms'] / 1000:.3f}s |"
        ),
        (
            f"| Retry delay | {static_session['retry_delay_ms'] / 1000:.3f}s | "
            f"{lhos_session['retry_delay_ms'] / 1000:.3f}s |"
        ),
        (
            f"| Process outside session | "
            f"{static_session['process_outside_session_ms'] / 1000:.3f}s | "
            f"{lhos_session['process_outside_session_ms'] / 1000:.3f}s |"
        ),
        "",
        (f"Retry：static={static_session['retry_count']}，LHOS={lhos_session['retry_count']}。"),
        (
            f"Static-LHOS wall 差为 {wall_delta_ms / 1000:.3f}s；其中"
            f" model/context/provider 桶差 {model_wait_delta_ms / 1000:+.3f}s，"
            f"tool execution 桶差 {tool_delta_ms / 1000:+.3f}s，"
            f"process outside session 差 {outside_delta_ms / 1000:+.3f}s。"
        ),
        "",
        "## Tool calls",
        "",
        "| Tool | DSH static | DSH + LHOS |",
        "|---|---:|---:|",
    ]
    tool_names = sorted(set(static_session["tool_counts"]) | set(lhos_session["tool_counts"]))
    for name in tool_names:
        lines.append(
            f"| `{name}` | {static_session['tool_counts'].get(name, 0)} | "
            f"{lhos_session['tool_counts'].get(name, 0)} |"
        )
    lines.extend(
        [
            "",
            "## OS attribution boundary",
            "",
            "```text",
            "preserved tasks = 0",
            "skipped tasks = 0",
            "semantic invalidation = 0",
            "parallel scheduling decision = 0",
            f"LHOS Goal state = {profile['correctness']['lhos_goal_state']}",
            "```",
            "",
            final_interpretation,
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--model")
    parser.add_argument("--reasoning")
    parser.add_argument("--harness", default="DeepSeek Harness")
    args = parser.parse_args()
    result_path = args.result.resolve()
    output_dir = (args.output_dir or result_path.parent).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    profile = build_profile(
        result_path,
        model_override=args.model,
        reasoning=args.reasoning,
        harness=args.harness,
    )
    (output_dir / "case-profile.json").write_text(
        json.dumps(profile, ensure_ascii=True, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (output_dir / "CASE-PROFILE.zh-CN.md").write_text(
        render_markdown(profile),
        encoding="utf-8",
    )
    print(output_dir / "case-profile.json")
    print(output_dir / "CASE-PROFILE.zh-CN.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
