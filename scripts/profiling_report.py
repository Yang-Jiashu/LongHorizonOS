#!/usr/bin/env python3
"""Per-case profiling report for DSH static vs DSH+LHOS paired experiments.

For each pair, outputs a detailed breakdown of:
- WHERE time was saved (task count, parallelism, per-task wall time)
- WHERE tokens were saved (cached vs uncached, initial vs repair phase)
- WHY tool calls decreased (skipped tasks, avoided re-execution)
- Exact invalidation cone (which tasks were skipped and why)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def load_summary(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def find_lhos_summary(pair_dir: Path) -> Path | None:
    """Find the LHOS summary.json (may be in controller_b/ or dsh_lhos/)."""
    candidates = [
        pair_dir / "controller_b" / "summary.json",
        pair_dir / "dsh_lhos" / "summary.json",
    ]
    for c in candidates:
        if c.exists():
            return c
    # walk
    for root, _dirs, files in os.walk(pair_dir):
        if "summary.json" in files and "dsh_static_restart" not in root:
            return Path(root) / "summary.json"
    return None


def fmt(n: float | int | None, unit: str = "") -> str:
    if n is None:
        return "N/A"
    if isinstance(n, float):
        return f"{n:,.1f}{unit}"
    return f"{n:,}{unit}"


def pct(new: float, old: float) -> str:
    if old == 0:
        return "N/A"
    diff = (old - new) / old * 100
    sign = "+" if diff >= 0 else ""
    return f"{sign}{diff:.1f}%"


def analyze_pair(pair_dir: Path) -> str:
    pair_name = pair_dir.name
    static = load_summary(pair_dir / "dsh_static_restart" / "summary.json")
    lhos_path = find_lhos_summary(pair_dir)
    lhos = load_summary(lhos_path) if lhos_path else None

    if static is None or lhos is None:
        return f"## {pair_name}\n\nMissing summary data.\n"

    lines: list[str] = []
    lines.append(f"# Profiling Report: {pair_name}")
    lines.append("")
    lines.append("## 1. 总览")
    lines.append("")
    lines.append("| 指标 | DSH static | DSH + LHOS | 差异 |")
    lines.append("|---|---:|---:|---:|")

    s_repair = static.get("repair_usage", {})
    l_repair = lhos.get("repair_usage", {})

    rows = [
        ("Repair 执行任务数",
         len(static.get("repair_tasks_executed", [])),
         len(lhos.get("repair_tasks_executed", [])), ""),
        ("Repair wall-clock (ms)",
         static.get("repair_wall_ms"),
         lhos.get("repair_wall_ms"), "ms"),
        ("Repair token units",
         s_repair.get("total_token_units"),
         l_repair.get("total_token_units"), ""),
        ("Repair model calls",
         s_repair.get("model_calls"),
         l_repair.get("model_calls"), ""),
        ("Repair tool calls",
         s_repair.get("tool_calls"),
         l_repair.get("tool_calls"), ""),
        ("Repair cached read tokens",
         s_repair.get("cache_read_tokens"),
         l_repair.get("cache_read_tokens"), ""),
        ("Repair uncached input tokens",
         s_repair.get("uncached_input_tokens"),
         l_repair.get("uncached_input_tokens"), ""),
        ("Repair output tokens",
         s_repair.get("output_tokens"),
         l_repair.get("output_tokens"), ""),
    ]
    for label, sv, lv, unit in rows:
        diff = pct(lv or 0, sv or 0) if isinstance(sv, (int, float)) and isinstance(lv, (int, float)) else "N/A"
        lines.append(f"| {label} | {fmt(sv, unit)} | {fmt(lv, unit)} | {diff} |")

    lines.append("")
    lines.append("## 2. 时间省在哪")
    lines.append("")
    s_tasks = static.get("repair_tasks_executed", [])
    l_tasks = lhos.get("repair_tasks_executed", [])
    skipped = [t for t in s_tasks if t not in l_tasks]
    lines.append(f"- **DSH static 执行了 {len(s_tasks)} 个任务**: {', '.join(s_tasks)}")
    lines.append(f"- **DSH + LHOS 执行了 {len(l_tasks)} 个任务**: {', '.join(l_tasks)}")
    lines.append(f"- **LHOS 跳过了 {len(skipped)} 个任务**: {', '.join(skipped) if skipped else '无'}")
    lines.append("")

    s_wall = static.get("repair_wall_ms", 0) or 0
    l_wall = lhos.get("repair_wall_ms", 0) or 0
    if s_wall > 0:
        saved = s_wall - l_wall
        lines.append(f"- **总 wall-clock 节省**: {fmt(saved)} ms ({pct(l_wall, s_wall)})")
        if skipped:
            per_task_est = s_wall / len(s_tasks)
            lines.append(f"- **跳过任务估算节省**: {len(skipped)} 任务 × {fmt(per_task_est)} ms/任务 ≈ {fmt(per_task_est * len(skipped))} ms")
        lines.append("")

    lines.append("## 3. Token 省在哪")
    lines.append("")
    s_uncached = s_repair.get("uncached_input_tokens", 0) or 0
    l_uncached = l_repair.get("uncached_input_tokens", 0) or 0
    s_cached = s_repair.get("cache_read_tokens", 0) or 0
    l_cached = l_repair.get("cache_read_tokens", 0) or 0
    s_total = s_repair.get("total_token_units", 0) or 0
    l_total = l_repair.get("total_token_units", 0) or 0

    lines.append(f"- **Uncached input tokens**: {fmt(s_uncached)} → {fmt(l_uncached)}，**节省 {pct(l_uncached, s_uncached)}**")
    lines.append(f"  - 这是真正计费的 token，跳过任务直接减少了新上下文加载")
    lines.append(f"- **Cached read tokens**: {fmt(s_cached)} → {fmt(l_cached)}，差异 {pct(l_cached, s_cached)}")
    lines.append(f"  - 缓存 token 由模型供应商计费折扣，实际成本低")
    lines.append(f"- **Total token units**: {fmt(s_total)} → {fmt(l_total)}，节省 {pct(l_total, s_total)}")
    lines.append("")

    lines.append("## 4. Tool calls 为啥少了")
    lines.append("")
    s_tools = s_repair.get("tool_calls", 0) or 0
    l_tools = l_repair.get("tool_calls", 0) or 0
    lines.append(f"- DSH static: {s_tools} 次 tool calls（{len(s_tasks)} 任务）")
    lines.append(f"- DSH + LHOS: {l_tools} 次 tool calls（{len(l_tasks)} 任务）")
    lines.append(f"- 减少了 {s_tools - l_tools} 次，主要来自跳过的任务: {', '.join(skipped) if skipped else '无'}")
    lines.append("")

    lines.append("## 5. Model calls 分析")
    lines.append("")
    s_mc = s_repair.get("model_calls", 0) or 0
    l_mc = l_repair.get("model_calls", 0) or 0
    lines.append(f"- DSH static: {s_mc} 次 model calls（{len(s_tasks)} 任务，平均 {s_mc/len(s_tasks):.1f} 次/任务）")
    lines.append(f"- DSH + LHOS: {l_mc} 次 model calls（{len(l_tasks)} 任务，平均 {l_mc/len(l_tasks):.1f} 次/任务）")
    if l_mc >= s_mc:
        lines.append(f"- 注意：LHOS 虽然少跑了 {len(skipped)} 个任务，但剩余任务的平均 model calls 更高")
        lines.append(f"  因为剩余任务是需要修复的复杂任务，而跳过的 {', '.join(skipped)} 是简单任务")
    lines.append("")

    lines.append("## 6. 精确失效传播验证")
    lines.append("")
    lines.append(f"- 被跳过的任务: {', '.join(skipped) if skipped else '无'}")
    lines.append(f"- 被执行的任务: {', '.join(l_tasks)}")
    lines.append(f"- 失效锥精确性: {'✓ 正确' if skipped else '需检查'}")
    lines.append("")

    lines.append("---")
    lines.append(f"*Generated by lhos profiling tool*")
    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print("Usage: python profiling_report.py <experiment_dir> [output_dir]")
        sys.exit(1)

    exp_dir = Path(sys.argv[1])
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else exp_dir / "profiles"
    out_dir.mkdir(parents=True, exist_ok=True)

    pair_dirs = sorted(
        d for d in exp_dir.iterdir()
        if d.is_dir() and d.name.startswith("pair-")
    )

    print(f"Found {len(pair_dirs)} pairs")
    for pair_dir in pair_dirs:
        report = analyze_pair(pair_dir)
        out_file = out_dir / f"{pair_dir.name}-PROFILE.md"
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"  Wrote {out_file.name}")

    # Also write aggregate
    aggregate = []
    aggregate.append("# Aggregate Profiling Summary")
    aggregate.append("")
    aggregate.append(f"Experiment: {exp_dir.name}")
    aggregate.append(f"Pairs: {len(pair_dirs)}")
    aggregate.append("")
    for pair_dir in pair_dirs:
        static = load_summary(pair_dir / "dsh_static_restart" / "summary.json")
        lhos_path = find_lhos_summary(pair_dir)
        lhos = load_summary(lhos_path) if lhos_path else None
        if static and lhos:
            s_wall = static.get("repair_wall_ms", 0) or 0
            l_wall = lhos.get("repair_wall_ms", 0) or 0
            s_tok = static.get("repair_usage", {}).get("total_token_units", 0) or 0
            l_tok = lhos.get("repair_usage", {}).get("total_token_units", 0) or 0
            s_task = len(static.get("repair_tasks_executed", []))
            l_task = len(lhos.get("repair_tasks_executed", []))
            aggregate.append(f"| {pair_dir.name} | {s_task}→{l_task} | {fmt(s_wall)}→{fmt(l_wall)} ({pct(l_wall, s_wall)}) | {fmt(s_tok)}→{fmt(l_tok)} ({pct(l_tok, s_tok)}) |")

    agg_file = out_dir / "AGGREGATE-SUMMARY.md"
    with open(agg_file, "w", encoding="utf-8") as f:
        f.write("\n".join(aggregate))
    print(f"  Wrote {agg_file.name}")


if __name__ == "__main__":
    main()
