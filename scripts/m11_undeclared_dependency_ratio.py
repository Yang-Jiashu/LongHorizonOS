"""M11 — how many of an agent's real dependencies are UNDECLARED?

WHY THIS IS THE LOAD-BEARING MEASUREMENT
----------------------------------------
`b4_content_hash_baseline.py` shows that a statically-declared-dependency
rebuilder (make / Bazel semantics) accrues under-invalidation in proportion to
the fraction of dependencies it cannot see.  That result is a *conditional*:
it only matters if agents really do consume artifacts they never declared.

If the undeclared fraction is ~0, the B4 result is a theoretical construction
and the whole argument for evidence-recorded dependencies collapses.  If it is
large, the argument holds.  This script measures it on a REAL agent trace.

DATA
----
`artifacts/stuck_recovery_debug_v3/llm-calls.jsonl` — 63 real model calls
(provider sensenova, model sensenova-6.7-flash-lite, 2026-08-04).  Each record
carries the full request and response body.

  * The request's Context Packet has an explicit ``Dependencies:`` section --
    this is the DECLARED dependency set handed to the node.
  * The response's ``parsed_output.tool_request`` is the action the agent chose.
    Read-type actions (filesystem op=read/list/exists, and shell commands such
    as cat/ls/find/grep/head/tail) reveal artifacts the agent ACTUALLY consumed.

CONSUMED - DECLARED = undeclared dependencies, i.e. exactly what a
static-declaration rebuilder is blind to.

CAVEATS (reported, not hidden)
------------------------------
  * One trace, one model, one workspace.  This is an existence-and-magnitude
    measurement, not a distribution.
  * `shell` command parsing is heuristic: we extract path-like tokens.
  * A read does not prove the read content influenced the verification verdict.
    So this is an UPPER bound on the true dependency set, and a LOWER bound on
    what a static declaration misses (since declared is nearly always empty).
"""

# ruff: noqa
from __future__ import annotations

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TRACE = REPO / "artifacts" / "stuck_recovery_debug_v3" / "llm-calls.jsonl"

READ_OPS = {"read", "list", "exists"}
WRITE_OPS = {"write", "append", "delete"}
SHELL_READERS = ("cat", "ls", "find", "grep", "head", "tail", "wc", "diff", "stat")
PATHLIKE = re.compile(
    r"[\w./-]*\.(?:py|txt|json|md|yaml|yml|toml|cfg|ini)\b|(?:^|\s)(?:src|tests?|configs?)(?:/[\w./-]+)?"
)


def load_records():
    out = []
    with TRACE.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            if line.strip():
                out.append(json.loads(line))
    return out


def _as_obj(body):
    """Bodies may be dicts, JSON strings, or TRUNCATED JSON strings."""
    if isinstance(body, dict):
        return body, False
    if not isinstance(body, str):
        return {}, True
    try:
        return json.loads(body), False
    except Exception:
        return {"__raw__": body}, True


def declared_deps(request_body) -> tuple[list[str], bool]:
    """Parse the Context Packet's Dependencies section."""
    obj, truncated = _as_obj(request_body)
    if "__raw__" in obj:
        text = obj["__raw__"]
    else:
        text = "\n".join(m.get("content", "") for m in obj.get("messages", []))
    m = re.search(
        r"Dependencies:\s*\\?n?\s*(.*?)(?=Constraints:|Previous failures:|\Z)", text, re.S
    )
    if not m:
        return [], truncated
    block = m.group(1).strip().strip("\\n").strip()
    if not block or block.lower().startswith("none"):
        return [], truncated
    parts = [p.strip("- \\n").strip() for p in re.split(r"\\n|\n", block)]
    return [p for p in parts if p and not p.lower().startswith("none")], truncated


def consumed_paths(response_body) -> tuple[set[str], set[str]]:
    """Return (read_paths, written_paths) implied by the chosen tool action."""
    response_body, _trunc = _as_obj(response_body)
    if "__raw__" in response_body:
        raw = response_body["__raw__"]
        reads0: set[str] = set()
        for tok in re.findall(r'"path"\s*:\s*"([^"]+)"', raw):
            reads0.add(tok)
        return reads0, set()
    parsed = (response_body or {}).get("parsed_output") or {}
    tr = parsed.get("tool_request") or {}
    name = tr.get("tool_name")
    args = tr.get("arguments") or {}
    reads: set[str] = set()
    writes: set[str] = set()
    if name == "filesystem":
        op = str(args.get("op", "")).lower()
        path = args.get("path")
        if path:
            if op in READ_OPS:
                reads.add(str(path))
            elif op in WRITE_OPS:
                writes.add(str(path))
    elif name == "shell":
        cmd = str(args.get("command", ""))
        head = cmd.strip().split()[:1]
        is_reader = bool(head) and any(head[0].endswith(r) for r in SHELL_READERS)
        for tok in re.findall(r"[\w./~-]+\.(?:py|txt|json|md|yaml|yml|toml|cfg|ini)\b", cmd):
            (reads if is_reader else writes).add(tok)
        for tok in re.findall(r"(?:^|\s)((?:src|tests?|configs?)(?:/[\w./-]+)?)", cmd):
            if is_reader:
                reads.add(tok.strip())
    return reads, writes


def main() -> int:
    if not TRACE.exists():
        print(f"trace not found: {TRACE}")
        return 2
    out_dir = REPO / "artifacts" / "agent_os_phase_d3"
    out_dir.mkdir(parents=True, exist_ok=True)

    recs = load_records()
    workers = [r for r in recs if r.get("role") == "worker"]

    per_node_declared: dict[str, set[str]] = defaultdict(set)
    per_node_read: dict[str, set[str]] = defaultdict(set)
    per_node_written: dict[str, set[str]] = defaultdict(set)
    tool_use = Counter()
    calls_with_declared = 0
    calls_with_read = 0
    truncated = 0

    for r in workers:
        node = r.get("node_id") or "?"
        d, trunc = declared_deps(r.get("request_body_json"))
        truncated += 1 if trunc else 0
        if d:
            calls_with_declared += 1
        per_node_declared[node].update(d)
        reads, writes = consumed_paths(r.get("response_body_json"))
        if reads:
            calls_with_read += 1
        per_node_read[node].update(reads)
        per_node_written[node].update(writes)
        parsed, _t2 = _as_obj(r.get("response_body_json"))
        tr = ((parsed or {}).get("parsed_output") or {}).get("tool_request") or {}
        if tr.get("tool_name"):
            op = (tr.get("arguments") or {}).get("op") or (
                str((tr.get("arguments") or {}).get("command", "")).strip().split()[:1] or [""]
            )[0]
            tool_use[f"{tr['tool_name']}:{op}"] += 1

    nodes = sorted(set(per_node_read) | set(per_node_declared))
    rows = []
    tot_read = tot_undeclared = 0
    for n in nodes:
        declared = per_node_declared[n]
        read = per_node_read[n]
        undeclared = read - declared
        tot_read += len(read)
        tot_undeclared += len(undeclared)
        rows.append(
            {
                "node_id": n,
                "declared_deps": sorted(declared),
                "artifacts_read": sorted(read),
                "artifacts_written": sorted(per_node_written[n]),
                "undeclared_reads": sorted(undeclared),
                "undeclared_ratio": (round(len(undeclared) / len(read), 3) if read else None),
            }
        )

    ratio = round(tot_undeclared / tot_read, 4) if tot_read else None
    report = {
        "measurement": "M11_undeclared_dependency_ratio",
        "trace": str(TRACE.relative_to(REPO)),
        "provider": recs[0].get("provider"),
        "model": recs[0].get("exact_model_id"),
        "worker_calls": len(workers),
        "records_with_truncated_body": truncated,
        "calls_that_declared_any_dependency": calls_with_declared,
        "calls_that_read_something": calls_with_read,
        "tool_action_histogram": dict(tool_use),
        "total_distinct_artifacts_read": tot_read,
        "total_undeclared_reads": tot_undeclared,
        "undeclared_dependency_ratio": ratio,
        "per_node": rows,
        "caveats": [
            "single trace, single model, single workspace",
            "shell path extraction is heuristic",
            "a read does not prove the content affected the verification verdict",
        ],
    }
    (out_dir / "m11-undeclared-dependency-ratio.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(f"trace  : {TRACE.name}  ({recs[0].get('exact_model_id')})")
    print(f"worker calls                      : {len(workers)}")
    print(f"records with truncated body        : {truncated}")
    print(f"calls declaring ANY dependency     : {calls_with_declared}")
    print(f"calls that read something          : {calls_with_read}")
    print(f"tool actions                       : {dict(tool_use)}")
    print(f"distinct artifacts read (total)     : {tot_read}")
    print(f"of which UNDECLARED                 : {tot_undeclared}")
    print(f"UNDECLARED DEPENDENCY RATIO         : {ratio}")
    print()
    for row in rows:
        print(
            f"  {row['node_id']:34s} declared={len(row['declared_deps']):2d} "
            f"read={len(row['artifacts_read']):2d} undeclared={len(row['undeclared_reads']):2d}"
        )
    print(f"\njson: {out_dir / 'm11-undeclared-dependency-ratio.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
