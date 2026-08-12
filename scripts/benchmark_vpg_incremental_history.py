"""Measure durable VPG entity-history growth for sequential small patches."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from itertools import pairwise
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lhos.runtimes.verified_progress import VerifiedProgressRuntime
from lhos.runtimes.verified_progress.patches import AddNodeOp, GraphPatchProposal


def _scalar(runtime: VerifiedProgressRuntime, sql: str, graph_id: str) -> int:
    row = runtime.store.conn.execute(sql, (graph_id,)).fetchone()
    return int(row[0])


def _run_case(size: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "vpg-history.db"
        runtime = VerifiedProgressRuntime(str(db_path))
        graph_id = runtime.create_graph(
            owner_pid="benchmark",
            graph_id=f"incremental-history-{size}",
        ).graph_id

        started = time.perf_counter()
        for index in range(size):
            runtime.submit_patch(
                GraphPatchProposal(
                    graph_id=graph_id,
                    expected_graph_version=index,
                    author_pid="benchmark",
                    idempotency_key=f"node-{index}",
                    operations=(
                        AddNodeOp(
                            node_id=f"node-{index}",
                            graph_id=graph_id,
                            node_type="task",
                            created_by_pid="benchmark",
                        ),
                    ),
                )
            )
        elapsed = time.perf_counter() - started

        nodes, edges = runtime.store.load_projection_snapshot(graph_id, size)
        node_history_rows = _scalar(
            runtime,
            "SELECT COUNT(*) FROM graph_node_history WHERE graph_id = ?",
            graph_id,
        )
        edge_history_rows = _scalar(
            runtime,
            "SELECT COUNT(*) FROM graph_edge_history WHERE graph_id = ?",
            graph_id,
        )
        snapshot_headers = _scalar(
            runtime,
            "SELECT COUNT(*) FROM graph_projection_snapshots WHERE graph_id = ?",
            graph_id,
        )
        history_payload_bytes = _scalar(
            runtime,
            "SELECT COALESCE(SUM(length(payload_json)), 0) "
            "FROM graph_node_history WHERE graph_id = ?",
            graph_id,
        )
        ready_frontier_payload_bytes = _scalar(
            runtime,
            "SELECT COALESCE(SUM(length(ready_frontier_json)), 0) "
            "FROM graph_events WHERE graph_id = ?",
            graph_id,
        )
        patch_payload_bytes = _scalar(
            runtime,
            "SELECT COALESCE(SUM(length(operations_json)), 0) "
            "FROM graph_patches WHERE graph_id = ?",
            graph_id,
        )
        runtime.store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        runtime.close()

        durable_files = [path for path in db_path.parent.glob(f"{db_path.name}*") if path.is_file()]
        old_full_history_rows = size * (size + 1) // 2
        return {
            "patches": size,
            "elapsed_seconds": round(elapsed, 6),
            "mean_commit_ms": round((elapsed / size) * 1000, 6),
            "database_bytes": db_path.stat().st_size,
            "durable_files_bytes": sum(path.stat().st_size for path in durable_files),
            "node_history_rows": node_history_rows,
            "edge_history_rows": edge_history_rows,
            "history_payload_bytes": history_payload_bytes,
            "ready_frontier_payload_bytes": ready_frontier_payload_bytes,
            "patch_payload_bytes": patch_payload_bytes,
            "snapshot_headers": snapshot_headers,
            "latest_node_count": len(nodes),
            "latest_edge_count": len(edges),
            "old_full_history_row_baseline": old_full_history_rows,
            "history_row_reduction_percent": round(
                (1 - node_history_rows / old_full_history_rows) * 100,
                6,
            ),
        }


def _evaluate(results: list[dict[str, Any]]) -> list[str]:
    violations: list[str] = []
    for result in results:
        size = int(result["patches"])
        if result["node_history_rows"] != size:
            violations.append(
                f"N={size}: expected {size} node revisions, got {result['node_history_rows']}"
            )
        if result["edge_history_rows"] != 0:
            violations.append(
                f"N={size}: expected no edge revisions, got {result['edge_history_rows']}"
            )
        if result["snapshot_headers"] != size + 1:
            violations.append(
                f"N={size}: expected {size + 1} snapshot headers, got {result['snapshot_headers']}"
            )
        if result["latest_node_count"] != size:
            violations.append(
                f"N={size}: expected {size} reconstructed nodes, got {result['latest_node_count']}"
            )
        # READY frontier summaries are constant-size per commit.  The total
        # event payload should therefore remain linear in the number of
        # versions, unlike the pre-summary full-list encoding.
        if result["ready_frontier_payload_bytes"] > size * 256:
            violations.append(
                f"N={size}: READY frontier event payload exceeds linear budget "
                f"({result['ready_frontier_payload_bytes']} bytes)"
            )

    for previous, current in pairwise(results):
        expected_ratio = current["patches"] / previous["patches"]
        actual_ratio = current["history_payload_bytes"] / previous["history_payload_bytes"]
        if actual_ratio > expected_ratio * 1.3:
            violations.append(
                f"N={previous['patches']}->{current['patches']}: history payload "
                f"grew {actual_ratio:.3f}x; linear budget is "
                f"{expected_ratio * 1.3:.3f}x"
            )
        frontier_ratio = (
            current["ready_frontier_payload_bytes"] / previous["ready_frontier_payload_bytes"]
        )
        if frontier_ratio > expected_ratio * 1.3:
            violations.append(
                f"N={previous['patches']}->{current['patches']}: READY frontier "
                f"payload grew {frontier_ratio:.3f}x; linear budget is "
                f"{expected_ratio * 1.3:.3f}x"
            )
    return violations


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark incremental VPG durable projection history.",
    )
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=[100, 200, 400],
        help="Sequential patch counts to measure.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional JSON report path.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if entity history is not linear for this workload.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    sizes = sorted(set(args.sizes))
    if not sizes or any(size <= 0 for size in sizes):
        raise SystemExit("--sizes must contain positive integers")

    results = [_run_case(size) for size in sizes]
    violations = _evaluate(results)
    report = {
        "benchmark": "vpg_incremental_projection_history",
        "workload": "one new task node per committed patch",
        "results": results,
        "violations": violations,
        "valid": not violations,
        "scope_note": (
            "This proves linear durable entity-history growth for this workload; "
            "it does not claim linear end-to-end commit time. READY frontier "
            "event summaries are also checked for linear durable payload."
        ),
    }
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 1 if args.check and violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
