"""Generate coverage summary JSON for core modules."""

import json
import subprocess
import sys

# Run coverage and capture JSON
subprocess.run(
    [
        sys.executable,
        "-m",
        "pytest",
        "--cov=src/lhos",
        "--cov-branch",
        "--cov-report=json:artifacts/audit/coverage.json",
        "-q",
    ],
    capture_output=True,
    text=True,
)

with open("artifacts/audit/coverage.json") as f:
    cov = json.load(f)

core_modules = {
    "state_machine": "src/lhos/graph/state_machine.py",
    "event_store": "src/lhos/infrastructure/db/sqlite_event_store.py",
    "graph_projection": "src/lhos/graph/projection.py",
    "readiness": "src/lhos/graph/readiness.py",
    "invalidation": "src/lhos/graph/invalidation.py",
    "verification_gate": "src/lhos/runtime/verification_gate.py",
    "recovery": "src/lhos/runtime/recovery.py",
    "fifo_scheduler": "src/lhos/runtime/fifo_scheduler.py",
    "cost_aware_scheduler": "src/lhos/runtime/cost_aware_scheduler.py",
    "context_compiler": "src/lhos/runtime/context_compiler.py",
    "budget_manager": "src/lhos/runtime/budget_manager.py",
    "checkpoint": "src/lhos/infrastructure/checkpoints/filesystem_checkpoint.py",
    "tool_idempotency": "src/lhos/runtime/tool_runtime.py",
}

summary = {}
for name, path in core_modules.items():
    if path in cov["files"]:
        f = cov["files"][path]
        total_branches = f["summary"]["num_branches"]
        covered_branches = f["summary"]["covered_branches"]
        if total_branches > 0:
            branch_pct = round(100.0 * covered_branches / total_branches, 2)
        else:
            branch_pct = 100.0
        summary[name] = {
            "path": path,
            "line_coverage_pct": round(f["summary"]["percent_covered"], 2),
            "num_statements": f["summary"]["num_statements"],
            "missing_lines": f["summary"]["missing_lines"],
            "num_branches": total_branches,
            "covered_branches": covered_branches,
            "missing_branches": f["summary"]["missing_branches"],
            "branch_coverage_pct": branch_pct,
            "below_80pct_branch": branch_pct < 80.0,
        }

with open("artifacts/audit/coverage-summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print(json.dumps(summary, indent=2))
