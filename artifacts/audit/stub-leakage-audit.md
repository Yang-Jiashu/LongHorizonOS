# Milestone 1C: Stub, Fake Implementation, and Hardcoded Value Audit

## Search Methodology

Executed full-project searches for:
1. `TODO|FIXME|NotImplemented|stub|mock|fake|pass$|return True|always_pass|hard.?code`
2. `oracle|ground_truth|future_failure|failure_injection|optimal_schedule|hidden`
3. `FakeWorker|FakeTool|MockWorker|MockTool`
4. `0\.010|0\.099|161/161|recovery overhead`

## Findings

### 1. Production Runtime imports FakeWorker — **BY DESIGN (MVP)**

- **File**: `src/lhos/bootstrap.py` lines 42, 131
- **Status**: `FakeWorker` is the production worker for the deterministic MVP.
- **Assessment**: This is explicitly documented in the README: "No real LLM calls anywhere: the scripted FakeWorker drives runs deterministically." The `ExecutorAgent` in `src/lhos/agents/executor.py` is the Phase 2 replacement but is NOT wired into `RuntimeStack`.
- **Risk**: Low for deterministic MVP. Must be replaced for Milestone 2.

### 2. Production Runtime imports FakeTool — **BY DESIGN (MVP)**

- **File**: `src/lhos/bootstrap.py` lines 26, 73
- **Status**: `FakeTool` is registered alongside `ShellTool` and `FilesystemTool` in the production tool registry.
- **Assessment**: `FakeTool` is needed for deterministic benchmark scripts. Real tools (`ShellTool`, `FilesystemTool`) are also registered and available.
- **Risk**: Low. The `FakeTool` is only invoked when node scripts reference `tool_name: "fake"`.

### 3. Verifier default unconditional pass — **NOT FOUND**

- **Status**: No verifier defaults to unconditional pass.
- `VerificationGate.verify()`: returns `passed=False` when no verification spec exists (line 107-110).
- `LlmJudgeVerifier`: raises `LlmJudgeDisabledError` when disabled (default), raises `VerificationError` when enabled but no LLM port.
- `VerifierRegistry.get()`: raises `VerificationError` for unknown verifier types.
- **Assessment**: Clean. No silent pass-through.

### 4. llm_judge stub — **SAFE**

- **File**: `src/lhos/verification/llm_verifier.py`
- **Status**: Registered in default registry but raises `LlmJudgeDisabledError` when `allow_llm_judge=False` (default in all modes).
- **Assessment**: Cannot be accidentally invoked. When enabled without an LLM port, it raises `VerificationError`.

### 5. Cost-aware Scheduler reads future failure info — **NOT FOUND**

- **File**: `src/lhos/runtime/cost_aware_scheduler.py`
- **Status**: The scheduler reads only:
  - Node metadata: `estimated_token_cost`, `estimated_time_ms`, `progress_weight`, `attempt_count`, `max_attempts`, `side_effect_level`, `tool_type`, `depends_on_stale`
  - Graph structure: `critical_path.criticality()`, `critical_path.unlock_score()`
  - Time: `ready_at` timestamp
- **Assessment**: No future failure information, no oracle data, no hidden costs. Clean.

### 6. Runtime reads oracle graph — **NOT FOUND**

- **Status**: Searched `src/lhos/runtime/` for `oracle|hidden|ground_truth|optimal|future_failure` — zero matches.
- The runtime modules (`controller.py`, `worker.py`, `scheduler.py`, `recovery.py`, etc.) do not import or reference any oracle modules.
- Oracle information (`OracleInfo`) is only accessed in:
  - `benchmarks/scoring.py` (for metric computation — analysis only)
  - `benchmarks/runner.py` (for `use_oracle_priorities` flag — only oracle modes)
  - `benchmarks/controlled/task_schema.py` (definition)
  - `benchmarks/transcript.py` (reads `task.spec.oracle_nodes` for node definitions — these are the task graph, not hidden oracle)
- **Assessment**: Clean. Runtime cannot access oracle graph.

### 7. Runtime reads true node costs or optimal schedule — **NOT FOUND**

- **Status**: The runtime uses `estimated_token_cost` and `estimated_time_ms` from node metadata. These are estimates set by the task generator and are available to all modes.
- The `true_costs` in `OracleInfo` are not defined (no such field exists). The oracle only has `critical_path`, `critical_path_seconds`, `affected_by_event`, and `priorities`.
- **Assessment**: Clean. No hidden true costs.

### 8. Benchmark metrics directly from preset — **PARTIALLY**

- **File**: `src/lhos/benchmarks/scoring.py`
- **Status**: 
  - Primary metrics (`success`, `verified_progress`, `total_tokens`, `tool_calls`) are computed from the event log and execution records — NOT from preset.
  - Analysis metrics (`replanning_amplification`, `critical_path_stretch`) use oracle data (`task.oracle.affected_by_event`, `task.oracle.critical_path_seconds`) as denominators/reference values.
  - `verified_progress` is computed from node `progress_weight` values — this is VPG internal progress.
- **Assessment**: The primary metrics are computed from runtime data. Oracle data is only used for analysis/reference metrics, not for determining success or progress. However, `verified_progress` being based on VPG internal weights is a concern for Milestone 1E (external grader independence).

### 9. Transcript baseline intentionally weakened — **NO (fair implementation)**

- **File**: `src/lhos/benchmarks/transcript.py`
- **Status**: The transcript baseline:
  - Uses the same verifier registry as graph modes (line 71)
  - Uses the same task scripts and failure/retry semantics
  - Uses the same node specifications and verification specs
  - Models tokens as `len(context)//4` (documented)
  - Writes artifacts directly to filesystem (no tool runtime)
  - Discards all progress on crash restart (documented baseline weakness)
  - Has no graph invalidation (the point of the comparison)
- **Assessment**: The differences between transcript and graph modes are the actual variables being measured (graph structure, invalidation, checkpoints), not artificial weakenings. The transcript baseline is fairly implemented.

### 10. Graph mode gets extra task info — **NO**

- **Status**: 
  - All modes receive the same `graph_spec` from `task.graph_spec()`.
  - Non-oracle modes get `priority = 0` (line 73-77 of `task_schema.py`).
  - Oracle modes get true priorities — but these are explicitly named `oracle_graph_*` and serve as upper bounds.
  - The `ControlledAdapter.get_environment_snapshot()` returns `environment_events` and `failure_injections` — but this is the benchmark adapter interface, not the Runtime itself.
  - The Runtime receives graph spec (nodes, edges, goal) through `InitialGraphBuilder`, which doesn't include oracle information.
- **Assessment**: Clean. Graph modes and transcript baseline receive the same task information.

### 11. Report numbers hardcoded — **NOT FOUND**

- Searched for `0\.010|0\.099|161/161|recovery overhead` — zero matches in source code.
- `model_cost_usd` is hardcoded to `0.0` in `controller.py` line 542 (documented: FakeWorker has no real cost).
- `graph_maintenance_tokens` and `verification_tokens` are hardcoded to `0` in scoring (documented: deterministic rules, no LLM).
- **Assessment**: No suspicious hardcoded numbers.

### 12. SemanticReconcilerStub — **SAFE**

- **File**: `src/lhos/agents/semantic_reconciler.py`
- **Status**: Raises `LhosError` when no LLM configured, `NotImplementedError` even with LLM. Not wired into `RuntimeStack`.
- **Assessment**: Cannot be accidentally invoked.

### 13. ExecutorAgent and PlannerAgent — **SAFE**

- **Files**: `src/lhos/agents/executor.py`, `src/lhos/agents/planner.py`
- **Status**: Require an LLM port in constructor. Not wired into `RuntimeStack`.
- **Assessment**: Phase 2 shells, not active in deterministic MVP.

### 14. MockLLM adapter — **SAFE**

- **File**: `src/lhos/infrastructure/llm/adapter.py`
- **Status**: Returns scripted responses. Not wired into `RuntimeStack`.
- **Assessment**: Test infrastructure, not production.

## Summary

| Check | Status |
|---|---|
| Production Runtime imports FakeWorker | BY DESIGN (MVP) |
| Production Runtime imports FakeTool | BY DESIGN (MVP) |
| Verifier default unconditional pass | NOT FOUND |
| llm_judge stub callable | SAFE (raises when disabled) |
| Cost-aware Scheduler reads future failures | NOT FOUND |
| Runtime reads oracle graph | NOT FOUND |
| Runtime reads true costs/optimal schedule | NOT FOUND |
| Benchmark metrics from preset | PARTIALLY (analysis metrics use oracle) |
| Transcript baseline intentionally weakened | NO (fair) |
| Graph mode gets extra task info | NO |
| Report numbers hardcoded | NOT FOUND |

## Issues Requiring Action

1. **VPG internal progress used as benchmark score**: `verified_progress` in scoring is computed from node `progress_weight` values, which is VPG internal progress. This violates the principle that "Runtime internal progress != external benchmark progress." (Addressed in Milestone 1E)

2. **Oracle data in scoring**: `replanning_amplification` and `critical_path_stretch` use oracle data. While these are analysis metrics (not primary success/progress metrics), they should be clearly labeled as oracle-dependent.

3. **FakeWorker in production**: Must be replaced with real LLM-backed worker for Milestone 2. The `ExecutorAgent` shell exists but needs completion.

4. **`model_cost_usd` hardcoded to 0.0**: Must be computed from real model costs in Milestone 2.
