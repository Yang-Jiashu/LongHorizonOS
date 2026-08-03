# Milestone 1B: Test & Coverage Report

## Environment

- **Working directory**: `/Users/jiashuyang/Documents/kimi/Workspaces/longhorizonOS/longhorizonos`
- **Python**: 3.11.15 (in `.venv-audit`)
- **OS**: macOS
- **Git commit**: `440efc6` (HEAD), tag `mvp-deterministic-v0` at `b8813d2`
- **Virtualenv**: `.venv-audit` (freshly created for audit)

## Test Results

```
161 passed in 6.43s
```

All 161 tests pass in the fresh virtualenv.

### Test breakdown

| Suite | Tests |
|---|---|
| `tests/e2e/test_tiny_repository_task.py` | 1 |
| `tests/integration/test_benchmark_run.py` | 7 |
| `tests/integration/test_checkpoint_restore.py` | 2 |
| `tests/integration/test_crash_injection.py` | 5 |
| `tests/integration/test_event_projection.py` | 1 |
| `tests/integration/test_invalidation_loop.py` | 3 |
| `tests/integration/test_run_resume.py` | 1 |
| `tests/integration/test_verification_commit.py` | 4 |
| `tests/unit/test_benchmark_metrics.py` | 14 |
| `tests/unit/test_context_compiler.py` | 7 |
| `tests/unit/test_generator.py` | 43 |
| `tests/unit/test_invalidation.py` | 3 |
| `tests/unit/test_patch_validator.py` | 6 |
| `tests/unit/test_readiness.py` | 10 |
| `tests/unit/test_scheduler.py` | 5 |
| `tests/unit/test_state_machine.py` | 49 |
| **Total** | **161** |

## Coverage Summary

Overall: 74% line coverage, 956 branches, 162 branch parts missing.

### Core Module Coverage

| Module | Line Coverage | Branch Coverage | Below 80%? |
|---|---|---|---|
| State Machine | 100.0% | 100.0% (4/4) | No |
| Event Store | 86.7% | 62.5% (5/8) | **YES** |
| Graph Projection | 95.9% | 91.7% (11/12) | No |
| Readiness | 97.8% | 96.7% (29/30) | No |
| Invalidation | 94.9% | 90.9% (20/22) | No |
| Verification Gate | 83.5% | 63.6% (14/22) | **YES** |
| Recovery | 71.8% | 71.4% (20/28) | **YES** |
| FIFO Scheduler | 100.0% | 100.0% (2/2) | No |
| Cost-aware Scheduler | 87.9% | 60.0% (6/10) | **YES** |
| Context Compiler | 84.9% | 73.2% (41/56) | **YES** |
| Budget Manager | 82.5% | 50.0% (1/2) | **YES** |
| Checkpoint (filesystem) | 86.1% | 62.5% (10/16) | **YES** |
| Tool Idempotency | 68.5% | 42.9% (6/14) | **YES** |

### Modules Below 80% Branch Coverage — Missing Branches

#### 1. Tool Idempotency (42.9% — CRITICAL)
- **File**: `src/lhos/runtime/tool_runtime.py`
- **Missing lines**: 44, 58, 62-63, 89-106
- **Missing branches**: 8/14
- **Root cause**: Rollback generation paths and error handling for idempotency key replay are not tested.
- **Risk**: Crash recovery idempotency guarantees are untested.

#### 2. Budget Manager (50.0%)
- **File**: `src/lhos/runtime/budget_manager.py`
- **Missing lines**: 23, 42-43, 73-75
- **Missing branches**: 1/2
- **Root cause**: Budget limit enforcement branches not exercised.

#### 3. Cost-aware Scheduler (60.0%)
- **File**: `src/lhos/runtime/cost_aware_scheduler.py`
- **Missing lines**: 58, 64, 78, 116
- **Missing branches**: 4/10
- **Root cause**: Side-effect risk multipliers, context switch cost, and `now=None` default branch not tested.

#### 4. Event Store (62.5%)
- **File**: `src/lhos/infrastructure/db/sqlite_event_store.py`
- **Missing lines**: 35, 61, 80
- **Missing branches**: 3/8
- **Root cause**: Error handling and edge cases in event append/list not tested.

#### 5. Verification Gate (63.6%)
- **File**: `src/lhos/runtime/verification_gate.py`
- **Missing lines**: 51, 61-63, 67-70, 107, 124, 129
- **Missing branches**: 8/22
- **Root cause**: Manual verification path, pending result handling, and evidence synthesis branches not tested.

#### 6. Checkpoint — filesystem (62.5%)
- **File**: `src/lhos/infrastructure/checkpoints/filesystem_checkpoint.py`
- **Missing lines**: 34, 36, 70, 73, 76, 85
- **Missing branches**: 6/16
- **Root cause**: Restore failure paths, tar extraction edge cases, and cleanup branches not tested.

#### 7. Context Compiler (73.2%)
- **File**: `src/lhos/runtime/context_compiler.py`
- **Missing lines**: 104, 125, 137, 149, 151, 180, 255-266
- **Missing branches**: 15/56
- **Root cause**: Context pruning edge cases, dependency hop limits, and cache miss paths not fully tested.

#### 8. Recovery (71.4%)
- **File**: `src/lhos/runtime/recovery.py`
- **Missing lines**: 46-48, 59-60, 74-80, 82-83, 121-141
- **Missing branches**: 8/28
- **Root cause**: Checkpoint restore-on-crash path, lease release for non-running nodes, and incomplete tool call detection not tested.

## Ruff Results

- **ruff check**: All checks passed (after adding configuration and fixing 97 issues)
- **ruff format --check**: All files formatted (after reformatting 61 files)
- **Configuration added**: `[tool.ruff]` section in `pyproject.toml` with `E, F, W, I, UP, B, SIM, RUF` rule sets

## Mypy Results

- **34 errors** in 10 files (checked 94 source files)
- **Configuration added**: `[tool.mypy]` section in `pyproject.toml`
- **Error categories**:
  - 12 `no-any-return`: Functions returning `Any` from typed functions (mostly `store.set_run_status()` calls)
  - 8 `union-attr`: `ControlledTask | None` attribute access in `adapter.py` (guarded by `_require_task()` at runtime)
  - 6 `arg-type`: `Any | None` passed where `str` expected in `patch_validator.py`
  - 4 `assignment`: Type narrowing issues in `bootstrap.py` (intentional polymorphism)
  - 3 `var-annotated`: Missing type annotations for `networkx` graph variables
  - 1 `arg-type`: `str` vs `NodeKind` in `transcript.py`
- **Assessment**: These are type annotation issues, not runtime bugs. The `patch_validator.py` and `adapter.py` issues should be fixed before production use.

## Files Without Coverage

The following source files have 0% coverage (no tests execute them):
- `src/lhos/agents/executor.py` (0%)
- `src/lhos/agents/planner.py` (0%)
- `src/lhos/agents/semantic_reconciler.py` (0%)
- `src/lhos/benchmarks/adapter.py` (0%)
- `src/lhos/cli/benchmark.py` (0%)
- `src/lhos/cli/inject.py` (0%)
- `src/lhos/cli/inspect.py` (0%)
- `src/lhos/cli/main.py` (0%)
- `src/lhos/cli/resume.py` (0%)
- `src/lhos/cli/run.py` (0%)
- `src/lhos/config.py` (0%)
- `src/lhos/infrastructure/llm/adapter.py` (0%)
- `src/lhos/infrastructure/llm/structured_output.py` (0%)
- `src/lhos/infrastructure/llm/usage_tracking.py` (0%)
- `src/lhos/infrastructure/telemetry/metrics_collector.py` (0%)
- `src/lhos/infrastructure/tools/filesystem_tool.py` (47%)
- `src/lhos/infrastructure/tools/shell_tool.py` (43%)

**Note**: The `agents/` package (executor, planner, semantic_reconciler) and `infrastructure/llm/` package have 0% coverage — these are the LLM integration stubs that would be activated in Milestone 2.
