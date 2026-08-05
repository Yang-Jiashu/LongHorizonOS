# Frozen Version — Phase B Audit Baseline

## Tag

- **Tag**: `agent-os-phase-b-v0`
- **Commit**: `23d2f1bf850cd27e6966089946192fe12256f20c`
- **Created**: 2026-08-05

## Working Directory Status

**CLEAN** — `git status --short` returns empty output. No uncommitted changes.

## Phase B Commits (6 commits)

### Commit 1: `9769188` — feat(agent-os): kernel domain models

**Files:**
- `src/lhos/agent_os/__init__.py`
- `src/lhos/agent_os/kernel/__init__.py`
- `src/lhos/agent_os/kernel/models.py` — PCB, ACB, KernelRequest/Event unions, ProcessCheckpoint, ProgramStepResult
- `src/lhos/agent_os/kernel/state_machine.py` — 7-state Process FSM, 8-state Action FSM, validate_transition()
- `src/lhos/agent_os/kernel/errors.py` — TerminalStateError, CapabilityDenied, DeadlockDetected, etc.

**Lines**: 543 insertions

### Commit 2: `132c3db` — feat(agent-os): storage layer, journal service, and process service

**Files:**
- `src/lhos/agent_os/storage/__init__.py`
- `src/lhos/agent_os/storage/schema.py` — Schema initialization, table creation
- `src/lhos/agent_os/storage/schema.sql` — 7 projection tables + journal_events
- `src/lhos/agent_os/storage/sqlite.py` — SQLiteStorage with transaction support
- `src/lhos/agent_os/services/__init__.py`
- `src/lhos/agent_os/services/journal.py` — JournalService with global offset + per-pid sequence
- `src/lhos/agent_os/services/process_service.py` — ProcessService managing PCB lifecycle

**Lines**: 865 insertions

### Commit 3: `fcdfb08` — feat(agent-os): action, capability, and lease services

**Files:**
- `src/lhos/agent_os/services/action_service.py` — ActionService (submit/admit/commit/fail/cancel)
- `src/lhos/agent_os/services/capability_service.py` — fnmatch-based capability checking
- `src/lhos/agent_os/services/lease_service.py` — Atomic acquire, deadlock detection, victim selection

**Lines**: 965 insertions

### Commit 4: `17f6b61` — feat(agent-os): kernel loop, dispatcher, drivers, programs, SDK, signals

**Files:**
- `src/lhos/agent_os/kernel/kernel.py` — tick() main loop, scheduling, signal delivery, recovery
- `src/lhos/agent_os/kernel/dispatcher.py` — 12 syscall handlers
- `src/lhos/agent_os/drivers/__init__.py`
- `src/lhos/agent_os/drivers/base.py` — BaseDriver protocol
- `src/lhos/agent_os/drivers/mock_device.py` — MockDeviceDriver with configurable delays/crash
- `src/lhos/agent_os/drivers/mock_model.py` — MockModelDriver
- `src/lhos/agent_os/programs/__init__.py`
- `src/lhos/agent_os/programs/base.py` — AgentProgram protocol
- `src/lhos/agent_os/programs/scripted.py` — ScriptedProgram for deterministic testing
- `src/lhos/agent_os/sdk/__init__.py`
- `src/lhos/agent_os/sdk/client.py` — AgentOSClient convenience wrapper
- `src/lhos/agent_os/services/signal_service.py` — Durable signal send/deliver/consume

**Lines**: 1697 insertions

### Commit 5: `498663f` — test(agent-os): 137 tests, 5 demo scenarios, and demo runner script

**Files:**
- `tests/agent_os/__init__.py`
- `tests/agent_os/test_action_state_machine.py`
- `tests/agent_os/test_architecture.py`
- `tests/agent_os/test_capabilities.py`
- `tests/agent_os/test_deadlocks.py`
- `tests/agent_os/test_demos.py`
- `tests/agent_os/test_isolation.py`
- `tests/agent_os/test_journal.py`
- `tests/agent_os/test_kernel_loop.py`
- `tests/agent_os/test_leases.py`
- `tests/agent_os/test_process_state_machine.py`
- `tests/agent_os/test_recovery.py`
- `tests/agent_os/test_signals.py`
- `scripts/run_phase_b_demos.py`

**Lines**: 2108 insertions

### Commit 6: `23d2f1b` — chore: milestone 2/3 leftover changes

**Files:**
- `pyproject.toml`
- `scripts/analyze_parse_failures.py`
- `scripts/analyze_v3_p4_p8.py`
- `scripts/recollect_v3_artifacts.py`
- `scripts/update_v3_summary.py`
- `scripts/analyze_3seed_results.py` (new)
- `scripts/analyze_milestone_2_3_costs.py` (new)
- `scripts/gen_token_accounting.py` (new)
- `scripts/run_3seed_pairing.py` (new)
- `scripts/run_multi_run_stress.py` (new)
- `src/lhos/benchmarks/capability_manifest.py`
- `src/lhos/benchmarks/modes.py`
- `src/lhos/domain/models.py`
- `src/lhos/infrastructure/db/connection.py`
- `src/lhos/infrastructure/db/schema.sql`
- `src/lhos/infrastructure/db/migrations/003_fix_execution_uniqueness.sql` (new)
- `src/lhos/runtime/controller.py`
- `tests/unit/test_milestone_2_2.py`
- `tests/integration/test_attempt_semantics.py` (new)
- `tests/integration/test_migration_compatibility.py` (new)
- `tests/integration/test_multi_run_isolation.py` (new)
- `tests/unit/test_minimal_lhos_mode.py` (new)

**Lines**: 2893 insertions, 98 deletions

## Out-of-Scope: Commit 6 Contents

**Commit 6 is NOT part of Phase B.** It contains pre-existing uncommitted changes from Milestone 2/3 work that were committed separately to keep the Phase B history clean. Specifically:

| File | Belongs to | Reason |
|------|-----------|--------|
| `pyproject.toml` | Milestone 2/3 | Build configuration changes |
| `scripts/analyze_*.py` | Milestone 2/3 | Analysis scripts for 3-seed pairing |
| `scripts/run_3seed_pairing.py` | Milestone 2/3 | Stress test runner |
| `scripts/run_multi_run_stress.py` | Milestone 2/3 | Multi-run isolation test |
| `scripts/gen_token_accounting.py` | Milestone 2/3 | Token cost accounting |
| `scripts/recollect_v3_artifacts.py` | Milestone 2/3 | Artifact recollection |
| `scripts/update_v3_summary.py` | Milestone 2/3 | V3 summary updates |
| `src/lhos/benchmarks/capability_manifest.py` | Milestone 2/3 | Benchmark capability manifest |
| `src/lhos/benchmarks/modes.py` | Milestone 2/3 | Benchmark mode additions |
| `src/lhos/domain/models.py` | Milestone 2/3 | Domain model changes (attempt semantics) |
| `src/lhos/infrastructure/db/connection.py` | Milestone 2/3 | DB connection changes |
| `src/lhos/infrastructure/db/schema.sql` | Milestone 2/3 | Schema changes |
| `src/lhos/infrastructure/db/migrations/003_*.sql` | Milestone 2/3 | Execution uniqueness migration |
| `src/lhos/runtime/controller.py` | Milestone 2/3 | Runtime controller changes |
| `tests/unit/test_milestone_2_2.py` | Milestone 2/3 | Milestone 2.2 test updates |
| `tests/integration/test_*.py` (3 files) | Milestone 2/3 | Integration tests |
| `tests/unit/test_minimal_lhos_mode.py` | Milestone 2/3 | Minimal lhos mode test |

**None of these files are imported by or affect `src/lhos/agent_os/`.**

## Git History (Last 10 commits)

```
23d2f1b chore: milestone 2/3 leftover changes
498663f test(agent-os): 137 tests, 5 demo scenarios, and demo runner script
17f6b61 feat(agent-os): kernel loop, dispatcher, drivers, programs, SDK, signals
fcdfb08 feat(agent-os): action, capability, and lease services
132c3db feat(agent-os): storage layer, journal service, and process service
9769188 feat(agent-os): kernel domain models
ebaa254 exp: stuck recovery debug v3
c6d85f7 test: add n3 root cause regression tests
b79c5ce fix: add structured retry feedback and bounded local repair
cca7629 Milestone 1验收: GO
```

## Verification

- **Working directory**: CLEAN
- **Tag**: `agent-os-phase-b-v0` → `23d2f1b`
- **Phase B code**: Commits 1-5 (`9769188`..`498663f`)
- **Out-of-scope**: Commit 6 (`23d2f1b`) — Milestone 2/3 leftovers, explicitly marked
- **Git history**: Not rewritten
