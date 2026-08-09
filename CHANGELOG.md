# Changelog

All notable changes to LongHorizonOS are documented here (user-value oriented).

## [v0.1.0] — Release Candidate

### Core V1 (frozen)
- State-centric operating substrate; Kernel execution plane + Semantic Control
  Plane; single authority per fact.
- Verified Progress Graph as the persistent semantic state substrate; D1+dD3
  incremental semantic reconciliation; exact-version Evidence; minimal Repair
  Frontier; Kernel ResourceLease ownership.

### Public SDK (`lhos.sdk`)
- `AgentOS` (composition root), `Agent`, `Goal`, `Task`, verifier/evidence
  guardian, `ScriptedExecutor`, `RunResult`, `StatusSnapshot`, typed errors.

### Real integrations
- OpenAI-compatible model adapter (stdlib transport; pluggable offline fake).
- Shell, Workspace, Git tools (capability-governed).
- `CommandVerifier` (real shell → Verification → Evidence → VPG).

### CLI / Observability
- `lhos status` / `lhos inspect` / `lhos graph` (+ `--json`), read-only over the
  public observability read models.

### Killer Demo
- `lhos demo recovery-repair`: worker crash → ownership recovery; ArtifactVersion
  mutation → Evidence applicability loss; selective invalidation; preserved
  VERIFIED work; minimal Repair Frontier; D2 repair with new Evidence; Goal
  reclosure.  Deterministic, no API key.

### Comparative Benchmark
- `lhos benchmark semantic-repair [--quick|--full]`: Full Restart vs
  Checkpoint/Resume vs LongHorizonOS with an independent correctness oracle.
  In one reproducible case (24% of a 50-task verified graph affected):
  LongHorizonOS reran 12 / preserved 38; Full Restart reran 50.

### Known limitations
- Single-host / local focus; OpenAI-compatible model adapter only; limited tool
  set; no distributed scheduler / web dashboard / general browser runtime; SDK is
  experimental v0.x; benchmark baselines are strategy abstractions.
