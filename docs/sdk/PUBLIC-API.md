# LongHorizonOS — Public SDK (E1) — API

**Status: EXPERIMENTAL SDK v0.x** — not SDK 1.0; semantics may change without
backward-compat guarantees.  It is a thin facade over the frozen Core V1; it does
not replace or redefine Core.

## Classification
- **PUBLIC (v0.x experimental)** — the objects a developer imports and uses.
- **EXPERIMENTAL** — surface that may change in E2/E3.
- **INHERITED (Core)** — `lhos.agent_os`, `lhos.runtimes.verified_progress`,
  `lhos.runtimes.multi_agent`, `lhos.runtimes.invalidation` remain as their Core
  classification (see `artifacts/core_v1_freeze/public-api-classification.md`).

## PUBLIC objects (E1)
| Symbol | Purpose | Stability |
|---|---|---|
| `lhos.sdk.AgentOS` / `OS` | Composition root: kernel + VPG + D2 + D3 wired together | EXPERIMENTAL |
| `lhos.sdk.Agent` | Developer-facing agent (→ real Kernel Process + AgentDescriptor) | EXPERIMENTAL |
| `lhos.sdk.Goal` | Goal builder (→ real VPG Goal node) | EXPERIMENTAL |
| `lhos.sdk.Task` | Task builder (depends_on / verify guardian) | EXPERIMENTAL |
| `lhos.sdk.AgentOS.add_agent` | Register an agent with the D2 Scheduler | EXPERIMENTAL |
| `lhos.sdk.AgentOS.goal` | Create a Goal | EXPERIMENTAL |
| `lhos.sdk.AgentOS.run(goal, ...)` | Drive scheduling + evidence to fixpoint | EXPERIMENTAL |
| `lhos.sdk.AgentOS.repair(goal, ...)` | Run D3 invalidation → affected/preserved/frontier | EXPERIMENTAL |
| `lhos.sdk.AgentOS.status(goal)` | Read-only StatusSnapshot | EXPERIMENTAL |
| `lhos.sdk.RunResult` | Structured run result | EXPERIMENTAL |
| `lhos.sdk.RepairOutcome` | Structured D3 invalidate outcome | EXPERIMENTAL |
| `lhos.sdk.StatusSnapshot` (+ `render_ascii`) | Public read-only state view | EXPERIMENTAL |
| `lhos.sdk.VerificationOutcome` | Verifier result | EXPERIMENTAL |
| `lhos.sdk.scripted_executor` | Deterministic no-API-key executor/verifier | EXPERIMENTAL |
| `lhos.sdk.callback_verifier`, `lhos.sdk.command_verifier` | Dev-facing verifiers | EXPERIMENTAL |
| errors (`AgentOSError`, `ConfigurationError`, …) | Typed error taxonomy | EXPERIMENTAL |

## INTERNAL (not public)
`lhos.sdk.os._compile_goal`, builder internals, `Goal.compile`, provider adapter
internals (`providers.py`) — still importable for power use but not part of the
stable developer contract.

## Guarantee
- The SDK never sets VERIFIED directly, never fabricates ownership, never creates
  a second graph, and never bypasses Evidence — it drives the same Core authority
  (VPG / Kernel Lease / D3) that the audited Core uses.
- NoGraph / single-agent VPG independence and all Core invariants are preserved.

## Should-change before a future SDK 1.0
- Consider exporting via a `longhorizonos` package alias (when the project decides
  its PyPI name).
- Public Kernel→providers adapter in `providers.py` may be promoted once it has a
  durable contract.
