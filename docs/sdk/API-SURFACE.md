# LongHorizonOS — SDK API Surface (E1, experimental)

| Public object | Purpose | Core mapping | Stability |
|---|---|---|---|
| `AgentOS`/`OS` | composition root | `create_kernel` + `VerifiedProgressRuntime` + `create_scheduler` + D3 | EXPERIMENTAL |
| `Agent` | agent definition | `process_service.spawn` + `AgentDescriptor` + capability grant | EXPERIMENTAL |
| `Goal` / `Task` | goal/task builder | one `GraphPatchProposal` (Goal/Task/depends_on) | EXPERIMENTAL |
| `Task(verify=...)` | verification guardian | runs verifier → Evidence + `ArtifactVersionBinding` → VPG derives VERIFIED | EXPERIMENTAL |
| `AgentOS.run` | drive work | `run_pass` (schedule + observe + reconcile) + executor + evidence | EXPERIMENTAL |
| `AgentOS.repair` | D3 invalidate | `InvalidationRuntime.invalidate` → affected/preserved/frontier | EXPERIMENTAL |
| `AgentOS.status` | read state | VPG snapshot + scheduler claims/attempts + D3 frontier | EXPERIMENTAL |
| `RunResult` | structured result | derived from VPG snapshot + scheduler | EXPERIMENTAL |
| `RepairOutcome` | D3 outcome | derived from D3 cone/frontier | EXPERIMENTAL |
| `StatusSnapshot` | read-only view | VPG + scheduler + D3 aggregation | EXPERIMENTAL |
| `scripted_executor` | deterministic executor | produces artifact + committed action for Evidence | EXPERIMENTAL |
| `callback_verifier` / `command_verifier` | verifiers | run outcome → evidence guardian | EXPERIMENTAL |
| errors | typed taxonomy | wraps Core exceptions | EXPERIMENTAL |

**EXPERIMENTAL SDK** — by design, not SDK 1.0.  Core V1 semantics and authority
(unfrozen) are preserved; the SDK is the developer entry point only.
