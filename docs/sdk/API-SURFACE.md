# LongHorizonOS — SDK API Surface (E1, experimental)

| Public object | Purpose | Core mapping | Stability |
|---|---|---|---|
| `AgentOS`/`OS` | composition root | `create_kernel` + `VerifiedProgressRuntime` + `create_scheduler` + D3 | EXPERIMENTAL |
| `Agent` | agent definition | `process_service.spawn` + `AgentDescriptor` + capability grant | EXPERIMENTAL |
| `Goal` / `Task` | goal/task builder | one `GraphPatchProposal` (Goal/Task/depends_on) | EXPERIMENTAL |
| `Task(verify=...)` | verification guardian | runs verifier → Evidence + `ArtifactVersionBinding` → VPG derives VERIFIED | EXPERIMENTAL |
| `AgentOS.run` | drive work | `run_pass` (schedule + observe + reconcile) + executor + evidence | EXPERIMENTAL |
| `AgentOS.run_async` | bounded concurrent work | `AsyncWorkerPool` + async executor/verifier + serialized semantic commit | EXPERIMENTAL |
| `AgentOS.deliver_interrupt` | cooperative live-attempt control | graph/epoch/claim/task/attempt fence → cancellation token → verifier commit quarantine | EXPERIMENTAL |
| `AgentOS.runtime_state` | immutable runtime projection | `GlobalRuntimeState` (Progress, Agent/Cognition, Context, logical Resources) | EXPERIMENTAL |
| `AgentOS.plan_frontier` | advisory frontier planning | deterministic `FrontierPolicy` / `SchedulingEpoch` | EXPERIMENTAL |
| `AgentOS.plan_compute_routing` | bounded compute-routing advisory | exact-version locality plus reuse/fresh, model-tier, context-budget, verification-strength labels | EXPERIMENTAL |
| `AgentOS.workspace_gateway` | mediated workspace provenance boundary | root/capability confinement, exact-byte hashes, atomic/CAS writes, Facts-backed strict version validation | EXPERIMENTAL |
| `AgentOS.repair` | D3 invalidate | `InvalidationRuntime.invalidate` → affected/preserved/frontier | EXPERIMENTAL |
| `AgentOS.status` | read state | VPG snapshot + scheduler claims/attempts + D3 frontier | EXPERIMENTAL |
| `RunResult` | structured result | derived from VPG snapshot + scheduler | EXPERIMENTAL |
| `RepairOutcome` | D3 outcome | derived from D3 cone/frontier | EXPERIMENTAL |
| `StatusSnapshot` | read-only view | VPG + scheduler + D3 aggregation | EXPERIMENTAL |
| `scripted_executor` | deterministic executor | produces artifact + committed action for Evidence | EXPERIMENTAL |
| `callback_verifier` / `command_verifier` | verifiers | run outcome → evidence guardian | EXPERIMENTAL |
| errors | typed taxonomy | wraps Core exceptions | EXPERIMENTAL |

`plan_compute_routing` is a read-only policy surface. It accepts explicit
candidate metadata and optional explicit Agent metadata, fails closed on stale
or unknown bindings, and never creates processes, claims work, dispatches a
provider, mutates Context, or changes Scheduler execution. Its labels are
provider-independent recommendations; provider integration and utility
benchmarking remain open.

**EXPERIMENTAL SDK** — by design, not SDK 1.0.  Core V1 semantics and authority
(unfrozen) are preserved; the SDK is the developer entry point only.
