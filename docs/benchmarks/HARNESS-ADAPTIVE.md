# Harness Adaptive Integration Benchmark

This benchmark is a bounded, controlled vertical slice for the claim that
LongHorizonOS manages long-running Agent computation above a Harness. It
compares a fixed-concurrency policy (`static`) with the opt-in adaptive policy
(`adaptive`) on the same declared four-task workload.

## Authoritative execution path

Every executor attempt is admitted and committed through the existing runtime:

```text
AgentOS.run_async
  -> Scheduler eligibility/resource admission
  -> TaskClaim + Attempt
  -> Kernel Lease/fencing token
  -> context-aware Agent executor
  -> exact-identity CallableHarnessAdapter START
  -> independent Task verifier
  -> VPG Evidence commit / Goal closure
```

The Harness is registered from inside the executor using the live
Claim/Attempt identity. `AgentOS.control_harness(START)` fences that identity
before invoking the Harness hook. Harness output is not semantic truth: the
normal verifier and VPG commit remain authoritative.

## Reproduce

From the repository root:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python -m lhos.cli.core benchmark harness-adaptive --json --delay-ms 10 --max-concurrency 2
```

The default `DeterministicLocalHarnessProvider` is local and reproducible. The
JSON report contains:

- `verified_task_ids` and final `verified_progress`;
- measured `elapsed_ms` (`wall_clock_measured=true`);
- scheduler attempts, Claim/Lease counts and `harness_control_events`;
- stale attempts, reexecuted task IDs and stale/rework token counts;
- declared usage fields (`input_tokens`, `output_tokens`,
  `verification_tokens`, cost proxy) and usage source;
- actual and policy-selected parallelism traces;
- an independent runtime audit showing no live Claim, reservation or Lease
  after completion.

The controlled baseline intentionally overlaps two tasks that both write
`workspace://shared`. It should produce one failed/stale attempt and one retry.
The adaptive run receives the explicit `ConflictGraph`, serializes those
writers, and keeps independent work parallel. Both modes must reach the same
four-task VERIFIED set.

## Optional provider plugin

An explicit provider factory can be supplied without placing credentials in the
repository:

```powershell
python -m lhos.cli.core benchmark harness-adaptive `
  --provider-factory my_provider:create_provider --json
```

`create_provider()` must return an object with a non-empty `provider_id` and
an `execute(request)` method. `request` is a
`HarnessProviderRequest`; the method may return a
`HarnessProviderResult` or an equivalent mapping:

```python
from lhos.benchmarks.harness_adaptive import HarnessProviderResult

class Provider:
    provider_id = "my-provider"

    async def execute(self, request):
        # Call a real model/API here if desired.
        return HarnessProviderResult(
            content="completion",
            input_tokens=123,
            output_tokens=45,
            model_cost_usd=0.001,
            usage_kind="provider_reported",
        )

def create_provider():
    return Provider()
```

The plugin is opt-in arbitrary Python and is not sandboxed by this benchmark.
Provider-reported usage is observational; it does not establish model quality
or production economics.

## Scope

This result supports only the bounded statement that, with complete explicit
read/write declarations, adaptive policy can avoid a known conflict while
preserving the real AgentOS ownership, Harness control, verifier and VPG
commit path. Wall-clock values are orientation-only. The benchmark does not
measure hidden provenance discovery, physical CPU/GPU/RAM/VRAM placement,
distributed scheduling, hard process preemption, or general Agent quality.
