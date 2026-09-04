# Bounded Online Resource Replanning

`tests/sdk/test_resource_replanning_e2e.py` and
`examples/resource_replanning_e2e.py` demonstrate a small end-to-end control
loop:

```text
caller samples telemetry
    -> apply_host_capacity("worker", sample)
    -> run_async(adaptive=True, resource_aware=True)
    -> caller samples again
    -> apply_host_capacity("worker", new_sample)
    -> run_async(...)
    -> VERIFIED Goal
```

The workload has four independent tasks that each request 500 logical RAM
bytes.  With a named pool at 1,000 bytes, the first bounded invocation selects
two tasks.  The caller then applies a second sample reporting 500 available
bytes; the next invocation re-observes the runtime state and selects one task
per epoch until the same Goal closes.

Run the example from the repository root:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python examples/resource_replanning_e2e.py
```

This is deliberately a bounded, caller-driven demonstration.  Telemetry
resampling is explicit; there is no background daemon.  The host bridge only
updates a named logical Scheduler pool and does not provide physical CPU/GPU
placement, process isolation, quota enforcement, preemption, or a physical
GPU benchmark.  Scheduler Claims/Leases remain authoritative, and VPG
Evidence remains the semantic commit authority.

The higher-level caller-owned supervisor now accepts the same
`max_parallelism` and `resource_aware` controls.  The focused E2E test
`tests/sdk/test_online_resource_supervisor_e2e.py` and runnable example
`examples/online_resource_supervisor.py` exercise:

```text
EventDrivenSupervisor.step()
  -> capacity=1000 -> dispatch [a,b]
caller applies capacity=500
EventDrivenSupervisor.step()
  -> dispatch [c]
EventDrivenSupervisor.step()
  -> dispatch [d] -> VERIFIED Goal
```

Run it with:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
python examples/online_resource_supervisor.py
```

This closes the earlier composition gap where `run_async()` supported
resource-aware batching but `execute_online_epoch(s)` and
`EventDrivenSupervisor` could not forward that policy.  Re-observation and
capacity application are still explicit and caller-owned.
