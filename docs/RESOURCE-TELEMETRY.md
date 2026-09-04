# Optional Host Resource Telemetry

LongHorizonOS now exposes a **best-effort host telemetry adapter**:

```python
from lhos.sdk import collect_host_resource_telemetry

telemetry = collect_host_resource_telemetry()
print(telemetry.cpu.total, telemetry.ram.available)
print(telemetry.gpu.is_available, telemetry.gpu.reason)
```

The public API is `collect_host_resource_telemetry()` and the immutable
`HostResourceTelemetry` / `ResourceTelemetryMetric` models in
`lhos.sdk.resource_telemetry`.

## Coverage

| Plane | Probe | Failure behavior |
| --- | --- | --- |
| CPU | `os.cpu_count()` (logical cores) | `is_available=False` |
| RAM | `GlobalMemoryStatusEx` on Windows, `os.sysconf` on POSIX | `is_available=False` |
| GPU | NVIDIA `nvidia-smi` CSV query | `is_available=False` |
| VRAM | Aggregate NVIDIA `nvidia-smi` memory values | `is_available=False` |

Unavailable values are represented by `None` plus a bounded `reason`; they are
**not** converted to zero. The adapter performs no scheduler mutation and is
not a physical resource allocator. In particular, this does not yet provide
GPU partitioning, quotas, admission control, or multi-host scheduling.

`ResourceRuntimeState` remains the authoritative projection of Scheduler
**logical** reservations. Applications that want to use host telemetry in a
policy may sample it explicitly, compare it with logical reservations, and
apply their own fail-closed policy.

## Explicit telemetry-to-logical-capacity bridge

LongHorizonOS also exposes a small, opt-in bridge:

```python
from lhos.sdk import (
    HostCapacityPolicy,
    collect_host_resource_telemetry,
    derive_host_capacity,
)

telemetry = collect_host_resource_telemetry()
policy = HostCapacityPolicy(
    cpu_reserve_fraction=0.20,
    ram_reserve_fraction=0.20,
    gpu_reserve_fraction=0.0,
    vram_reserve_fraction=0.15,
)
decision = derive_host_capacity(telemetry, policy)

if decision.available:
    result = os_.apply_host_capacity("worker", telemetry, policy)
```

`derive_host_capacity()` converts authoritative *available* CPU cores, RAM,
GPU count, and VRAM into a logical `ResourceVector`, always rounding down after
the configured reserve fraction. Required unknown metrics fail closed and
produce no partial vector. A CPU-only pool must opt out explicitly with
`require_gpu=False`; unknown GPU/VRAM remain visible in the decision audit and
are never described as observed zero-capacity hardware. `model_slots` are
caller declarations because host telemetry cannot discover model-server
capacity.

`AgentOS.apply_host_capacity(...)` explicitly updates only the named registered
Agent pool. It first calls the Scheduler's atomic logical-capacity update. If
the derived capacity is below active reservations, the operation is rejected
without changing registry/SDK capacity. It does not modify any other pool and
is disabled on read-only runtimes.

This bridge is **not physical placement or enforcement**. It does not pin
processes to CPUs, partition GPUs/VRAM, reserve host memory, set OS quotas,
monitor drift continuously, preempt workers, or coordinate multiple hosts. A
telemetry sample can become stale immediately after observation. The derived
vector is only a conservative logical Scheduler admission ceiling until a
caller explicitly samples and applies again.

For deterministic tests or embedding environments, callers may inject
`command_runner` and `now`:

```python
telemetry = collect_host_resource_telemetry(
    command_runner=my_runner,
    now=my_clock,
)
```
