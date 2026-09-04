# LongHorizonOS Implementation Progress

**Reporting slot:** 2026-08-15 23:15 (Asia/Shanghai)  
**File finalized:** 2026-08-15 23:27 (Asia/Shanghai)  
**Workspace:** local `LongHorizonOS-main` checkout  
**Release boundary:** experimental single-host research alpha (`v0.1.x`)

## This interval's implementation target

Document the newly available host-resource telemetry adapter without
over-claiming it as physical resource scheduling:

- expose CPU/RAM and optional NVIDIA GPU/VRAM observations;
- preserve explicit unavailable/error states instead of fabricating zero
  capacity;
- keep telemetry out of Scheduler/Claim/Lease admission and placement;
- synchronize implementation status, issue inventory, and roadmap wording;
- record a focused regression result.

## Implemented

- `src/lhos/sdk/resource_telemetry.py` provides the optional
  `collect_host_resource_telemetry()` side-channel adapter.
- CPU and RAM are sampled from the host; NVIDIA GPU count and aggregate VRAM
  are sampled through the stable `nvidia-smi` CSV interface when available.
- Every metric carries `is_available`, source, and bounded failure reason.
  Missing `nvidia-smi`, malformed output, timeout, and contradictory quantities
  fail closed; unavailable does not mean zero capacity.
- The adapter is exported through the SDK but does not mutate
  `RuntimeStateView`, Scheduler resources, Claims, Kernel Leases, placement,
  quotas, or isolation.
- `docs/IMPLEMENTATION-STATUS.md`, `docs/ISSUE-INVENTORY.md`, and
  `docs/ROADMAP.md` now distinguish:
  - **implemented:** host telemetry observation side channel;
  - **open:** physical admission/placement, device isolation, quotas,
    preemption, and distributed inventory.

## Focused verification

```text
pytest -q tests/sdk/test_resource_telemetry.py
5 passed in 0.56s
```

The gate covers deterministic CPU/RAM observation, injected NVIDIA CSV
aggregation, missing-tool reporting, malformed-output fail-closed behavior,
and configuration validation. It does not claim physical scheduling or
placement correctness because the adapter intentionally has no such authority.

## Still not implemented / not claimed

- Telemetry-driven resource admission or physical CPU/GPU/RAM/VRAM placement.
- Shared host/device inventory, isolation, quotas, fairness, preemption, or
  starvation guarantees.
- Distributed scheduling, multi-host coordination, or real-model/GPU
  performance evidence.

## Next action

Add an explicit telemetry-to-admission design only after defining a single
authoritative host/device inventory and fencing protocol. Until then, keep the
adapter observational and keep all public release text bounded to the
single-host research-alpha contract.
