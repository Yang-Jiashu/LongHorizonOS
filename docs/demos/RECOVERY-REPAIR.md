# LongHorizonOS — Recovery + Semantic Reconciliation Demo

## What this demo proves
One command shows the Core's distinctive behavior end-to-end: worker crash →
**execution ownership recovery**; ArtifactVersion mutation → **Evidence
applicability loss**; **selective semantic invalidation**; **preserved VERIFIED
work**; **minimal Repair Frontier**; D2 repair with **new exact-version Evidence**;
and **semantic reclosure** (Goal CLOSED → REOPENED → CLOSED).

## Run it
```
pip install .            # or install the wheel
lhos demo recovery-repair
lhos demo recovery-repair --json      # machine-readable summary
lhos demo recovery-repair --paced     # slower, GIF/video friendly
lhos demo recovery-repair --live-model  # OPTIONAL real-model mode (not required)
```
No API key, no network, no tests, wheel-runnable outside the repo.  Deterministic.

## Acts (all derived from REAL Core state — formatter never reconstructs truth)
1. **Build verified progress** — a real Goal with 4 Tasks (Research→Implement→
   Review causal branch + Independent Analysis branch) verified via real
   Resources/Shell/Evidence → VPG derives VERIFIED → GOAL CLOSED.
2. **Worker failure** — a real worker process SIGKILL (or controlled termination
   fallback) → Kernel process FAILED → ownership reconciled.
3. **World changed** — real workspace file `source.py@v1 → @v2` committed as an
   exact ArtifactVersion via the Workspace↔Artifact bridge.
4. **Semantic reconciliation** — real D3: old Evidence stays historical but loses
   current applicability; only causal Tasks go STALE; the independent branch stays
   VERIFIED/PRESERVED; minimal Repair Frontier = [Inspect]; Review stays blocked.
5. **Local repair** — D2 re-schedules with new exact-version Evidence → VERIFIED,
   frontier advances to Review → VERIFIED → GOAL CLOSED.

## Why this is not "full restart"
`full_restart_avoided` is YES only because preserved VERIFIED work was NOT
re-executed; `preserved_verified_tasks > 0` and those stayed verified through the
mutation while only the minimally-affected set was repaired.

## Deterministic default vs live-model
Default uses a deterministic scripted executor + real Shell/Workspace (no API
key).  `--live-model` may use an OpenAI-compatible provider, but goes through the
same Core/VPG execution path with no extra semantic authority (DEMO-G10).

## Parts which use real Core
Kernel (process/lease), Verified Progress Graph (validity/closure),
Multi-Agent Scheduler (claims/ownership), D3 (invalidation/Repair Frontier),
Artifact FS bridge (exact ArtifactVersion), Shell verification, and E3
observability read models (StatusView / graph renderer).  Nothing is hardcoded.
