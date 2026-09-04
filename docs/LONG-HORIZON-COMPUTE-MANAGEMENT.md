# Long-Horizon Compute Management

**Status:** target architecture / research design  
**Release boundary:** the repository is currently an experimental,
single-host `v0.1.x` research alpha. The mechanisms below are not all
implemented; every “should” is a future design requirement unless the
implementation status documents say otherwise.

## Core position

> **LongHorizonOS treats long-running agent execution as a stateful online
> computation problem. It continuously observes semantic progress, cognition,
> context, and resources, and dynamically schedules, reuses, interrupts,
> parallelizes, and repairs computation to minimize the cost of verified goal
> completion.**

The five control verbs are:

> **Schedule · Reuse · Interrupt · Parallelize · Repair**

Two shorter statements define the system boundary:

> **Harnesses make long-running Agents possible; LongHorizonOS makes
> long-running Agent computation efficient.**

> **Graph represents the evolving computation; the OS continuously schedules
> from the Graph.**

This is broader than “run the next graph node.” A long-horizon Agent may have
already spent tokens, time, money, context capacity, and external side effects.
When the world or the goal changes, the runtime must decide what remains valid,
what must be reconsidered, and where additional computation has the highest
expected value.

The current implementation demonstrates a narrower primitive—graph-relative
invalidation and selective repair with single-host ownership and logical
resource admission. This document describes the system direction without
turning that primitive into a production claim.

## Harnesses are managed execution units

An Agent Harness and LongHorizonOS solve different layers of the problem:

| Layer | Primary question | Typical responsibility |
|---|---|---|
| Model | What response or action should be generated? | Inference |
| Harness/session runtime | How can one Agent loop keep working reliably? | Tools, session state, checkpoint/resume, retry, local verification |
| LongHorizonOS | Where should the next unit of long-running compute go? | Global validity, scheduling epochs, parallelism, interruption, reuse, repair |

The Harness owns the mechanics inside one admitted execution. LongHorizonOS
owns the global decision about whether a Harness session should `START`,
`CONTINUE`, `DEFER`, `PREEMPT`, or `REBASE`. This allows existing Harnesses to
be adapted as execution units instead of being replaced.

The bounded public session contract is defined in
[`HARNESS-SESSION-PROTOCOL.md`](HARNESS-SESSION-PROTOCOL.md). Process isolation,
arbitrary callback termination, and checkpoint portability across unrelated
Harness implementations remain separate capabilities.

## The Graph is runtime control state

The VPG is not merely a record that task A completed before task B. It
represents why progress is valid, which exact versions justify it, and how a
change alters the remaining computation:

```text
Agent or world produces change
  -> update versioned Semantic Progress Graph
  -> derive validity, READY/repair frontier, critical path, unlock value,
     safe parallel structure, and stale cognition
  -> recompute the next scheduling epoch
  -> continue / defer / parallelize / preempt / rebase
  -> execute through a Harness
  -> verify and update the Graph again
```

Every graph-derived signal is relative to the declared VPG. Missing semantic
dependencies and unobserved world changes remain unknown; a correct graph
algorithm cannot repair an omitted edge.

## Global runtime state: four first-class classes

The target runtime maintains four coupled but separately authoritative state
classes. **Execution ownership is a cross-layer safety constraint**, not a
replacement for any of these four classes.

### 1. Progress / Semantic state

This state answers **what is true, useful, and still reusable**:

- VPG nodes and dependency/provenance edges;
- Artifact, Evidence, verifier, requirement, and external-fact versions;
- `READY`, `VERIFIED`, `STALE`, invalidated, and Goal-closure state;
- repair frontier, confidence/coverage, and semantic epoch;
- reusable outputs and their exact validity conditions.

Only this authority may close a Goal. “The Agent said it finished” and
“the executor returned” are observations, not semantic proof.

### 2. Agent / Cognition state

This state answers **what an Agent currently believes, plans, and is able to
continue thinking about**:

- active plan / subgoal hypotheses and alternatives;
- beliefs, assumptions, uncertainty, contradictions, and confidence;
- cognitive phase, tool/model capabilities, and estimated next-step value;
- stale cognition markers when a premise or instruction changed;
- deliberation budget, retry history, and replanning checkpoints.

`Stale Cognition` is distinct from ordinary task invalidation: a task can still
be executable while the Agent’s plan, assumptions, or explanation are no
longer trustworthy. The target runtime should invalidate or quarantine those
beliefs before allowing them to drive new side effects.

### 3. Context state

This state answers **what information is available to an Agent at this
attempt, in what form and with what locality**:

- working-set manifests, context pages, snapshots, and restore points;
- token/byte budgets, eviction and pinning state;
- read-set/version bindings and observation coverage;
- context relevance, recency, and **Cognitive Locality**;
- context snapshots tied to attempt, semantic epoch, and provenance digest.

Context is a managed compute resource and a semantic input. A context snapshot
must therefore be versioned and attributable; it is not an invisible prompt
string or an unbounded cache.

### 4. Resource state

This state answers **where and with what capacity an attempt can run**:

- logical CPU, RAM, GPU, VRAM, model-slot, I/O, tool, token, and dollar
  budgets;
- reservations, releases, placement, queueing, and allocation epochs;
- priority, fairness, starvation, preemption, and conflict constraints;
- later: host/device telemetry, topology, isolation, and multi-host capacity.

The current Scheduler supports logical per-Agent admission only. Physical
telemetry, isolation, quotas, fairness, and distributed placement are target
work, not current guarantees.

## Cross-layer safety: execution ownership

Every attempt must carry an unambiguous `Process -> Agent -> Task ->
Attempt -> Claim -> Lease/fencing` identity. Ownership checks apply when work
starts, when it renews, when it is interrupted, and immediately before any
semantic or external-effect commit.

This constraint prevents stale workers from committing, but it does not decide
what the Agent should believe or which task is semantically valid. Those
decisions remain in Progress/Semantic and Agent/Cognition state.

## Online policy and verified-progress utility

The target control loop uses an adaptive online policy:

```text
State(t) = {
  Progress/Semantic(t),
  Agent/Cognition(t),
  Context(t),
  Resource(t),
  ownership/observation safety(t)
}

Policy(t) = π(State(t))
```

The policy chooses among reuse, planning, execution, interruption,
parallelization, and repair. A useful optimization objective is:

```text
maximize  Expected Verified Progress Gain
          / (Token Cost + Time Cost + Dollar Cost
             + Context Cost + Expected Rework)
```

This ratio is a decision heuristic, not a license to trade away correctness.
The policy is constrained by:

- no false `VERIFIED` or false Goal closure;
- no stale-cognition commit;
- no stale-worker or superseded-lease commit;
- no resource oversubscription or permanent deadlock;
- uncertain irreversible effects fail closed.

The policy should learn from observed completion, verification, invalidation,
latency, resource use, and rework rather than assume a static task graph is
optimal.

## Adaptive Policy Engine before `WHO/WHERE`

The target architecture inserts an **Adaptive Policy Engine** between semantic
eligibility and the existing execution placement layer:

```text
Progress/Semantic + Agent/Cognition + Context + Resource
  -> Adaptive Policy Engine: WHAT / WHEN
  -> D2 Scheduler: WHO / WHERE
  -> Claim + Lease/fencing
  -> Agent/tool attempt
  -> verification + observation
  -> state update at epoch t+1
```

### `WHAT`

Select the next useful operation: reuse an output, continue cognition,
replan, execute a tool, repair stale progress, or gather missing evidence.

### `WHEN`

Choose timing and urgency: execute now, wait for capacity, batch, prefetch
context, interrupt, retry, or defer until an observation stabilizes.

### `WHO/WHERE`

The existing D2 Scheduler remains responsible for deterministic Agent
eligibility, Claim, logical admission, and (in the target) device/host
placement. It must not invent semantic readiness or silently alter cognition.

## State-transition loop

Each online epoch should perform the following closed loop:

```text
observe and validate State(t)
  -> detect semantic changes and Stale Cognition
  -> derive reusable set, runnable frontier, conflicts, and repair frontier
  -> Adaptive Policy chooses WHAT/WHEN
  -> D2 chooses WHO/WHERE and reserves resources
  -> issue Claim + Lease/fencing
  -> build/load attempt Context snapshot
  -> execute, verify, observe side effects
  -> commit only if semantic epoch + ownership still match
  -> publish State(t+1), metrics, and wakeups
```

If an observation changes during execution, the result must be revalidated
against the newer epoch before it can become `VERIFIED`.

## Five runtime actions

### `Schedule`

Admit a semantically runnable operation when dependencies, cognition,
context, resource, effect, and ownership constraints pass. Admission is
durable and identifies the exact attempt.

### `Reuse`

Reuse an exact-version output or context snapshot when its semantic,
observation, and cognition conditions still hold. A task-ID cache hit alone is
not sufficient.

### `Interrupt`

Perform a **Semantic Interrupt** when an input, requirement, belief,
observation, lease, or effect outcome invalidates the current attempt. The
operation may be cancelled, quarantined, or allowed to finish only for
non-semantic cleanup. Current Python callbacks are cooperative; killable
process isolation is future work.

### `Parallelize`

Use **Dynamic Parallelism**: derive runnable work from the current frontier,
resource state, and a conflict graph rather than statically launching every
node. Independent branches may run concurrently; writes with a conflict edge
must be serialized, fenced, or speculatively executed with reconciliation.

### `Repair`

Propagate semantic and cognitive staleness through the accepted
provenance/dependency graph, preserve unaffected verified progress, and choose
the smallest safe repair frontier. Unknown coverage expands the frontier or
blocks closure; it never silently authorizes a minimum-repair claim.

## Conflict graph and locality

The target runtime maintains a dynamic conflict graph over operations and
resources:

- read/write conflicts and incompatible external effects;
- shared context pages, model slots, and device memory;
- ownership and lease conflicts;
- semantic conflicts between competing beliefs or plans.

The graph enables safe dynamic parallelism and exposes deadlock cycles before
admission. It also supports **Cognitive Locality**: keep related beliefs,
evidence, and context pages near the Agent/attempt that can use them, while
evicting or transferring low-value state under budget pressure.

## Implementation order and current status

The eight-step sequence remains the intended research order. The parenthetical
labels distinguish what is already bounded in this repository from future
system guarantees:

1. **Connect Context VM to `AgentOS.run()` and `run_async()` (bounded
   implemented).** Each scheduled SDK Attempt gets an attributable Context VM
   load/snapshot when a Context authority is available. Explicit task manifests
   are supported; an absent manifest receives a small empty default. This is
   not a sandbox or an automatic read tracer.
2. **Add `AgentSnapshot` and runtime provenance (bounded implemented).**
   Attempt state records Agent/task, graph and semantic epoch, read-set,
   write-set, `ContextIdentity`, progress, resource binding, and computation
   cost. Durable reopen restores these fields and stale-cognition events.
3. **Validate the read-set at commit time (implemented on the main SDK path).**
   A changed or unavailable binding is quarantined as `STALE_COGNITION` or
   `READ_SET_UNAVAILABLE`; no new Evidence is written and cleanup is fenced to
   the exact Claim. This is not a general rebase or interrupt loop.
4. **Expose `RuntimeStateView` (implemented, read-only).** Progress/Semantic,
   Agent/Cognition, Context, and logical Resource state are projected into an
   immutable `GlobalRuntimeState` through `AgentOS.runtime_state(goal)`.
5. **Introduce `FrontierPolicy` and `SchedulingEpoch` (implemented,
   opt-in proposal).** The deterministic policy ranks the observed
   READY/repair frontier and emits a bounded batch hint; it does not call the
   Scheduler or acquire ownership.
6. **Implement Dynamic Parallelism and a Conflict Graph (implemented,
   opt-in/explicit-access-only).** A deterministic greedy batch is derived from
   declared read/write sets; unknown access is serial-only. There is no
   automatic provenance, resource fitting, or default Scheduler integration.
7. **Add Semantic Interrupt and cooperative preemption/rebase (bounded
   implemented).** `SemanticInterruptPolicy` emits auditable
   `CONTINUE`/`DEFER`/`PREEMPT`/`REBASE`/`REVERIFY` decisions.
   `AgentOS.deliver_interrupt(...)` validates graph/epoch/claim/task/attempt
   identity and routes cooperative requests to live token-aware async SDK
   executors. The verifier-to-Evidence commit fence rejects late or ignored
   interrupt completions. Automatic world watchers, Context rebase, Lease
   handoff, and force-kill isolation remain open.
8. **Add Cognitive Locality plus model/verifier routing (bounded advisory
   and explicit adapter implemented).** `ComputeRoutingPolicy` and
   `AgentOS.plan_compute_routing(...)` now provide deterministic,
   fail-closed recommendations for exact context overlap, warm-vs-fresh Agent
   use, bounded context budget, provider-independent model tier, and
   verification strength. This policy is read-only and metadata-driven: it
   does not create/reuse processes, automatically select providers, mutate
   live Context, or alter Scheduler/Kernel execution. An explicit
   `ComputeProviderRegistry` may invoke registered model/verifier/context
   hooks after Claim/Context setup; automatic provider scheduling, process
   lifecycle management, physical placement, and utility/cost benchmarking
   remain open.

Only after this sequence should the project expand into symbol-level
provenance, semantic projection, speculative execution, quota-aware
scheduling, workspace locks, cross-process workers, physical device
enforcement, and multi-host coordination.

The current repository contains foundations for all of these steps, including
VPG validity, Scheduler Attempts, the SDK Context VM path, durable
`AgentSnapshot`, logical resource admission, Claims, Leases, fencing,
   `RuntimeStateView`, opt-in policy proposals, and bounded compute-routing
   advisory/adapter. That does **not** mean the
eight-step adaptive control loop is already integrated: the default
`AgentOS.run()`/`run_async()` path still uses the existing Scheduler, and policy
output does not itself claim, lease, preempt, rebase, or perform automatic
provider selection; the Stage-8 policy remains a recommendation surface and
the provider registry is an explicit execution adapter.

## Benchmark design: static vs adaptive

Every claim needs a regression test and a reproducible benchmark. Compare at
least:

1. **Static baseline:** fixed task DAG, fixed context allocation, fixed
   concurrency/resource policy, and full restart or oracle task-DAG repair.
2. **Adaptive policy:** online `Policy(t)=π(State(t))` with dynamic context,
   parallelism, interruption, reuse, and repair decisions.

Measure:

| Dimension | Metrics |
|---|---|
| Verified progress | expected verified progress gain, false closure, stale cognition accepted |
| Work/utility | tokens, wall time, dollars, context bytes/tokens, expected rework |
| Repair | work saved vs full restart and oracle DAG; frontier size; time to reclosure |
| Scheduling | makespan, throughput, queue delay, critical-path utilization, reuse rate |
| Parallelism | safe parallel speedup, conflict-induced serialization, deadlock cycles |
| Resources | utilization, oversubscription, starvation, preemption, placement failures |
| Durability/ownership | replay success, stale commits, duplicate claims, recovery latency |
| Context | snapshot/restore latency, hit rate, eviction, locality, coverage gaps |
| Tail/scale | p50/p95/p99 and `N=400/1,000/10,000` history/entities |

The repository’s existing synthetic gates remain useful, but they do not
measure adaptive cognition, physical GPU use, or real model economics. Future
results must record commit, environment, workload, policy parameters, and raw
traces.

## Honest current boundary

Current `v0.1.x` evidence supports a **single-host, graph-relative semantic
repair runtime** with logical resource admission, Kernel-fenced ownership,
durable Scheduler projections, explicit Context/AgentSnapshot observations,
commit-time stale-cognition quarantine, opt-in deterministic policy proposals,
and bounded cooperative interrupt delivery for token-aware async workers.

It does not yet provide automatic hidden-dependency discovery, a default
Adaptive Policy controller, universal Semantic Interrupt watcher/SDK
orchestration, automatic Context rebase or Lease handoff, automatic
provider-aware Locality/model/verifier scheduling, general belief revision, universal I/O
mediation, exactly-once irreversible effects, killable sandbox execution,
physical GPU/VRAM enforcement, or distributed multi-writer scheduling.
