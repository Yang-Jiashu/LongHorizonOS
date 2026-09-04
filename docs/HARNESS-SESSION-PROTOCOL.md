# Harness Session Protocol

**Status:** bounded experimental implementation (`harness-session.v1`)  
**Scope:** one in-process or externally adapted Agent Harness session  
**Non-goal:** this protocol is not a replacement for the Scheduler, Kernel
Claim/Lease authority, VPG verification, or a process sandbox.

## Position in LongHorizonOS

> **Harness makes long-running agents possible; LongHorizonOS makes
> long-running agent computation efficient.**

A Harness owns one Agent execution loop: model calls, tools, session history,
checkpoint/resume, retry, and local verification. LongHorizonOS is the control
plane above those execution units. It observes the evolving Progress Graph and
decides whether a Harness should `START`, `CONTINUE`, `CHECKPOINT`, `REBASE`, or
`PREEMPT`.

```text
Semantic Progress Graph + Runtime State
                    |
              OS policy epoch
                    |
        START / CONTINUE / CHECKPOINT
              REBASE / PREEMPT
                    |
              Harness session
                    |
          operational result + trace
                    |
       normal provenance + VPG verifier
```

The control result says only what happened to the Harness process. A result
with `completed=True` is **not** Evidence and cannot by itself make a VPG Task
`VERIFIED`.

## Protocol objects

`HarnessSessionIdentity` binds a session to:

- `session_id`;
- `graph_id`, `graph_version`, and `semantic_epoch`;
- `task_id`, `agent_id`, `claim_id`, and `attempt_id`.

The Claim and Attempt fields are identity fences. A replacement owner must
create a new identity; an old request cannot control it.

`HarnessSessionSnapshot` is the read-only lifecycle projection. It contains a
monotonic `revision`, state, optional `checkpoint_id`, progress, and the last
request id.

`HarnessCapabilities` is an executable declaration, not an aspirational enum:

- every adapter must support `START`;
- `CHECKPOINT` requires `checkpoint_scope="session"`;
- `PREEMPT` requires `preemption_mode="cooperative"`;
- `REBASE` requires `rebase_mode="in_place"`.

`HarnessControlRequest` carries the operation, exact session identity,
`expected_revision`, optional checkpoint identity, and (for `REBASE`) a
forward graph/semantic target. Requests are immutable and have a canonical
fingerprint.

`HarnessControlResult` reports `APPLIED`, `UNSUPPORTED`, `REJECTED`, or
`FAILED`, with before/after snapshots. It is an operational acknowledgement,
not a semantic commit.

## Lifecycle and safety rules

The bounded state machine is:

```text
CREATED --START--> RUNNING --CHECKPOINT--> CHECKPOINTED
   |                  |  ^                   |
   |                  |  |                   |
   |                  +--+--CONTINUE---------+
   |                  |                      |
   |                  +--REBASE------------> RUNNING
   |                  +--PREEMPT-----------> PREEMPTED
   +--START (legacy callable)--------------> COMPLETED
```

`CONTINUE` may run from `RUNNING` or `CHECKPOINTED`. `REBASE` may run from
either state but must advance the graph version or semantic epoch; it updates
the session's cognition basis only after the Harness acknowledges the hook.
`PREEMPT` is cooperative and terminal in this bounded adapter. An
uncooperative callback is not force-killed by this module.

Every accepted request increments `revision`. A request with an old revision,
wrong session/Claim/Attempt identity, wrong checkpoint id, or a backward
rebase is rejected before the hook runs.

The adapter caches recent request results by `request_id` and request
fingerprint. Replaying the same request is byte-stable and does not invoke the
hook again. Reusing a request id for different content is rejected.

## Compatibility adapter

`CallableHarnessAdapter(identity, executor=callable)` wraps the existing SDK
executor shape (`executor(task_id)` or `executor()`). It intentionally declares
only `START`, runs once, and transitions to `COMPLETED`. This preserves
backward compatibility without claiming that a one-shot callable can
checkpoint, rebase, or preempt.

To expose a real session lifecycle, pass a `start=` hook and any of
`continue_handler=`, `checkpoint=`, `rebase=`, and `preempt=` hooks. Hooks
receive `(request, snapshot)` and return `HarnessHookOutcome` (or a plain
value, which is treated as an operational output).

### Bounded durable replay

For a file-backed `AgentOS`, accepted `HARNESS_CONTROL` events are part of the
hash-verified Scheduler journal.  Re-registering a replacement
`CallableHarnessAdapter` with the **same complete session identity** replays the
bounded logical session projection (identity, revision, lifecycle state,
checkpoint id, progress, and request-idempotency index).  Replay is
fail-closed on schema, identity, revision, event-id, fingerprint, or duplicate
request inconsistencies.

This is not process checkpointing.  Replay does **not** restore callback
memory, model/tool context, prompts, outputs/details, Python stacks, or
in-flight code; a replayed result is therefore a logical acknowledgement, not
a byte-identical reconstruction of the original output.  It also does not
create, renew, release, or transfer Scheduler Claims/Kernel Leases.  A new
session cannot take over a Claim with existing Harness history until an
explicit ownership-handoff protocol exists.

## Current implementation matrix

| Requirement from `Mind-VLA-笔记.md` | Status |
|---|---|
| Harness is an execution unit below the OS policy plane | **Implemented as a standalone protocol** |
| `START` / `CONTINUE` lifecycle | **Implemented and tested** |
| Session `CHECKPOINT` identity | **Implemented and tested** |
| Cooperative `PREEMPT` acknowledgement | **Implemented in-process; no hard kill** |
| In-place `REBASE` with forward graph/epoch fence | **Implemented and tested** |
| Legacy callable compatibility | **Implemented and tested** |
| Request idempotency and revision/identity fencing | **Implemented in-memory and with bounded durable replay; tested** |
| Durable session journal/reopen | **Implemented for bounded logical snapshots and request replay; callback/model state remains open** |
| Worker-pool cooperative interrupt delivery | **Implemented and tested for token-aware async dispatchers** |
| AgentOS `run_async()` direct executor interrupt delivery | **Implemented and tested via `AgentOS.deliver_interrupt(...)`; verifier commit fence also tested** |
| DeepSeek headless process preempt/timeout | **Implemented with Windows Job Object / POSIX process group; one-shot only** |
| External Harness measured usage in AgentOS ledger | **Implemented for success, verifier failure, execution failure and cooperative preempt partial outcomes** |
| End-to-end Harness-session control from AgentOS policy epochs | **Open** |
| Automatic provenance from arbitrary hidden reads | **Open** |
| Killable sandbox, physical GPU telemetry, distributed Harnesses | **Process tree implemented for DSH; sandbox/physical placement/distribution open** |
| Exactly-once arbitrary external side effects | **Open** |

## Integration boundary

The protocol deliberately does not mutate Claims, Leases, or VPG state.
`AsyncWorkerPool` and `AgentOS.deliver_interrupt(...)` provide an exact-claim
cooperative token path for built-in async dispatchers that explicitly opt in to
`cancellation_token=`; ignored requests are quarantined before operational
success, and the SDK verifier-to-Evidence commit fence rejects a late
interrupt completion.  `AgentOS.register_harness()` and
`control_harness()` provide an explicit, exact-identity bridge and journal the
bounded control result; they do not automatically route every policy epoch or
control arbitrary third-party Harness processes. Direct Harness
`REBASE`/`PREEMPT` through this bridge remain rejected because that call cannot
atomically update Scheduler ownership. The explicit
`apply_live_context_rebase()` façade offers a narrower, **non-atomic**
release-then-acquire path: it persists a handoff intent, fences the old Claim,
admits a fresh Attempt, detaches the old session, and returns replacement
identities. Callers must register a new Harness for that Attempt and use
`recover_handoff()` for an `IN_DOUBT`/`FAILED_CLOSED` witness. Built-in
token-aware SDK attempts may still use the direct cooperative interrupt path.

## DeepSeek Harness adapter

`lhos.integrations.harness.DeepSeekHarnessAdapter` is the external Harness
adapter used by the DSH benchmarks. It directly owns a Node DSH child, parses
the durable JSONL session trace once, and emits typed `harness-phase.v1`
events. Each attempt records the Scheduler/Attempt binding, provider usage,
retry/failure class, candidate typed-tool provenance, credential fingerprint,
and process-tree termination result.

That binding is observational at the DSH process boundary. It fences AgentOS
commit and accounting, but DSH tool effects do not yet carry the lease fencing
token through an AgentOS workspace/action gateway. A stale process can
therefore be stopped and denied Evidence, but already-written workspace bytes
cannot be automatically rolled back.

The adapter currently supports semantic phase contracts and one-shot DSH
`headless` execution. It does **not** claim that an arbitrary headless DSH
session can be resumed or rebased in place. Native `ctx.agents.resume()` and
live `agent.cancel()` require a future authenticated DSH bridge/daemon. Until
that bridge exists, semantic rebase creates a fresh fenced Attempt, and
unknown shell/network effects remain fail-closed.
