# Semantic Interrupts

LongHorizonOS has two deliberately separate interrupt layers:

1. `SemanticInterruptPolicy` turns an explicit observation into a deterministic
   `CONTINUE`, `DEFER`, `PREEMPT`, `REBASE`, or `REVERIFY` decision.
2. `AsyncWorkerPool` provides a bounded **cooperative delivery primitive** for a
   currently running attempt. A dispatcher must explicitly opt in by accepting
   `cancellation_token=` and must observe the token.

The second layer is real execution control, but it is not a force-kill
mechanism. Scheduler Claims and Kernel Leases remain the ownership authority;
the pool never releases or transfers them implicitly.

## Read-only planning

```python
epoch = os_.plan_interrupts(
    goal,
    [SemanticInterrupt(
        graph_id=graph_id,
        graph_version=graph_version,
        kind=SemanticInterruptKind.ARTIFACT_CHANGED,
        reason="API artifact changed",
        affected_task_ids=("frontend",),
    )],
)
```

Planning is deterministic and does not mutate VPG, Scheduler claims, attempts,
leases, or resources.

## Cooperative delivery

For a running, token-aware worker, the pool can route a request to the exact
`claim_id`/attempt identity:

```python
delivery = pool.request_interrupt(
    claim_id,
    action="rebase",                 # or "preempt"
    interrupt_id="api-change-18",
    decision_hash=epoch.decision_hash,
    reason="API artifact changed",
)
```

The bounded result distinguishes `NOT_RUNNING`, stale/identity rejection,
non-preemptible legacy dispatchers, and accepted cooperative requests. The
durable transition callback can record:

```text
REQUESTED -> DELIVERED -> OBSERVED -> CANCELLED
```

`OBSERVED` is emitted only after the executor polls/awaits/raises through the
token. If a token-aware executor returns successfully after a request without
observing it, the pool quarantines that completion as an unobserved
cooperative interrupt; it cannot cross the operational-success fence. Cleanup
is still exact-claim and lease-fenced.

The SDK exposes `AgentOS.deliver_interrupt(...)` for a live `run_async()` batch.
It registers the active worker pool, validates graph/epoch/claim/task/attempt
identity, routes the request to the exact claim, and records bounded
`SEMANTIC_INTERRUPT_ACKNOWLEDGED` phases in the Scheduler journal. This is a
direct control-plane primitive for the built-in SDK executor path; it does not
discover world changes or coordinate arbitrary third-party Harness sessions.
The general watcher-driven orchestration loop remains open.

## Optional durable audit

Pass `persist=True` when the proposal itself must be replayable/auditable:

```python
epoch = os_.plan_interrupts(
    goal,
    interrupts,
    epoch_id=next_epoch,
    persist=True,
)
```

This appends one `SchedulerEventType.SEMANTIC_INTERRUPT_PROPOSED` event to the
Scheduler event journal. The event contains only bounded identities,
versions, actions, interrupt IDs, and the proposal hash; it never stores
prompts, model output, artifact contents, or full Context snapshots.

The event identity is deterministic for `(graph_id, epoch_id,
decision_hash)`, so retrying the same proposal is idempotent. A conflicting
payload under the same identity fails closed. The journal write and
Scheduler projection use the existing SQLite transaction/hash-chain path.
The external Scheduler audit hook accepts only
`SEMANTIC_INTERRUPT_PROPOSED`; lifecycle events such as `CLAIM_COMPLETED`
cannot be forged through this policy boundary.

`persist=True` is unavailable on a read-only `AgentOS`.

## What this does not provide

The following remain outside this bounded primitive:

1. force-killing arbitrary Python code or sandbox/process isolation;
2. implicit Claim release, Lease handoff, or distributed ownership transfer;
3. automatic incremental Context rebase or third-party Harness orchestration;
4. filesystem/API/requirement watchers and automatic provenance/dependency
   discovery;
5. general replanning after an interrupt.

These are intentionally separate follow-on capabilities rather than implied by
the cooperative token path.
