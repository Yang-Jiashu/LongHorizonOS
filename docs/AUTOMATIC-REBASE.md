# Bounded Automatic Context Rebase

LongHorizonOS now has a small automatic repair path on the async SDK
execution loop:

```text
execute
  -> read-set freshness fence
  -> stale cognition quarantine
  -> explicit ContextManifest refresh
  -> REBASE or FULL_RELOAD decision
  -> normal Scheduler admission of a fresh Attempt
  -> verify and commit
```

The path is caller-bounded. It does not start a daemon, retain a Python stack,
force-kill a callback, or transfer ownership atomically.

## What Is Automatic

`AgentOS.run_async` accepts:

```python
result = await os.run_async(
    goal,
    max_dispatches=1,
    max_steps=1,
    automatic_rebase=True,
    max_automatic_rebase_dispatches=1,
)
```

When a `context_v1` Attempt fails its exact artifact read-set freshness fence,
the runtime:

1. marks the old Attempt `STALE_COGNITION`;
2. releases only the old Claim using the existing exact-claim fence;
3. reads current artifact versions and hashes from `FactsProvider`;
4. compares those changes with the task's explicit `ContextManifest`;
5. emits a deterministic `REBASE` (some refs changed) or `FULL_RELOAD` (all
   refs changed) decision;
6. admits a replacement Claim/Attempt through the normal Scheduler path; and
7. materializes a new Context VM snapshot before invoking the callback.

The replacement callback receives the complete refreshed context plus bounded
metadata:

```python
ctx.automatic_rebase_action       # "rebase" or "full_reload"
ctx.automatic_rebase_decision     # immutable AutomaticRebaseDecision
ctx.automatic_rebase_delta        # changed-page projection
ctx.automatic_rebase_manifest     # manifest used for this Attempt
```

The changed-page projection is an audit/control hint. It is not semantic proof;
the replacement Attempt still has to pass the ordinary provenance and Evidence
commit fences.

## Fail-Closed Cases

Automatic repair is refused when:

- the stale Attempt has no durable `AgentSnapshot`;
- the task has no explicit `ContextManifest`;
- a read is unknown, unversioned, or missing a content hash;
- the changed read is not represented by exactly one manifest ref;
- the current version or hash cannot be obtained from `FactsProvider`;
- the bounded re-dispatch budget is exhausted; or
- the graph version changes before replacement admission.

In these cases no replacement callback is invoked and the `RunResult` carries a
bounded `automatic_rebase_records` entry plus a failure reason.

## Ownership Boundary

The automatic execution loop creates a **fresh** Scheduler Attempt after the
old Attempt has been quarantined and released. It does not reuse an old
Harness session identity and does not claim an atomic
Scheduler/Kernel/Harness/VPG transaction.

The authority-backed `apply_live_context_rebase(...)` façade now exposes the
same bounded handoff slice for explicit live sessions:

1. persist a `PREPARED` handoff intent;
2. persist `COMMITTING`;
3. fence/release the exact source Claim and admit a replacement Claim/Attempt;
4. detach the old Harness binding; and
5. return the replacement Claim/Attempt ids.

This is **release-then-acquire**, not an atomic cross-plane transaction. A crash
between the durable markers can leave a recovery witness (`IN_DOUBT` or
`FAILED_CLOSED`); callers must invoke `recover_handoff(...)` and register a
fresh Harness for the replacement Attempt. The old session can never control
the replacement Claim because all control paths retain exact Claim/Attempt
identity fences.

## Audit Surface

`RunResult.meta` includes:

- `automatic_rebase_enabled`;
- `automatic_rebase_max_dispatches`;
- `regular_dispatches`;
- `automatic_rebase_dispatches`;
- `automatic_rebase_records`; and
- `automatic_rebase_pending`.

Each planned decision contains stable plan and decision hashes, source Claim /
Attempt / Context snapshot identities, changed refs, and the selected action.
The old Attempt remains durably visible as `STALE_COGNITION`; only the fresh
Attempt can publish Evidence.
