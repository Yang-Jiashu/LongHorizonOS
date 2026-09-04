# Bounded Ownership Handoff Contract

**Status:** implemented bounded single-scheduler intent protocol
(`prepare_handoff` / `commit_handoff` / `recover_handoff`) plus the legacy
`handoff_task` compatibility path  
**Scope:** one in-process `MultiAgentScheduler` projection plus its injected
Kernel lease provider and durable Scheduler event journal  
**Non-goal:** this is **not** a cross-service atomic transaction between
Scheduler, Kernel, Harness, and VPG.

## What the operation guarantees

The intent protocol records an exact source Claim/Attempt identity and a
caller-supplied `handoff_id` *before* ownership mutation:

1. `prepare_handoff(...)` validates and durably journals the intent. It does
   not release a lease or call a Harness.
2. `commit_handoff(...)` durably journals `COMMITTING`, then delegates to the
   existing exact/fenced `handoff_task(...)` path.
3. A successful replacement is durably marked `COMMITTED`; failed or
   interrupted paths are recorded as `FAILED_CLOSED`/`IN_DOUBT`.
4. `recover_handoff(...)` aborts an untouched `PREPARED` intent, recognizes an
   already materialized replacement, and otherwise fails closed without
   guessing ownership.

The compatibility path `handoff_task(...)` still uses:

1. Validate source Claim, Attempt, semantic epoch, replacement eligibility,
   graph readiness, and resources.
2. Release the exact source Kernel Lease.
3. Mark the source Attempt/Claim terminal (`STALE_COGNITION`/`RELEASED` for
   `REBASE`, or `PREEMPTED`/`RELEASED` for `PREEMPT`).
4. Admit a replacement through the normal fenced claim path.

The result is the serializable `ClaimHandoffResult` DTO. Its status is one of
`TRANSFERRED`, `REPLAYED`, `REFUSED`, or `FAILED_CLOSED`.

## Fail-closed and idempotency rules

- A refusal **before** source lease release leaves the source Claim/Lease
  untouched.
- If source lease release cannot be confirmed, the operation returns
  `FAILED_CLOSED` and does not mutate the source projection.
- Once source release succeeds, replacement admission may still fail. In that
  case the source epoch remains fenced and the result is `FAILED_CLOSED`; the
  caller must rely on normal Scheduler reconciliation/cleanup before retrying.
- A successful handoff can be replayed with the same request identity and
  returns `REPLAYED` without creating another replacement Claim.
- `handoff_id` is an idempotency key, **not an ownership capability**. Reusing
  it with a different source Claim, source Attempt, replacement Agent, or
  action returns `REFUSED`; reusing it for another graph/task is also refused.
  It never releases or retargets the existing replacement.
- A stale worker can only release its exact `source_claim_id`; it cannot
  release a replacement Claim created by the handoff.

## What is intentionally not guaranteed

The operation remains release-first across multiple authority boundaries. The
durable intent is a recovery witness, **not** a two-phase commit coordinator:
it does not atomically update a Kernel lease, a Harness session, and VPG state
in one database transaction. A crash after `COMMITTING` can therefore produce
`IN_DOUBT`, which is intentionally fail-closed. It does not transfer a
running Python stack, model context, or arbitrary external side effects.
`Harness REBASE/PREEMPT` remains refused by the current bridge until a future
coordinator can fence Scheduler Claim, Kernel Lease, Harness session, and VPG
state together.
