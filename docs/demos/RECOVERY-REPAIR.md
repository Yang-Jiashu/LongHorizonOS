# Recovery and Semantic Reconciliation Demo

This demo is the shortest way to understand what makes LongHorizonOS different.

It exercises the real Core end to end:

```text
worker failure
    -> execution ownership recovery

ArtifactVersion change
    -> old Evidence loses current applicability
    -> affected tasks become STALE
    -> unaffected VERIFIED work is preserved
    -> the Graph derives a minimum Repair Frontier
    -> new exact-version Evidence restores Goal closure
```

## Run it

```bash
pip install .

lhos demo recovery-repair
lhos demo recovery-repair --json
lhos demo recovery-repair --paced
```

The demo is deterministic, offline, and requires no API key. It can run from an
installed wheel outside the repository.

## What happens

### 1. Build verified progress

The demo creates a Goal with two branches:

```text
Inspect -> Implement -> Review

Independent Analysis
```

Tasks execute through Kernel-backed Agents and produce Evidence. The VPG derives
the tasks as `VERIFIED` and closes the Goal.

### 2. Recover from worker failure

A worker process is terminated. The Kernel transitions the process state and
releases the exclusive task Lease. Scheduler reconciliation recovers ownership
without deleting semantic progress.

### 3. Change the world

The workspace artifact changes:

```text
source.py@v1 -> source.py@v2
```

The new content is registered as a real ArtifactVersion.

### 4. Reconcile semantic truth

Evidence bound to `source.py@v1` remains immutable historical evidence, but it
is no longer current-applicable to `source.py@v2`.

The invalidation runtime derives the causal affected cone:

- affected tasks become `STALE`;
- downstream tasks are invalidated through `DEPENDS_ON`;
- unrelated tasks remain `VERIFIED`;
- the closed Goal reopens;
- the Graph derives the minimum Repair Frontier.

The Scheduler does not contain invalidation semantics. It simply consumes the
new frontier produced by the Graph.

### 5. Repair locally

Only frontier tasks are scheduled. Each stale task requires fresh Evidence
created after invalidation and bound to the current ArtifactVersion.

As repaired tasks verify, the Graph advances the frontier until the Goal closes
again.

## Why this is not a full restart

The demo reports `full_restart_avoided = true` only when:

- at least one previously verified task remains preserved;
- preserved tasks are not re-executed;
- only the causally affected set is repaired;
- the final Goal returns to `CLOSED`.

## Which parts are real

The formatter reads the outcome from real runtime state. The demo uses:

- Kernel Process and ResourceLease lifecycle;
- Verified Progress Graph validity and Goal closure;
- Multi-Agent Scheduler claims and attempts;
- versioned Artifact facts;
- exact-version Evidence;
- causal invalidation and Repair Frontier derivation;
- read-only status and graph projections.

The scenario is scripted for determinism, but semantic truth is not hardcoded.
Changing the execution provider does not grant it additional semantic
authority: the VPG remains the source of `READY`, `VERIFIED`, `STALE`, and Goal
closure.
