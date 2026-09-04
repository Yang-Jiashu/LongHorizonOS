# LongHorizonOS Framework Improvement Design

Date: 2026-08-23

## 1. Diagnosis

The current system is not missing another isolated heuristic. Its main
problem is that the existing Harness, AgentOS, VPG, verifier, and checkpoint
components are not connected into one control loop:

```text
Harness phase
  -> observation
  -> OS state update
  -> action decision
  -> Harness action
  -> independent verifier
  -> VPG Evidence commit
```

The current LHTB path has a specialized version of this loop, but the public
DeepSeek adapter is still one-shot. Therefore the accurate claim is:

```text
DeepSeek rc.8 + Harbor/LHTB integration: active
arbitrary production Harness wrapper: not complete
Claude Code stateful adapter: not complete
```

## 2. Improvement Goals

The improved framework must satisfy four goals:

1. **Correctness first**: stale cognition and stale Evidence cannot close a
   Goal as VERIFIED.
2. **No-op safety**: one-shot or unsupported Harnesses must bypass LHOS and
   run through the native Harness path.
3. **Quality-constrained efficiency**: a token/time reduction counts as a win
   only when quality is non-inferior.
4. **Low online overhead**: action selection uses a fixed-size control block
   and does not rescan the full transcript or graph on every phase.

## 3. Control Architecture

```text
DeepSeek / Claude Code / other Harness
              |
        HarnessDriver
   vendor process/session protocol
              |
     ManagedHarnessSession
  observe / continue / checkpoint
              |
       HarnessObservationBridge
  phase cursor, usage, effects, verifier signal
              |
       HarnessSupervisor
  identity, leases, actions, handoff, retry
              |
     AgentOS + VPG + Scheduler
  resources, versions, Evidence, repair frontier
```

The Harness remains responsible for model calls, tools, and conversation
storage. LongHorizonOS is responsible for global state, resource admission,
version fences, action selection, and VERIFIED Evidence.

## 4. P0: Close the Runtime Loop

### 4.1 Canonical Execution Identity

There are currently two partial identity types:

- `HarnessExecutionBinding`
- `HarnessSessionIdentity`

They must be unified into one canonical fence used by every phase, control
request, checkpoint, and Evidence commit:

```python
HarnessExecutionIdentity(
    task_id,
    graph_id,
    graph_version,
    agent_id,
    claim_id,
    attempt_id,
    lease_id,
    fencing_token,
    process_id,
    workspace_id,
    workspace_epoch,
    context_manifest_hash,
    session_id,
    session_generation,
    phase_cursor,
)
```

Every operation must reject a stale process, lease, graph version, workspace
epoch, context manifest, or session cursor.

### 4.2 Harness Observation Bridge

Add a public bridge:

```python
AgentOS.ingest_harness_phase(
    event,
    verifier_progress=None,
    artifact_bindings=(),
)
```

The bridge must:

- validate the canonical identity;
- reject cursor rollback;
- deduplicate `(session_id, phase_seq, idempotency_key)`;
- monotonically accumulate cost and progress;
- update the Scheduler `AgentSnapshot`;
- expose phase state to `GlobalRuntimeState`;
- persist only bounded counters, URI hashes, and digests.

It must not persist prompt text, model output, tool arguments, or credentials.

### 4.3 Public DeepSeek Session Adapter

Keep the existing one-shot adapter as the compatibility path. Add an opt-in:

```python
DeepSeekSessionAdapter(HarnessSessionAdapter)
```

Capabilities:

```text
START
CONTINUE
CHECKPOINT
PREEMPT
RESTART_COMPACTED
```

The adapter owns DeepSeek/Cordis details. The Supervisor owns action ordering,
identity fencing, and generation replacement. A cross-session compact restart
must not be faked as a normal `CONTINUE`.

### 4.4 External Completion and Verification

Add a public completion boundary:

```python
AgentOS.complete_harness_attempt(
    claim_id,
    operational_result,
    phase_cursor,
    artifact_bindings=(),
)
```

The operation must:

1. ingest the final phase;
2. validate the exact Claim, Attempt, lease, graph, and workspace identity;
3. run the independent task verifier;
4. attach Evidence atomically;
5. mark VERIFIED only after verifier success.

The invariant is:

```text
Harness COMPLETED != VPG VERIFIED
```

## 5. P1: Verified-Progress Recovery Ladder

Use a default-off candidate state machine. The action set is:

```text
RESUME
COMPACT
RESTART
ROLLBACK
STOP_SUCCESS / STOP_FAIL
```

### Actions

| Action | Cognition | Workspace |
|---|---|---|
| RESUME | preserve exact session | preserve |
| COMPACT | new generation with bounded handoff | preserve |
| RESTART | fresh cognition, no old transcript | preserve |
| ROLLBACK | fresh cognition after restore | restore Cbest/C0 |
| STOP | terminate | freeze |

### State Machine

```text
RUNNING
  -> WAITING_VERIFIER
       -> verifier pass       -> STOP_SUCCESS
       -> healthy             -> RESUME
       -> context pressure    -> COMPACT
       -> repeated failure    -> RESTART
       -> quality regression  -> ROLLBACK
       -> fatal/budget error  -> STOP_FAIL
```

Recovery level is monotonic while no external progress is observed:

```text
0 RESUME
1 COMPACT
2 RESTART
3 ROLLBACK
4 STOP_FAIL
```

This prevents resume/compact oscillation.

## 6. O(1) Task Control Block

Do not scan the full observation history on the hot path. Maintain a fixed
size `TaskControlBlock`:

```python
TaskControlBlock(
    task_id,
    graph_version,
    session_id,
    session_generation,
    workspace_epoch,
    event_cursor,
    previous_event_delta,
    cache_total,
    cache_rate_ewma,
    max_token_streak,
    no_artifact_streak,
    completed_unverified_streak,
    failure_digest,
    failure_streak,
    verifier_passed,
    current_score,
    best_score,
    c0_checkpoint,
    best_checkpoint,
    recovery_level,
    cooldown,
    remaining_tokens,
    remaining_time,
    last_action,
    decision_seq,
)
```

Update and action selection are:

```text
TCB update: O(1)
action decision: O(1)
checkpoint lookup: O(1)
global ready heap: O(log N)
```

Only a bounded `deque(maxlen=3)` is retained for handoff generation. Full
history remains an append-only audit stream.

## 7. Quality-Constrained Efficiency

Existing `QualityConstrainedEfficiencyGuard` is the correct policy primitive,
but it must be connected at the verifier boundary.

The acceptance order is:

```text
1. quality relation
2. quality non-inferiority gate
3. efficiency gate
```

Define:

```text
measurement_eligible
quality_relation
quality_gate_passed
efficiency_eligible
```

Do not use `result_eligible` alone to claim an efficiency win.

The default behavior should be:

- one-shot: native bypass;
- unsupported session capability: native fallback;
- low-confidence estimate: native fallback;
- predicted quality regression: native fallback;
- observed quality regression: restore a compatible best checkpoint if
  available, otherwise restart/fail closed;
- only positive net token/time/tool estimate with quality non-inferiority may
  apply the optimization.

## 8. Checkpoint Semantics

A marker is not a restorable computation checkpoint. Add a descriptor:

```python
CheckpointDescriptor(
    checkpoint_id,
    scope,                 # marker/logical/workspace/executable
    task_id,
    graph_version,
    semantic_epoch,
    claim_id,
    attempt_id,
    context_hash,
    read_set_hash,
    workspace_manifest_hash,
    verifier_score,
    verified,
)
```

For the first safe implementation:

- `C0`: initial workspace snapshot;
- `Cbest`: best verified or quality-improving snapshot;
- snapshot scope: verifier-input workspace only;
- never snapshot credentials, DSH_HOME, verifier logs, or hidden tests;
- restore in an isolated clone;
- final reverify before promotion;
- any hash, lease, graph, or task mismatch fails closed.

Best-checkpoint selection must be a separate ablation from context reuse. Both
Fresh and LHOS must receive the same checkpoint infrastructure in a fair
comparison.

## 9. Algorithmic Controller

After P0 observability is available, replace fixed heuristics with an online
action score:

```text
Q(computation, action) =
    expected durable verified progress
    / (expected token + time + verifier cost)
    - stale risk
    - conflict risk
    - switch/restart cost
```

Hard constraints remain authoritative:

- resource capacity;
- conflict graph;
- lease/fencing;
- graph version;
- required context;
- unknown-I/O safety.

The controller must not choose a lower-cost action when its quality lower
bound violates the configured non-inferiority margin.

## 10. Implementation Order

### Phase 1: Safe, Default-Off

1. canonical identity wrapper;
2. fixed-size TCB;
3. observation bridge DTOs;
4. quality-gated profiling;
5. native one-shot bypass;
6. candidate recovery state machine;
7. unit and replay tests.

### Phase 2: DeepSeek Integration

1. public `DeepSeekSessionAdapter`;
2. START/CONTINUE/COMPACT supervisor;
3. phase ingestion into AgentOS;
4. external completion/verifier API;
5. two-phase PREEMPT/REBASE handoff.

### Phase 3: Checkpoint and Rollback

1. workspace manifest snapshot;
2. C0/Cbest catalog;
3. isolated restore and reverify;
4. score-aware rollback experiment.

### Phase 4: Paper Algorithm

1. VPG-Reclaim action utility;
2. critical-path/fan-out value;
3. resource/provider shadow prices;
4. offline oracle and synthetic trace evaluation;
5. repeated DeepSeek and Claude Code adapter experiments.

## 11. Acceptance Criteria

The framework improvement is accepted only when:

- one-shot tasks have no measurable LHOS controller overhead;
- stale phase/Evidence submission rate is zero;
- invalid identity/control requests fail closed;
- quality gate is non-inferior on the pre-registered cohort;
- token/time/tool gains are measured only after the quality gate;
- checkpoint restore passes isolated final reverify;
- R4 legacy path remains behaviorally unchanged when the candidate flag is
  disabled;
- at least one public DeepSeek session path and one second Harness adapter pass
  the same conformance tests before claiming generic Harness support.

## 12. Current Status

As of 2026-08-23:

- R4 natural 46-task run is still active and must not be mixed with candidate
  R5 results.
- Policy v3 is integrated and has passed focused and non-slow tests.
- The quality guard is implemented as an opt-in, default-off pure policy layer.
- Real workspace checkpoint/restore and public DeepSeek session attachment are
  still missing.

The next correct engineering target is not another threshold. It is closing:

```text
phase observation
 -> canonical identity
 -> action dispatcher
 -> verifier completion
 -> VPG Evidence
```

