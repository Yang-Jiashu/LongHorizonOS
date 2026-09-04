# Harness Optimization Integration

LongHorizonOS can wrap a long-running Agent Harness without replacing the
Harness's execution loop. The Harness remains responsible for executing model
and tool calls; LongHorizonOS observes bounded phase state and controls:

- whether to resume the current cognitive session;
- when to compact and restart cognition while preserving the workspace;
- how many independent Harness jobs may run concurrently;
- which jobs must be isolated because of CPU, memory, verifier, or unknown
  resource requirements.

## 1. Generic semantic-context policy

The generic API is Harness-neutral:

```python
from lhos.integrations.harness import (
    HarnessContinuationAction,
    HarnessPhaseObservation,
    HarnessUsage,
    SemanticContextPolicy,
)

policy = SemanticContextPolicy(
    min_phases_before_restart=3,
    cache_tokens_per_call_threshold=24_000,
    cache_growth_ratio_threshold=1.75,
    cooldown_phases=2,
    max_restarts=2,
)

history: list[HarnessPhaseObservation] = []

current = HarnessPhaseObservation(
    phase_index=3,
    # Usage is the delta for this phase, not cumulative usage.
    usage=HarnessUsage(
        uncached_input_tokens=800,
        cache_read_tokens=48_000,
        output_tokens=500,
        model_calls=1,
        tool_calls=2,
    ),
    event_count=1200,
    read_set=("workspace://src/service.py",),
    write_set=("workspace://src/service.py",),
    verifier_passed=False,
    session_id="session-current",
    elapsed_ms=60_000,
)

decision = policy.decide(
    history,
    current,
    original_instruction="Repair the service and pass verification.",
)

if decision.action is HarnessContinuationAction.RESUME:
    harness.resume()
else:
    # Keep the workspace, replace the conversation/session, and pass only the
    # bounded semantic handoff returned by the policy.
    harness.restart_with_compacted_context(
        decision.bounded_handoff_items,
    )
```

The decision log contains hashes, counters, bounded scores, and artifact URI
counts. It does not serialize prompt text, tool payloads, or credentials.

## 2. DeepSeek Harness integration

The LHTB DeepSeek adapter enables semantic context control for the LHOS arm.
Its default policy is:

```text
minimum phases before restart:       3
cache-read tokens per model call:    24,000
cache growth ratio:                  1.75
restart cooldown:                    2 phases
maximum controlled restarts:         2
maximum semantic handoff items:      12
maximum semantic handoff characters: 2,048
```

At every verifier rejection the adapter:

1. parses the current durable DSH trace;
2. computes phase-delta usage and new read/write URIs;
3. asks `SemanticContextPolicy` whether to resume or restart;
4. either resumes the current DSH session or starts a new DSH generation;
5. preserves `/app`, so source files and generated artifacts survive a
   cognitive restart;
6. aggregates usage across all generations;
7. persists the decision in `dsh-semantic-control.json`.

The following evidence is recorded:

```text
dsh_resume_evidence_mode=adaptive_context_control
dsh_semantic_context_control=true
dsh_session_generation_count
dsh_controlled_restarts_cumulative
dsh_semantic_decisions_cumulative
```

`max-tokens` is treated as a recoverable checkpoint only when DSH persisted a
valid session, new events, and new model usage. A non-zero process with no
durable progress remains a failure.

GNU `timeout --kill-after` may return exit 137. It is accepted as a time-slice
checkpoint only when execution reached the slice deadline and durable model
progress was recorded. An early 137 remains an OOM/external-kill failure.

## 3. Secure provider credentials

Provider credentials are not placed in Docker command arguments.

The adapter:

1. creates a host mode-0600 temporary file;
2. uploads it to the container;
3. detects the actual container Agent UID/GID;
4. changes the file owner and keeps mode 0600;
5. reads and deletes it at command start;
6. removes both host and container temporary files on all exit paths.

Persisted observability contains only the transport name:

```text
credential_transport=ephemeral-mode-0600-file
```

## 4. Resource-aware Harness job scheduling

For a batch of independent Harness jobs:

```powershell
python scripts/run_lhtb_software5_pair.py prepare `
  --resource-aware-pairs `
  --max-concurrency 3 `
  --pair-capacity-cpus 12 `
  --pair-capacity-memory-mb 15360 `
  ...

python scripts/run_lhtb_software5_pair.py run `
  --resource-aware-pairs `
  --max-concurrency 3 `
  --pair-capacity-cpus 12 `
  --pair-capacity-memory-mb 15360 `
  ...
```

The scheduler uses deterministic first-fit waves and persists:

```text
pair-admission.json
```

For `environment_mode=separate`, the verifier CPU and memory are added to the
main environment peak. Unknown resource requirements fail closed into an
exclusive wave.

Legacy mode remains capped at two task pairs for compatibility.

## 5. Current boundary

This integration now actively controls cognitive session residency and outer
Harness job placement. It still does not automatically infer a complete
Task/Artifact/Evidence DAG from an arbitrary Harness transcript.

Critical-path scheduling, selective repair, semantic preemption, and model
routing require the Harness adapter to expose:

- semantic phases;
- artifact/version identities;
- verifier observations;
- typed read/write sets;
- resource/model candidates;
- pause, resume, restart, and preempt hooks.

DeepSeek Harness phase adapters already support explicit phase graphs. Other
Harness integrations should map their native lifecycle to the same bounded
observation and control contract.

