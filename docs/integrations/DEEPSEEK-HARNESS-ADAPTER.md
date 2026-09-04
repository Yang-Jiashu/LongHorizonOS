# DeepSeek Harness Adapter

## Scope

`DeepSeekHarnessAdapter` is the runtime boundary between LongHorizonOS and
the current DeepSeek Harness CLI. It is responsible for process ownership,
credential isolation, durable session parsing, usage accounting, failure
classification, typed-tool provenance candidates, and attempt records.
Semantic truth remains in the LongHorizonOS verifier/VPG; a DSH `turn/end` or
process exit is never treated as `VERIFIED` by itself.

## Execution path

```text
AgentOS Scheduler/Claim/Attempt/ExecutionContext
        |
        v
DeepSeekHarnessAdapter.execute(context, task_id)
        |
        +-- direct Node DSH process (argv, no shell)
        +-- DSH_HOME/session JSONL
        +-- process-tree timeout/preempt cleanup
        +-- one shared parser -> HarnessPhaseEvent + AttemptRecord
        +-- independent verifier -> Evidence/VPG transition
```

The dynamic-coding static and LHOS arms, plus both host-native SWE arms, call
this adapter directly. The old Python worker remains only as a compatibility
CLI; it is no longer the primary benchmark execution path.

Node 22 or newer is required. The adapter checks Node and DSH versions before
launch and stores both versions plus the Cordis patch SHA-256 in each attempt.
It also parses the patch before launch and requires its provider, model,
reasoning setting, credential environment and base URL route to match the
declared adapter configuration.

## Binding and events

Every attempt carries an observational `HarnessExecutionBinding` for graph version,
semantic epoch, task, agent, claim, attempt, process, lease and context
manifest identity. `HarnessPhaseEvent.phase_seq` is strictly monotonic within
an attempt: sequence `0` is the adapter `STARTING` event and DSH observations
start at sequence `1`. Usage is committed once per logical `(session file,
turn, step)` call; replayed assistant messages, usage chunks and tool calls are
deduplicated. Provider calls separated by `llm/retry-started` receive distinct
retry generations, so failed and successful calls are both counted. A retry
without a usage payload still counts as a model call, while its unknown token
buckets remain zero rather than being invented.

Malformed/torn JSONL rows do not discard already committed usage. They mark the
trace as unknown and therefore fail closed for automatic provenance/rebase.
JSONL is streamed line by line instead of loaded as one hours-long file.

Before the child runs, the adapter atomically writes a fixed-length
`deepseek-harness-intent.v1` record with PID and exact binding. Completion
atomically writes the event stream and attempt record, then removes the intent.
An orphan-reconciliation daemon is not implemented yet; a surviving intent is
currently an operator/recovery diagnostic.

The binding is not a filesystem effect fence. DSH still operates in the
workspace directly, so a stale write cannot be rolled back by the adapter.
AgentOS prevents stale Evidence from committing, but production effect fencing
requires a future DSH bridge that routes every tool/effect through a gateway
carrying the lease fencing token.

## Credentials

Credentials are accepted from an in-memory `credential_values` tuple or a
named environment variable. The adapter removes the key pool and secret-like
parent variables (`*_TOKEN`, `*_SECRET`, `*_PASSWORD`, `*_API_KEY`, and
credential names), then injects only the selected provider key. Additional
tool credentials require an explicit `allowed_secret_env` allowlist. Only a
short SHA-256 fingerprint is persisted. Prompts, stdout/stderr, failure
messages and persisted tool arguments are redacted.

## Failure and cancellation

The adapter distinguishes authentication, content policy, rate limit, provider
5xx, network transient, invalid request, blocked/cancelled/max-token turns,
timeout, preempt, malformed protocol and non-zero exit failures. Successful
assistant stdout is never scanned for failure keywords. Retry policy is
bounded and exponential; `harness_max_attempts` is separate from Scheduler
`max_attempts`, and retry backoff observes semantic cancellation.

On Windows the child is assigned to a kill-on-close Job Object; on POSIX it
runs in a process group. Timeout covers both parent exit and output-pipe drain,
so a descendant cannot remain alive by inheriting stdout/stderr. Partial usage
from failure or preempt is attached to the worker outcome and enters the
AgentOS measured usage ledger before verification.

## Current boundary

DSH `--profile headless` is one-shot. This adapter does not claim native
`ctx.agents.resume()`, in-place semantic rebase, `session.flush()` checkpoint
semantics, or live `agent.cancel()` inside the DSH AgentLoop. Those require an
authenticated DSH bridge/daemon. Until then:

- a semantic repair creates a fresh fenced Attempt;
- a flush-like durable log is not advertised as a paused checkpoint;
- unknown shell/network/database effects remain `known=False`;
- typed read/edit tools are only candidate provenance until mapped to a
  versioned Artifact;
- operational completion still requires an independent verifier before Goal
  closure.
- DSH observations are parsed after the process ends; live phase streaming and
  native session control remain bridge work.
- the positional headless task is still visible in argv. Oversized commands
  fail before launch and require a bridge/task-file transport.

## Verified on August 21, 2026

- Fake-provider direct execution, retry, redaction, malformed JSONL, partial
  preempt, bounded output and descendant termination are covered by tests.
- AgentOS records successful, verifier-failed and execution-failed Harness
  usage in its measured ledger.
- A live local run used Node `v24.19.0` and DeepSeek Harness `0.1.0-rc.8`,
  created a real DSH `session-*`, emitted four phase events, preserved the
  exact Claim binding and correctly classified the provider response to an
  invalid placeholder credential. See
  `artifacts/deepseek-adapter-real-failure-smoke-node24-20260821/`.
- A real external StepFun run used `step-3.7-flash` with `medium` reasoning,
  completed three model calls and two tool calls, consumed 24,848 token units,
  produced the exact output artifact, passed the independent verifier, and
  closed the AgentOS Goal. See
  `artifacts/deepseek-adapter-stepfun37-success-smoke-20260821/`.
- Exact-key scanning across source, tests, scripts, docs and the success
  artifact reported zero matches; the temporary process environment was
  cleared after the run.

## Minimal integration

```python
config = DeepSeekHarnessConfig(
    node=node_path,
    dsh=dsh_path,
    patch=cordis_patch,
    provider="deepseek",
    model="deepseek-v4-flash",
    reasoning_effort="low",
    credential_env="DEEPSEEK_API_KEY",
    base_url="https://token.sensenova.cn/v1",
    base_url_env="DEEPSEEK_BASE_URL",
    credential_values=(selected_key,),
)
adapter = DeepSeekHarnessAdapter(
    config,
    workspace=workspace,
    run_root=run_root,
    phases=(DeepSeekHarnessPhase(
        phase_id="implement",
        prompt=prompt,
        inputs=("requirements/spec.json",),
        outputs=("src/package.py",),
    ),),
)
record = await adapter.execute(execution_context, "implement")
```

The returned record is profiling/operational evidence. The caller must still
run the task verifier and publish Evidence through the normal AgentOS/VPG
path.

## Real success smoke

Set the patch-declared credential environment variable in the parent process
through a local secret manager, then run:

```powershell
$env:PYTHONPATH = (Resolve-Path src).Path
python scripts/run_deepseek_adapter_smoke.py `
  --node C:\path\to\node.exe `
  --dsh C:\path\to\@deepseek-ai\dsh\lib\bin.js `
  --patch benchmarks\real_dsh_dynamic_coding\sensenova-pi-ai.cordis.patch.yml `
  --run-root artifacts\deepseek-adapter-real-success-smoke
```

The script has no `--api-key` option. It creates one exact file, runs an
independent verifier, requires the Goal to close, and writes
`smoke-summary.json` with versions, patch hash, usage, events and failure class.
