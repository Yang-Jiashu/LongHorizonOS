# LHTB Software Five-Case DSH Pair

This quick local pilot compares two DeepSeek Harness execution policies on five
Long-Horizon Terminal-Bench software-engineering tasks:

```text
DSH fresh-session attempts
DSH exact-session continuation controlled by LongHorizonOS
```

Both arms use the custom Harbor agent:

```text
scripts.lhtb_dsh_harbor_agent:LHTBDeepSeekHarnessAgent
```

They do not use Harbor Terminus-2.

## Selected Cases

| Priority | Task | Official agent budget | First-batch reason |
|---:|---|---:|---|
| 1 | `great-expectations-audit` | 3600s | Small Python data pipeline |
| 2 | `alp-paper-reproduction` | 3600s | Small Python implementation |
| 3 | `foldseek-paper-reproduction` | 3600s | Pure Python, moderate fixtures |
| 4 | `unison-paper-reproduction` | 5400s | Very small Docker context |
| 5 | `langchain-version-migration` | 5400s | Representative migration and warm image cache |

`commit0-multilib-tdd` and `riscv-core-debug` are deferred because their
official agent/build budgets are much larger.

## Environment

```text
tasks: D:\LHTB-local-pilot\tasks
runtime: D:\LHTB-dsh-runtime
Node: 24.19.0 Linux x64
DSH: 0.1.0-rc.8
model: step-3.7-flash
reasoning: medium
quick-pilot agent budget: 300s per arm
maximum task-pair concurrency: 2
```

The task tree is a generated local-pilot copy. Task payloads are official; the
copy changes only declared network flags where container-local model access
requires it. This is therefore:

```text
official task payload + local-pilot environment
official leaderboard score = false
```

## Arms

### DSH Fresh

After an interim verifier rejection, Harbor invokes the agent again. The
baseline agent gives every invocation a new `DSH_HOME`, producing an independent
session and replaying context from scratch.

The current custom adapter applies its generated create runner in both arms, so
this is accurately named `fresh-session attempts`, not an untouched stock
headless implementation.

### LHOS Resume

Harbor runs with:

```text
HB_CONTINUE_MODE=same_conversation
```

The custom agent keeps one durable DSH home and calls `ctx.agents.resume()` with
the locked session ID after verifier rejection.

The result is labeled `verified_context_reuse` only when all resume-gate checks
pass:

- at least two DSH invocations;
- at least one completed resume invocation;
- `resume_count >= 1`;
- the same single session JSONL remains authoritative;
- the session ID is valid;
- durable event count is nonzero;
- the adapter reports `ctx.agents.resume`.

Otherwise the result is labeled `direct_compatibility`.

## Fairness

For each task, both arms have the same:

- local-pilot task directory;
- prebuilt Docker image ID;
- Linux Node and DSH runtime;
- StepFun Cordis patch;
- model and reasoning level;
- 300-second agent timeout;
- hidden verifier;
- one Harbor attempt and one trial.

The only intended configuration difference is the agent arm:

```text
baseline -> arm=baseline, no HB_CONTINUE_MODE
LHOS     -> arm=lhos, HB_CONTINUE_MODE=same_conversation
```

Task pairs alternate AB/BA order. At most two task pairs run concurrently.

## Commands

Prepare configs and validate imports without reading a model credential:

```powershell
python scripts/run_lhtb_software5_pair.py prepare `
  --lhtb-root C:\Users\yangjiashu\Temp\LHTB `
  --tasks-root D:\LHTB-local-pilot\tasks `
  --runtime-root D:\LHTB-dsh-runtime `
  --output artifacts\lhtb-dsh-software5-quick-20260821 `
  --agent-timeout-seconds 300 `
  --worker-timeout-seconds 1800 `
  --max-concurrency 2
```

Build the five images once before either timed arm:

```powershell
python scripts/run_lhtb_software5_pair.py prebuild `
  --output artifacts\lhtb-dsh-software5-quick-20260821
```

After a separate resume smoke passes, run the pair:

```powershell
python scripts/run_lhtb_software5_pair.py run `
  --output artifacts\lhtb-dsh-software5-quick-20260821 `
  --jobs-dir D:\LHTB-dsh-software5-jobs `
  --credential-env STEPFUN_API_KEY `
  --max-concurrency 2
```

Regenerate summaries without model calls:

```powershell
python scripts/run_lhtb_software5_pair.py summarize `
  --output artifacts\lhtb-dsh-software5-quick-20260821
```

## Outputs

```text
manifest.json
prebuild.json
configs/
runs/<task>/dsh_fresh.json
runs/<task>/lhos_resume.json
worker-status/
logs/
result.json
RESULTS.zh-CN.md
secret-scan.json
```

## Interpretation

Passing the resume gate proves that the second computation continued the exact
persisted DSH session instead of starting a new conversation. It does not by
itself prove a statistically significant speedup. Token, model-call, tool-call,
and time comparisons still require repeated paired AB/BA runs.

If the resume gate does not pass, no context-reuse or OS-acceleration claim is
made for that task.
