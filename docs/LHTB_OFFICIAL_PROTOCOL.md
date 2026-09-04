# LHTB leaderboard alignment boundaries

The public LHTB leaderboard settings are separate from the timeout declared by
an individual `task.toml`. At the pinned official checkout
`84d7ba5ee34fae6c11f0d7cb8ed5faa73a9ece54`, the leaderboard YAMLs share:

- the Terminus-2 reference agent;
- all 46 tasks, one trial per task;
- `override_timeout_sec: 5400` and `timeout_multiplier: 1.0` for every task;
- Docker with `delete: true` (`force_build` is model-specific);
- JSON parsing/summarisation (`parser_name: json`, `enable_summarize: true`,
  `proactive_summarization_threshold: 8000`,
  `record_terminal_session: true`);
- mean reward over 46 tasks, with execution/parser errors scored as zero;
  solved means reward `>= 0.95`.

Those static YAML fields are not the whole execution protocol. LHTB ships a
**modified Harbor**, not stock upstream Harbor. Thirty of the 46 task files set
`continue_until_timeout = true`; the current hardened protocol repeats the
agent/interim-verifier cycle until reward `>= 1.0` or the budget expires, gives
only binary rejection feedback, freezes the agent during interim verification,
and hides verifier artifacts before resuming. Stock Harbor ignores the task
flag and therefore cannot reproduce this behavior.

The official repository also distinguishes two result generations. Its July
2026 leaderboard snapshot predates the binary-feedback default and verifier
isolation; new hardened runs must be reported separately rather than merged
with that snapshot. Consequently, "matches the leaderboard YAML fields" and
"reproduces a published leaderboard score" are different claims.

## What the pair-runner mode checks

The pair runner exposes the shared static settings as an opt-in preparation
mode (`--official-protocol` and `--official-contract` are aliases):

```powershell
$env:DOCKER_DEFAULT_PLATFORM = 'linux/amd64'
python scripts/run_lhtb_software5_pair.py prepare `
  --lhtb-root C:\Users\yangjiashu\Temp\LHTB `
  --tasks-root D:\LHTB-local-pilot\tasks `
  --official-leaderboard-contract `
  --official-model-yaml configs\leaderboard\deepseek-v4-pro.yaml `
  --output D:\LHTB-results\official-contract-prepared
```

The command fails closed for a task subset, local-image filtering, task-
declared timeouts (`--task-declared-timeouts`, with the old
`--official-timeouts` spelling retained as an alias), `--time-to-verified`, or
an artificial controller slice. It checks the pinned source commit and, when
`DOCKER_DEFAULT_PLATFORM` is set, requires `linux/amd64`. The generated
`manifest.json` records the alignment scope and a `protocol_deviations` list.

For the LHOS question itself, the pair is a deterministic, within-task paired
contrast under the fixed custom DSH/Harbor harness. Both arms use the same
custom DSH agent, Harbor `same_conversation`, binary verifier feedback, and
the same declared task/model/image/config inputs. `dsh_fresh` reconstructs the
original instruction in a new DSH session after rejection; `lhos_resume` keeps
the persisted DSH conversation and applies the LHOS semantic-context policy.
The declared estimand is the outcome contrast of this full LHOS policy bundle,
not a pure context-reuse effect and not a Terminus-2 leaderboard effect. The
manifest records this as `controlled_pair_experiment`.

The controlled claim is intentionally **partial**. The outer launcher differs
(direct worker for the control arm versus AgentOS plus worker for LHOS), the
continuation driver is itself part of the treatment, pinned Docker verifier
artifact isolation is not verified, and priority-parity arm order is balanced
but not randomized. One-shot tasks have no continuation and are context-
mechanism NA. Arm-only materialization and merge are recorded as
`separate_arm_batches`; they must not be described as same-batch interleaving
or as a randomized causal estimate. Report task-level paired ITT/outcome
counts separately from the post-treatment resume-mechanism cohort.

### Budget comparison

The published leaderboard config uses one uniform budget: `5400s` per task.
The task files also declare several reference budgets (`3600`, `5400`, `10800`,
`14400`, `18000`, `21600`, and `28800` seconds). A fixed `900s` pilot is a
short-budget stress condition, not an official timeout tier.

For a clean budget curve on one fixed long-horizon cohort, prepare conditions
with the sweep helper. Preparation does not call the model provider:

```powershell
python scripts/prepare_lhtb_budget_sweep.py `
  --lhtb-root C:\Users\yangjiashu\Temp\LHTB `
  --tasks-root D:\LHTB-local-pilot\tasks `
  --runtime-root D:\LHTB-dsh-runtime `
  --task-names alp-paper-reproduction climate-netcdf-extreme-event-audit `
    materials-phase-diagram-audit matpower-opf-regression `
    microscopy-cell-count-qc-audit unison-paper-reproduction `
  --budgets 600 900 1800 3600 5400 `
  --output-root D:\LHTB-results\lhos-budget-sweep
```

Each condition has `time_slice_seconds=null`, the same task order and paired
YAML parity, and a fixed global `override_timeout_sec` for both arms. Run each
prepared directory with the ordinary `run` command, then combine completed
conditions:

```powershell
python scripts/summarize_lhtb_budget_sweep.py `
  --sweep-root D:\LHTB-results\lhos-budget-sweep
```

The primary table is task-level paired ITT (`LHOS - fresh`) with missing or
invalid rewards imputed to zero. It reports fresh/LHOS mean reward, resolved
rate, pairwise wins and ties for each budget. One-shot tasks remain in this
outcome denominator but are mechanism-NA; resume-gate results are a secondary
post-treatment cohort. For task-declared reference strata, use
`run_lhtb_software5_pair.py prepare --task-declared-timeouts --no-time-slice`
and read `budget_tier_profiling` in its result. Neither output is an official
leaderboard score.

`--official-model-yaml` is optional and is a reference audit, not an execution
override. It accepts only a `configs/leaderboard/*.yaml` file inside the pinned
LHTB checkout, rejects the file if its content differs from the Git `HEAD`
blob, and validates/records the sanitized reference's `agent.name`,
`model_name`, `n_concurrent_trials`, `force_build`, dataset path, ordered 46
task names, `record_terminal_session`, and shared timeout/parser settings. The
manifest stores only those non-secret fields plus the Git blob digest. Supplying
a reference YAML does not
change the custom DSH agent or claim that its inference settings match that
model.

The complete LHTB worktree dirty state is also captured at preparation time as
`source_worktree_dirty`, `source_worktree_dirty_paths`, and structured
`source_worktree` entries. A dirty reference model YAML is rejected outright;
other dirty paths are disclosed as protocol deviations. In particular, the
runner's deliberate `docker-compose-prebuilt.yaml` `pull_policy: never` patch
is recorded as a known source mutation rather than hidden.

This is only a **shared-setting and post-hoc metric projection**. It does not
validate parity with the official modified-Harbor continuation loop, verifier
isolation, or a model-specific leaderboard YAML.

## Why its scores are not leaderboard-comparable

The runner intentionally uses
`scripts.lhtb_dsh_harbor_agent:LHTBDeepSeekHarnessAgent` instead of Terminus-2,
uses one dataset config per task/arm, and normally uses a Windows local-pilot
task tree with `allow_internet` changes for provider access. Its parser and
summarizer keys are recorded for comparison but are not executed with stock
Terminus-2 semantics. Such runs always retain:

```text
official_score: false
leaderboard_comparable: false
official_protocol_complete: false
```

The paired arms are intentionally routed through the same Harbor
`same_conversation` path so verifier feedback semantics are not an arm
variable. This controls the comparison within the custom harness; it does not
turn the custom DSH adapter into the official Terminus-2 harness, and it does
not repair limitations of the pinned Harbor checkout's isolation behavior.

For a genuinely comparable new hardened run, use the official `tasks/`
payload, the LHTB-bundled modified Harbor (or the repository's validated 0.20.x
patch), the Terminus-2 agent, binary verifier feedback with verifier isolation,
the exact model-specific leaderboard YAML, provider credentials/networking
that do not modify task policy, and an amd64 Docker environment. Do not use
stock upstream Harbor, and do not report a local-pilot or custom-harness score
as an official benchmark score.

The task-declared timeout mode remains available for mechanism and budget
experiments. It is labelled `agent_timeout_mode: task_declared` and is
off-leaderboard.
