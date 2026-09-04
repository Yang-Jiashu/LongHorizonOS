# Case Profile: SWE-bench Lite pytest-11143

## Case

```text
instance: pytest-dev__pytest-11143
base: 6995257cf470d2143ad1683824962de4071c0eb7
mode: host-native Windows smoke + Linux container validation
```

## What this case proves

This is a **single-task control case**. Both arms run the same real DeepSeek
Harness task and produce the same one-line source patch. LongHorizonOS has no
invalidation cone to compute, no independent branch to preserve, and no second
READY task to schedule.

It therefore profiles the execution path and evaluator correctness, not the
selective-repair advantage.

## Observed DSH execution

| Metric | DSH static | DSH + LHOS |
|---|---:|---:|
| Provider token units | 849,492 | 438,733 |
| Model calls | 29 | 20 |
| Tool calls | 28 | 20 |
| DSH wall-clock | 306.062 s | 235.641 s |

The final Linux container evaluator passed both patches:

```text
115 passed, 1 skipped
```

## Attribution

The token/time difference is **not an OS causal saving** in this single-task
case. It is explained by different model trajectories, retries, and repeated
editing/test loops. The structural OS difference is effectively zero:

```text
preserved tasks: 0
skipped tasks: 0
repair frontier: 1 task
parallel scheduling decision: none
```

This case is retained as a negative/control profile so the project does not
claim that LHOS accelerates every single Agent call.
