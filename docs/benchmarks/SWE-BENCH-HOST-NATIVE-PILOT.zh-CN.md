# SWE-bench Lite Host-Native Pilot

## Scope

This is a **host-native Windows proxy**, not the official SWE-bench score.
The official SWE-bench evaluator uses a Docker-based environment; this pilot
avoids Docker and runs the public repository checkout, DSH, and selected public
tests directly in a temporary Python environment.

Instance:

```text
pytest-dev__pytest-11143
base_commit=6995257cf470d2143ad1683824962de4071c0eb7
```

The task fixes pytest assertion rewriting when the first expression in a Python
file is a non-string constant such as `0`.

## Arms

Both arms use the same DeepSeek Harness headless executor, SenseNova
`deepseek-v4-flash`, the same prompt, tools, workspace snapshot, and external
test patch:

```text
DSH static
  -> one DSH task
  -> external SWE test patch
  -> target + selected PASS_TO_PASS tests

DSH + LHOS
  -> Task / Attempt / Claim / Kernel Lease
  -> same DSH task executor
  -> independent verifier
  -> VERIFIED Goal
```

This is a mechanism/authority smoke, not a multi-task selective-repair
experiment. A single SWE issue cannot demonstrate invalidation-cone savings.

## Result

Artifact:

```text
artifacts/swebench-host-native-pytest11143-20260820/result.json
```

| Metric | DSH static | DSH + LHOS |
|---|---:|---:|
| Provider token units | 849,492 | 438,733 |
| Model calls | 29 | 20 |
| Wall-clock | 307.594 s | 238.000 s |
| Target + selected regression tests | 2 passed | 2 passed |
| LHOS Goal | n/a | `closed` |

Relative to this static controller:

```text
token ratio = 0.516465
wall-clock speedup = 1.292412x
```

The token difference is an observation from one model run, not a statistically
powered estimate. It includes provider-reported uncached input, cache-read,
cache-write and output buckets.

## Platform limitation

The full public `PASS_TO_PASS` set for this old pytest instance includes a
Windows-specific `sys.pycache_prefix` test that fails because of host-platform
path semantics. Therefore this artifact is reported as:

```text
SWE-bench Lite host-native smoke/proxy
```

It must not be reported as an official SWE-bench resolved instance or compared
with the official leaderboard.

## Additional Flask attempt

`pallets__flask-4992` was also attempted without Docker. Both DSH arms produced
an implementation using `mode="rb"` while the public contract test requires
`text=False`; both arms therefore failed the external evaluator. This is a
valid failure observation and is not counted as a speedup.

## Next benchmark stage

For a stronger public result:

1. Use strict host-native cases already verified on Windows:
   Flask-4992, Django-17087, and Sphinx-11445.
2. Lock each case's Python version, dependency versions, base SHA, test-patch
   SHA, and evaluator command.
3. Run at least 5 paired repetitions per case before drawing a direction.
4. Build a multi-issue episode from two or more related public tasks in the same
   repository; only that extension can measure preserved Evidence, invalidation,
   repair frontier, and selective re-execution.
5. Re-run the final matrix with the official Docker/remote evaluator when a
   canonical SWE-bench score is required.
