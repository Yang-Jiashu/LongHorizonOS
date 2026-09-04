# Real DeepSeek Harness Dynamic Coding Pilot

This benchmark compares the same DeepSeek Harness task executor under two
controllers:

- `dsh_static_restart`: a fixed DAG that re-executes every task after a
  requirement mutation.
- `dsh_lhos`: the same DAG, prompts, model, tools, concurrency and verifier,
  with LongHorizonOS retaining valid Evidence and repairing only the invalidated
  semantic cone.

The model edits a real Python package and runs public `pytest` tests. A hidden
test suite outside the model workspace is the final correctness authority.
Provider-reported usage is read from DeepSeek Harness session events; it is not
estimated from task counts.

The pilot contains two independent branches:

```text
pricing_core -> pricing_api ---+
                               +-> integration
audit -------------------------+
```

The v2 mutation changes the pricing contract. The oracle affected set is
`pricing_core`, `pricing_api`, and `integration`; `audit` must remain valid.

This is a plumbing/pilot workload, not a paper-scale result. A publishable run
needs paired repetitions, alternating arm order, a stronger resume baseline,
and additional repositories.
