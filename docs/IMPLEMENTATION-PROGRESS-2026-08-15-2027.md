# Implementation Progress - August 15, 2026 20:27 CST

## Completed since the last checkpoint

- Captured reproducible evidence artifacts:
  - `artifacts/online-supervisor-20260815-2018.json`
  - `artifacts/online-compute-20260815-2018.json`
- Confirmed the current SDK supervisor demo reaches a closed Goal through the
  real SDK path (`uses_real_sdk=true`, `daemon_started=false`, `uses_llm=false`).
- Confirmed the controlled adaptive benchmark reaches the same four-task
  verified set with `2760 -> 1440` simulated tokens and `1320 -> 0`
  stale/repeated-work tokens.
- Finished documentation synchronization. Current full-regression evidence is
  consistently reported as `3237 passed, 1 skipped, 18 deselected, 30 warnings
  in 532.89s`; earlier counts are labeled historical.

## Active implementation

### Automatic stale-cognition repair

The active branch is implementing a bounded main-path fresh-attempt flow:

```text
commit-time read-set check
  -> STALE_COGNITION quarantine
  -> exact old Claim/Lease release
  -> normal Scheduler admission
  -> fresh Attempt + ContextSnapshot
  -> verifier and fenced Evidence commit
```

The flow must fail closed on incomplete provenance, graph races, Context
authority failure, or replacement-admission failure. Existing manual live
Harness `REBASE/FULL_RELOAD` refusal remains unchanged.

### Harness-path benchmark

The active branch is adding a benchmark that uses the actual AgentOS
Scheduler/Claim/Attempt/Lease/executor/verifier/VPG path and registers an exact
Harness session per attempt. Static and adaptive policies share the same
deterministic provider/workload; the report will separate measured local
wall-clock and ownership evidence from synthetic token/cost accounting.

## Remaining after this checkpoint

- Review both branches and resolve API/test conflicts.
- Run focused tests, Ruff, Mypy, compileall, package-install smoke, and the two
  JSON CLIs.
- Run the full non-slow regression once source/tests stabilize.
- Keep open claims explicit: no daemon, hidden-provenance universal discovery,
  physical GPU scheduling, distributed consensus, killable arbitrary callbacks,
  or universal exactly-once irreversible effects.

## Timing

- Branch review and focused gates: next 30-60 minutes.
- Full regression and packaging evidence: following 1-2 hours.
- Initial two-day result: bounded online control, fresh-attempt stale repair,
  Harness-path benchmark, reproducible commands, and honest research-alpha
  boundary.
