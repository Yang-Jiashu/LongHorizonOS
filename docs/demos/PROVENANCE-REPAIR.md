# Provenance Coverage and Selective Repair Demo

This deterministic, offline demo shows the v0.2 provenance boundary:

```text
record versioned inputs
    -> compare declared vs observed provenance
    -> fail closed on an unknown input under strict policy
    -> filing.csv@v1 -> filing.csv@v2
    -> propagate invalidation through the declared graph
    -> preserve an unrelated VERIFIED branch
    -> repair the affected cone
    -> replay a durable hash-chained JSONL journal
```

## Run

```bash
pip install .
lhos demo provenance-repair --json
```

The command does not require an API key. The JSON result is suitable for CI
smoke tests:

```json
{
  "hidden_probe_coverage": "UNKNOWN",
  "strict_fail_closed": true,
  "affected_tasks": ["ComputeValuation", "FetchReport", "WriteConclusion"],
  "preserved_tasks": ["IndependentResearch"],
  "repair_frontier": ["FetchReport"],
  "durable_replay": true
}
```

## What this proves

* Explicitly declared inputs can be compared with observed versioned reads.
* `STRICT` provenance policy denies an executor that reports an unknown read,
  instead of silently treating the result as complete.
* A changed input invalidates a **graph-relative** downstream cone and leaves
  an unrelated branch untouched.
* The provenance trace is append-only, hash chained, and replayable after
  closing and reopening the JSONL store.

## What this does not prove

The demo intentionally reports `automatic_dependency_discovery = false`.
The recorder is explicit at tool/executor boundaries; it cannot magically
observe arbitrary Python, browser, or operating-system reads. The dependency
graph is also supplied by the scenario. Consequently this is a provenance
coverage and safe-repair primitive, not a claim of complete open-world
dependency discovery or semantic equivalence.
