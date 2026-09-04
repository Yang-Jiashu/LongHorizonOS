# Explicit Workspace Watcher

`WorkspaceObservationWatcher` is the first bounded world-observation loop in
the SDK. It polls a caller-supplied list of `WorkspaceTool` resources,
compares SHA-256 hashes, and issues an authority-backed `ObservationToken` for
changed bytes.

```python
from lhos.integrations.tools.workspace import WorkspaceTool
from lhos.sdk import AgentOS

runtime = AgentOS("state.sqlite")
watcher = runtime.workspace_watcher(
    goal,
    WorkspaceTool("./workspace"),
    ("workspace://requirements.md", "workspace://src/api.py"),
    task_ids_by_resource={
        "workspace://requirements.md": ("plan", "implementation"),
        "workspace://src/api.py": ("tests",),
    },
)

watcher.initialize()
poll = watcher.poll_and_plan(epoch_id=1, persist=True)
```

To reconcile a changed declared input into the semantic graph in the same
polling loop (rather than only producing an interrupt proposal), use:

```python
poll = watcher.poll_and_reconcile(epoch_id=2, persist=True)
print(poll.repair_outcomes)
```

For each changed file this performs an authority-token transition check,
marks only explicitly declared consumers and their causal downstream tasks
`STALE`, reopens the Goal when needed, and reports the repair frontier. It
still does **not** claim work, transfer a Lease, force-stop a callback, or
automatically rebase a third-party Harness. A failed reconciliation must be
treated as an unacknowledged observation and retried.

The loop is intentionally conservative:

- only explicitly listed workspace files are observed;
- changed bytes receive a new monotonic, graph-bound observation token;
- a deletion produces an `ARTIFACT_CHANGED` interrupt without inventing a
  synthetic version;
- an unassigned resource produces an interrupt with no targets, which the
  interrupt policy keeps unhandled instead of broadening repair;
- the watcher does not claim work, release Leases, rebase Context, or
  force-stop a Harness.

It therefore closes the following bounded path:

```text
explicit workspace poll
    -> content hash/version observation
    -> ARTIFACT_CHANGED interrupt
    -> SemanticInterruptPolicy proposal
```

With `poll_and_reconcile`, the bounded path additionally includes:

```text
changed declared inputs in one poll
    -> validate every transition
    -> one atomic batched VPG/D3 invalidation + repair-frontier refresh
    -> interrupt proposal for live cooperative runtimes
```

If the batch fails, the watcher rolls back only the submitted resources to their
previous baseline and retries them on a later poll; explicitly watched but
unassigned resources remain interrupt-only observations and are not widened into
the repair batch.

The current watcher-only integration gate passes **18 focused tests**. The
overlapping observation-token/authority/watcher gate passes **35 tests**.
These counts are focused gates and are not additive. The final frozen-tree
full non-slow suite completed on August 14, 2026 with
`3037 passed, 1 skipped, 18 deselected, 30 warnings` in `296.70s`; the
reproducible log is `artifacts/full-test-nonslow-frozen-20260814.log`.
Repository-wide CI is not yet fully green because its format gate reports
52 files; correctness jobs exclude slow tests and a separate
`slow-benchmarks` job runs them.

It is **not** universal provenance discovery. Direct Python/file/network,
browser, tool, and subprocess access outside the watcher or mediated gateways
remains outside the observation set.
