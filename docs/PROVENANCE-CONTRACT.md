# Provenance contract (observed-graph semantics)

**Status:** proposed normative contract for the next implementation phase.  
**It does not claim that the complete gateway is implemented in v0.1.**

## 1. Purpose

LongHorizonOS uses provenance to decide whether old Evidence still applies
after the world changes. This contract defines exactly when a “minimum repair”
statement is sound and what the runtime must do when an input was not observed.

The central rule is:

> **Minimum repair is guaranteed only relative to the dependency/provenance
> graph that the runtime actually observed and accepted.**

The runtime must never silently upgrade an incomplete graph into a complete one.

## 2. Terms

- **Artifact:** an addressable external or generated value identified by a
  canonical URI/ID, immutable version, and content hash.
- **Observation:** a signed/durable record that a gateway read, wrote, or
  otherwise checked an Artifact or external fact at an observation epoch.
- **Read-set:** all inputs consumed by one attempt, including URI/ID, version,
  hash, source metadata (for example ETag), and access mode.
- **Write-set:** all outputs or mutations produced by one attempt, with the
  resulting Artifact version/hash and effect classification.
- **Evidence:** immutable historical verifier output bound to exact Artifact
  versions and an operational Action.
- **Declared dependency:** a `DEPENDS_ON` edge supplied in the graph spec.
- **Observed dependency:** a dependency inferred from a mediated read/write or
  explicit tool result and recorded in the provenance index.
- **Coverage:** whether the runtime has evidence that all relevant reads were
  mediated and recorded for an attempt.
- **Semantic epoch:** the graph/observation generation against which an
  attempt was admitted.

## 3. Required provenance record

The next implementation should persist a canonical record equivalent to:

```json
{
  "attempt_id": "attempt-...",
  "claim_id": "claim-...",
  "semantic_epoch": 42,
  "reads": [
    {
      "uri": "vpg://workspace/config.json",
      "artifact_id": "workspace/config.json",
      "version": 7,
      "content_hash": "sha256:...",
      "source": "workspace-gateway",
      "observed_at": "2026-08-12T00:00:00Z"
    }
  ],
  "writes": [
    {
      "uri": "vpg://workspace/report.md",
      "artifact_id": "workspace/report.md",
      "version": 3,
      "content_hash": "sha256:...",
      "effect_class": "idempotent"
    }
  ],
  "declared_dependency_ids": ["task-a"],
  "inferred_dependency_ids": ["artifact:workspace/config.json"],
  "coverage": "COMPLETE",
  "provenance_digest": "sha256:..."
}
```

The exact wire schema may evolve, but field meaning and fail-closed behavior
must remain stable.

## 4. Coverage states

Every completed attempt must carry one of:

| Coverage | Meaning | Can it claim minimum repair? | Required behavior |
|---|---|---:|---|
| `COMPLETE` | All declared and relevant external reads/writes passed through mediated gateways; the read-set is durably sealed. | Yes, relative to the accepted observation set. | Persist digest and bind Evidence to it. |
| `PARTIAL` | Some accesses were mediated, but one or more declared channels were bypassed or could not be proven complete. | No. | Mark result non-minimal; either invalidate conservatively or require a fresh audit. |
| `UNKNOWN` | The runtime cannot establish what was read/written (raw callback, lost trace, untrusted source, or missing watcher). | No. | Fail closed: do not create `VERIFIED` Evidence that depends on completeness; require re-execution/audit. |

`COMPLETE` does not mean the runtime knows all facts in the universe. It means
the configured authority boundary was closed and the accepted channels were
observed.

## 5. Observation and version rules

1. Artifact versions are issued by the Artifact service or an authenticated
   observation adapter. A caller-provided integer is not, by itself, evidence
   of new bytes.
2. A version record must include a content hash. Reusing a version for different
   bytes is rejected.
3. Deletion, rollback, corruption, and watcher gaps are explicit observations,
   not silently treated as “no change”.
4. External HTTP/API facts must include source identity and a validator such as
   ETag, response hash, or snapshot token. A timestamp alone is insufficient.
5. An Evidence binding is exact-version: Evidence for `artifact@v1` does not
   prove `artifact@v2`.
6. If current truth cannot be read or verified, applicability is `UNKNOWN` and
   semantic closure must fail closed.

## 6. Dependency and invalidation rules

1. A declared `DEPENDS_ON` edge is a semantic assertion and is retained in the
   VPG.
2. A mediated read that is not represented by a declared edge creates an
   observed provenance dependency (or causes the task to be marked incomplete
   until an explicit edge is admitted).
3. When an observed input version/hash changes, Evidence bound to the old
   observation loses applicability.
4. Invalidation propagates only along accepted causal edges. Unrelated
   `VERIFIED` branches remain preserved.
5. The resulting frontier is the minimum **for the accepted graph and current
   facts**. If coverage is `PARTIAL`/`UNKNOWN`, the result must be labeled
   conservative/non-minimal, never advertised as globally minimal.
6. A verifier or model/configuration change is an input change when it can
   affect the verification predicate; it must be versioned or conservatively
   invalidate the affected Evidence.

## 7. Attempt, ownership, and Evidence binding

An Evidence commit is admissible only when all of the following match the
currently active execution:

```text
claim_id
attempt_id
semantic_epoch
lease generation/fencing token
provenance_digest
```

An old worker, a duplicate delivery, or an Evidence record from another
attempt must be rejected or recorded as non-authoritative. Scheduler claim
completion must be derived from the matching semantic attempt, not merely from
the Task's current `VERIFIED` bit.

## 8. Side effects

Reads and writes that can be observed outside the process must use a mediated
ToolGateway/ExecutionContext. Each effect is classified as:

- `pure` — no external mutation;
- `idempotent` — repeat is safe under a stable key;
- `compensatable` — repeat/rollback has a defined compensating action;
- `irreversible` — repeat may cause harm;
- `unknown` — safety is not established.

For `irreversible` and `unknown` effects:

- the sink must consume an idempotency/fencing token or the action remains
  `UNCERTAIN`;
- a crash after dispatch and before acknowledgement must not trigger blind
  retry;
- semantic `VERIFIED` requires reconciliation evidence, not an executor's
  return value alone.

The existing transactional outbox is an **at-least-once** delivery primitive;
it is not a universal exactly-once protocol for arbitrary external systems.

## 9. Public API transition

Until mediated provenance is available:

- the existing `Task.depends_on` and explicit artifact repair API remain
  supported as an experimental compatibility path;
- callers must treat the result as graph-relative;
- raw callbacks should be labeled `UNKNOWN` in secure/research mode, or the
  integration must document the trusted boundary explicitly;
- release reports must include provenance coverage and the list of observed
  channels.

The proposed secure path is:

```python
async def executor(ctx: ExecutionContext, task_id: str) -> AttemptOutput:
    source = await ctx.read_artifact("vpg://workspace/source.py")
    result = await ctx.write_artifact("vpg://workspace/report.md", content)
    return AttemptOutput(...)
```

No implementation should expose this example as available until the gateway,
capability checks, durable read/write records, and cancellation semantics are
in place.

## 10. Required test matrix

| Scenario | Expected result |
|---|---|
| Declared input changes | Exact causal cone is stale; unrelated verified work is preserved. |
| Hidden file/API read changes | No false `VERIFIED`; result is `UNKNOWN`/conservative repair. |
| Same version, different bytes | Artifact authority rejects the write. |
| Missing/deleted artifact | Evidence applicability is not `True`; closure fails closed. |
| Watcher/trace lost | Coverage becomes `PARTIAL` or `UNKNOWN`; no minimum claim. |
| Stale worker submits old Evidence | Commit rejected by claim/attempt/epoch/fence checks. |
| Crash after irreversible dispatch | Action becomes `UNCERTAIN`; no blind retry. |
| Verifier/configuration version changes | Affected Evidence is rechecked or invalidated. |

## 11. Compatibility and research claims

This contract does not claim automatic dependency discovery, world-model
completeness, belief revision, or a general planner. It defines a falsifiable
foundation on which those features may later be evaluated. A future paper or
release should report:

- provenance coverage distribution;
- under/over-invalidation under hidden-read injection;
- repair work versus full restart **and** oracle task-DAG checkpoint;
- false closure and stale-commit rates;
- side-effect uncertainty/reconciliation outcomes;
- hardware, provider, and workload details.

