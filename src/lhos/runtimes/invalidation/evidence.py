"""D3 — Evidence applicability derivation (§4, §5, §29).

History immutability: Evidence nodes and their artifact bindings are NOT
touched.  `evidence_applicability_for_graph` READS the current ArtifactVersion
truth (via ArtifactFactProvider) + the stored EvidenceNode bindings and
DERIVES for each Evidence whether it still applies to the CURRENT artifact
version.

Seed A (§5): Task current-output version changed.
  If a Task's producing ArtifactRef binding points to version v_old and the
  current committed version of that artifact is v_new > v_old, the old
  Evidence (which verified v_old) no longer proves the CURRENT output.
  -> applicability == False.

Seed B (§5): backing artifact corrupted / missing.
  If the content hash no longer validates against ArtifactFactProvider
  (verify_binding False), the Evidence's backing artifact is invalid.

Seed C (§5): source Action / Event invalid.
  If the source Action (or Event) no longer passes the authoritative fact
  adapter, applicability is lost.

We never mutate the historical Evidence row.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from .models import (
    EvidenceApplicability,
    InvalidationCause,
    InvalidationCauseType,
)

# Provider callables (dependency-injected; never direct Kernel import)
VerifyBinding = Callable[[str, int, str], bool]  # (artifact_id, version, hash) -> bool
ActionValid = Callable[[str], bool]  # action_id -> bool
EventValid = Callable[[str], bool]  # event_id -> bool


def evidence_applicability_for_graph(
    graph_id: str,
    graph_version: int,
    evidence_nodes: dict[str, Any],  # evidence_id -> EvidenceNode
    *,
    verify_binding: VerifyBinding | None = None,
    action_valid: ActionValid | None = None,
    event_valid: EventValid | None = None,
    current_output_versions: dict[str, int] | None = None,
) -> tuple[EvidenceApplicability, ...]:
    """Derive current applicability for each Evidence in the graph.

    current_output_versions maps artifact_id -> currently-committed version.
    """
    verds: list[EvidenceApplicability] = []
    for eid in sorted(evidence_nodes.keys()):
        ev = evidence_nodes[eid]
        reason = ""
        applies = True
        cause_id: str | None = None

        bindings = ev.artifact_bindings
        for b in bindings:
            # Seed A: output superseded?
            if current_output_versions is not None:
                cur = current_output_versions.get(b.artifact_id)
                if cur is not None and cur > b.version:
                    applies = False
                    reason = f"artifact {b.artifact_id} version {b.version} superseded by {cur}"
                    cause_id = f"cause:{graph_id}:v{graph_version}:out:{b.artifact_id}"
                    break
            # Seed B: backing artifact integrity
            if verify_binding is not None:
                ok = verify_binding(b.artifact_id, b.version, b.content_hash)
                if not ok:
                    applies = False
                    reason = f"backing artifact {b.artifact_id}@{b.version} invalid"
                    cause_id = f"cause:{graph_id}:v{graph_version}:art:{b.artifact_id}"
                    break
        if (
            applies
            and ev.source_action_id
            and action_valid is not None
            and not action_valid(ev.source_action_id)
        ):
            # Seed C: source Action authoritative validity
            applies = False
            reason = f"source action {ev.source_action_id} no longer valid"
            cause_id = f"cause:{graph_id}:v{graph_version}:act:{ev.source_action_id}"
        if applies and ev.source_event_ids and event_valid is not None:
            for evid in ev.source_event_ids:
                if not event_valid(evid):
                    applies = False
                    reason = f"source event {evid} no longer valid"
                    cause_id = f"cause:{graph_id}:v{graph_version}:evt:{evid}"
                    break

        verds.append(
            EvidenceApplicability(
                graph_id=graph_id,
                graph_version=graph_version,
                evidence_id=eid,
                applies=applies,
                reason=reason,
                cause_id=cause_id,
                derived_at_version=graph_version,
            )
        )
    # Deterministic: already sorted by eid.
    return tuple(verds)


def causes_from_applicability(
    graph_id: str,
    graph_version: int,
    verds: tuple[EvidenceApplicability, ...],
) -> tuple[InvalidationCause, ...]:
    """Build InvalidationCause seeds from the set of Evidence that lost
    applicability (artifacts superseded / corrupted / action-event invalid).

    These are the AUTHORITATIVE seeds because they are derived from
    ArtifactVersion truth + fact adapters, not from Scheduler bookkeeping.
    """
    causes: list[InvalidationCause] = []
    for v in verds:
        if v.applies:
            continue
        ctype: InvalidationCauseType = "ARTIFACT_VERSION_SUPERSEDED"
        if v.cause_id and ":art:" in v.cause_id:
            ctype = "EVIDENCE_ARTIFACT_INVALID"
        elif v.cause_id and ":act:" in v.cause_id:
            ctype = "SOURCE_ACTION_INVALID"
        elif v.cause_id and ":evt:" in v.cause_id:
            ctype = "SOURCE_EVENT_INVALID"
        causes.append(
            InvalidationCause(
                cause_id=v.cause_id or f"cause:{graph_id}:v{graph_version}:{v.evidence_id}",
                graph_id=graph_id,
                graph_version=graph_version,
                cause_type=cast(InvalidationCauseType, ctype),
                evidence_id=v.evidence_id,
                reason=v.reason,
            )
        )
    # Associate with the evidence's source node id if available via the graph.
    causes.sort(key=lambda c: c.cause_id)
    return tuple(causes)
