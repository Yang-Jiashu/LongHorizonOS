"""D3 — version-aware causal invalidation + local repair.

LongHorizonOS Layer-4 semantic runtime that derives, from the VPG graph +
ArtifactVersion truth + operational fact truth, a deterministic minimal set
of semantic invalidation and the minimal Repair Frontier, WITHOUT mutating
Artifact/Evidence history and WITHOUT claiming or dispatching tasks.
"""

from .models import (
    D3_EVENT_TYPES,
    D3Event,
    EvidenceApplicability,
    InvalidationCause,
    InvalidationCone,
    InvalidationProof,
    InvalidationResult,
    RepairCandidate,
    RepairFrontier,
)
from .runtime import InvalidationRuntime, InvalidGraphVersionRace

__all__ = [
    "D3_EVENT_TYPES",
    "D3Event",
    "EvidenceApplicability",
    "InvalidGraphVersionRace",
    "InvalidationCause",
    "InvalidationCone",
    "InvalidationProof",
    "InvalidationResult",
    "InvalidationRuntime",
    "RepairCandidate",
    "RepairFrontier",
]
