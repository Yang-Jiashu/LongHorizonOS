"""Runtime provenance primitives (v0.2 experimental API).

This package records observed dependencies and computes coverage.  It is
independent from the VPG/D3 semantic authority; callers must pass the
resulting evidence through the normal verifier/invalidation pipeline.
"""

from .coverage import assess_coverage, coverage_from_events
from .models import (
    CoverageReport,
    CoverageStatus,
    ProvenanceEvent,
    ProvenanceOp,
    ProvenanceOperation,
)
from .policy import (
    CoverageDecision,
    CoveragePolicy,
    ProvenanceCoverageError,
    enforce_coverage,
    evaluate_coverage,
)
from .recorder import (
    ActionGateway,
    EffectBoundaryError,
    EffectClass,
    EffectDeclaration,
    EffectReceipt,
    EffectRequest,
    EffectSubmissionError,
    ExecutionContext,
    ProvenanceRecorder,
    normalize_effect_class,
)
from .store import (
    GENESIS_HASH,
    InMemoryProvenanceStore,
    JSONLProvenanceStore,
    JsonlProvenanceStore,
    MemoryProvenanceStore,
    ProvenanceStore,
    ProvenanceStoreCorruption,
)

__all__ = [
    "GENESIS_HASH",
    "ActionGateway",
    "CoverageDecision",
    "CoveragePolicy",
    "CoverageReport",
    "CoverageStatus",
    "EffectBoundaryError",
    "EffectClass",
    "EffectDeclaration",
    "EffectReceipt",
    "EffectRequest",
    "EffectSubmissionError",
    "ExecutionContext",
    "InMemoryProvenanceStore",
    "JSONLProvenanceStore",
    "JsonlProvenanceStore",
    "MemoryProvenanceStore",
    "ProvenanceCoverageError",
    "ProvenanceEvent",
    "ProvenanceOp",
    "ProvenanceOperation",
    "ProvenanceRecorder",
    "ProvenanceStore",
    "ProvenanceStoreCorruption",
    "assess_coverage",
    "coverage_from_events",
    "enforce_coverage",
    "evaluate_coverage",
    "normalize_effect_class",
]
