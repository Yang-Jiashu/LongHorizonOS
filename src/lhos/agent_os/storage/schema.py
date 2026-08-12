"""SQLite schema DDL as Python constants.

This module provides the DDL strings for the Agent OS Phase B schema.
The .sql file contains the same statements for reference.
"""

CREATE_JOURNAL_EVENTS = """
CREATE TABLE IF NOT EXISTS journal_events (
    event_id TEXT PRIMARY KEY,
    journal_offset INTEGER NOT NULL UNIQUE,
    pid TEXT NOT NULL,
    process_sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    causation_id TEXT,
    correlation_id TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

CREATE_JOURNAL_META = """
CREATE TABLE IF NOT EXISTS journal_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
)
"""

CREATE_PROCESSES = """
CREATE TABLE IF NOT EXISTS processes_projection (
    pid TEXT PRIMARY KEY,
    parent_pid TEXT,
    program_id TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL,
    effective_priority INTEGER NOT NULL,
    capability_set_id TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    resource_group_id TEXT NOT NULL,
    program_state_ref TEXT,
    pending_request_id TEXT,
    wait_condition_json TEXT,
    checkpoint_ref TEXT,
    exit_code TEXT,
    result_ref TEXT,
    event_cursor INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
)
"""

CREATE_ACTIONS = """
CREATE TABLE IF NOT EXISTS actions_projection (
    action_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL,
    device_type TEXT NOT NULL,
    operation TEXT NOT NULL,
    arguments_json TEXT NOT NULL,
    state TEXT NOT NULL,
    resource_claims_json TEXT NOT NULL DEFAULT '[]',
    lease_ids_json TEXT NOT NULL DEFAULT '[]',
    fencing_tokens_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT,
    side_effect_class TEXT NOT NULL DEFAULT 'pure',
    recovery_policy TEXT NOT NULL DEFAULT 'retry',
    timeout_seconds INTEGER,
    result_json TEXT,
    error_json TEXT,
    submitted_at TEXT NOT NULL,
    finished_at TEXT
)
"""

CREATE_LEASES = """
CREATE TABLE IF NOT EXISTS leases_projection (
    lease_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    owner_pid TEXT NOT NULL,
    mode TEXT NOT NULL,
    fencing_token INTEGER NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    renewable INTEGER NOT NULL DEFAULT 1,
    revocable INTEGER NOT NULL DEFAULT 1
)
"""

CREATE_RESOURCE_FENCES = """
CREATE TABLE IF NOT EXISTS resource_fencing_tokens (
    resource_id TEXT PRIMARY KEY,
    last_token INTEGER NOT NULL
)
"""

CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals_projection (
    signal_id TEXT PRIMARY KEY,
    target_pid TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    source_pid TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0
)
"""

CREATE_PROGRAM_STATES = """
CREATE TABLE IF NOT EXISTS program_states (
    pid TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

CREATE_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL,
    journal_offset INTEGER NOT NULL,
    process_sequence INTEGER NOT NULL,
    pcb_snapshot_json TEXT NOT NULL,
    program_state_ref TEXT NOT NULL,
    wait_condition_json TEXT,
    mailbox_cursor INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
)
"""

CREATE_CAPABILITY_SETS = """
CREATE TABLE IF NOT EXISTS capability_sets (
    set_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL UNIQUE,
    capabilities_json TEXT NOT NULL DEFAULT '[]'
)
"""

CREATE_LEASE_WAITERS = """
CREATE TABLE IF NOT EXISTS lease_waiters (
    pid TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    wait_since TEXT NOT NULL,
    PRIMARY KEY (pid, resource_id)
)
"""

# ── Phase C1: Artifact FS Tables ─────────────────────────────────────────────

CREATE_ARTIFACTS_PROJECTION = """
CREATE TABLE IF NOT EXISTS artifacts_projection (
    artifact_id TEXT PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    canonical_uri TEXT NOT NULL UNIQUE,
    current_version INTEGER NOT NULL DEFAULT 0,
    artifact_type TEXT NOT NULL DEFAULT 'file',
    created_by_pid TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    deleted INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}'
)
"""

CREATE_ARTIFACT_VERSIONS_PROJECTION = """
CREATE TABLE IF NOT EXISTS artifact_versions_projection (
    artifact_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    content_ref TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    parent_version INTEGER,
    committed_by_pid TEXT NOT NULL,
    committed_action_id TEXT NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY (artifact_id, version)
)
"""

CREATE_ARTIFACT_HANDLES_PROJECTION = """
CREATE TABLE IF NOT EXISTS artifact_handles_projection (
    handle_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    opened_version INTEGER,
    expected_version INTEGER,
    lease_id TEXT,
    transaction_id TEXT,
    opened_at TEXT NOT NULL,
    closed_at TEXT
)
"""

CREATE_WRITE_TRANSACTIONS_PROJECTION = """
CREATE TABLE IF NOT EXISTS write_transactions_projection (
    transaction_id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL,
    pid TEXT NOT NULL,
    expected_version INTEGER,
    staged_content_ref TEXT NOT NULL DEFAULT '',
    staged_content_hash TEXT NOT NULL DEFAULT '',
    staged_size_bytes INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'open',
    idempotency_key TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT
)
"""

CREATE_NAMESPACES_PROJECTION = """
CREATE TABLE IF NOT EXISTS namespaces_projection (
    namespace_id TEXT PRIMARY KEY,
    owner_pid TEXT NOT NULL,
    root_uri TEXT NOT NULL,
    quota_bytes INTEGER,
    max_open_handles INTEGER,
    created_at TEXT NOT NULL
)
"""

CREATE_MOUNTS_PROJECTION = """
CREATE TABLE IF NOT EXISTS mounts_projection (
    mount_id TEXT PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    mount_point TEXT NOT NULL,
    source_namespace_id TEXT NOT NULL,
    source_prefix TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'private',
    created_at TEXT NOT NULL
)
"""

CREATE_ARTIFACT_WATCHES_PROJECTION = """
CREATE TABLE IF NOT EXISTS artifact_watches_projection (
    watch_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL,
    namespace_id TEXT NOT NULL,
    uri_prefix TEXT NOT NULL,
    created_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
)
"""

CREATE_IDEMPOTENCY_INDEX = """
CREATE TABLE IF NOT EXISTS artifact_idempotency (
    idempotency_key TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    pid TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    result_state TEXT NOT NULL,
    result_version INTEGER,
    created_at TEXT NOT NULL,
    PRIMARY KEY (idempotency_key, artifact_id, pid)
)
"""

CREATE_SNAPSHOTS_PROJECTION = """
CREATE TABLE IF NOT EXISTS namespace_snapshots_projection (
    snapshot_id TEXT PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    artifact_versions_json TEXT NOT NULL DEFAULT '{}',
    content_refs_json TEXT NOT NULL DEFAULT '{}',
    created_by_pid TEXT NOT NULL,
    created_at TEXT NOT NULL
)
"""

# Transactional outbox for durable internal-state -> external-delivery intents.
#
# The outbox is deliberately independent from any particular broker or driver.
# A row becomes visible to a dispatcher only after the SQLite transaction that
# created it commits.  Delivery is therefore at-least-once: a process crash
# between an external call and ``ack`` leaves the row claimable again.
CREATE_TRANSACTIONAL_OUTBOX = """
CREATE TABLE IF NOT EXISTS transactional_outbox (
    outbox_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    destination TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    headers_json TEXT NOT NULL DEFAULT '{}',
    aggregate_type TEXT,
    aggregate_id TEXT,
    causation_id TEXT,
    correlation_id TEXT,
    transaction_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_flight', 'delivered', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TEXT NOT NULL,
    claimed_by TEXT,
    claim_token TEXT,
    claimed_at TEXT,
    claim_expires_at TEXT,
    last_error_json TEXT,
    delivery_result_json TEXT,
    created_at TEXT NOT NULL,
    delivered_at TEXT
)
"""

ALL_DDL = [
    CREATE_JOURNAL_EVENTS,
    CREATE_JOURNAL_META,
    CREATE_PROCESSES,
    CREATE_ACTIONS,
    CREATE_LEASES,
    CREATE_RESOURCE_FENCES,
    CREATE_SIGNALS,
    CREATE_PROGRAM_STATES,
    CREATE_CHECKPOINTS,
    CREATE_CAPABILITY_SETS,
    CREATE_LEASE_WAITERS,
    # Phase C1: Artifact FS
    CREATE_ARTIFACTS_PROJECTION,
    CREATE_ARTIFACT_VERSIONS_PROJECTION,
    CREATE_ARTIFACT_HANDLES_PROJECTION,
    CREATE_WRITE_TRANSACTIONS_PROJECTION,
    CREATE_NAMESPACES_PROJECTION,
    CREATE_MOUNTS_PROJECTION,
    CREATE_ARTIFACT_WATCHES_PROJECTION,
    CREATE_IDEMPOTENCY_INDEX,
    CREATE_SNAPSHOTS_PROJECTION,
    CREATE_TRANSACTIONAL_OUTBOX,
]

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_journal_offset ON journal_events(journal_offset)",
    "CREATE INDEX IF NOT EXISTS idx_journal_pid ON journal_events(pid, process_sequence)",
    "CREATE INDEX IF NOT EXISTS idx_processes_state ON processes_projection(state)",
    "CREATE INDEX IF NOT EXISTS idx_actions_pid ON actions_projection(pid)",
    "CREATE INDEX IF NOT EXISTS idx_actions_state ON actions_projection(state)",
    "CREATE INDEX IF NOT EXISTS idx_leases_owner ON leases_projection(owner_pid)",
    "CREATE INDEX IF NOT EXISTS idx_leases_resource ON leases_projection(resource_id)",
    "CREATE INDEX IF NOT EXISTS idx_signals_target ON signals_projection(target_pid, consumed)",
    # Phase C1: Artifact FS indexes
    "CREATE INDEX IF NOT EXISTS idx_artifacts_ns ON artifacts_projection(namespace_id)",
    "CREATE INDEX IF NOT EXISTS idx_artifacts_uri ON artifacts_projection(canonical_uri)",
    "CREATE INDEX IF NOT EXISTS idx_versions_artifact ON artifact_versions_projection(artifact_id)",
    "CREATE INDEX IF NOT EXISTS idx_handles_pid ON artifact_handles_projection(pid)",
    "CREATE INDEX IF NOT EXISTS idx_handles_artifact ON artifact_handles_projection(artifact_id)",
    "CREATE INDEX IF NOT EXISTS idx_txns_artifact ON write_transactions_projection(artifact_id)",
    "CREATE INDEX IF NOT EXISTS idx_txns_pid ON write_transactions_projection(pid)",
    "CREATE INDEX IF NOT EXISTS idx_txns_idem ON write_transactions_projection(idempotency_key)",
    "CREATE INDEX IF NOT EXISTS idx_mounts_ns ON mounts_projection(namespace_id)",
    "CREATE INDEX IF NOT EXISTS idx_watches_pid ON artifact_watches_projection(pid)",
    "CREATE INDEX IF NOT EXISTS idx_watches_ns ON artifact_watches_projection(namespace_id)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_ready "
    "ON transactional_outbox(status, available_at, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_claim_expiry "
    "ON transactional_outbox(status, claim_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_aggregate "
    "ON transactional_outbox(aggregate_type, aggregate_id)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_transaction ON transactional_outbox(transaction_id)",
]
