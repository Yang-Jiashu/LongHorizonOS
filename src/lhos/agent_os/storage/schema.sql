"""SQLite schema for Agent OS Phase B."""

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
)"""

CREATE_JOURNAL_META = """
CREATE TABLE IF NOT EXISTS journal_meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
)"""

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
)"""

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
)"""

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
)"""

CREATE_RESOURCE_FENCES = """
CREATE TABLE IF NOT EXISTS resource_fencing_tokens (
    resource_id TEXT PRIMARY KEY,
    last_token INTEGER NOT NULL
)"""

CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals_projection (
    signal_id TEXT PRIMARY KEY,
    target_pid TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    source_pid TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0
)"""

CREATE_PROGRAM_STATES = """
CREATE TABLE IF NOT EXISTS program_states (
    pid TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""

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
)"""

CREATE_CAPABILITY_SETS = """
CREATE TABLE IF NOT EXISTS capability_sets (
    set_id TEXT PRIMARY KEY,
    pid TEXT NOT NULL UNIQUE,
    capabilities_json TEXT NOT NULL DEFAULT '[]'
)"""

CREATE_LEASE_WAITERS = """
CREATE TABLE IF NOT EXISTS lease_waiters (
    pid TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    wait_since TEXT NOT NULL,
    PRIMARY KEY (pid, resource_id)
)"""

# Transactional outbox. See ``services/outbox.py`` for the delivery contract.
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
)"""

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
    "CREATE INDEX IF NOT EXISTS idx_outbox_ready "
    "ON transactional_outbox(status, available_at, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_claim_expiry "
    "ON transactional_outbox(status, claim_expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_aggregate "
    "ON transactional_outbox(aggregate_type, aggregate_id)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_transaction "
    "ON transactional_outbox(transaction_id)",
]
