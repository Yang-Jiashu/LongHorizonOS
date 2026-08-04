-- LongHorizonOS SQLite schema (spec section 19). WAL mode is enabled by the
-- connection layer. Statements are intentionally verbatim from the spec.

CREATE TABLE runs(
    id TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE events(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    payload_json TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL,
    causation_id TEXT,
    correlation_id TEXT,
    idempotency_key TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, sequence),
    UNIQUE(run_id, idempotency_key)
);

CREATE TABLE nodes(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    title TEXT NOT NULL,
    specification TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL,
    schedulable INTEGER NOT NULL,
    priority REAL NOT NULL,
    progress_weight REAL NOT NULL,
    estimated_token_cost INTEGER,
    estimated_time_ms INTEGER,
    estimated_tool_calls INTEGER,
    actual_token_cost INTEGER NOT NULL,
    actual_time_ms INTEGER NOT NULL,
    actual_tool_calls INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    verification_attempts INTEGER NOT NULL DEFAULT 0,
    parse_attempts INTEGER NOT NULL DEFAULT 0,
    tool_attempts INTEGER NOT NULL DEFAULT 0,
    verification_spec_json TEXT,
    metadata_json TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE edges(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_node_id TEXT NOT NULL,
    target_node_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    active INTEGER NOT NULL,
    version INTEGER NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(run_id, source_node_id, target_node_id, kind)
);

CREATE TABLE evidence(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    uri TEXT,
    content_hash TEXT,
    summary TEXT,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE executions(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    context_hash TEXT NOT NULL,
    model_name TEXT,
    status TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    tool_calls INTEGER NOT NULL,
    cost_usd REAL NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    result_json TEXT,
    error_json TEXT,
    checkpoint_before TEXT,
    checkpoint_after TEXT,
    UNIQUE(run_id, node_id, attempt_number)
);

CREATE TABLE checkpoints(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    checkpoint_type TEXT NOT NULL,
    location TEXT NOT NULL,
    manifest_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX idx_events_run_sequence ON events(run_id, sequence);
CREATE INDEX idx_nodes_run_state ON nodes(run_id, state);
CREATE INDEX idx_edges_source ON edges(run_id, source_node_id);
CREATE INDEX idx_edges_target ON edges(run_id, target_node_id);
CREATE INDEX idx_executions_run_node ON executions(run_id, node_id, attempt_number);
CREATE INDEX idx_executions_run ON executions(run_id);
