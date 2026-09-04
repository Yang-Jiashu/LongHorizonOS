-- Migration 001: LLM call log table (spec Phase 2B-B3).
-- Stores every real model call for audit and cost accounting.
-- Does not break existing deterministic run replay (additive only).

CREATE TABLE IF NOT EXISTS llm_calls(
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    node_id TEXT,
    execution_id TEXT,
    role TEXT NOT NULL,               -- planner | worker | reconciler | verifier
    provider TEXT NOT NULL,            -- sensenova | mock | ...
    exact_model_id TEXT NOT NULL,     -- never silently changed
    prompt_name TEXT,
    prompt_version TEXT,
    prompt_file_hash TEXT,
    request_hash TEXT NOT NULL,
    response_hash TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cached_input_tokens INTEGER NOT NULL DEFAULT 0,
    reasoning_tokens INTEGER,
    total_tokens INTEGER NOT NULL,
    cost_usd REAL NOT NULL DEFAULT 0.0,
    latency_ms INTEGER NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    parse_failure_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'success',  -- success | provider_error | parse_failed
    error_type TEXT,                          -- exception class name or error category
    causation_id TEXT,                        -- links repair calls to the original call
    request_body_json TEXT NOT NULL,   -- sanitized: no API keys
    response_body_json TEXT NOT NULL,  -- may reference artifacts for long outputs
    timestamp TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_llm_calls_run ON llm_calls(run_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_node ON llm_calls(node_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_role ON llm_calls(role);
CREATE INDEX IF NOT EXISTS idx_llm_calls_status ON llm_calls(status);
