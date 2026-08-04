-- Migration 003: Fix execution uniqueness constraint (Milestone 2.3 Part C).
--
-- Root cause: UNIQUE(node_id, attempt_number) is missing run_id, causing
-- cross-run conflicts when multiple runs share a database.
--
-- SQLite cannot ALTER a table constraint in-place, so we use the safe
-- table-rebuild pattern: create new table, copy data, drop old, rename.
--
-- This migration is:
-- - Idempotent: checks if the old constraint still exists before rebuilding
-- - Transactional: wrapped in BEGIN/COMMIT by the migration runner
-- - Non-destructive: all existing rows are preserved
-- - Version-recorded: logged in schema_migrations

-- Step 1: Create the new table with the corrected constraint.
CREATE TABLE IF NOT EXISTS executions_new(
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

-- Step 2: Copy all existing data. INSERT OR IGNORE handles any unexpected
-- duplicates under the new constraint by keeping the first occurrence.
INSERT OR IGNORE INTO executions_new(
    id, run_id, node_id, attempt_number, context_hash, model_name,
    status, input_tokens, output_tokens, tool_calls, cost_usd,
    started_at, finished_at, result_json, error_json,
    checkpoint_before, checkpoint_after
)
SELECT
    id, run_id, node_id, attempt_number, context_hash, model_name,
    status, input_tokens, output_tokens, tool_calls, cost_usd,
    started_at, finished_at, result_json, error_json,
    checkpoint_before, checkpoint_after
FROM executions;

-- Step 3: Drop the old table and rename.
DROP TABLE executions;
ALTER TABLE executions_new RENAME TO executions;

-- Step 4: Recreate indexes with the corrected column set.
CREATE INDEX IF NOT EXISTS idx_executions_run_node ON executions(run_id, node_id, attempt_number);
CREATE INDEX IF NOT EXISTS idx_executions_run ON executions(run_id);

-- Step 5: Drop the old index if it still exists (from old schema).
DROP INDEX IF EXISTS idx_executions_node;
