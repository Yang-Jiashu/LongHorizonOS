-- Migration 002: Add separate attempt counters per failure type (Milestone 2.2 Step 4).
-- verification_attempts: incremented only when verification fails.
-- parse_attempts: incremented when structured output parsing fails.
-- tool_attempts: incremented when a tool call fails (transient).

ALTER TABLE nodes ADD COLUMN verification_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN parse_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE nodes ADD COLUMN tool_attempts INTEGER NOT NULL DEFAULT 0;
