"""Backward-compatible SQLite schema migrations for Agent OS projections."""

from __future__ import annotations

import sqlite3

from lhos.agent_os.kernel.models import RecoveryPolicy, SideEffectClass
from lhos.agent_os.services.action_service import ActionService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


def _create_legacy_actions_schema(db_path: str) -> None:
    """Create the action projection before recovery metadata was persisted."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE actions_projection (
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
            timeout_seconds INTEGER,
            result_json TEXT,
            error_json TEXT,
            submitted_at TEXT NOT NULL,
            finished_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO actions_projection
            (action_id, pid, device_type, operation, arguments_json, state,
             submitted_at)
        VALUES ('legacy-action', 'legacy-pid', 'model/mock', 'generate', '{}',
                'submitted', '2026-08-12T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()


def test_legacy_actions_projection_migrates_and_supports_submit_read(tmp_path) -> None:
    db_path = str(tmp_path / "legacy-actions.sqlite")
    _create_legacy_actions_schema(db_path)

    storage = SQLiteStorage(db_path)
    try:
        columns = {
            row["name"]
            for row in storage.conn.execute("PRAGMA table_info(actions_projection)").fetchall()
        }
        assert {"side_effect_class", "recovery_policy", "retry_count"} <= columns

        journal = JournalService(storage)
        actions = ActionService(storage, journal)

        # Existing rows receive conservative historical defaults.
        legacy = actions.get_action("legacy-action")
        assert legacy is not None
        assert legacy.side_effect_class == SideEffectClass.PURE
        assert legacy.recovery_policy == RecoveryPolicy.RETRY
        assert legacy.retry_count == 1

        submitted = actions.submit(
            pid="new-pid",
            device_type="tool/mock",
            operation="publish",
            side_effect_class=SideEffectClass.IDEMPOTENT,
            recovery_policy=RecoveryPolicy.INSPECT,
        )
        restored = actions.get_action(submitted.action_id)
        assert restored is not None
        assert restored.side_effect_class == SideEffectClass.IDEMPOTENT
        assert restored.recovery_policy == RecoveryPolicy.INSPECT
        assert restored.retry_count == 0
    finally:
        storage.close()
