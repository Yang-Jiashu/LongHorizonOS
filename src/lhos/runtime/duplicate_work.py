"""Duplicate work detection (Milestone 2.2 Step 6).

Tracks redundant tool calls, no-op writes, reverted writes, and repeated reads
within a single node's worker loop. The first version records metrics and
provides feedback to the worker; it blocks the 3rd identical tool call.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field


class DuplicateWorkMetrics(BaseModel):
    """Accumulated duplicate work metrics for a single node execution."""

    duplicate_tool_call_count: int = 0
    no_op_write_count: int = 0
    reverted_write_count: int = 0
    repeated_read_count: int = 0
    repeated_failing_command_count: int = 0
    blocked_call_count: int = 0

    @property
    def total_duplicates(self) -> int:
        return (
            self.duplicate_tool_call_count
            + self.no_op_write_count
            + self.reverted_write_count
            + self.repeated_read_count
            + self.repeated_failing_command_count
        )


@dataclass
class _ToolCallRecord:
    """Internal record of a tool call for dedup detection."""

    tool_name: str
    arguments_hash: str
    workspace_hash: str | None = None
    result_hash: str | None = None
    success: bool = True
    count: int = 1


class DuplicateWorkDetector:
    """Detects and records duplicate work within a single node execution.

    Usage:
        detector = DuplicateWorkDetector()
        for each tool call:
            result = detector.check_and_record(
                node_id, tool_name, arguments, workspace_hash, result
            )
            if result.blocked:
                # skip this call, provide feedback to worker
            else:
                # execute the tool
    """

    MAX_IDENTICAL_CALLS = 2  # Block on the 3rd identical call

    def __init__(self) -> None:
        self._records: dict[str, _ToolCallRecord] = {}
        self._file_hashes: dict[str, str] = {}  # path -> last known hash
        self._failed_commands: dict[str, int] = {}  # command_hash -> count
        self.metrics = DuplicateWorkMetrics()

    @staticmethod
    def _normalize_arguments(arguments: dict[str, Any]) -> str:
        """Create a stable hash from tool arguments, ignoring volatile fields."""
        # Ignore timeout_seconds and idempotency_key for dedup purposes.
        filtered = {
            k: v for k, v in arguments.items() if k not in {"timeout_seconds", "idempotency_key"}
        }
        return hashlib.sha256(
            json.dumps(filtered, sort_keys=True, default=str).encode()
        ).hexdigest()[:16]

    def _call_key(self, node_id: str, tool_name: str, args_hash: str) -> str:
        return f"{node_id}:{tool_name}:{args_hash}"

    def check_and_record(
        self,
        node_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        workspace_hash: str | None = None,
        result_hash: str | None = None,
        result_success: bool = True,
    ) -> DuplicateCheckResult:
        """Check if a tool call is a duplicate and record it.

        Returns a DuplicateCheckResult indicating whether to block, and a
        feedback message for the worker.
        """
        args_hash = self._normalize_arguments(arguments)
        key = self._call_key(node_id, tool_name, args_hash)

        existing = self._records.get(key)

        # Detect duplicate reads.
        if tool_name in {"filesystem", "fs"} and arguments.get("op") == "read":
            path = arguments.get("path", "")
            file_key = f"{node_id}:read:{path}"
            if file_key in self._file_hashes and workspace_hash:
                # Same file read with same workspace hash = repeated read.
                self.metrics.repeated_read_count += 1
                existing_read = self._records.get(file_key)
                if existing_read:
                    existing_read.count += 1
                    if existing_read.count >= self.MAX_IDENTICAL_CALLS:
                        return DuplicateCheckResult(
                            blocked=False,
                            duplicate=True,
                            feedback=(
                                f"WARNING: You have already read '{path}' with the same "
                                f"workspace state. The content has not changed. "
                                f"Do not read this file again unless you have modified it."
                            ),
                            metrics=self.metrics,
                        )
            # Record the read-specific info but don't return early —
            # fall through to general duplicate detection below so that
            # repeated reads without workspace_hash are still caught.
            self._file_hashes[file_key] = workspace_hash or ""
            if file_key not in self._records:
                self._records[file_key] = _ToolCallRecord(
                    tool_name=tool_name,
                    arguments_hash=args_hash,
                    workspace_hash=workspace_hash,
                )

        # Detect no-op writes (same content before and after).
        if tool_name in {"filesystem", "fs"} and arguments.get("op") == "write":
            path = arguments.get("path", "")
            new_content = arguments.get("content", "")
            new_hash = hashlib.sha256(new_content.encode()).hexdigest()[:16]
            old_hash = self._file_hashes.get(f"write:{path}")
            if old_hash == new_hash:
                self.metrics.no_op_write_count += 1
                return DuplicateCheckResult(
                    blocked=False,
                    duplicate=True,
                    feedback=(
                        f"WARNING: Writing to '{path}' with the same content as before. "
                        f"This is a no-op. The file already contains this content."
                    ),
                    metrics=self.metrics,
                )
            self._file_hashes[f"write:{path}"] = new_hash

        # Detect repeated failing commands.
        if tool_name in {"shell", "bash"} and not result_success:
            cmd = arguments.get("command", "")
            cmd_hash = hashlib.sha256(cmd.encode()).hexdigest()[:16]
            fail_count = self._failed_commands.get(cmd_hash, 0) + 1
            self._failed_commands[cmd_hash] = fail_count
            if fail_count >= 2:
                self.metrics.repeated_failing_command_count += 1
                if fail_count >= self.MAX_IDENTICAL_CALLS:
                    return DuplicateCheckResult(
                        blocked=True,
                        duplicate=True,
                        feedback=(
                            f"BLOCKED: Command '{cmd[:100]}' has failed {fail_count} times. "
                            f"Do not repeat this exact command. Try a different approach."
                        ),
                        metrics=self.metrics,
                    )
                return DuplicateCheckResult(
                    blocked=False,
                    duplicate=True,
                    feedback=(
                        f"WARNING: Command '{cmd[:100]}' has failed {fail_count} times. "
                        f"Consider trying a different approach."
                    ),
                    metrics=self.metrics,
                )

        # General duplicate tool call detection.
        if existing:
            existing.count += 1
            existing.result_hash = result_hash
            existing.success = result_success
            self.metrics.duplicate_tool_call_count += 1

            if existing.count >= self.MAX_IDENTICAL_CALLS + 1:
                # Block on the 3rd identical call.
                self.metrics.blocked_call_count += 1
                return DuplicateCheckResult(
                    blocked=True,
                    duplicate=True,
                    feedback=(
                        f"BLOCKED: This exact tool call ({tool_name} with same arguments) "
                        f"has been made {existing.count} times. You are repeating the same "
                        f"action. Try a different approach or use claim_done if the task is done."
                    ),
                    metrics=self.metrics,
                )
            return DuplicateCheckResult(
                blocked=False,
                duplicate=True,
                feedback=(
                    f"WARNING: This tool call ({tool_name} with same arguments) "
                    f"has been made {existing.count} times. Avoid repeating identical actions."
                ),
                metrics=self.metrics,
            )

        # New call — record it.
        self._records[key] = _ToolCallRecord(
            tool_name=tool_name,
            arguments_hash=args_hash,
            workspace_hash=workspace_hash,
            result_hash=result_hash,
            success=result_success,
        )
        return DuplicateCheckResult(blocked=False, duplicate=False, metrics=self.metrics)

    def get_feedback_for_worker(self) -> str | None:
        """Return a summary of duplicate work for the worker context."""
        if self.metrics.total_duplicates == 0:
            return None
        parts = [f"Duplicate work detected (total: {self.metrics.total_duplicates}):"]
        if self.metrics.duplicate_tool_call_count:
            parts.append(f"  - Duplicate tool calls: {self.metrics.duplicate_tool_call_count}")
        if self.metrics.no_op_write_count:
            parts.append(f"  - No-op file writes: {self.metrics.no_op_write_count}")
        if self.metrics.repeated_read_count:
            parts.append(f"  - Repeated file reads: {self.metrics.repeated_read_count}")
        if self.metrics.repeated_failing_command_count:
            parts.append(
                f"  - Repeated failing commands: {self.metrics.repeated_failing_command_count}"
            )
        if self.metrics.blocked_call_count:
            parts.append(f"  - Blocked calls: {self.metrics.blocked_call_count}")
        parts.append("Avoid repeating identical actions. Try a different approach.")
        return "\n".join(parts)


class DuplicateCheckResult(BaseModel):
    """Result of a duplicate check."""

    blocked: bool = False
    duplicate: bool = False
    feedback: str | None = None
    metrics: DuplicateWorkMetrics = Field(default_factory=DuplicateWorkMetrics)
