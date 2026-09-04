from __future__ import annotations

import sys
from pathlib import Path

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationSpec
from lhos.ports.verifier import VerificationContext
from lhos.verification.command_verifier import CommandVerifier


def _node() -> GraphNode:
    return GraphNode(
        id="command-node",
        run_id="test-run",
        kind="subtask",
        title="Verify command",
        specification="Run a deterministic verifier command",
    )


def _spec(command: str | list[str]) -> VerificationSpec:
    return VerificationSpec(
        verifier_type="command",
        parameters={"command": command},
        timeout_seconds=10,
    )


def _context(tmp_path: Path, *, trusted: bool = False) -> VerificationContext:
    return VerificationContext(
        run_id="test-run",
        workspace_dir=str(tmp_path),
        command_trusted=trusted,
    )


def test_external_command_is_denied_by_default(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec([sys.executable, "-c", "print('should-not-run')"]),
        _context(tmp_path),
    )
    assert result.passed is False
    assert "requires trusted=True" in result.summary


def test_host_can_enable_external_command_without_shell(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec([sys.executable, "-c", "print('verified')"]),
        _context(tmp_path, trusted=True),
    )
    assert result.passed is True
    assert result.evidence[0]["metadata"]["stdout_tail"].strip() == "verified"


def test_shell_syntax_remains_denied_without_host_opt_in(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec("echo safe | echo unsafe"),
        _context(tmp_path, trusted=True),
    )
    assert result.passed is False
    assert "shell syntax is disabled" in result.summary


def test_quoted_code_is_parsed_as_argv_without_shell(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec(f'"{sys.executable}" -c "print(\'quoted; code\')"'),
        _context(tmp_path, trusted=True),
    )
    assert result.passed is True
    assert result.evidence[0]["metadata"]["stdout_tail"].strip() == "quoted; code"


def test_quoted_shell_characters_are_literal_arguments(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec('echo "safe | literal; text"'),
        _context(tmp_path),
    )
    assert result.passed is True
    assert result.evidence[0]["metadata"]["stdout_tail"].strip() == "safe | literal; text"


def test_unterminated_quote_is_denied(tmp_path):
    result = CommandVerifier().verify(
        _node(),
        _spec('echo "unterminated'),
        _context(tmp_path),
    )
    assert result.passed is False
    assert "unterminated quote" in result.summary
