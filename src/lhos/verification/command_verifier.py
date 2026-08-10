"""command and exit_code verifiers (spec 14.2)."""

from __future__ import annotations

import subprocess

from lhos.domain.models import GraphNode
from lhos.domain.verification import VerificationResult, VerificationSpec
from lhos.ports.verifier import VerificationContext
from lhos.subprocess_policy import CommandPolicyError, run_command


class CommandVerifier:
    verifier_type = "command"

    def verify(
        self,
        node: GraphNode,
        spec: VerificationSpec,
        context: VerificationContext,
    ) -> VerificationResult:
        command = spec.parameters.get("command")
        if not command:
            return VerificationResult(passed=False, summary="command verifier: no command")
        expected = spec.parameters.get("expected_exit_code", 0)
        try:
            proc = run_command(
                command,
                cwd=context.workspace_dir,
                timeout=spec.timeout_seconds,
                trusted=context.command_trusted,
                allow_shell=context.command_allow_shell,
                allow_network=context.command_allow_network,
            )
        except subprocess.TimeoutExpired:
            return VerificationResult(
                passed=False,
                summary=f"command timed out after {spec.timeout_seconds}s: {command}",
            )
        except (CommandPolicyError, OSError, ValueError) as exc:
            return VerificationResult(
                passed=False,
                summary=f"command denied or unavailable: {exc}",
                evidence=[
                    {
                        "evidence_type": "command_policy",
                        "summary": f"{command} was not executed",
                        "metadata": {"command": command, "error": str(exc)},
                    }
                ],
            )
        passed = proc.returncode == expected
        tail = (proc.stdout or proc.stderr or "").strip()[-400:]
        return VerificationResult(
            passed=passed,
            summary=f"`{command}` exit={proc.returncode} (expected {expected}). {tail}",
            evidence=[
                {
                    "evidence_type": "command_output",
                    "summary": f"{command} -> exit {proc.returncode}",
                    "content_hash": None,
                    "metadata": {
                        "command": command,
                        "exit_code": proc.returncode,
                        "stdout_tail": proc.stdout[-2000:],
                        "stderr_tail": proc.stderr[-2000:],
                    },
                }
            ],
        )


class ExitCodeVerifier:
    """Checks the exit code reported by the worker's last command."""

    verifier_type = "exit_code"

    def verify(
        self,
        node: GraphNode,
        spec: VerificationSpec,
        context: VerificationContext,
    ) -> VerificationResult:
        expected = spec.parameters.get("expected", spec.parameters.get("expected_exit_code", 0))
        actual = context.worker_result.get("exit_code")
        passed = actual == expected
        return VerificationResult(
            passed=passed,
            summary=f"worker exit_code={actual} (expected {expected})",
            evidence=[
                {
                    "evidence_type": "exit_code",
                    "summary": f"exit_code {actual}",
                    "metadata": {"expected": expected, "actual": actual},
                }
            ],
        )
