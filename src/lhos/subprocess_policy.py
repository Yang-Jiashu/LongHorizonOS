"""Shared subprocess policy for local tool execution.

The default path never invokes a command interpreter.  A small set of
cross-platform built-ins is evaluated directly; all other programs require
``trusted=True`` and run with an argv list plus a sanitized environment.
Interpreter-backed shell syntax is an additional explicit opt-in.
"""

from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


class CommandPolicyError(ValueError):
    """The requested command is outside the configured execution policy."""


@dataclass(frozen=True)
class CommandExecution:
    returncode: int
    stdout: str = ""
    stderr: str = ""


_NETWORK_PROGRAMS = {
    "curl",
    "ftp",
    "nc",
    "netcat",
    "powershell",
    "pwsh",
    "scp",
    "ssh",
    "telnet",
    "wget",
}
_SAFE_ENV_KEYS = {
    "COMSPEC",
    "HOME",
    "HOMEDRIVE",
    "HOMEPATH",
    "LANG",
    "LC_ALL",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "TMPDIR",
    "USERPROFILE",
    "WINDIR",
}


def run_command(
    command: str | Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
    trusted: bool = False,
    allow_shell: bool = False,
    allow_network: bool = False,
) -> CommandExecution:
    """Execute one command under the shared local-process policy."""
    if allow_shell:
        if not trusted:
            raise CommandPolicyError("shell interpreter mode requires trusted=True")
        proc = subprocess.run(
            command if isinstance(command, str) else list(command),
            shell=True,
            cwd=str(cwd) if cwd is not None else None,
            env=_sanitized_env(env),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return CommandExecution(proc.returncode, proc.stdout or "", proc.stderr or "")

    commands = _parse_commands(command)
    deadline = None if timeout is None else time.monotonic() + timeout
    stdout: list[str] = []
    stderr: list[str] = []
    for argv in commands:
        remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
        result = _run_one(
            argv,
            cwd=cwd,
            env=env,
            timeout=remaining,
            trusted=trusted,
            allow_network=allow_network,
        )
        stdout.append(result.stdout)
        stderr.append(result.stderr)
        if result.returncode != 0:
            return CommandExecution(result.returncode, "".join(stdout), "".join(stderr))
    return CommandExecution(0, "".join(stdout), "".join(stderr))


def _parse_commands(command: str | Sequence[str]) -> list[list[str]]:
    if isinstance(command, str):
        return _parse_command_string(command)
    argv = [str(part) for part in command]
    if not argv or any(not part for part in argv):
        raise CommandPolicyError("command argv must contain non-empty strings")
    return [argv]


def _parse_command_string(command: str) -> list[list[str]]:
    """Parse a shell-like command into argv without invoking a shell."""
    commands: list[list[str]] = []
    argv: list[str] = []
    token: list[str] = []
    token_started = False
    quote: str | None = None
    index = 0

    def flush_token() -> None:
        nonlocal token_started
        if token_started:
            argv.append("".join(token))
            token.clear()
            token_started = False

    while index < len(command):
        char = command[index]
        if char in "\r\n":
            raise CommandPolicyError(
                "shell syntax is disabled; pass argv or explicitly enable allow_shell"
            )

        if quote is not None:
            if char == quote:
                quote = None
            elif (
                quote == '"'
                and char == "\\"
                and index + 1 < len(command)
                and command[index + 1] in {'"', "\\"}
            ):
                index += 1
                token.append(command[index])
            else:
                token.append(char)
            token_started = True
            index += 1
            continue

        if char in {"'", '"'}:
            quote = char
            token_started = True
        elif char.isspace():
            flush_token()
        elif char == "&":
            if index + 1 < len(command) and command[index + 1] == "&":
                flush_token()
                if not argv:
                    raise CommandPolicyError("invalid empty command in command chain")
                commands.append(argv)
                argv = []
                index += 1
            else:
                raise CommandPolicyError(
                    "shell syntax is disabled; pass argv or explicitly enable allow_shell"
                )
        elif char in {"|", ";", "<", ">", "`"} or (
            char == "$" and index + 1 < len(command) and command[index + 1] == "("
        ):
            raise CommandPolicyError(
                "shell syntax is disabled; pass argv or explicitly enable allow_shell"
            )
        else:
            token.append(char)
            token_started = True
        index += 1

    if quote is not None:
        raise CommandPolicyError("unterminated quote in command")
    flush_token()
    if not argv:
        if commands:
            raise CommandPolicyError("invalid empty command in command chain")
        raise CommandPolicyError("command must not be empty")
    commands.append(argv)
    if any(not item[0] for item in commands):
        raise CommandPolicyError("command executable must not be empty")
    return commands


def _run_one(
    argv: list[str],
    *,
    cwd: str | Path | None,
    env: Mapping[str, str] | None,
    timeout: float | None,
    trusted: bool,
    allow_network: bool,
) -> CommandExecution:
    builtin = _run_builtin(argv, cwd, timeout)
    if builtin is not None:
        return builtin
    if not trusted:
        raise CommandPolicyError(
            f"external program {argv[0]!r} requires trusted=True or an OS/container sandbox"
        )
    executable = Path(argv[0]).name.lower()
    if executable.endswith(".exe"):
        executable = executable[:-4]
    if not allow_network and executable in _NETWORK_PROGRAMS:
        raise CommandPolicyError(f"network-capable program {argv[0]!r} is disabled")
    proc = subprocess.run(
        argv,
        shell=False,
        cwd=str(cwd) if cwd is not None else None,
        env=_sanitized_env(env),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return CommandExecution(proc.returncode, proc.stdout or "", proc.stderr or "")


def _run_builtin(
    argv: list[str],
    cwd: str | Path | None,
    timeout: float | None,
) -> CommandExecution | None:
    name = argv[0].lower()
    if name == "true" and len(argv) == 1:
        return CommandExecution(0)
    if name == "false" and len(argv) == 1:
        return CommandExecution(1)
    if name == "echo":
        return CommandExecution(0, " ".join(argv[1:]) + os.linesep)
    if name == "sleep" and len(argv) == 2:
        duration = float(argv[1])
        if timeout is not None and duration > timeout:
            time.sleep(timeout)
            raise subprocess.TimeoutExpired(argv, timeout)
        time.sleep(duration)
        return CommandExecution(0)
    if name == "test" and len(argv) == 3 and argv[1] in {"-e", "-f", "-d"}:
        root = Path(cwd).resolve() if cwd is not None else Path.cwd().resolve()
        candidate = (root / argv[2]).resolve()
        exists = {
            "-e": candidate.exists,
            "-f": candidate.is_file,
            "-d": candidate.is_dir,
        }[argv[1]]()
        return CommandExecution(0 if exists else 1)
    return None


def _sanitized_env(extra: Mapping[str, str] | None) -> dict[str, str]:
    clean = {key: value for key, value in os.environ.items() if key.upper() in _SAFE_ENV_KEYS}
    if extra:
        clean.update({str(key): str(value) for key, value in extra.items()})
    return clean
