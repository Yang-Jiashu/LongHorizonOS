"""Child-side runtime and structured stdout protocol for a killable Harness.

:class:`~lhos.sdk.subprocess_harness.SubprocessHarnessAdapter` runs Agent work
in a real child process so LongHorizonOS can terminate it.  This module defines
the narrow contract between that parent and its child:

* The child may print exactly one *usage* line, ``USAGE_SENTINEL`` followed by a
  single JSON object, to report ``{tokens_in, tokens_out, cost_microusd}``.  The
  parent measures wall-clock time itself; the child only reports what it knows.
* The child may print a ``READY_SENTINEL`` line once it has left import/startup
  and entered its real work phase.  The parent uses this to preempt work rather
  than a process that is still importing.
* The child may install the undeclared-read recorder
  (:func:`lhos.sdk.read_recorder.install_read_recorder`), which prints one
  ``READS_SENTINEL`` line at exit reporting the absolute paths it opened for
  reading.  This reference child does so on ``--record-reads`` or when the
  parent sets ``LHOS_HARNESS_RECORD_READS`` in the child env.

Both directions are line oriented and defensive: a child that prints garbage,
partial JSON, or nothing at all must never crash the parent.  Every stream is
read and written as UTF-8.

Running ``python -m lhos.sdk.harness_child`` starts a reference child whose
behaviour is fully driven by command-line flags (sleep duration, emitted usage,
whether to ignore cooperative termination signals, exit code).  It exists so
preemption, timeout, and usage capture are demonstrable and testable without a
real model call.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from contextlib import suppress
from typing import IO, Any, Final

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from .read_recorder import READS_SENTINEL, install_read_recorder

# A JSON usage report is one stdout line: ``<USAGE_SENTINEL> {json}``.  The
# sentinel is deliberately unlikely to collide with ordinary program output.
USAGE_SENTINEL: Final[str] = "__LHOS_HARNESS_USAGE__"
# Printed once the child has entered its real work phase (after imports).
READY_SENTINEL: Final[str] = "__LHOS_HARNESS_READY__"


class ChildUsage(BaseModel):
    """Measured resource usage a child self-reports to the parent Harness."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tokens_in: StrictInt = Field(default=0, ge=0)
    tokens_out: StrictInt = Field(default=0, ge=0)
    cost_microusd: StrictInt = Field(default=0, ge=0)


def format_usage_line(
    *,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_microusd: int = 0,
) -> str:
    """Return the exact single stdout line a child prints to report usage."""

    payload = {
        "tokens_in": int(tokens_in),
        "tokens_out": int(tokens_out),
        "cost_microusd": int(cost_microusd),
    }
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return f"{USAGE_SENTINEL} {encoded}"


def parse_usage_line(line: str) -> ChildUsage | None:
    """Parse one candidate usage line, returning ``None`` for anything invalid.

    This is intentionally total: a child that emits a malformed sentinel line,
    non-integer counts, extra keys, or plain garbage yields ``None`` rather than
    raising.  The parent treats ``None`` as "no self-reported usage" and still
    reports its own measured wall-clock time.
    """

    if not isinstance(line, str):
        return None
    stripped = line.strip()
    if not stripped.startswith(USAGE_SENTINEL):
        return None
    remainder = stripped[len(USAGE_SENTINEL) :].strip()
    if not remainder:
        return None
    try:
        raw = json.loads(remainder)
    except (ValueError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        return ChildUsage.model_validate(raw)
    except Exception:
        return None


def emit_usage(
    *,
    tokens_in: int = 0,
    tokens_out: int = 0,
    cost_microusd: int = 0,
    stream: IO[str] | None = None,
) -> None:
    """Write one usage line and flush so the parent sees it before any kill."""

    target = stream if stream is not None else sys.stdout
    target.write(
        format_usage_line(
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_microusd=cost_microusd,
        )
    )
    target.write("\n")
    with suppress(Exception):
        target.flush()


def emit_ready(stream: IO[str] | None = None) -> None:
    """Announce that the child has entered its real work phase."""

    target = stream if stream is not None else sys.stdout
    target.write(READY_SENTINEL + "\n")
    with suppress(Exception):
        target.flush()


def _install_signal_traps() -> None:
    """Ignore cooperative termination signals to model an uncooperative child.

    On POSIX this makes ``terminate()`` (SIGTERM) and Ctrl-C (SIGINT) no-ops so
    only a hard ``kill()`` (SIGKILL) stops the process.  On Windows the handlers
    are best-effort; ``TerminateProcess`` cannot be trapped regardless, which is
    exactly the point of the parent's hard-kill fallback.
    """

    def _ignore(_signum: int, _frame: Any) -> None:
        return None

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        with suppress(OSError, ValueError, RuntimeError):
            signal.signal(sig, _ignore)


def _sleep_until(deadline: float) -> None:
    """Sleep until ``deadline`` (monotonic), resisting signal-interrupted waits."""

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            time.sleep(min(remaining, 0.25))
        except InterruptedError:
            # A trapped-and-ignored signal can cut the wait short; keep going so
            # an uncooperative child does not accidentally exit early.
            continue


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m lhos.sdk.harness_child",
        description="Reference killable Harness child process.",
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="work duration in seconds")
    parser.add_argument("--tokens-in", type=int, default=0)
    parser.add_argument("--tokens-out", type=int, default=0)
    parser.add_argument("--cost-microusd", type=int, default=0)
    parser.add_argument("--exit-code", type=int, default=0, help="exit code after work")
    parser.add_argument(
        "--ignore-termination",
        action="store_true",
        help="trap and ignore SIGTERM/SIGINT so only a hard kill stops the child",
    )
    parser.add_argument(
        "--emit-usage",
        dest="emit_usage",
        action="store_true",
        default=True,
        help="print a usage line before working (default)",
    )
    parser.add_argument("--no-emit-usage", dest="emit_usage", action="store_false")
    parser.add_argument(
        "--corrupt-usage",
        action="store_true",
        help="print a sentinel-prefixed but malformed usage line (parser stress)",
    )
    parser.add_argument(
        "--record-reads",
        action="store_true",
        help="install the undeclared-read recorder and report observed reads at exit",
    )
    parser.add_argument(
        "--read-file",
        dest="read_files",
        action="append",
        default=[],
        metavar="PATH",
        help="open PATH for reading during work (repeatable); demonstrates read recording",
    )
    parser.add_argument(
        "--corrupt-reads",
        action="store_true",
        help="print a sentinel-prefixed but malformed reads line (parser stress)",
    )
    return parser


def _env_flag(name: str) -> bool:
    """Return True when environment variable ``name`` is set to a truthy value."""

    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() not in ("", "0", "false", "no")


def main(argv: list[str] | None = None) -> int:
    """Run the reference child; return the process exit code."""

    args = _build_parser().parse_args(argv)

    if args.ignore_termination:
        _install_signal_traps()

    # Report usage before working.  This models an LLM call that has already
    # spent tokens by the time it blocks, so the parent still captures measured
    # cost even when it must hard-kill the child mid-flight.
    if args.corrupt_usage:
        sys.stdout.write(f"{USAGE_SENTINEL} {{not-valid-json\n")
        with suppress(Exception):
            sys.stdout.flush()
    elif args.emit_usage:
        emit_usage(
            tokens_in=args.tokens_in,
            tokens_out=args.tokens_out,
            cost_microusd=args.cost_microusd,
        )

    # Read observation is opt-in and installed before the work phase so it never
    # captures this child's own import/startup file reads -- only real work.
    if args.corrupt_reads:
        sys.stdout.write(f"{READS_SENTINEL} {{not-valid-json\n")
        with suppress(Exception):
            sys.stdout.flush()
    elif args.record_reads or _env_flag("LHOS_HARNESS_RECORD_READS"):
        install_read_recorder()

    emit_ready()

    # Opening these files models a task's real (possibly undeclared) input reads.
    for path in args.read_files:
        with suppress(OSError), open(path, encoding="utf-8") as handle:
            handle.read()

    if args.sleep > 0:
        _sleep_until(time.monotonic() + args.sleep)

    return int(args.exit_code)


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())
