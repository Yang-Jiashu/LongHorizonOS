"""Child-side undeclared-read recorder and its single-line stdout protocol.

A child process this SDK controls can make its Python-level file reads
*observable* with no model and no API call.  This module monkeypatches the three
Python entry points to a file read -- :func:`builtins.open`, :func:`io.open`, and
:meth:`pathlib.Path.open` -- records every resolved absolute path opened in a
readable mode, and emits the set once at interpreter exit as a single
sentinel-prefixed JSON line on stdout.

It reuses the exact convention of the usage line in
:mod:`lhos.sdk.harness_child`: one line, ``<READS_SENTINEL> {json}``, defensively
parsed, so a child that prints garbage, partial JSON, or nothing at all never
crashes the parent.  Do not invent a second protocol; this is the same one.

The line is emitted whenever the recorder is installed -- an empty read set
prints ``[]``.  That distinction is load-bearing.  The parent must tell
"observed, read nothing" (``[]``) apart from "never observed" (no line at all),
so that a hard crash before ``atexit`` fails *closed* to "unobserved" rather than
to a false "no undeclared reads".

What this DOES NOT and CANNOT catch (deliberately understated):

* **C-extension reads.**  A read performed inside a C/Cython extension (numpy, a
  database driver, ``sqlite3``, etc.) that opens the OS file itself never passes
  through the Python ``open`` layer and is invisible here.
* **``os.open`` and raw file descriptors.**  Only the high-level ``open`` family
  is patched.  ``os.open``, ``os.read``, ``os.fdopen`` over a pre-opened fd, and
  anything driving a raw descriptor are not observed.
* **``mmap``.**  Memory-mapped file access bypasses ``open`` entirely.
* **Grandchild processes.**  Only this interpreter is instrumented.  A file read
  by a subprocess this child spawns is not recorded; the patch is not inherited
  across ``exec``.

Because observed reads are a subset of true reads, an *absence* of undeclared
reads is not proof of soundness, while a *presence* is a true counterexample.
Treat the signal as one-directional.
"""

from __future__ import annotations

import atexit
import builtins
import io
import json
import os
import pathlib
import sys
import threading
from collections.abc import Iterable
from contextlib import suppress
from typing import IO, Any, Final

# One stdout line: ``<READS_SENTINEL> {json-array}``.  Distinct from the usage
# sentinel so the two reports never collide on the shared stdout channel.
READS_SENTINEL: Final[str] = "__LHOS_HARNESS_READS__"

_lock = threading.Lock()
_recorded: set[str] = set()
_installed = False

# Captured before any patching so the wrappers always call the genuine article.
# ``builtins.open`` and ``io.open`` are the same underlying object in CPython;
# one saved reference is enough for both.
_real_open = builtins.open
_real_path_open = pathlib.Path.open


def _is_readable_mode(mode: Any) -> bool:
    """Return True when ``mode`` can read: default 'r', explicit 'r', or '+'."""

    if not isinstance(mode, str):
        # An unusual non-str mode: be inclusive rather than silently drop a read.
        return True
    return "r" in mode or "+" in mode


def _record(path_like: Any, mode: Any) -> None:
    """Resolve and store one opened path; never raises into the caller's open()."""

    if not _is_readable_mode(mode):
        return
    with suppress(Exception):
        if isinstance(path_like, int):
            # A bare file descriptor carries no path we could attribute.
            return
        text = os.fsdecode(os.fspath(path_like))
        resolved = os.path.abspath(text)
        with _lock:
            _recorded.add(resolved)


def format_reads_line(paths: Iterable[str]) -> str:
    """Return the exact single stdout line a child prints to report reads."""

    unique = sorted({str(path) for path in paths})
    encoded = json.dumps(unique, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return f"{READS_SENTINEL} {encoded}"


def parse_reads_line(line: str) -> list[str] | None:
    """Parse one candidate reads line, returning ``None`` for anything invalid.

    Total by construction: a malformed sentinel line, a JSON object where a list
    was expected, non-string entries, or plain garbage all yield ``None`` rather
    than raising.  An empty list parses to ``[]`` -- "observed, read nothing" --
    which is deliberately distinct from ``None`` -- "not observed".
    """

    if not isinstance(line, str):
        return None
    stripped = line.strip()
    if not stripped.startswith(READS_SENTINEL):
        return None
    remainder = stripped[len(READS_SENTINEL) :].strip()
    if not remainder:
        return None
    try:
        raw = json.loads(remainder)
    except (ValueError, TypeError):
        return None
    if not isinstance(raw, list):
        return None
    if not all(isinstance(item, str) for item in raw):
        return None
    return sorted(set(raw))


def emit_reads(paths: Iterable[str] | None = None, stream: IO[str] | None = None) -> None:
    """Write one reads line and flush so the parent sees it before teardown."""

    if paths is None:
        with _lock:
            paths = sorted(_recorded)
    target = stream if stream is not None else sys.stdout
    target.write(format_reads_line(paths))
    target.write("\n")
    with suppress(Exception):
        target.flush()


def recorded_reads() -> list[str]:
    """Return the sorted absolute paths observed so far (in-process inspection)."""

    with _lock:
        return sorted(_recorded)


def install_read_recorder(*, emit_at_exit: bool = True) -> None:
    """Monkeypatch the ``open`` family to record read paths; idempotent.

    Patches :func:`builtins.open`, :func:`io.open`, and :meth:`pathlib.Path.open`.
    All three are required: ``pathlib``'s accessor caches its own reference to the
    original ``io.open`` at import time, so patching ``io.open`` alone would miss
    reads made through ``Path.open``.

    This mutates process-global state and must only run inside a child the SDK
    owns -- never in the parent, whose own file I/O must stay unpatched.
    """

    global _installed
    with _lock:
        if _installed:
            return
        _installed = True

    def _open(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        _record(file, mode)
        return _real_open(file, mode, *args, **kwargs)

    def _path_open(self: pathlib.Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        _record(self, mode)
        return _real_path_open(self, mode, *args, **kwargs)

    builtins.open = _open  # type: ignore
    io.open = _open  # type: ignore
    pathlib.Path.open = _path_open  # type: ignore

    if emit_at_exit:
        atexit.register(emit_reads)


__all__ = [
    "READS_SENTINEL",
    "emit_reads",
    "format_reads_line",
    "install_read_recorder",
    "parse_reads_line",
    "recorded_reads",
]
