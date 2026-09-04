"""Compare a child's observed file reads against a task's declared inputs.

The conflict graph, the safe-parallelism decisions, and invalidation in this
system are all derived from *declared* task read/write sets, on the stated
premise that "undeclared reads cannot be inferred".  For work that runs in a
child process the SDK controls, that premise is no longer absolute: a child's
Python-level reads are observable (see :mod:`lhos.sdk.read_recorder`), so the
declared read set can be checked against what actually happened.

This module is pure and deterministic.  Given observed absolute paths, a task's
declared inputs, and an explicit workspace root, it returns sorted diffs.  It
performs no I/O and consults no clock.

**Path normalization is the crux.**  Declared identities look like
``workspace://sub/file``, a bare artifact id, or an absolute path; observations
are always absolute paths.  Both sides are mapped to an absolute path and
compared case-foldedly via :func:`os.path.normcase` (so Windows drive/case
differences do not masquerade as undeclared reads).  The workspace root is taken
as an explicit argument and never guessed.

**Fail-closed.**  If a child produced no observation at all -- the recorder was
not installed, or the child died before its ``atexit`` report -- the result is
*unobserved*: :attr:`UndeclaredReadReport.undeclared_reads` is ``None``, never an
empty tuple.  A false "all declared" is worse than no metric, because the
conflict graph's soundness would look confirmed when it was never checked.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

from .runtime_state import UnavailableField

_WORKSPACE_SCHEME: Final[str] = "workspace://"
_UNOBSERVED_REASON: Final[str] = (
    "child produced no read observation (recorder not installed, or the child "
    "exited before its atexit report); undeclared reads are unobserved, not zero"
)


class UndeclaredReadReport(BaseModel):
    """Deterministic diff of observed reads against declared task inputs.

    When ``observed`` is ``False`` the comparison never ran against real data:
    ``undeclared_reads`` and ``declared_but_unread`` are ``None`` (not empty
    tuples) and ``unavailable`` explains why.  When ``observed`` is ``True`` the
    two diff fields are sorted tuples of absolute paths, possibly empty.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    observed: bool
    observed_reads: tuple[str, ...] = ()
    undeclared_reads: tuple[str, ...] | None = None
    declared_but_unread: tuple[str, ...] | None = None
    unavailable: UnavailableField | None = None


def _normalize_workspace_root(workspace_root: str) -> str:
    if not isinstance(workspace_root, str) or not workspace_root.strip():
        raise ValueError("workspace_root must be a non-empty path")
    return os.path.abspath(workspace_root)


def _join_workspace_relative(root: str, relative: str) -> str:
    parts = [part for part in relative.replace("\\", "/").split("/") if part not in ("", ".")]
    if not parts:
        return root
    return os.path.abspath(os.path.join(root, *parts))


def normalize_declared_input(identity: str, workspace_root: str) -> str:
    """Map one declared input identity to an absolute filesystem path.

    Three forms are accepted, matching how identities appear in this system:

    * ``workspace://sub/dir/file`` -- workspace-scheme; the remainder is a path
      relative to ``workspace_root``.
    * ``/abs/path`` (or a drive-absolute path on Windows) -- used as given.
    * ``bare-artifact-id`` -- any other string; treated as a name relative to
      ``workspace_root``.
    """

    root = _normalize_workspace_root(workspace_root)
    text = identity.strip()
    if text.startswith(_WORKSPACE_SCHEME):
        return _join_workspace_relative(root, text[len(_WORKSPACE_SCHEME) :])
    if os.path.isabs(text):
        return os.path.abspath(text)
    return _join_workspace_relative(root, text)


def _key(path: str) -> str:
    return os.path.normcase(path)


def compare_reads(
    observed_reads: Sequence[str] | None,
    declared_inputs: Iterable[str],
    *,
    workspace_root: str,
) -> UndeclaredReadReport:
    """Diff observed reads against declared inputs, resolved under ``workspace_root``.

    ``observed_reads`` is a sequence of absolute paths, or ``None`` when the
    attempt produced no observation.  ``None`` yields a fail-closed *unobserved*
    report; an empty sequence is a real observation of "read nothing".
    """

    root = _normalize_workspace_root(workspace_root)

    declared_by_key: dict[str, str] = {}
    for identity in declared_inputs:
        normalized = normalize_declared_input(identity, root)
        declared_by_key.setdefault(_key(normalized), normalized)

    if observed_reads is None:
        return UndeclaredReadReport(
            observed=False,
            observed_reads=(),
            undeclared_reads=None,
            declared_but_unread=None,
            unavailable=UnavailableField(name="undeclared_reads", reason=_UNOBSERVED_REASON),
        )

    observed_by_key: dict[str, str] = {}
    for path in observed_reads:
        normalized = os.path.abspath(str(path))
        observed_by_key.setdefault(_key(normalized), normalized)

    declared_keys = set(declared_by_key)
    observed_keys = set(observed_by_key)

    undeclared = tuple(sorted(observed_by_key[key] for key in observed_keys - declared_keys))
    unread = tuple(sorted(declared_by_key[key] for key in declared_keys - observed_keys))

    return UndeclaredReadReport(
        observed=True,
        observed_reads=tuple(sorted(observed_by_key.values())),
        undeclared_reads=undeclared,
        declared_but_unread=unread,
        unavailable=None,
    )


def report_from_usage(
    usage: Mapping[str, Any],
    declared_inputs: Iterable[str],
    *,
    workspace_root: str,
) -> UndeclaredReadReport:
    """Build a report from a ``_ChildProcess.usage()`` dict.

    ``usage['observed_reads']`` is a sorted list of absolute paths when the child
    reported reads, or ``None`` when nothing was observed (fail-closed).  A value
    of any other type is treated as unobserved rather than trusted.
    """

    observed = usage.get("observed_reads")
    if observed is not None and not isinstance(observed, (list, tuple)):
        observed = None
    return compare_reads(observed, declared_inputs, workspace_root=workspace_root)


__all__ = [
    "UndeclaredReadReport",
    "compare_reads",
    "normalize_declared_input",
    "report_from_usage",
]
