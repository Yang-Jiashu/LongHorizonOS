"""TaskRequirements decoding.

Per Section 10 the Scheduler does NOT read free-text Task descriptions and
does NOT invoke models to infer fit.  It reads ONLY the structured
``scheduler`` section of the TaskNode payload.
"""

from __future__ import annotations

from .models import TaskRequirements


def decode_task_requirements(
    task_id: str,
    task_node_payload: dict,
) -> TaskRequirements:
    """Build a TaskRequirements from a TaskNode's payload dict.

    The payload dict is the JSON stored in graph_nodes_projection for the
    TaskNode.  The Scheduling contract lives under the ``scheduler`` key of
    ``metadata``.
    """
    meta = task_node_payload.get("metadata") or {}
    sched = meta.get("scheduler") if isinstance(meta, dict) else {}
    sdk = meta.get("sdk") if isinstance(meta, dict) else {}
    if not isinstance(sched, dict):
        sched = {}
    if not isinstance(sdk, dict):
        sdk = {}

    return TaskRequirements(
        task_id=task_id,
        task_kind=sched.get("task_kind", "") or task_node_payload.get("task_kind", ""),
        preferred_agent=str(sdk.get("agent", "")),
        required_specializations=_to_tuple(sched.get("required_specializations")),
        preferred_specializations=_to_tuple(sched.get("preferred_specializations")),
        required_tools=_to_tuple(sched.get("required_tools")),
        required_capabilities=_to_tuple(sched.get("required_capabilities")),
        priority=_as_int(sched.get("priority"), 0),
        estimated_cost=_as_int(sched.get("estimated_cost"), 0),
        max_attempts=_as_int_or_none(sched.get("max_attempts")),
    )


def _to_tuple(v: object) -> tuple[str, ...]:
    if v is None:
        return ()
    if isinstance(v, (list, tuple)):
        return tuple(str(x) for x in v if str(x))
    return (str(v),) if v else ()


def _as_int(v: object, default: int) -> int:
    if v is None or not isinstance(v, (int, float, str, bytes, bytearray)):
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _as_int_or_none(v: object) -> int | None:
    if v is None:
        return None
    if not isinstance(v, (int, float, str, bytes, bytearray)):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None
