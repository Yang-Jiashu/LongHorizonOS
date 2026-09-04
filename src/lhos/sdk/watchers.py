"""Explicit workspace observation and semantic-interrupt polling.

The watcher is deliberately bounded.  It polls only resources supplied by
the caller, hashes the exact bytes it reads, and issues an authority-backed
``ObservationToken`` for a changed file.  It does not intercept arbitrary
``open`` calls, network traffic, browser state, or subprocess I/O.

This module turns an explicit observation boundary into a small online loop:

``poll -> observe changed bytes -> emit ARTIFACT_CHANGED interrupt``.

The returned interrupt is still a proposal input for ``AgentOS.plan_interrupts``;
the watcher never claims work, releases a Lease, or force-stops a callback.
Deletion is reported without minting a synthetic version and therefore remains
fail-closed until a caller supplies a new authoritative observation.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .observation import ObservationToken
from .semantic_interrupt import (
    InterruptEpoch,
    SemanticInterrupt,
    SemanticInterruptKind,
)

WORKSPACE_WATCHER_SCHEMA_VERSION: Final[Literal["workspace-watcher.v1"]] = "workspace-watcher.v1"


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class WorkspaceChangeKind(StrEnum):
    """Result for one explicitly watched workspace resource."""

    INITIALIZED = "initialized"
    UNCHANGED = "unchanged"
    CHANGED = "changed"
    DELETED = "deleted"
    ERROR = "error"


class WorkspaceObservationChange(_FrozenModel):
    """Immutable observation result for one poll cycle."""

    schema_version: Literal["workspace-watcher.v1"] = WORKSPACE_WATCHER_SCHEMA_VERSION
    goal_id: str = Field(min_length=1)
    graph_id: str = ""
    artifact_id: str = Field(min_length=1)
    resource_uri: str = Field(min_length=1)
    kind: WorkspaceChangeKind
    previous_observation: ObservationToken | None = None
    observation: ObservationToken | None = None
    content_hash: str | None = None
    error: str | None = None
    observed_at: datetime = Field(default_factory=_utcnow)

    @field_validator("goal_id", "artifact_id", "resource_uri", mode="before")
    @classmethod
    def _non_empty_identity(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("workspace watcher identities must be non-empty")
        return normalized

    @field_validator("graph_id", mode="before")
    @classmethod
    def _normalize_graph_id(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("content_hash", mode="before")
    @classmethod
    def _normalize_hash(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().lower()
        if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
            raise ValueError("content_hash must be a SHA-256 hex digest")
        return normalized

    @field_validator("error", mode="before")
    @classmethod
    def _normalize_error(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @property
    def changed(self) -> bool:
        """Whether this poll observed a semantic input change."""

        return self.kind in {WorkspaceChangeKind.CHANGED, WorkspaceChangeKind.DELETED}


class WorkspaceWatchPoll(_FrozenModel):
    """One deterministic watcher result plus optional interrupt planning."""

    schema_version: Literal["workspace-watcher.v1"] = WORKSPACE_WATCHER_SCHEMA_VERSION
    goal_id: str = Field(min_length=1)
    graph_id: str = ""
    changes: tuple[WorkspaceObservationChange, ...] = ()
    # ``repair_outcomes`` contains bounded ``RepairOutcome.as_dict()``
    # projections for changes that were explicitly reconciled into the VPG.
    # Keeping this as JSON-shaped data avoids coupling the immutable watcher
    # DTO to the mutable SDK result dataclass.
    repair_outcomes: tuple[dict[str, Any], ...] = ()
    interrupts: tuple[SemanticInterrupt, ...] = ()
    epoch: InterruptEpoch | None = None

    @field_validator("goal_id", mode="before")
    @classmethod
    def _goal_non_empty(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("goal_id must be non-empty")
        return normalized

    @field_validator("graph_id", mode="before")
    @classmethod
    def _graph_normalized(cls, value: Any) -> str:
        return str(value or "").strip()


class WorkspaceInterruptRoute(_FrozenModel):
    """Result of one explicit watcher -> Harness control pass.

    This is deliberately a one-shot, caller-invoked bridge.  The watcher
    creates and validates graph-bound :class:`SemanticInterrupt` values, the
    policy derives actions, and only exact active Attempt identities are
    forwarded to ``AgentOS.deliver_interrupt``.  A route result is an audit
    projection; it never claims work, creates a Lease, or force-kills a
    callback.
    """

    schema_version: Literal["workspace-watcher.v1"] = WORKSPACE_WATCHER_SCHEMA_VERSION
    goal_id: str = Field(min_length=1)
    graph_id: str = ""
    poll: WorkspaceWatchPoll
    epoch: InterruptEpoch
    deliveries: tuple[dict[str, Any], ...] = ()
    blocked: tuple[dict[str, Any], ...] = ()
    reconcile_outcomes: tuple[dict[str, Any], ...] = ()
    reconcile_error: str | None = None

    @field_validator("goal_id", mode="before")
    @classmethod
    def _goal_non_empty(cls, value: Any) -> str:
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("goal_id must be non-empty")
        return normalized

    @field_validator("graph_id", mode="before")
    @classmethod
    def _graph_normalized(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("reconcile_error", mode="before")
    @classmethod
    def _error_normalized(cls, value: Any) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None


class WorkspaceObservationWatcher:
    """Poll explicitly declared workspace files and issue observation tokens.

    ``task_ids_by_resource`` is optional.  When omitted and a concrete Goal
    object is supplied, task ``inputs``/``outputs`` declarations are used to
    derive affected tasks.  A resource without a declared consumer still
    yields an interrupt with an empty target set; the interrupt policy then
    keeps it unhandled rather than broadening the repair scope.
    """

    def __init__(
        self,
        agent_os: Any,
        goal: Any,
        workspace: Any,
        resources: Iterable[str],
        *,
        task_ids_by_resource: Mapping[str, Iterable[str]] | None = None,
    ) -> None:
        if agent_os is None:
            raise TypeError("agent_os is required")
        if not callable(getattr(agent_os, "observe_workspace_artifact", None)):
            raise TypeError("agent_os must expose observe_workspace_artifact")
        if not callable(getattr(agent_os, "plan_interrupts", None)):
            raise TypeError("agent_os must expose plan_interrupts")
        if not callable(getattr(workspace, "read_bytes", None)):
            raise TypeError("workspace must expose read_bytes(rel)")
        if not callable(getattr(workspace, "resolve", None)):
            raise TypeError("workspace must expose resolve(rel)")

        self._agent_os = agent_os
        self._goal = goal
        self._goal_id = str(getattr(goal, "goal_id", goal)).strip()
        if not self._goal_id:
            raise ValueError("goal must identify a non-empty goal_id")
        self._workspace = workspace
        self._resources = self._normalize_resources(workspace, resources)
        explicit_task_map = self._normalize_task_map(
            workspace,
            task_ids_by_resource or {},
        )
        self._task_ids_by_resource = (
            explicit_task_map
            if task_ids_by_resource is not None
            else self._derive_task_map(workspace, goal)
        )
        # Explicit mappings are a convenience, not an authority.  Reject
        # unknown task ids up front when the Goal object is concrete; this
        # prevents an interrupt-only path from advertising a scope that the
        # reconciliation gateway would later reject.
        known_task_ids = {
            str(getattr(task, "task_id", "")).strip()
            for task in tuple(getattr(goal, "tasks", ()) or ())
            if str(getattr(task, "task_id", "")).strip()
        }
        unknown_mapped = sorted(
            {
                task_id
                for task_ids in self._task_ids_by_resource.values()
                for task_id in task_ids
                if known_task_ids and task_id not in known_task_ids
            }
        )
        if unknown_mapped:
            raise ValueError(
                "task_ids_by_resource contains unknown Goal tasks: " + ", ".join(unknown_mapped)
            )
        if known_task_ids:
            declared_by_task = {
                task_id: {
                    self._normalize_resource(workspace, resource)
                    for resource in (
                        tuple(getattr(task, "inputs", ()) or ())
                        + tuple(getattr(task, "outputs", ()) or ())
                    )
                    if str(resource).strip()
                }
                for task_id, task in (
                    (str(getattr(task, "task_id", "")).strip(), task)
                    for task in tuple(getattr(goal, "tasks", ()) or ())
                    if str(getattr(task, "task_id", "")).strip()
                )
            }
            undeclared_mapped = sorted(
                {
                    f"{resource}:{task_id}"
                    for resource, task_ids in self._task_ids_by_resource.items()
                    for task_id in task_ids
                    if resource not in declared_by_task.get(task_id, set())
                }
            )
            if undeclared_mapped:
                raise ValueError(
                    "task_ids_by_resource contains tasks that do not declare "
                    "the watched resource: " + ", ".join(undeclared_mapped)
                )
        self._baseline: dict[str, ObservationToken] = {}
        # ``poll`` advances the in-memory baseline before the semantic
        # reconciliation call.  Keep failed transitions so a transient
        # reconciliation error cannot turn the next poll into a silent
        # ``UNCHANGED`` result.
        self._pending_changes: dict[str, WorkspaceObservationChange] = {}
        self._missing: set[str] = set()
        self._initialized = False

    @staticmethod
    def _normalize_resource(workspace: Any, resource: str) -> str:
        raw = str(resource).strip()
        if not raw:
            raise ValueError("watched workspace resources must be non-empty")
        for prefix in ("workspace://", "vpg://workspace/"):
            if raw.startswith(prefix):
                raw = raw[len(prefix) :]
                if raw.startswith("/"):
                    raw = raw.lstrip("/")
                break
        try:
            resolved = workspace.resolve(raw)
            root = getattr(workspace, "root", None)
            if root is not None:
                relative = resolved.relative_to(root)
                return str(relative.as_posix())
            return str(resolved)
        except Exception as exc:
            raise ValueError(f"invalid watched workspace resource {raw!r}: {exc}") from exc

    @classmethod
    def _normalize_resources(cls, workspace: Any, resources: Iterable[str]) -> tuple[str, ...]:
        if isinstance(resources, str):
            resources = (resources,)
        normalized = {cls._normalize_resource(workspace, item) for item in resources}
        if not normalized:
            raise ValueError("at least one workspace resource must be watched")
        return tuple(sorted(normalized))

    @classmethod
    def _normalize_task_map(
        cls,
        workspace: Any,
        task_map: Mapping[str, Iterable[str]],
    ) -> dict[str, tuple[str, ...]]:
        normalized: dict[str, tuple[str, ...]] = {}
        for resource, task_ids in task_map.items():
            key = cls._normalize_resource(workspace, resource)
            if isinstance(task_ids, str):
                task_ids = (task_ids,)
            values = tuple(sorted({str(item).strip() for item in task_ids if str(item).strip()}))
            if values:
                normalized[key] = values
        return normalized

    @classmethod
    def _derive_task_map(
        cls,
        workspace: Any,
        goal: Any,
    ) -> dict[str, tuple[str, ...]]:
        """Derive consumers from explicit Goal task input/output declarations.

        This is intentionally not semantic inference: only strings already
        present in ``Task.inputs``/``Task.outputs`` are considered.  A task
        with no declarations contributes no watcher edge.
        """

        collected: dict[str, set[str]] = {}
        for task in tuple(getattr(goal, "tasks", ()) or ()):
            task_id = str(getattr(task, "task_id", "")).strip()
            if not task_id:
                continue
            resources = tuple(getattr(task, "inputs", ()) or ()) + tuple(
                getattr(task, "outputs", ()) or ()
            )
            for resource in resources:
                try:
                    normalized = cls._normalize_resource(workspace, resource)
                except (TypeError, ValueError):
                    # A malformed declaration must not make watcher
                    # construction broaden scope or crash an otherwise
                    # explicit resource watch.  The gateway/coverage layer
                    # remains responsible for reporting that declaration.
                    continue
                collected.setdefault(normalized, set()).add(task_id)
        return {
            resource: tuple(sorted(task_ids))
            for resource, task_ids in sorted(collected.items())
            if task_ids
        }

    @property
    def resources(self) -> tuple[str, ...]:
        return self._resources

    @property
    def initialized(self) -> bool:
        return self._initialized

    @property
    def baseline(self) -> tuple[ObservationToken, ...]:
        """Return the current known observations in stable resource order."""

        return tuple(
            self._baseline[resource] for resource in self._resources if resource in self._baseline
        )

    @property
    def pending_changes(self) -> tuple[WorkspaceObservationChange, ...]:
        """Return failed semantic transitions awaiting a later retry."""

        return tuple(
            self._pending_changes[resource]
            for resource in self._resources
            if resource in self._pending_changes
        )

    def initialize(self) -> tuple[WorkspaceObservationChange, ...]:
        """Capture the first exact snapshot for each existing resource."""

        changes: list[WorkspaceObservationChange] = []
        for resource in self._resources:
            changes.append(self._capture_initial(resource))
        self._initialized = True
        return tuple(changes)

    def poll(self) -> tuple[WorkspaceObservationChange, ...]:
        """Poll resources and issue new tokens only when bytes changed."""

        if not self._initialized:
            return self.initialize()
        changes: list[WorkspaceObservationChange] = []
        for resource in self._resources:
            changes.append(self._poll_one(resource))
        return tuple(changes)

    def poll_and_plan(
        self,
        *,
        epoch_id: int = 0,
        persist: bool = False,
    ) -> WorkspaceWatchPoll:
        """Poll, convert changes to interrupts, and route them through policy."""

        changes = self.poll()
        interrupts = tuple(
            interrupt
            for change in changes
            for interrupt in (self.interrupt_for(change),)
            if interrupt is not None
        )
        graph_id = self._graph_id(changes)
        epoch = self._agent_os.plan_interrupts(
            self._goal,
            interrupts,
            epoch_id=epoch_id,
            persist=persist,
        )
        return WorkspaceWatchPoll(
            goal_id=self._goal_id,
            graph_id=graph_id,
            changes=changes,
            interrupts=interrupts,
            epoch=epoch,
        )

    def poll_and_reconcile(
        self,
        *,
        epoch_id: int = 0,
        persist: bool = False,
    ) -> WorkspaceWatchPoll:
        """Poll, reconcile declared changes into VPG, then plan interrupts.

        Only content changes with an authority-backed previous/current token
        pair and explicit declared consumer tasks are reconciled.  Deletions,
        unassigned resources, and read errors remain fail-closed observation
        events; they do not invent versions or broaden the repair scope.
        All changed resources in one poll are reconciled through one batch
        transaction.  It updates semantic state, but does not claim work,
        transfer Leases, force-stop callbacks, or automatically rebase a
        Harness.
        """

        changes = self.poll()
        repair_outcomes: list[dict[str, Any]] = []
        changed_transitions: list[tuple[Any, Any, tuple[str, ...], str]] = []
        submitted_resources: set[str] = set()
        try:
            for change in changes:
                if (
                    change.kind is not WorkspaceChangeKind.CHANGED
                    or change.previous_observation is None
                    or change.observation is None
                ):
                    continue
                task_ids = self._task_ids_by_resource.get(change.artifact_id, ())
                # An explicitly watched but unassigned resource remains an
                # interrupt-only observation.  The OS reconciliation gateway
                # requires at least one declared consumer and will fail closed
                # if called with an arbitrary mapping.
                if not task_ids:
                    continue
                changed_transitions.append(
                    (
                        change.previous_observation,
                        change.observation,
                        task_ids,
                        change.resource_uri,
                    )
                )
                submitted_resources.add(change.artifact_id)
            if changed_transitions:
                batch_reconcile = getattr(
                    self._agent_os,
                    "reconcile_observations",
                    None,
                )
                if callable(batch_reconcile):
                    outcome = self._agent_os.reconcile_observations(
                        self._goal,
                        changed_transitions,
                    )
                elif len(changed_transitions) == 1:
                    # Compatibility for pre-batch AgentOS-like integrations.
                    previous, current, task_ids, resource_uri = changed_transitions[0]
                    outcome = self._agent_os.reconcile_observation(
                        self._goal,
                        observation=current,
                        previous_observation=previous,
                        affected_task_ids=task_ids,
                        resource_uri=resource_uri,
                    )
                else:
                    raise TypeError(
                        "agent_os must expose reconcile_observations for "
                        "multi-resource watcher reconciliation"
                    )
                repair_outcomes.append(outcome.as_dict())
                for previous, current, _task_ids, _resource_uri in changed_transitions:
                    del previous
                    self._pending_changes.pop(current.artifact_id, None)
        except Exception:
            # ``poll`` updates baselines before this method can call the
            # semantic gateway.  Restore every changed resource that was not
            # durably reconciled, including resources after the one that
            # failed, so a later call retries the complete pending transition
            # rather than losing it as an in-memory ``UNCHANGED`` result.
            for change in changes:
                if (
                    change.kind is WorkspaceChangeKind.CHANGED
                    and change.previous_observation is not None
                    and change.observation is not None
                    and change.artifact_id in submitted_resources
                ):
                    self._baseline[change.artifact_id] = change.previous_observation
                    self._missing.discard(change.artifact_id)
                    self._pending_changes[change.artifact_id] = change
            raise

        interrupts = tuple(
            interrupt
            for change in changes
            for interrupt in (self.interrupt_for(change),)
            if interrupt is not None
        )
        graph_id = self._graph_id(changes)
        epoch = self._agent_os.plan_interrupts(
            self._goal,
            interrupts,
            epoch_id=epoch_id,
            persist=persist,
        )
        return WorkspaceWatchPoll(
            goal_id=self._goal_id,
            graph_id=graph_id,
            changes=changes,
            repair_outcomes=tuple(repair_outcomes),
            interrupts=interrupts,
            epoch=epoch,
        )

    def poll_and_route(
        self,
        *,
        epoch_id: int = 0,
        persist: bool = False,
        reconcile_after_delivery: bool = False,
    ) -> WorkspaceInterruptRoute:
        """Poll once and route cooperative actions to exact running Attempts.

        This is the bounded ``watcher -> policy -> Scheduler fence -> Harness``
        seam.  It is not a daemon and does not create ownership.  Delivery is
        intentionally attempted *before* optional reconciliation: advancing
        the VPG first would make the running Attempt's graph snapshot stale and
        the existing ``deliver_interrupt`` fence would correctly reject it.
        """

        changes = self.poll()
        return self.route_observation(
            changes,
            epoch_id=epoch_id,
            persist=persist,
            reconcile_after_delivery=reconcile_after_delivery,
        )

    def route_observation(
        self,
        observation: WorkspaceWatchPoll | Iterable[WorkspaceObservationChange],
        *,
        epoch_id: int = 0,
        persist: bool = False,
        reconcile_after_delivery: bool = False,
    ) -> WorkspaceInterruptRoute:
        """Route one supplied watcher observation through the live SDK.

        ``observation`` may be a prior ``WorkspaceWatchPoll`` or an explicit
        sequence of immutable changes.  The sequence is revalidated against
        this watcher's goal/resources before any Harness control request is
        sent.  Deleted/error/unassigned/unknown changes remain fail-closed and
        are returned in ``blocked`` rather than being broadened to the whole
        graph.
        """

        if isinstance(observation, WorkspaceWatchPoll):
            if observation.goal_id != self._goal_id:
                raise ValueError("watch observation goal_id does not match watcher")
            changes = tuple(observation.changes)
            supplied_interrupts = tuple(observation.interrupts)
        else:
            if isinstance(observation, (str, bytes, Mapping)):
                raise TypeError("watch observation must be a poll or iterable of changes")
            changes = tuple(observation)
            supplied_interrupts = ()
        for change in changes:
            if not isinstance(change, WorkspaceObservationChange):
                raise TypeError("watch observation contains an invalid change")
            if change.goal_id != self._goal_id:
                raise ValueError("watch change goal_id does not match watcher")
            if change.artifact_id not in self._resources:
                raise ValueError(f"watch change resource {change.artifact_id!r} is not watched")

        graph_id = self._graph_id(changes)
        if (
            isinstance(observation, WorkspaceWatchPoll)
            and observation.graph_id
            and observation.graph_id != graph_id
        ):
            raise ValueError("watch observation graph_id does not match watcher")

        interrupts: list[SemanticInterrupt] = []
        blocked: list[dict[str, Any]] = []
        supplied_by_id: dict[str, SemanticInterrupt] = {}
        supplied_change_artifacts: set[str] = set()
        changes_by_artifact = {change.artifact_id: change for change in changes}
        for interrupt in supplied_interrupts:
            if not isinstance(interrupt, SemanticInterrupt):
                raise TypeError("watch observation contains an invalid interrupt")
            previous = supplied_by_id.get(interrupt.interrupt_id)
            if previous is not None:
                if previous != interrupt:
                    raise ValueError(
                        f"watch observation contains conflicting interrupt "
                        f"{interrupt.interrupt_id!r}"
                    )
                continue
            supplied_by_id[interrupt.interrupt_id] = interrupt
            if interrupt.graph_id != graph_id:
                blocked.append(
                    {
                        "interrupt_id": interrupt.interrupt_id,
                        "reason": "interrupt_graph_mismatch",
                    }
                )
                continue
            if not interrupt.affected_task_ids and not interrupt.affected_attempt_ids:
                blocked.append(
                    {
                        "interrupt_id": interrupt.interrupt_id,
                        "reason": "no_explicit_interrupt_targets",
                    }
                )
                continue
            if interrupt.kind is SemanticInterruptKind.ARTIFACT_CHANGED:
                # An ARTIFACT_CHANGED value routed through this workspace
                # boundary must remain bound to an exact non-deletion change
                # observed by this watcher.  Other interrupt kinds (for
                # example WRITE_CONFLICT or AGENT_STALLED) are valid
                # graph-bound runtime events and need not be derivable from a
                # file hash transition.
                artifact_id = str(interrupt.metadata.get("artifact_id", "")).strip()
                bound_change = changes_by_artifact.get(artifact_id)
                declared_targets = set(self._task_ids_by_resource.get(artifact_id, ()))
                if (
                    interrupt.metadata.get("watcher") != WORKSPACE_WATCHER_SCHEMA_VERSION
                    or bound_change is None
                    or bound_change.kind is not WorkspaceChangeKind.CHANGED
                    or bound_change.previous_observation is None
                    or bound_change.observation is None
                    or not set(interrupt.affected_task_ids).issubset(declared_targets)
                    or bool(interrupt.affected_attempt_ids)
                    or interrupt.metadata.get("previous_hash")
                    != bound_change.previous_observation.content_hash
                    or interrupt.metadata.get("new_hash") != bound_change.observation.content_hash
                ):
                    blocked.append(
                        {
                            "interrupt_id": interrupt.interrupt_id,
                            "artifact_id": artifact_id,
                            "reason": "artifact_interrupt_not_bound_to_valid_change",
                        }
                    )
                    continue
                supplied_change_artifacts.add(artifact_id)
            interrupts.append(interrupt)

        for change in changes:
            derived_interrupt = self.interrupt_for(change)
            if derived_interrupt is None:
                if change.changed or change.kind is WorkspaceChangeKind.ERROR:
                    blocked.append(
                        {
                            "artifact_id": change.artifact_id,
                            "kind": change.kind.value,
                            "reason": (
                                "unknown_or_incomplete_observation"
                                if change.kind is not WorkspaceChangeKind.ERROR
                                else (change.error or "observation_error")
                            ),
                        }
                    )
                continue
            if change.artifact_id in supplied_change_artifacts:
                # Re-routing a prior WorkspaceWatchPoll must preserve the
                # original interrupt identity instead of manufacturing a
                # duplicate ARTIFACT_CHANGED event for the same transition.
                continue
            # Deletion has no authoritative replacement version.  It remains
            # an observation/repair input but must never be delivered as if a
            # safe rebase target were known.
            if change.kind is WorkspaceChangeKind.DELETED:
                blocked.append(
                    {
                        "interrupt_id": derived_interrupt.interrupt_id,
                        "artifact_id": change.artifact_id,
                        "reason": "deleted_observation_fail_closed",
                    }
                )
                continue
            if (
                not derived_interrupt.affected_task_ids
                and not derived_interrupt.affected_attempt_ids
            ):
                blocked.append(
                    {
                        "interrupt_id": derived_interrupt.interrupt_id,
                        "artifact_id": change.artifact_id,
                        "reason": "no_explicit_consumers",
                    }
                )
                continue
            interrupts.append(derived_interrupt)

        epoch = self._agent_os.plan_interrupts(
            self._goal,
            tuple(interrupts),
            epoch_id=epoch_id,
            persist=persist,
        )
        if graph_id and epoch.graph_id != graph_id:
            raise ValueError("watch observation graph_id does not match policy epoch")

        # Index exact attempts from the authoritative Scheduler projection.
        attempts = tuple(getattr(getattr(self._agent_os, "scheduler", None), "attempts", ()))
        deliveries: list[dict[str, Any]] = []
        delivered_keys: set[tuple[str, str]] = set()
        for decision in epoch.decisions:
            action = getattr(decision.action, "value", str(decision.action))
            if action not in {"preempt", "rebase"}:
                blocked.append(
                    {
                        "target_kind": decision.target_kind,
                        "target_id": decision.target_id,
                        "action": action,
                        "reason": "policy_action_not_deliverable",
                    }
                )
                continue
            candidates = [
                attempt
                for attempt in attempts
                if (
                    (decision.target_kind == "attempt" and attempt.attempt_id == decision.target_id)
                    or (decision.target_kind == "task" and attempt.task_id == decision.target_id)
                )
            ]
            if not candidates:
                blocked.append(
                    {
                        "target_kind": decision.target_kind,
                        "target_id": decision.target_id,
                        "action": action,
                        "reason": "no_exact_running_attempt",
                    }
                )
                continue
            for attempt in sorted(candidates, key=lambda item: item.attempt_id):
                key = (str(attempt.claim_id), str(attempt.attempt_id))
                if key in delivered_keys:
                    continue
                delivered_keys.add(key)
                if int(getattr(attempt, "graph_version", epoch.graph_version)) != int(
                    epoch.graph_version
                ):
                    blocked.append(
                        {
                            "target_kind": decision.target_kind,
                            "target_id": decision.target_id,
                            "attempt_id": attempt.attempt_id,
                            "action": action,
                            "reason": "attempt_graph_version_stale",
                        }
                    )
                    continue
                interrupt_id = decision.interrupt_ids[0] if decision.interrupt_ids else ""
                reason = (
                    decision.reasons[0] if decision.reasons else "workspace observation changed"
                )
                try:
                    delivery = self._agent_os.deliver_interrupt(
                        self._goal,
                        claim_id=attempt.claim_id,
                        task_id=attempt.task_id,
                        attempt_id=attempt.attempt_id,
                        action=action,
                        expected_graph_version=int(epoch.graph_version),
                        expected_semantic_epoch=int(attempt.semantic_epoch),
                        interrupt_id=interrupt_id,
                        decision_hash=epoch.decision_hash,
                        reason=reason,
                    )
                    projection = _delivery_projection(delivery)
                    if bool(getattr(delivery, "accepted", False)):
                        deliveries.append(projection)
                    else:
                        blocked.append(
                            {
                                "target_kind": decision.target_kind,
                                "target_id": decision.target_id,
                                "attempt_id": attempt.attempt_id,
                                "action": action,
                                "reason": "delivery_rejected",
                                "delivery_status": projection.get("status", "unknown"),
                                "delivery": projection,
                            }
                        )
                except Exception as exc:
                    blocked.append(
                        {
                            "target_kind": decision.target_kind,
                            "target_id": decision.target_id,
                            "attempt_id": attempt.attempt_id,
                            "action": action,
                            "reason": _error_text(exc),
                        }
                    )

        reconcile_outcomes: list[dict[str, Any]] = []
        reconcile_error: str | None = None
        # ``poll()`` advances the in-memory baseline before this optional
        # semantic commit.  Keep the exact transitions submitted to the
        # authority so a failed commit (including a response lost after a
        # durable commit) cannot make the next poll silently report
        # ``UNCHANGED`` and lose the repair input.
        submitted_changes = tuple(
            change
            for change in changes
            if (
                change.kind is WorkspaceChangeKind.CHANGED
                and change.previous_observation is not None
                and change.observation is not None
                and self._task_ids_by_resource.get(change.artifact_id, ())
            )
        )
        if reconcile_after_delivery:
            transitions = tuple(
                (
                    change.previous_observation,
                    change.observation,
                    self._task_ids_by_resource.get(change.artifact_id, ()),
                    change.resource_uri,
                )
                for change in changes
                if (
                    change.kind is WorkspaceChangeKind.CHANGED
                    and change.previous_observation is not None
                    and change.observation is not None
                    and self._task_ids_by_resource.get(change.artifact_id, ())
                )
            )
            if transitions:
                try:
                    reconcile = getattr(
                        self._agent_os,
                        "reconcile_observations",
                        None,
                    )
                    if callable(reconcile):
                        outcome = reconcile(self._goal, transitions)
                    elif len(transitions) == 1:
                        previous_token, current_token, task_ids, resource_uri = transitions[0]
                        outcome = self._agent_os.reconcile_observation(
                            self._goal,
                            observation=current_token,
                            previous_observation=previous_token,
                            affected_task_ids=task_ids,
                            resource_uri=resource_uri,
                        )
                    else:
                        raise TypeError(
                            "agent_os must expose reconcile_observations for "
                            "multi-resource watcher routing"
                        )
                    reconcile_outcomes.append(outcome.as_dict())
                    for change in submitted_changes:
                        self._pending_changes.pop(change.artifact_id, None)
                except Exception as exc:
                    reconcile_error = _error_text(exc)
                    blocked.append(
                        {
                            "reason": "reconcile_failed_after_delivery",
                            "error": reconcile_error,
                        }
                    )
                    # Restore only transitions that were submitted to the
                    # semantic authority.  Deletions, read errors, and
                    # explicitly unassigned resources remain observation-only
                    # and must not be widened into a repair retry.
                    for change in submitted_changes:
                        assert change.previous_observation is not None
                        self._baseline[change.artifact_id] = change.previous_observation
                        self._missing.discard(change.artifact_id)
                        self._pending_changes[change.artifact_id] = change

        # Always return a poll projection tied to the epoch planned by this
        # route.  If the caller supplied an earlier ``WorkspaceWatchPoll``,
        # retaining its old epoch would make the audit object internally
        # inconsistent (and could expose stale decisions to a replaying
        # caller).  Changes/repair outcomes are preserved; interrupts are the
        # validated, fail-closed subset actually submitted to policy.
        prior_repair_outcomes = (
            observation.repair_outcomes if isinstance(observation, WorkspaceWatchPoll) else ()
        )
        poll = WorkspaceWatchPoll(
            goal_id=self._goal_id,
            graph_id=graph_id,
            changes=changes,
            repair_outcomes=prior_repair_outcomes,
            interrupts=tuple(interrupts),
            epoch=epoch,
        )
        return WorkspaceInterruptRoute(
            goal_id=self._goal_id,
            graph_id=epoch.graph_id,
            poll=poll,
            epoch=epoch,
            deliveries=tuple(deliveries),
            blocked=tuple(blocked),
            reconcile_outcomes=tuple(reconcile_outcomes),
            reconcile_error=reconcile_error,
        )

    def interrupt_for(
        self,
        change: WorkspaceObservationChange,
    ) -> SemanticInterrupt | None:
        """Convert a changed/deleted transition into a bounded interrupt."""

        if change.kind is WorkspaceChangeKind.CHANGED:
            if change.previous_observation is None or change.observation is None:
                return None
        elif change.kind is WorkspaceChangeKind.DELETED:
            # A first poll of a missing file is not a mutation of a known
            # observation.  Only a deletion after an observed baseline emits
            # an interrupt.
            if change.previous_observation is None:
                return None
        else:
            return None

        previous = change.previous_observation
        task_ids = self._task_ids_by_resource.get(change.artifact_id, ())
        metadata: dict[str, Any] = {
            "watcher": WORKSPACE_WATCHER_SCHEMA_VERSION,
            "resource_uri": change.resource_uri,
            "artifact_id": change.artifact_id,
            "deleted": change.kind is WorkspaceChangeKind.DELETED,
            "previous_version": previous.version if previous is not None else None,
            "previous_hash": previous.content_hash if previous is not None else None,
        }
        if change.observation is not None:
            metadata.update(
                {
                    "new_version": change.observation.version,
                    "new_hash": change.observation.content_hash,
                }
            )
        if change.error:
            metadata["error"] = change.error
        return SemanticInterrupt(
            graph_id=change.graph_id or self._graph_id(()),
            graph_version=self._current_graph_version(),
            kind=SemanticInterruptKind.ARTIFACT_CHANGED,
            reason=(
                f"workspace artifact {change.artifact_id!r} was deleted"
                if change.kind is WorkspaceChangeKind.DELETED
                else f"workspace artifact {change.artifact_id!r} changed"
            ),
            affected_task_ids=task_ids,
            metadata=metadata,
        )

    def _capture_initial(self, resource: str) -> WorkspaceObservationChange:
        try:
            payload = self._workspace.read_bytes(resource)
        except FileNotFoundError:
            self._missing.add(resource)
            return self._change(
                resource,
                WorkspaceChangeKind.DELETED,
                error="workspace resource is missing at initialization",
            )
        except Exception as exc:
            return self._change(resource, WorkspaceChangeKind.ERROR, error=_error_text(exc))

        digest = hashlib.sha256(bytes(payload)).hexdigest()
        try:
            token = self._agent_os.observe_workspace_artifact(
                self._goal,
                self._workspace,
                resource,
                expected_hash=digest,
            )
        except Exception as exc:
            return self._change(resource, WorkspaceChangeKind.ERROR, error=_error_text(exc))
        self._baseline[resource] = token
        self._missing.discard(resource)
        return self._change(
            resource,
            WorkspaceChangeKind.INITIALIZED,
            observation=token,
            content_hash=token.content_hash,
        )

    def _poll_one(self, resource: str) -> WorkspaceObservationChange:
        previous = self._baseline.get(resource)
        try:
            payload = self._workspace.read_bytes(resource)
        except FileNotFoundError:
            if resource in self._missing:
                return self._change(
                    resource,
                    WorkspaceChangeKind.UNCHANGED,
                    previous_observation=previous,
                    error="workspace resource remains missing",
                )
            self._missing.add(resource)
            return self._change(
                resource,
                WorkspaceChangeKind.DELETED,
                previous_observation=previous,
                error="workspace resource was deleted",
            )
        except Exception as exc:
            return self._change(
                resource,
                WorkspaceChangeKind.ERROR,
                previous_observation=previous,
                error=_error_text(exc),
            )

        digest = hashlib.sha256(bytes(payload)).hexdigest()
        if previous is not None and digest == previous.content_hash:
            self._missing.discard(resource)
            return self._change(
                resource,
                WorkspaceChangeKind.UNCHANGED,
                previous_observation=previous,
                content_hash=digest,
            )

        try:
            token = self._agent_os.observe_workspace_artifact(
                self._goal,
                self._workspace,
                resource,
                expected_hash=digest,
            )
        except Exception as exc:
            return self._change(
                resource,
                WorkspaceChangeKind.ERROR,
                previous_observation=previous,
                content_hash=digest,
                error=_error_text(exc),
            )
        self._baseline[resource] = token
        self._missing.discard(resource)
        return self._change(
            resource,
            WorkspaceChangeKind.CHANGED
            if previous is not None
            else WorkspaceChangeKind.INITIALIZED,
            previous_observation=previous,
            observation=token,
            content_hash=token.content_hash,
        )

    def _change(
        self,
        resource: str,
        kind: WorkspaceChangeKind,
        *,
        previous_observation: ObservationToken | None = None,
        observation: ObservationToken | None = None,
        content_hash: str | None = None,
        error: str | None = None,
    ) -> WorkspaceObservationChange:
        graph_id = (
            observation.graph_id
            if observation is not None
            else previous_observation.graph_id
            if previous_observation is not None
            else self._graph_id(())
        )
        return WorkspaceObservationChange(
            goal_id=self._goal_id,
            graph_id=graph_id,
            artifact_id=resource,
            resource_uri=f"workspace://{resource}",
            kind=kind,
            previous_observation=previous_observation,
            observation=observation,
            content_hash=content_hash,
            error=error,
        )

    def _graph_id(self, changes: Iterable[WorkspaceObservationChange]) -> str:
        for change in changes:
            if change.graph_id:
                return change.graph_id
        for token in self._baseline.values():
            if token.graph_id:
                return str(token.graph_id)
        try:
            state = self._agent_os.runtime_state(self._goal)
            return str(state.graph_id)
        except Exception:
            return ""

    def _current_graph_version(self) -> int:
        try:
            state = self._agent_os.runtime_state(self._goal)
            return int(state.progress.graph_version)
        except Exception:
            # A watcher cannot safely invent a version.  A zero version is
            # accepted by the DTO and causes a later policy call to remain
            # conservative if the graph is not available.
            return 0


def _error_text(exc: BaseException, *, limit: int = 240) -> str:
    text = f"{type(exc).__name__}: {exc}".replace("\r", " ").replace("\n", " ").strip()
    return text[:limit] if text else type(exc).__name__


def _delivery_projection(delivery: Any) -> dict[str, Any]:
    """Return a bounded JSON-shaped projection of ``InterruptDelivery``."""

    fields = (
        "claim_id",
        "status",
        "graph_id",
        "graph_version",
        "task_id",
        "attempt_id",
        "semantic_epoch",
        "action",
        "interrupt_id",
        "decision_hash",
        "reason",
        "preemptible",
        "delivered",
        "observed",
        "accepted",
        "acknowledged",
    )
    result: dict[str, Any] = {}
    for field in fields:
        try:
            value = getattr(delivery, field)
        except AttributeError:
            continue
        if isinstance(value, StrEnum):
            value = value.value
        result[field] = value
    return result


__all__ = [
    "WORKSPACE_WATCHER_SCHEMA_VERSION",
    "WorkspaceChangeKind",
    "WorkspaceInterruptRoute",
    "WorkspaceObservationChange",
    "WorkspaceObservationWatcher",
    "WorkspaceWatchPoll",
]
