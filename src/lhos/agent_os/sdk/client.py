"""SDK Client — convenience facade for creating and running an AgentKernel.

Usage:
    from lhos.agent_os.sdk.client import create_kernel

    kernel = create_kernel(":memory:")
    pid = await kernel.spawn(program)
    await kernel.run_until_idle()

For artifact FS:
    from lhos.agent_os.sdk.client import create_kernel_with_artifacts

    kernel, artifact_sdk = create_kernel_with_artifacts(":memory:", "/tmp/cas")
    artifact_sdk.write("p1", "workspace:///file.txt", b"hello")
"""

from __future__ import annotations

from pathlib import Path

from lhos.agent_os.artifacts.namespace_service import NamespaceService
from lhos.agent_os.artifacts.projections import ArtifactProjections
from lhos.agent_os.artifacts.service import ArtifactFSService
from lhos.agent_os.drivers.local_artifact_storage import LocalArtifactStorageDriver
from lhos.agent_os.kernel.dispatcher import SyscallDispatcher
from lhos.agent_os.kernel.kernel import AgentKernel
from lhos.agent_os.kernel.models import Clock
from lhos.agent_os.sdk.artifact_sdk import ArtifactSDK
from lhos.agent_os.services.action_service import ActionService
from lhos.agent_os.services.capability_service import CapabilityService
from lhos.agent_os.services.journal import JournalService
from lhos.agent_os.services.lease_service import LeaseService
from lhos.agent_os.services.process_service import ProcessService
from lhos.agent_os.services.signal_service import SignalService
from lhos.agent_os.storage.sqlite import SQLiteStorage


def create_kernel(db_path: str = ":memory:") -> AgentKernel:
    """Create a fully wired AgentKernel with all services."""
    storage = SQLiteStorage(db_path)
    journal = JournalService(storage)
    clock = Clock()
    process_service = ProcessService(storage, journal, clock)
    action_service = ActionService(storage, journal)
    capability_service = CapabilityService(storage, journal)
    lease_service = LeaseService(storage, journal)
    signal_service = SignalService(storage, journal, process_service)

    dispatcher = SyscallDispatcher(
        storage,
        journal,
        process_service,
        action_service,
        capability_service,
        lease_service,
        signal_service,
    )

    kernel = AgentKernel(
        storage,
        journal,
        process_service,
        action_service,
        capability_service,
        lease_service,
        signal_service,
        dispatcher,
        clock,
    )

    return kernel


def create_kernel_with_artifacts(
    db_path: str = ":memory:",
    cas_root: str | Path = "/tmp/lhos-cas",
) -> tuple[AgentKernel, ArtifactSDK]:
    """Create a fully wired AgentKernel with Artifact FS services.

    Returns (kernel, artifact_sdk) tuple.
    """
    storage = SQLiteStorage(db_path)
    journal = JournalService(storage)
    clock = Clock()
    process_service = ProcessService(storage, journal, clock)
    action_service = ActionService(storage, journal)
    capability_service = CapabilityService(storage, journal)
    lease_service = LeaseService(storage, journal)
    signal_service = SignalService(storage, journal, process_service)

    # Artifact FS services
    artifact_projections = ArtifactProjections(storage)
    storage_driver = LocalArtifactStorageDriver(cas_root)
    artifact_service = ArtifactFSService(
        artifact_projections,
        storage_driver,
        journal,
        capability_service,
        lease_service,
        signal_service,
    )
    namespace_service = NamespaceService(artifact_projections, journal)
    artifact_sdk = ArtifactSDK(artifact_service, namespace_service)

    dispatcher = SyscallDispatcher(
        storage,
        journal,
        process_service,
        action_service,
        capability_service,
        lease_service,
        signal_service,
    )

    kernel = AgentKernel(
        storage,
        journal,
        process_service,
        action_service,
        capability_service,
        lease_service,
        signal_service,
        dispatcher,
        clock,
    )

    # Attach artifact services to kernel for projection rebuild
    kernel._artifact_service = artifact_service  # type: ignore[attr-defined]
    kernel._namespace_service = namespace_service  # type: ignore[attr-defined]
    kernel._artifact_sdk = artifact_sdk  # type: ignore[attr-defined]

    return kernel, artifact_sdk


def rebuild_from_journal(db_path: str) -> AgentKernel:
    """Rebuild all projections from the journal and return a kernel."""
    kernel = create_kernel(db_path)
    handlers = [
        kernel._process_service,
        kernel._action_service,
        kernel._capability_service,
        kernel._lease_service,
        kernel._signal_service,
    ]
    # Include artifact services if attached
    if hasattr(kernel, "_artifact_service"):
        handlers.append(kernel._namespace_service)  # type: ignore[attr-defined]
        handlers.append(kernel._artifact_service)  # type: ignore[attr-defined]
    kernel._journal.rebuild_projections(handlers)
    return kernel
