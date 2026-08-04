"""SDK Client — convenience facade for creating and running an AgentKernel.

Usage:
    from lhos.agent_os.sdk.client import create_kernel

    kernel = create_kernel(":memory:")
    pid = await kernel.spawn(program)
    await kernel.run_until_idle()
"""

from __future__ import annotations

from lhos.agent_os.kernel.dispatcher import SyscallDispatcher
from lhos.agent_os.kernel.kernel import AgentKernel
from lhos.agent_os.kernel.models import Clock
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
    kernel._journal.rebuild_projections(handlers)
    return kernel
