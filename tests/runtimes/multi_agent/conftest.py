"""Shared pytest fixtures for the D2 Multi-Agent Scheduler test suite.

Each test gets a fresh in-memory AgentKernel + VPG runtime.  Agent
ram/agent_os test helpers all resolve into the injected providers
exposed on the ``World`` fixture.
"""

from __future__ import annotations

import pytest

from lhos.agent_os.sdk.client import create_kernel
from lhos.runtimes.verified_progress import VerifiedProgressRuntime

from tests.runtimes.multi_agent.test_providers import (
    FakeFactsMinimal,
    KernelCapabilityProvider,
    KernelLeaseProvider,
    KernelProcessProvider,
    VPGAdapter,
)


class World:
    """Fixture bundle binding together Kernel, VPG, providers, Scheduler."""

    def __init__(
        self,
        kernel: Any,
        vpg_rt: VerifiedProgressRuntime,
        vpg: VPGAdapter,
        proc: KernelProcessProvider,
        lease: KernelLeaseProvider,
        cap: KernelCapabilityProvider,
    ) -> None:
        self.kernel = kernel
        self.vpg_rt = vpg_rt
        self.vpg = vpg
        self.proc = proc
        self.lease = lease
        self.cap = cap

    @property
    def pid_of(self) -> object:
        class _P:
            def __init__(self, kernel: Any) -> None:
                self._k = kernel

            def spawn_any(self, program_id: str = "agent") -> str:
                pcb = self._k._process_service.spawn(program_id)
                return pcb.pid

        return _P(self.kernel)


@pytest.fixture
def world() -> World:
    kernel = create_kernel(":memory:")
    # Bind VPG + Kernel to the same underlying SQLite so leases/process
    # projections that VPG facts providers might inspect share storage.
    vpg_rt = VerifiedProgressRuntime(":memory:")
    vpg = VPGAdapter(vpg_rt)
    proc = KernelProcessProvider(kernel)
    lease = KernelLeaseProvider(kernel)
    cap = KernelCapabilityProvider(kernel)
    return World(kernel, vpg_rt, vpg, proc, lease, cap)


@pytest.fixture
def facts() -> FakeFactsMinimal:
    return FakeFactsMinimal({})


@pytest.fixture
def graph(world: World, facts: FakeFactsMinimal):
    """Create a fresh graph AND rebind VPG to use the given facts provider
    through a fresh runtime that still shares the kernel storage.

    Returns (graph_id, world_world, rt).
    """
    from lhos.runtimes.multi_agent.test_providers import FakeFactsWithCommittedAction
    rt = VerifiedProgressRuntime(":memory:")
    rec = rt.create_graph(owner_pid="agent-1")
    return rec.graph_id, world, rt
