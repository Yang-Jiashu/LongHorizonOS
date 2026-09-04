"""Demonstrate a real, killable subprocess Harness and preemption.

Run it directly to see LongHorizonOS start a long-sleeping child "agent",
observe it reach its work phase, then PREEMPT it -- actually terminating the OS
process and reaping it -- while still recovering the child's self-reported token
usage plus the parent-measured wall-clock time::

    python examples/harness_subprocess_preemption.py

The same file doubles as the example *child script that sleeps*.  When invoked
with ``--child`` it plays the role of a long-running agent: it reports usage,
announces readiness, and then sleeps while ignoring cooperative termination
signals -- so only the parent's hard kill can stop it.  The driver below spawns
this very file in ``--child`` mode through :class:`SubprocessHarnessAdapter`.

Nothing here is a daemon and nothing claims ownership: the adapter is a plain
Harness execution unit, and PREEMPT flows through the ordinary control boundary.
"""

from __future__ import annotations

import asyncio
import sys
import time

from lhos.sdk.harness import (
    HarnessOperation,
    HarnessResultStatus,
    HarnessSessionIdentity,
    HarnessSessionState,
)
from lhos.sdk.harness_child import emit_ready, emit_usage
from lhos.sdk.subprocess_harness import SubprocessHarnessAdapter


def run_child() -> int:
    """Play a long-running agent: report usage, then sleep uncooperatively."""

    import signal
    from contextlib import suppress

    # Ignore cooperative termination so this stands in for an agent stuck in a
    # blocking call that will not check any in-process cancellation token.
    def _ignore(_signum: int, _frame: object) -> None:
        return None

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            with suppress(OSError, ValueError, RuntimeError):
                signal.signal(sig, _ignore)

    # Report the tokens/cost spent before blocking so the parent still measures
    # real cost even when it must hard-kill us mid-flight.
    emit_usage(tokens_in=512, tokens_out=128, cost_microusd=7300)
    emit_ready()

    deadline = time.monotonic() + 600.0  # a "4-minute blocking call", and then some
    while time.monotonic() < deadline:
        try:
            time.sleep(0.25)
        except InterruptedError:
            continue
    return 0


async def run_demo() -> None:
    identity = HarnessSessionIdentity(
        session_id="demo-session",
        graph_id="demo-graph",
        graph_version=1,
        semantic_epoch=0,
        task_id="demo-task",
        agent_id="demo-agent",
        claim_id="demo-claim",
        attempt_id="demo-attempt",
    )
    adapter = SubprocessHarnessAdapter(
        identity,
        [sys.executable, __file__, "--child"],
        grace_period=1.0,
        timeout=None,
    )
    try:
        started = await adapter.control(adapter.make_request(HarnessOperation.START))
        print(f"START -> {started.after.state.value} (pid={adapter.pid})")

        if not adapter.wait_ready(timeout=30.0):
            print("child did not report readiness in time")
            return
        print("child is running its blocking work phase")

        elapsed = time.monotonic()
        result = await adapter.control(
            adapter.make_request(HarnessOperation.PREEMPT, reason="operator preempt")
        )
        elapsed = time.monotonic() - elapsed

        assert result.status is HarnessResultStatus.APPLIED
        assert result.after.state is HarnessSessionState.PREEMPTED
        assert adapter.child.poll() is not None  # process is gone, reaped
        print(f"PREEMPT -> {result.after.state.value} in {elapsed:.2f}s; child reaped")
        print(
            "measured usage:",
            {
                key: result.details.get(key)
                for key in ("wall_time_ms", "tokens_in", "tokens_out", "cost_microusd")
            },
        )
    finally:
        adapter.close()


def main() -> int:
    if "--child" in sys.argv[1:]:
        return run_child()
    asyncio.run(run_demo())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
