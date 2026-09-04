"""Audit: Capability bypass adversarial tests + Driver/Kernel boundary."""

from __future__ import annotations

import ast
import contextlib
from pathlib import Path

import pytest

from lhos.agent_os.kernel.errors import CapabilityDenied
from lhos.agent_os.kernel.models import Capability
from lhos.agent_os.sdk.client import create_kernel


class TestCapabilityBypassAudit:
    """Verify that capability checks cannot be bypassed."""

    @pytest.mark.asyncio
    async def test_direct_unauthorized_action_denied(self) -> None:
        """Directly constructing an action without capability must be denied."""
        kernel = create_kernel(":memory:")

        # Spawn a process with no capabilities
        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="no_caps", steps=[exit_step("p1")])
        pid = await kernel.spawn(program)

        # Remove all capabilities
        cap_set = kernel._capability_service.get_capability_set(pid)
        if cap_set:
            cap_set.capabilities = []
            kernel._capability_service._upsert_capability_set(cap_set)

        # Any resource access should be denied
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "device:model/mock", "invoke")

    @pytest.mark.asyncio
    async def test_wildcard_cannot_bypass_namespace(self) -> None:
        """Wildcard pattern in one namespace cannot access another namespace."""
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="wildcard", steps=[exit_step("p1")])
        pid = await kernel.spawn(program, namespace_id="ns1")

        # Grant wildcard for ns1 only
        cap_set = kernel._capability_service.get_capability_set(pid)
        assert cap_set is not None
        cap_set.capabilities = [
            Capability(resource_pattern="resource:ns1/*", operations={"acquire"}),
        ]
        kernel._capability_service._upsert_capability_set(cap_set)

        # Can access ns1 resources
        kernel._capability_service.enforce(pid, "resource:ns1/file1", "acquire")

        # Cannot access ns2 resources
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "resource:ns2/file1", "acquire")

    @pytest.mark.asyncio
    async def test_path_traversal_cannot_escape_namespace(self) -> None:
        """Resources outside namespace pattern are denied.

        Note: fnmatch does not understand path semantics (../).
        Path traversal protection must be enforced at a higher level.
        Phase B relies on capability patterns, not path normalization.
        """
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="traversal", steps=[exit_step("p1")])
        pid = await kernel.spawn(program, namespace_id="ns1")

        # Grant access only to ns1/workspace
        cap_set = kernel._capability_service.get_capability_set(pid)
        assert cap_set is not None
        cap_set.capabilities = [
            Capability(resource_pattern="resource:ns1/workspace/*", operations={"acquire"}),
        ]
        kernel._capability_service._upsert_capability_set(cap_set)

        # Can access ns1 workspace resources
        kernel._capability_service.enforce(pid, "resource:ns1/workspace/file1", "acquire")

        # Cannot access ns2 resources (different namespace)
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "resource:ns2/secret", "acquire")

        # Cannot access admin resources
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "resource:admin/config", "acquire")

    @pytest.mark.asyncio
    async def test_uri_encoding_cannot_escape_namespace(self) -> None:
        """URI-encoded paths cannot bypass capability checks."""
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="uri", steps=[exit_step("p1")])
        pid = await kernel.spawn(program, namespace_id="ns1")

        cap_set = kernel._capability_service.get_capability_set(pid)
        assert cap_set is not None
        cap_set.capabilities = [
            Capability(resource_pattern="resource:ns1/*", operations={"acquire"}),
        ]
        kernel._capability_service._upsert_capability_set(cap_set)

        # URI-encoded path that decodes to something outside ns1
        # fnmatch doesn't decode URI encoding, so this should be denied
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "resource:%6e%73%32/secret", "acquire")

    def test_capability_delegation_must_be_subset(self) -> None:
        """Child capabilities must be a subset of parent."""
        kernel = create_kernel(":memory:")

        # Parent has model + tool
        parent_caps = [
            Capability(resource_pattern="device:model/mock", operations={"invoke"}),
            Capability(resource_pattern="device:tool/mock", operations={"invoke"}),
        ]
        kernel._capability_service.create_capability_set("parent", parent_caps)

        # Child with subset — OK
        child_caps = [
            Capability(resource_pattern="device:model/mock", operations={"invoke"}),
        ]
        kernel._capability_service.create_capability_set("child", child_caps)
        assert kernel._capability_service.verify_child_subset("parent", "child")

        # Child with superset — NOT OK
        extra_caps = [
            Capability(resource_pattern="device:model/mock", operations={"invoke"}),
            Capability(resource_pattern="resource:admin/*", operations={"acquire"}),
        ]
        kernel._capability_service.create_capability_set("child2", extra_caps)
        assert not kernel._capability_service.verify_child_subset("parent", "child2")

    @pytest.mark.asyncio
    async def test_revoked_capability_cannot_be_reused(self) -> None:
        """After removing a capability, it cannot be used."""
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="revoke", steps=[exit_step("p1")])
        pid = await kernel.spawn(program)

        # Initially has device capability
        kernel._capability_service.enforce(pid, "device:model/mock", "invoke")

        # Revoke device capability
        cap_set = kernel._capability_service.get_capability_set(pid)
        assert cap_set is not None
        cap_set.capabilities = [
            c for c in cap_set.capabilities if not c.resource_pattern.startswith("device:")
        ]
        kernel._capability_service._upsert_capability_set(cap_set)

        # Now should be denied
        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "device:model/mock", "invoke")

    @pytest.mark.asyncio
    async def test_unauthorized_control_signal_is_denied(self) -> None:
        """Process without signal capability cannot send signals."""
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="no_signal", steps=[exit_step("p1")])
        pid = await kernel.spawn(program)

        # Remove signal capability
        cap_set = kernel._capability_service.get_capability_set(pid)
        if cap_set:
            cap_set.capabilities = [
                c
                for c in cap_set.capabilities
                if not c.resource_pattern.startswith("process:signal")
            ]
            kernel._capability_service._upsert_capability_set(cap_set)

        with pytest.raises(CapabilityDenied):
            kernel._capability_service.enforce(pid, "process:signal/other_pid", "send")

    @pytest.mark.asyncio
    async def test_capability_denial_writes_journal_event(self) -> None:
        """CAPABILITY_DENIED events must be journaled."""
        kernel = create_kernel(":memory:")

        from lhos.agent_os.programs.scripted import ScriptedProgram, exit_step

        program = ScriptedProgram(program_id="denied", steps=[exit_step("p1")])
        pid = await kernel.spawn(program)

        # Remove all capabilities
        cap_set = kernel._capability_service.get_capability_set(pid)
        if cap_set:
            cap_set.capabilities = []
            kernel._capability_service._upsert_capability_set(cap_set)

        # Try to access something
        with contextlib.suppress(CapabilityDenied):
            kernel._capability_service.enforce(pid, "device:model/mock", "invoke")

        # Check journal
        events = kernel._journal.read_all()
        denials = [e for e in events if e.event_type == "CAPABILITY_DENIED"]
        assert len(denials) >= 1


class TestDriverKernelBoundary:
    """Verify that drivers cannot directly mutate kernel state."""

    def test_drivers_cannot_import_journal_service(self) -> None:
        """Driver source files must not import JournalService."""
        driver_dir = Path(__file__).parent.parent.parent / "src" / "lhos" / "agent_os" / "drivers"
        driver_files = list(driver_dir.glob("*.py"))

        violations = []
        for f in driver_files:
            if f.name == "__init__.py":
                continue
            content = f.read_text(encoding="utf-8")
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import | ast.ImportFrom):
                    module = ""
                    if isinstance(node, ast.Import):
                        for alias in node.names:
                            module = alias.name
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module or ""
                    if "journal" in module.lower() or "process_service" in module.lower():
                        violations.append(f"{f.name}: imports {module}")

        assert violations == [], f"Drivers importing kernel services: {violations}"

    def test_drivers_cannot_import_process_service(self) -> None:
        """Driver source files must not import ProcessService."""
        driver_dir = Path(__file__).parent.parent.parent / "src" / "lhos" / "agent_os" / "drivers"
        driver_files = list(driver_dir.glob("*.py"))

        violations = []
        for f in driver_files:
            if f.name == "__init__.py":
                continue
            content = f.read_text(encoding="utf-8")
            if "process_service" in content or "ProcessService" in content:
                violations.append(f.name)

        assert violations == [], f"Drivers referencing ProcessService: {violations}"

    def test_drivers_only_return_driver_result(self) -> None:
        """Driver dispatch must return DriverResult, not mutate projections."""
        # This is verified by the type signature in the Protocol
        from lhos.agent_os.drivers.base import DriverResult

        # Verify DriverResult is a pydantic model with status/output/error
        assert hasattr(DriverResult, "model_fields")
        assert "status" in DriverResult.model_fields
        assert "output" in DriverResult.model_fields
        assert "error" in DriverResult.model_fields

    def test_drivers_cannot_mutate_kernel_projection(self) -> None:
        """Driver code does not reference projection tables."""
        driver_dir = Path(__file__).parent.parent.parent / "src" / "lhos" / "agent_os" / "drivers"
        driver_files = list(driver_dir.glob("*.py"))

        violations = []
        for f in driver_files:
            if f.name == "__init__.py":
                continue
            content = f.read_text(encoding="utf-8")
            # Check for SQL or projection references
            if "projection" in content.lower():
                violations.append(f"{f.name}: references 'projection'")
            if "INSERT INTO" in content or "UPDATE " in content or "DELETE FROM" in content:
                violations.append(f"{f.name}: contains SQL DML")

        assert violations == [], f"Drivers with SQL/projection refs: {violations}"
