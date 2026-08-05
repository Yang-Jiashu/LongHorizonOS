# Architecture Dependency Audit — Phase B

## Objective

Verify that the Agent OS kernel has no dependencies on legacy system code, drivers are isolated from kernel internals, and the import graph has no circular dependencies.

## Methodology

1. **Static AST analysis**: Parse all `.py` files in `src/lhos/agent_os/` and extract import statements.
2. **Driver isolation check**: Verify driver files don't import JournalService, ProcessService, or reference SQL projections.
3. **Circular import detection**: Build a dependency graph and run DFS cycle detection.
4. **Legacy dependency check**: Verify no imports from `lhos.domain`, `lhos.infrastructure`, `lhos.runtime`, or other legacy modules.

## Results

### Source File Count

| Metric | Value |
|--------|-------|
| Total source files analyzed | 26 |
| Files with legacy imports | 0 |
| Circular imports detected | 0 |
| Driver violations (AST-based) | 0 |
| Driver violations (text-based) | 1 (false positive) |

### Import Graph Summary

```
agent_os/
├── kernel/
│   ├── models.py        → (external: pydantic, fnmatch, datetime, enum, uuid)
│   ├── errors.py        → (none)
│   ├── state_machine.py → kernel.models, kernel.errors
│   ├── dispatcher.py    → kernel.models, services.*, storage.sqlite
│   └── kernel.py        → kernel.*, services.*, storage.*, drivers.*, programs.*
├── services/
│   ├── journal.py       → kernel.models, storage.sqlite
│   ├── process_service  → kernel.models, kernel.state_machine, services.journal, storage.sqlite
│   ├── action_service   → kernel.models, kernel.state_machine, services.journal, storage.sqlite
│   ├── capability_svc   → kernel.errors, kernel.models, services.journal, storage.sqlite
│   ├── lease_service    → kernel.errors, kernel.models, services.journal, storage.sqlite
│   └── signal_service   → kernel.models, services.journal, storage.sqlite
├── storage/
│   ├── schema.py        → (none)
│   └── sqlite.py        → storage.schema
├── drivers/
│   ├── base.py          → (external: typing, pydantic)
│   ├── mock_device.py   → drivers.base (external: asyncio, typing)
│   └── mock_model.py    → drivers.base (external: asyncio, typing)
├── programs/
│   ├── base.py          → kernel.models (external: typing, pydantic)
│   └── scripted.py      → kernel.models, programs.base
└── sdk/
    └── client.py        → kernel.*, services.*, storage.sqlite
```

### Legacy Dependencies

**Result**: ✅ ZERO legacy dependencies.

No file in `src/lhos/agent_os/` imports from:
- `lhos.domain.*`
- `lhos.infrastructure.*`
- `lhos.runtime.*`
- `lhos.benchmarks.*`
- Any other legacy module

### Driver Isolation

**AST-based check** (from `test_audit_capability_bypass.py`):

| Check | Result |
|-------|--------|
| Drivers don't import JournalService | ✅ PASS |
| Drivers don't import ProcessService | ✅ PASS |
| Drivers don't reference SQL projections | ✅ PASS |
| Drivers don't contain SQL DML | ✅ PASS |
| DriverResult is a proper pydantic model | ✅ PASS |

**Text-based false positive**: `mock_device.py` contains the word "journal" in a comment (`# side effect journal (independent effect store)`). This is a description of the driver's own effect store, not an import of the kernel's JournalService. The AST-based check correctly identifies no actual import.

### Circular Imports

**Result**: ✅ ZERO circular imports.

The dependency graph is a DAG (Directed Acyclic Graph):
- `kernel/models.py` is the root — depends only on external libraries.
- `kernel/errors.py` is also a root — no dependencies.
- Services depend on `kernel/models`, `kernel/state_machine`, and `storage/sqlite`.
- `kernel/kernel.py` and `sdk/client.py` are top-level consumers that depend on everything.
- No module depends on itself (directly or transitively).

### Layer Architecture

```
Layer 0 (Foundation):  models.py, errors.py, storage/schema.py, storage/sqlite.py
Layer 1 (State):       state_machine.py
Layer 2 (Services):    journal.py, process_service.py, action_service.py,
                       capability_service.py, lease_service.py, signal_service.py
Layer 3 (Interface):   drivers/base.py, programs/base.py
Layer 4 (Implement):   drivers/mock_*.py, programs/scripted.py, kernel/dispatcher.py
Layer 5 (Composition): kernel/kernel.py
Layer 6 (Facade):      sdk/client.py
```

Each layer only imports from lower-numbered layers. No upward dependencies.

## Conclusion

Architecture dependencies are clean:
- ✅ Zero legacy dependencies — Agent OS is fully self-contained.
- ✅ Zero circular imports — dependency graph is a DAG.
- ✅ Drivers are isolated from kernel internals (no JournalService, ProcessService, or projection access).
- ✅ Clear layered architecture with no upward dependencies.
- ✅ 26 source files, all within the `lhos.agent_os` namespace.
