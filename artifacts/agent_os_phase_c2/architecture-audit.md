# Phase C2 — Architecture Audit

Generated: 2026-08-06T10:08:06.900252+00:00

## Forbidden kernel imports

All files under `src/lhos/agent_os/context/` only import from
`lhos.agent_os.kernel.models`. No other kernel modules are referenced.

Test: `test_architecture.py::TestNoForbiddenKernelImports` (4 checks).

## No Prompt/LLM/Planner domain strings

Models / service / SDK contain none of: `tasknode`, `goal`, `evidence`,
`vpg`, `planner`, `prompt`, `llm` (case-insensitive).

Test: `test_architecture.py::TestNoMechanismDomainStrings` (3 checks).

## No circular imports

Importing `lhos.agent_os.context` in a fresh interpreter succeeds.

Test: `test_architecture.py::TestNoCircularImports`.

## TokenEstimator is runtime-checkable Protocol

`isinstance(fake, TokenEstimator)` works structurally.

Test: `test_architecture.py::TestTokenEstimatorProtocol`.

## Process isolation documented

`lhos.agent_os.context.__init__.py` (or `ContextService.__doc__`) mentions
"process-isolated" or "Context VM".

Test: `test_architecture.py::TestContextVMProcessIsolation`.

## Gate

- **arch_passed**: 11
- **arch_failed**: 0
