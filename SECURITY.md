# Security Policy

## Reporting a security issue

Please report security issues **privately** to the repository owner's
maintained security contact.

> **Security contact pending owner configuration.** Until the owner publishes a
> private reporting channel / email, treat public-only disclosure as the fallback
> and DO NOT post credentials, tokens, or secrets in an issue.

If an explicit security contact has been published, use that.  Do not open a
public issue that exposes a live secret.

## What is security-sensitive

- **Capability / Namespace boundary**: the Kernel CapabilityService + Namespace
  decide resource access.  A bypass would let a process read/write artifacts it
  must not.
- **ResourceLease ownership**: Kernel leases linearize task ownership.  A double
  ownership / lease-liveness bug could allow a stale worker to act as owner.
- **Artifact / Evidence integrity**: exact-version Evidence.  Any way for an old
  Evidence to validate a newer ArtifactVersion, or for mutation to mutate a
  historical Evidence, is a semantic-integrity issue.
- **Shell / Workspace**: the ShellTool runs commands and WorkspaceTool reads/writes
  files under a root.  These are **not** OS sandboxes; host-level isolation may
  require containers/VMs.  Capability must be explicitly granted.
- **Context VM**: version-bound content materialization and process isolation.

## Secret handling

- Model API keys are loaded from environment / configuration (e.g.
  `LHOS_MODEL_API_KEY`) — never hardcoded.
- Do not commit `.env`, tokens, or credential files.

## Supported release status

This is a **v0.1.0 Release Candidate**.  Core V1 architecture is frozen; the SDK,
integrations, CLI, and observability are experimental.  Security-sensitive
behaviors in the Core are the highest priority for fixes and private reports.
