# Security Policy

## Supported version

LongHorizonOS is currently at experimental `v0.1.0`. Security fixes target
the latest release and the `main` branch.

## Report a vulnerability

Use the repository's private **Security → Report a vulnerability** flow. This
creates a private GitHub Security Advisory visible only to the reporter and
repository maintainers.

Do not disclose a vulnerability, credential, token or exploit in a public
issue, discussion or pull request.

Before public launch, the repository owner must enable **Private vulnerability
reporting** in the GitHub repository security settings. If the private report
button is unavailable, contact the repository owner privately and wait for a
private channel to be enabled.

Please include:

- affected version or commit;
- impact and affected trust boundary;
- minimal reproduction steps;
- proof-of-concept code when safe;
- any known mitigation.

Maintainers should acknowledge a complete report within 7 days, provide a
triage decision within 14 days, and coordinate disclosure after a fix or
mitigation is available.

## Security boundaries

Security-sensitive areas include:

- Capability and Namespace authorization;
- Kernel Lease ownership and cleanup;
- ArtifactVersion and Evidence integrity;
- canonical URI and workspace path confinement;
- command execution trust flags and network policy;
- crash recovery, Journal replay and projection rebuild.

The built-in authority checks are not a host-level sandbox. External commands
marked `trusted=True` execute with the permissions of the host process.
Shell-interpreter mode additionally requires `allow_shell=True`, and
network-capable execution requires `allow_network=True`.

Run untrusted code in a container or VM with appropriate filesystem, process
and network isolation.

## Secrets

Load model credentials from environment or configuration, such as
`LHOS_MODEL_API_KEY`. Never commit `.env`, tokens, private keys or credentials.
