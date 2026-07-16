# Security Policy

## Supported versions

Security fixes are applied to the current `main` branch and the latest tagged release when a compatible backport is practical. Older prereleases and retired architecture lines are not supported.

## Reporting a vulnerability

Use GitHub private vulnerability reporting for this repository when it is available. Do not include exploit details, credentials, production data, or unpublished proof-of-concept code in a public issue.

When private reporting is unavailable, open a minimal public issue asking the maintainers to establish a private contact channel. Include only the affected component and a way to reach you; wait for a private channel before sharing technical details.

Reports should state the affected revision or release, deployment assumptions, impact, reproduction prerequisites, and whether the issue crosses an existing trust boundary. The maintainers will acknowledge a valid private report, coordinate remediation and disclosure, and credit the reporter when requested and appropriate.

## Scope

The supported security boundary includes strict configuration and Workflow parsing, HTTP ingress and egress policy, command sandbox setup, Artifact and checkpoint persistence, Evidence integrity, release packaging, and dependency pinning. Denial of service that bypasses configured resource ceilings, symlink or path traversal, sandbox escape, authentication bypass, secret disclosure, persistence corruption, and unsafe recovery behavior are in scope.
