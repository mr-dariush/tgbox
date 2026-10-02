# Security Policy

tgbox manages MTProto sessions, cryptographic handshakes, and concurrent network connections. Vulnerabilities in these components can expose session keys or degrade service availability.

## Supported Versions

Security fixes apply only to the active branch and the latest minor release tags:

| Version | Go Toolchain | Status |
| :--- | :--- | :--- |
| `main` | Go 1.27.x | Supported |
| Latest tag (`vX.Y.Z`) | Go 1.27.x | Supported |
| Older releases | Any | Unsupported |

## Reporting Vulnerabilities

Do not open public GitHub issues, pull requests, or discussion threads for security defects.

Submit all reports privately through [GitHub Security Advisories](https://github.com/mr-dariush/tgbox/security/advisories/new).

Each report must include:
1. Defect summary: the vulnerable component and the mechanics of the attack vector.
2. Impact assessment: what an attacker gains (for example: session extraction, unauthorized RPC execution, lock bypass, or denial of service).
3. Reproducer: minimal Go code or test fixture demonstrating the failure mode.
4. Environment details: OS, architecture, Go toolchain version, and the active network profile.

## Scope

### In Scope
- MTProto obfuscation defects, header generation flaws, or broken Perfect Forward Secrecy (PFS).
- Concurrency bugs leading to session corruption or distributed lock bypasses (`store/redis`).
- Memory exhaustion and panics triggered by malformed incoming network frames.
- State machine bypasses in `auth/headless.go`.

### Out of Scope
- Upstream Telegram server limits, `FLOOD_WAIT` responses, and account bans.
- Attacks requiring root or physical access to the host running the bot or userbot process.
- Vulnerabilities in third-party libraries not triggered by tgbox itself.

## Response Timelines

- Initial acknowledgment: within 72 hours.
- Triage and severity assessment: within 7 business days.
- Coordinated disclosure and patch delivery: within 14 to 30 calendar days.

Confirmed vulnerabilities receive a private patch, a public security advisory, and CVE assignment where appropriate. Contributors receive full credit unless they request anonymity.