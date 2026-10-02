# Contributing to tgbox

Thank you for contributing to `tgbox`. This document provides the technical requirements, tooling instructions, and architectural standards enforced across this repository.

## 1. Prerequisites and Tooling

The supported Go version is **1.27.1+**.

Bootstrap your local development environment (pinned tooling and repository Git hooks) with one command:

```bash
make setup
```

This target executes two operations:
1. `make tools` - Installs pinned binaries from `_tools/go.mod` into `.bin`:
    - `golangci-lint` (v2 pinned static analysis engine)
    - `govulncheck` (official Go vulnerability scanner)
    - `gofumpt` (strict formatting standard)
2. `make githooks` - Configures Git to use version-controlled `.githooks`:
    - `prepare-commit-msg`: Automatically injects the Conventional Commits template.
    - `commit-msg`: Enforces Conventional Commits syntax, character limits (72/80), and lowercase headers.
    - `pre-push`: Blocks direct pushes to `main` on canonical origin and runs fast `go vet` verification.
    - `post-checkout`: Cleans ephemeral coverage profiles, benchmark dumps, and temporary test databases.

## 2. Development Workflow and Local Verification

All contributions must pass the complete pre-commit quality gate before opening a Pull Request:

```bash
make check
```

This single command executes:
1. `make tidy` - Validates dependency hygiene and rejects untidy `go.mod` / `go.sum` files.
2. `make lint` - Executes static analysis (`golangci-lint run ./...`).
3. `make test-race` - Runs all unit and integration tests with the Go Data Race detector (`-race`).
4. `make vuln` - Runs vulnerability database scans (`govulncheck ./...`).

To automatically format your code and fix auto-fixable lint issues:

```bash
make fix
```

## 3. Concurrency and Architectural Guidelines

`tgbox` is a high-concurrency MTProto framework. Contributions must follow these invariants:

- **Goroutine Leak Prevention:** Every spawned goroutine must have a deterministic lifecycle tied directly to a `context.Context` cancellation or a synchronized termination channel.
- **Race-Free State:** Shared mutable state must be guarded using atomic primitives (`sync/atomic`) or read-write locks (`sync.RWMutex`). Never mutate package-level variables.
- **Bounded Allocations:** Hot paths (e.g., dispatcher routing, buffer pools, network packet parsing) must minimize heap allocations. Use `network.BufferPool` for byte slice recycling where applicable.
- **Line of Sight Error Flow:** Return early on errors using guard clauses. Keep the happy path aligned to the left indentation edge. Maximum statement nesting depth is strictly 2.
- **Minimal Concrete Types:** Accept concrete types, return minimal interfaces only when polymorphism or mocking is demonstrably required.

## 4. Commit Message Standard

Commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```text
<type>(<optional scope>): <description>

[optional body]

[optional footer(s)]
```

Common types:
- `feat`: A new feature or public capability.
- `fix`: A defect resolution.
- `perf`: Code change that improves execution speed or reduces allocations.
- `refactor`: Code change that neither fixes a bug nor adds a feature.
- `docs`: Documentation updates or GoDoc corrections.
- `test`: Adding or correcting tests with zero functional changes.
- `chore`: Maintenance tasks, dependency bumps, or CI updates.

## 5. Submitting Pull Requests

1. Fork the repository to your personal GitHub account.
2. Create a feature branch off `main` (`git checkout -b feat/my-new-feature`).
3. Ensure all tests and linters pass (`make check`).
4. Commit your changes with conventional messages.
5. Push to your personal fork and open a Pull Request against `main`.
6. Complete the checklist provided in the Pull Request template.