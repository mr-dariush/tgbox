## Summary of Changes

<!-- Provide a brief, focused description of the change, bug fix, or feature. -->

Fixes # <!-- Add issue number if applicable -->

## Change Classification

- [ ] Bug fix (non-breaking change fixing a defect)
- [ ] New feature (non-breaking architectural addition)
- [ ] Breaking change (modifies existing exported contracts or signatures)
- [ ] Performance improvement (benchmarked memory allocation reduction)
- [ ] Documentation / Refactoring / Chore

## Pre-Submission Verification Gate

Before submitting this PR, verify that all items below have been validated locally:

- [ ] **Dependency Hygiene:** Ran `make tidy` and confirmed `go.mod` and `go.sum` are clean.
- [ ] **Static Analysis:** Ran `make lint` and resolved all `golangci-lint` issues without unapproved exemptions.
- [ ] **Data Race Safety:** Ran `make test-race` (`go test -v -race ./...`) with zero concurrency warnings.
- [ ] **Security Scan:** Ran `make vuln` (`govulncheck ./...`) with zero reported vulnerabilities.
- [ ] **Documentation Contract:** Exported functions, types, and methods have godoc comments starting with the declared symbol name.
- [ ] **Diff Blast Radius:** Confirmed no unrelated sibling functions, formatting drifts, or extraneous files were touched.
- [ ] **Commit Hygiene:** PR title and commit messages adhere to Conventional Commits (`feat:`, `fix:`, `perf:`, `refactor:`, `docs:`, `test:`, `chore:`).

## Additional Context or Benchmarks

<!-- If this PR optimizes hot paths, paste before/after benchmark results using testing.AllocsPerRun or benchstat. -->