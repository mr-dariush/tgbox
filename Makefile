# ==============================================================================
# TGBOX
# ==============================================================================

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Local cache for compiled tooling binaries
BIN_DIR := $(CURDIR)/.bin
TOOLS_DIR := $(CURDIR)/_tools
export PATH := $(BIN_DIR):$(PATH)

# Dynamic Environment
GO ?= go
GOHOSTOS ?= $(shell $(GO) env GOHOSTOS)
GOHOSTARCH ?= $(shell $(GO) env GOHOSTARCH)
COVERAGE_FILE := coverage.txt
COVERAGE_HTML := coverage.html

.DEFAULT_GOAL := help

# ------------------------------------------------------------------------------
# 1. HELP & DISCOVERY
# ------------------------------------------------------------------------------
.PHONY: help
help: ## Display this automated help menu
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ------------------------------------------------------------------------------
# 2. DETERMINISTIC TOOL INSTALLATION (Pinned via _tools module)
# ------------------------------------------------------------------------------
$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(BIN_DIR)/golangci-lint: $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum | $(BIN_DIR)
	@echo "==> Building and caching pinned golangci-lint in $(BIN_DIR)..."
	@cd $(TOOLS_DIR) && $(GO) mod tidy
	@cd $(TOOLS_DIR) && GOBIN=$(BIN_DIR) $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint
	@touch $(BIN_DIR)/golangci-lint

$(BIN_DIR)/govulncheck: $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum | $(BIN_DIR)
	@echo "==> Building and caching pinned govulncheck in $(BIN_DIR)..."
	@cd $(TOOLS_DIR) && $(GO) mod tidy
	@cd $(TOOLS_DIR) && GOBIN=$(BIN_DIR) $(GO) install golang.org/x/vuln/cmd/govulncheck
	@touch $(BIN_DIR)/govulncheck

$(BIN_DIR)/gofumpt: $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum | $(BIN_DIR)
	@echo "==> Building and caching pinned gofumpt in $(BIN_DIR)..."
	@cd $(TOOLS_DIR) && $(GO) mod tidy
	@cd $(TOOLS_DIR) && GOBIN=$(BIN_DIR) $(GO) install mvdan.cc/gofumpt
	@touch $(BIN_DIR)/gofumpt

.PHONY: tools
tools: $(BIN_DIR)/golangci-lint $(BIN_DIR)/govulncheck $(BIN_DIR)/gofumpt ## Install all developer tools pinned in _tools/go.mod into .bin

.PHONY: tools-sync
tools-sync: $(BIN_DIR) ## Synchronize and tidy _tools module dependencies
	@echo "==> Tidying _tools module..."
	@cd $(TOOLS_DIR) && $(GO) mod tidy


.PHONY: githooks
githooks: ## Configure Git to use version-controlled .githooks directory
	@echo "==> Configuring Git hooks path to .githooks..."
	@git config core.hooksPath .githooks
	@chmod +x .githooks/*
	@echo "==> Git hooks active."

.PHONY: setup
setup: tools githooks ## Setup local development environment (tools + hooks)
	@echo "==> [READY] Development environment fully initialized."

# ------------------------------------------------------------------------------
# 3. LINTING, FORMATTING & STATIC ANALYSIS
# ------------------------------------------------------------------------------
.PHONY: lint
lint: $(BIN_DIR)/golangci-lint ## Run golangci-lint across all packages
	@echo "==> Running golangci-lint..."
	@$(BIN_DIR)/golangci-lint run ./...

.PHONY: fmt
fmt: $(BIN_DIR)/gofumpt ## Format code strictly using gofumpt and gci rules
	@echo "==> Running gofumpt..."
	@$(BIN_DIR)/gofumpt -w .

.PHONY: fix
fix: fmt $(BIN_DIR)/golangci-lint ## Automatically fix formatters and auto-fixable lint issues
	@echo "==> Running golangci-lint auto-fix..."
	@$(BIN_DIR)/golangci-lint run --fix ./...
	@$(MAKE) tidy

# ------------------------------------------------------------------------------
# 4. TESTING & BENCHMARKING
# ------------------------------------------------------------------------------
.PHONY: test
test: ## Run unit tests quickly without race detector
	@echo "==> Running standard unit tests..."
	@$(GO) test -v ./...

.PHONY: test-race
test-race: ## Run all tests with Go Data Race detector enabled
	@echo "==> Running tests with Race Detector (-race)..."
	@CGO_ENABLED=1 $(GO) test -v -race -timeout 5m ./...

.PHONY: cover coverage
cover coverage: ## Generate test coverage profile and render HTML report
	@echo "==> Generating coverage profile..."
	@CGO_ENABLED=1 $(GO) test -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@$(GO) tool cover -func=$(COVERAGE_FILE)
	@$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "==> Coverage HTML report generated at $(COVERAGE_HTML)"

.PHONY: bench
bench: ## Run package benchmarks and memory allocation metrics
	@echo "==> Running core performance benchmarks..."
	@$(GO) test -run=^$$ -bench=. -benchmem ./...

# ------------------------------------------------------------------------------
# 5. SECURITY & HYGIENE CHECKS
# ------------------------------------------------------------------------------
.PHONY: vuln vulncheck
vuln vulncheck: $(BIN_DIR) ## Scan codebase for supply-chain vulnerabilities via govulncheck
	@if [ ! -f "$(BIN_DIR)/govulncheck" ]; then $(MAKE) tools; fi
	@echo "==> Running govulncheck security scanner..."
	@$(BIN_DIR)/govulncheck ./...

.PHONY: tidy
tidy: ## Tidy and verify dependencies in root and _tools modules
	@echo "==> Tidying root module..."
	@$(GO) mod tidy
	@$(MAKE) tools-sync
	@git diff --exit-code go.mod go.sum || (echo "Error: root module dependencies are not tidy. Run 'make tidy' and commit." && exit 1)
	@git diff --exit-code $(TOOLS_DIR)/go.mod $(TOOLS_DIR)/go.sum || (echo "Error: _tools dependencies are not tidy. Run 'make tidy' and commit." && exit 1)

.PHONY: check verify all
check verify all: tidy lint test-race vuln ## Complete Pre-Commit & CI validation gatekeeper
	@echo "==> [SUCCESS] All enterprise quality gates passed cleanly!"

# ------------------------------------------------------------------------------
# 6. CLEANUP
# ------------------------------------------------------------------------------
.PHONY: clean
clean: ## Remove test databases, coverage profiles, and local binary caches
	@echo "==> Cleaning generated artifacts..."
	@rm -rf $(BIN_DIR) $(COVERAGE_FILE) $(COVERAGE_HTML) *.db *.peers.db *.session.db
	@$(GO) clean -testcache