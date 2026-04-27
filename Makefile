# ─── Valkey Makefile ──────────────────────────────────────────────────────────

BINARY_SERVER = valkey-server
BINARY_CLI    = valkey-cli
BUILD_DIR     = bin

GO       = go
GOFLAGS  =
LDFLAGS  =

SERVER_PKG = ./cmd/server
CLI_PKG    = ./cmd/cli

.PHONY: all build server cli test test-race test-cstore bench bench-short bench-cstore lint vet fmt clean run run-cli help

## ─── default ────────────────────────────────────────────────────────────────

all: build

## ─── build ──────────────────────────────────────────────────────────────────

build: server cli ## Build server and CLI binaries

server: ## Build server binary
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_SERVER) $(SERVER_PKG)

cli: ## Build CLI binary
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_CLI) $(CLI_PKG)

## ─── test ───────────────────────────────────────────────────────────────────

test: ## Run all tests
	$(GO) test ./... -count=1 -timeout=30s

test-race: ## Run all tests with race detector
	$(GO) test -race ./... -count=1 -timeout=60s

test-v: ## Run all tests with verbose output
	$(GO) test -v ./... -count=1 -timeout=30s

## ─── bench ──────────────────────────────────────────────────────────────────

bench: ## Run all benchmarks (full)
	$(GO) test -bench=. -benchmem -count=1 -timeout=120s ./internal/...

bench-short: ## Run benchmarks (shorter, 3s per bench)
	$(GO) test -bench=. -benchmem -benchtime=3s -count=1 -timeout=120s ./internal/...

bench-store: ## Run store-only benchmarks
	$(GO) test -bench=. -benchmem -count=1 -timeout=60s ./internal/store/

bench-server: ## Run server-only benchmarks
	$(GO) test -bench=. -benchmem -count=1 -timeout=120s ./internal/server/

bench-cstore: ## Run CStore vs GoStore comparison benchmarks
	$(GO) test -tags cstore -bench=. -benchmem -count=1 -timeout=300s ./internal/store/cstore/

test-cstore: ## Run CStore tests
	$(GO) test -tags cstore -v -count=1 ./internal/store/cstore/

## ─── code quality ───────────────────────────────────────────────────────────

vet: ## Run go vet
	$(GO) vet ./...

fmt: ## Format code
	gofmt -s -w .

lint: vet fmt ## Run vet + fmt

## ─── run ────────────────────────────────────────────────────────────────────

run: server ## Build and run the server
	./$(BUILD_DIR)/$(BINARY_SERVER)

run-cli: cli ## Build and run the CLI
	./$(BUILD_DIR)/$(BINARY_CLI)

## ─── clean ──────────────────────────────────────────────────────────────────

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)
	$(GO) clean -testcache

## ─── help ───────────────────────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# claude --resume "javascript-client-valkey-bun"
