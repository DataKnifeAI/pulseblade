VERSION ?= 0.1.0

CARGO ?= cargo

.PHONY: all
all: build

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build workspace
	$(CARGO) build --workspace

.PHONY: release
release: ## Build release binaries
	$(CARGO) build --workspace --release

.PHONY: test
test: ## Run tests
	$(CARGO) test --workspace

.PHONY: fmt
fmt: ## Format Rust sources
	$(CARGO) fmt --all

.PHONY: fmt-check
fmt-check: ## Check formatting (CI)
	$(CARGO) fmt --all -- --check

.PHONY: lint
lint: ## Clippy with warnings denied
	$(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: vet
vet: fmt-check lint ## Static checks (fmt + clippy)

.PHONY: ci
ci: vet test ## Local quality gate (matches CI jobs)

.PHONY: run
run: ## Run this host's node (collectors + HTTP MCP on 127.0.0.1:7171)
	$(CARGO) run -p pulseblade -- node

.PHONY: install
install: ## Install the pulseblade binary into ~/.cargo/bin
	$(CARGO) install --path crates/pulseblade --locked

.PHONY: clean
clean: ## Remove target/
	$(CARGO) clean
