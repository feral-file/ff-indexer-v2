.PHONY: build run clean install help

BINARY_NAME=temporal-benchmark
BUILD_DIR=.
GO=go

help: ## Display this help message
	@echo "Temporal Workflow Benchmark Tool"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the benchmark binary
	@echo "Building $(BINARY_NAME)..."
	@$(GO) build -o $(BUILD_DIR)/$(BINARY_NAME) .
	@echo "✓ Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

install: build ## Install the binary to $GOPATH/bin
	@echo "Installing $(BINARY_NAME) to $$GOPATH/bin..."
	@$(GO) install
	@echo "✓ Installed successfully"

run: build ## Build and run with default settings (requires WORKFLOW_ID env var)
	@if [ -z "$(WORKFLOW_ID)" ]; then \
		echo "Error: WORKFLOW_ID environment variable is required"; \
		echo "Usage: make run WORKFLOW_ID=your-workflow-id"; \
		exit 1; \
	fi
	@echo "Running benchmark for workflow: $(WORKFLOW_ID)"
	@./$(BINARY_NAME) -workflow-id $(WORKFLOW_ID)

clean: ## Remove built binaries
	@echo "Cleaning up..."
	@rm -f $(BUILD_DIR)/$(BINARY_NAME)
	@echo "✓ Clean complete"

test: ## Run tests
	@echo "Running tests..."
	@$(GO) test -v ./...

fmt: ## Format code
	@echo "Formatting code..."
	@$(GO) fmt ./...
	@echo "✓ Format complete"

lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run || true
	@echo "✓ Lint complete"

