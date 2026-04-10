.PHONY: help build build-full rebuild rebuild-full run run-full quickstart quickstart-full \
	up down start stop restart logs ps clean config \
	up-infra up-app dev

# Docker Compose settings
# Use clean environment to ensure .env files take precedence over host shell variables
# Keep only essential system variables (PATH, HOME, USER) and load from .env files
DOCKER_COMPOSE := env -i \
	PATH="$$PATH" \
	HOME="$$HOME" \
	USER="$$USER" \
	docker compose -f tools/docker/Docker-compose.yaml \
	--env-file config/.env \
	--env-file config/.env.local
DOCKER_COMPOSE_FLAGS := 
# Drop containers from removed compose services (e.g. old per-worker images).
DOCKER_COMPOSE_UP := $(DOCKER_COMPOSE) up -d --remove-orphans

# Service names
INFRA_SERVICES := postgres temporal-postgres temporal temporal-ui nats redis
APP_SERVICE := ff-indexer
ALL_APP_SERVICES := $(APP_SERVICE)

# Colors for output
COLOR_RESET := \033[0m
COLOR_BOLD := \033[1m
COLOR_GREEN := \033[32m
COLOR_YELLOW := \033[33m
COLOR_BLUE := \033[34m

##@ General

help: ## Display this help message
	@echo "$(COLOR_BOLD)FF-Indexer v2 - Make Commands$(COLOR_RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make $(COLOR_BLUE)<target>$(COLOR_RESET)\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  $(COLOR_BLUE)%-25s$(COLOR_RESET) %s\n", $$1, $$2 } /^##@/ { printf "\n$(COLOR_BOLD)%s$(COLOR_RESET)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build Commands

build: ## Build lightweight image (default, no media processing)
	@echo "$(COLOR_YELLOW)Building ff-indexer (lightweight mode)...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build $(APP_SERVICE)
	@echo "$(COLOR_GREEN)✓ ff-indexer built (lightweight: ~112MB)$(COLOR_RESET)"

rebuild: ## Rebuild lightweight image (no cache)
	@echo "$(COLOR_YELLOW)Rebuilding ff-indexer (lightweight mode, no cache)...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build --no-cache $(ALL_APP_SERVICES)
	@echo "$(COLOR_GREEN)✓ ff-indexer rebuilt (lightweight)$(COLOR_RESET)"

build-full: ## Build full image with media processing (CGO + vips + chromium)
	@echo "$(COLOR_YELLOW)Building ff-indexer (full mode with media processing)...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build --build-arg CGO_ENABLED=1 $(APP_SERVICE)
	@echo "$(COLOR_GREEN)✓ ff-indexer built (full: ~730MB, media enabled)$(COLOR_RESET)"

rebuild-full: ## Rebuild full image with media processing (no cache)
	@echo "$(COLOR_YELLOW)Rebuilding ff-indexer (full mode with media, no cache)...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build --no-cache --build-arg CGO_ENABLED=1 $(APP_SERVICE)
	@echo "$(COLOR_GREEN)✓ ff-indexer rebuilt (full, media enabled)$(COLOR_RESET)"

##@ Run Commands

up: ## Start all services
	@echo "$(COLOR_GREEN)Starting all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE_UP)
	@echo "$(COLOR_GREEN)✓ All services started$(COLOR_RESET)"
	@$(MAKE) ps

up-infra: ## Start only infrastructure services (postgres, temporal, nats, redis)
	@echo "$(COLOR_GREEN)Starting infrastructure services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE_UP) $(INFRA_SERVICES)
	@echo "$(COLOR_GREEN)✓ Infrastructure services started$(COLOR_RESET)"
	@$(MAKE) ps

up-app: up-infra ## Start NATS stream setup + ff-indexer application
	@echo "$(COLOR_GREEN)Starting ff-indexer...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE_UP) nats-setup
	@$(DOCKER_COMPOSE_UP) $(APP_SERVICE)
	@echo "$(COLOR_GREEN)✓ ff-indexer started$(COLOR_RESET)"
	@$(MAKE) ps

start: up ## Alias for up

run: build up ## Build and start (lightweight mode)
	@echo ""
	@echo "$(COLOR_GREEN)✓ ff-indexer running in lightweight mode$(COLOR_RESET)"

run-full: build-full up ## Build and start (full mode with media processing)
	@echo ""
	@echo "$(COLOR_GREEN)✓ ff-indexer running in full mode (media processing enabled)$(COLOR_RESET)"

##@ Control Commands

down: ## Stop and remove all services
	@echo "$(COLOR_YELLOW)Stopping all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) down --remove-orphans

down-app: ## Stop and remove ff-indexer application container
	@echo "$(COLOR_YELLOW)Stopping ff-indexer...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) down $(APP_SERVICE)

down-infra: ## Stop and remove infrastructure services
	@echo "$(COLOR_YELLOW)Stopping infrastructure services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) down $(INFRA_SERVICES)

stop: ## Stop all services (keep containers)
	@echo "$(COLOR_YELLOW)Stopping all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) stop

stop-app: ## Stop ff-indexer container (keep infra)
	@echo "$(COLOR_YELLOW)Stopping ff-indexer...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) stop $(APP_SERVICE)

stop-infra: ## Stop infrastructure services
	@echo "$(COLOR_YELLOW)Stopping infrastructure services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) stop $(INFRA_SERVICES)

restart: ## Restart all services
	@echo "$(COLOR_YELLOW)Restarting all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) restart

restart-app: ## Restart ff-indexer container
	@$(DOCKER_COMPOSE) restart $(APP_SERVICE)

restart-infra: ## Restart infrastructure services
	@$(DOCKER_COMPOSE) restart $(INFRA_SERVICES)

##@ Monitoring Commands

logs: ## Show logs for all services (follow mode)
	@$(DOCKER_COMPOSE) logs -f

logs-app: ## Show logs for ff-indexer
	@$(DOCKER_COMPOSE) logs -f $(APP_SERVICE)

logs-infra: ## Show logs for infrastructure services
	@$(DOCKER_COMPOSE) logs -f $(INFRA_SERVICES)

ps: ## Show status of all services
	@$(DOCKER_COMPOSE) ps

##@ Development Commands

shell-app: ## Open shell in ff-indexer container
	@$(DOCKER_COMPOSE) exec $(APP_SERVICE) sh

config: ## Validate and view docker-compose configuration
	@$(DOCKER_COMPOSE) config

env-app: ## Show FF_INDEXER environment variables in ff-indexer container
	@$(DOCKER_COMPOSE) exec $(APP_SERVICE) env | grep FF_INDEXER | sort

##@ Cleanup Commands

clean: down ## Stop services and remove volumes (WARNING: deletes data)
	@echo "$(COLOR_YELLOW)⚠️  Removing all volumes and data...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) down -v
	@echo "$(COLOR_GREEN)✓ Cleanup complete$(COLOR_RESET)"

clean-images: ## Remove all built images
	@echo "$(COLOR_YELLOW)Removing all built images...$(COLOR_RESET)"
	@docker images | grep ff-indexer | awk '{print $$3}' | xargs -r docker rmi -f
	@echo "$(COLOR_GREEN)✓ Images removed$(COLOR_RESET)"

prune: ## Prune Docker system (removes unused data)
	@echo "$(COLOR_YELLOW)Pruning Docker system...$(COLOR_RESET)"
	@docker system prune -f
	@echo "$(COLOR_GREEN)✓ Prune complete$(COLOR_RESET)"

##@ Setup Commands

setup: ## Initial setup - create env files if they don't exist
	@if [ ! -f config/.env.local ]; then \
		echo "$(COLOR_YELLOW)Creating config/.env.local from sample...$(COLOR_RESET)"; \
		cp config/.env config/.env.local; \
		echo "$(COLOR_YELLOW)⚠️  Please update config/.env.local with your credentials$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_GREEN)config/.env.local already exists$(COLOR_RESET)"; \
	fi

check-env: ## Check if required environment files exist
	@echo "$(COLOR_BLUE)Checking environment configuration...$(COLOR_RESET)"
	@if [ -f config/.env ]; then \
		echo "$(COLOR_GREEN)✓ config/.env exists$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_YELLOW)✗ config/.env missing - run 'make setup'$(COLOR_RESET)"; \
	fi
	@if [ -f config/.env.local ]; then \
		echo "$(COLOR_GREEN)✓ config/.env.local exists$(COLOR_RESET)"; \
	else \
		echo "$(COLOR_YELLOW)✗ config/.env.local missing - run 'make setup'$(COLOR_RESET)"; \
	fi

##@ Testing Commands

test-config: check-env ## Test docker-compose configuration
	@echo "$(COLOR_BLUE)Testing docker-compose configuration...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) config > /dev/null 2>&1 && \
		echo "$(COLOR_GREEN)✓ Configuration is valid$(COLOR_RESET)" || \
		echo "$(COLOR_YELLOW)✗ Configuration has errors$(COLOR_RESET)"

health: ## Check health status of all services
	@echo "$(COLOR_BLUE)Service Health Status:$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Health}}"

##@ Check Commands

lint: ## Run linters in Docker (CGO files skipped - use lint-local for full check)
	@echo "$(COLOR_BLUE)Running linters in Docker...$(COLOR_RESET)"
	@echo "$(COLOR_YELLOW)Note: CGO files are skipped in Docker$(COLOR_RESET)"
	@docker run --rm -v "$(PWD):/app" -w /app -e CGO_ENABLED=0 golangci/golangci-lint:v2.1.6 golangci-lint run --verbose
	@echo "$(COLOR_GREEN)✓ Linters passed$(COLOR_RESET)"

lint-local: ## Run linters locally with full CGO support (requires libvips: brew install vips)
	@echo "$(COLOR_BLUE)Running linters locally with CGO support...$(COLOR_RESET)"
	@golangci-lint run --verbose
	@echo "$(COLOR_GREEN)✓ Linters passed$(COLOR_RESET)"

imports: ## Format imports
	@echo "$(COLOR_BLUE)Formatting imports...$(COLOR_RESET)"
	@goimports -w -local "github.com/feral-file/ff-indexer-v2" .
	@echo "$(COLOR_GREEN)✓ Imports formatted$(COLOR_RESET)"

test: ## Run tests
	@echo "$(COLOR_BLUE)Running tests...$(COLOR_RESET)"
	@go test -cover ./...
	@echo "$(COLOR_GREEN)✓ Tests passed$(COLOR_RESET)"

check: imports lint-local test ## Run linters, format imports, and tests
	@echo "$(COLOR_GREEN)✓ All checks passed$(COLOR_RESET)"

##@ Quick Start

quickstart: setup build up ## Complete setup and start (lightweight mode, recommended)
	@echo ""
	@echo "$(COLOR_GREEN)╔════════════════════════════════════════════════════════╗$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  🎉  FF-Indexer v2 is running!                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Mode:         Lightweight (~112MB)                    ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  API:          http://localhost:8081                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Temporal UI:  http://localhost:8080                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  NATS Monitor: http://localhost:18222                  ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make logs' to view logs                         ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make ps' to view service status                 ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Note: Media processing disabled in lightweight mode   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Use 'make quickstart-full' for media support         ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)╚════════════════════════════════════════════════════════╝$(COLOR_RESET)"
	@echo ""

quickstart-full: setup build-full up ## Complete setup and start (full mode with media processing)
	@echo ""
	@echo "$(COLOR_GREEN)╔════════════════════════════════════════════════════════╗$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  🎉  FF-Indexer v2 is running (Full Mode)!            ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Mode:         Full with Media (~730MB)                ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  API:          http://localhost:8081                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Temporal UI:  http://localhost:8080                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  NATS Monitor: http://localhost:18222                  ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Media Processing: ✓ ENABLED                           ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║    - libvips 8.18.2 (image processing)                 ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║    - Chromium (HTML rendering)                         ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make logs' to view logs                         ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make ps' to view service status                 ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)╚════════════════════════════════════════════════════════╝$(COLOR_RESET)"
	@echo ""

dev: up-infra ## Start development mode (only infrastructure; run ff-indexer locally)
	@echo "$(COLOR_GREEN)Development mode: Infrastructure running$(COLOR_RESET)"
	@echo "$(COLOR_YELLOW)Run the binary:$(COLOR_RESET)"
	@echo "  go run ./cmd/ff-indexer -config cmd/ff-indexer/config.yaml"
	@echo ""
	@echo "$(COLOR_BLUE)Available services:$(COLOR_RESET)"
	@echo "  PostgreSQL: localhost:5432"
	@echo "  Temporal:   localhost:7233"
	@echo "  NATS:       localhost:4222"
	@echo "  Redis:      localhost:6379"
	@echo ""
	@echo "$(COLOR_YELLOW)Note: Update config to use nats://localhost:4222$(COLOR_RESET)"

