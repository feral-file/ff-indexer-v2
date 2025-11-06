.PHONY: help build build-api build-ethereum-event-emitter build-tezos-event-emitter build-event-bridge build-worker-core build-worker-media \
	up down start stop restart logs ps clean config \
	up-infra up-emitters up-workers up-api \
	build-all rebuild

# Docker Compose settings
# Use clean environment to ensure .env files take precedence over host shell variables
# Keep only essential system variables (PATH, HOME, USER) and load from .env files
DOCKER_COMPOSE := env -i \
	PATH="$$PATH" \
	HOME="$$HOME" \
	USER="$$USER" \
	docker compose -f tools/docker/docker-compose.yaml \
	--env-file config/.env \
	--env-file config/.env.local
DOCKER_COMPOSE_FLAGS := 

# Service names
INFRA_SERVICES := postgres temporal-postgres temporal nats
EMITTER_SERVICES := ethereum-event-emitter tezos-event-emitter
WORKER_SERVICES := worker-core worker-media
APP_SERVICES := event-bridge api
ALL_APP_SERVICES := $(EMITTER_SERVICES) $(WORKER_SERVICES) $(APP_SERVICES)

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

build-all: ## Build all application services in sequence
	@echo "$(COLOR_GREEN)Building all services...$(COLOR_RESET)"
	@$(MAKE) build-ethereum-event-emitter
	@$(MAKE) build-tezos-event-emitter
	@$(MAKE) build-event-bridge
	@$(MAKE) build-worker-core
	@$(MAKE) build-worker-media
	@$(MAKE) build-api
	@echo "$(COLOR_GREEN)✓ All services built successfully$(COLOR_RESET)"

build: build-all ## Alias for build-all

build-api: ## Build API service
	@echo "$(COLOR_YELLOW)Building API service...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build api

build-ethereum-event-emitter: ## Build Ethereum event emitter service
	@echo "$(COLOR_YELLOW)Building Ethereum event emitter...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build ethereum-event-emitter

build-tezos-event-emitter: ## Build Tezos event emitter service
	@echo "$(COLOR_YELLOW)Building Tezos event emitter...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build tezos-event-emitter

build-event-bridge: ## Build event bridge service
	@echo "$(COLOR_YELLOW)Building event bridge...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build event-bridge

build-worker-core: ## Build worker-core service
	@echo "$(COLOR_YELLOW)Building worker-core...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build worker-core

build-worker-media: ## Build worker-media service
	@echo "$(COLOR_YELLOW)Building worker-media...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build worker-media

rebuild: ## Rebuild all services (no cache)
	@echo "$(COLOR_GREEN)Rebuilding all services (no cache)...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) build --no-cache $(ALL_APP_SERVICES)

##@ Run Commands

up: ## Start all services
	@echo "$(COLOR_GREEN)Starting all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(COLOR_GREEN)✓ All services started$(COLOR_RESET)"
	@$(MAKE) ps

up-infra: ## Start only infrastructure services (postgres, temporal, nats)
	@echo "$(COLOR_GREEN)Starting infrastructure services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) up -d $(INFRA_SERVICES)
	@echo "$(COLOR_GREEN)✓ Infrastructure services started$(COLOR_RESET)"
	@$(MAKE) ps

up-emitters: up-infra ## Start event emitters (requires infrastructure)
	@echo "$(COLOR_GREEN)Starting event emitters...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) up -d nats-setup
	@$(DOCKER_COMPOSE) up -d $(EMITTER_SERVICES)
	@echo "$(COLOR_GREEN)✓ Event emitters started$(COLOR_RESET)"
	@$(MAKE) ps

up-workers: up-infra ## Start worker services (requires infrastructure)
	@echo "$(COLOR_GREEN)Starting worker services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) up -d $(WORKER_SERVICES)
	@echo "$(COLOR_GREEN)✓ Worker services started$(COLOR_RESET)"
	@$(MAKE) ps

up-api: up-infra up-workers ## Start API service (requires infrastructure and workers)
	@echo "$(COLOR_GREEN)Starting API service...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) up -d api
	@echo "$(COLOR_GREEN)✓ API service started$(COLOR_RESET)"
	@$(MAKE) ps

start: up ## Alias for up

run: build-all up ## Build and start all services

##@ Control Commands

down: ## Stop and remove all services
	@echo "$(COLOR_YELLOW)Stopping all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) down

stop: ## Stop all services (keep containers)
	@echo "$(COLOR_YELLOW)Stopping all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) stop

restart: ## Restart all services
	@echo "$(COLOR_YELLOW)Restarting all services...$(COLOR_RESET)"
	@$(DOCKER_COMPOSE) restart

restart-api: ## Restart only API service
	@$(DOCKER_COMPOSE) restart api

restart-workers: ## Restart worker services
	@$(DOCKER_COMPOSE) restart $(WORKER_SERVICES)

restart-emitters: ## Restart event emitters
	@$(DOCKER_COMPOSE) restart $(EMITTER_SERVICES)

##@ Monitoring Commands

logs: ## Show logs for all services (follow mode)
	@$(DOCKER_COMPOSE) logs -f

logs-api: ## Show logs for API service
	@$(DOCKER_COMPOSE) logs -f api

logs-workers: ## Show logs for worker services
	@$(DOCKER_COMPOSE) logs -f $(WORKER_SERVICES)

logs-emitters: ## Show logs for event emitters
	@$(DOCKER_COMPOSE) logs -f $(EMITTER_SERVICES)

logs-bridge: ## Show logs for event bridge service
	@$(DOCKER_COMPOSE) logs -f event-bridge

logs-infra: ## Show logs for infrastructure services
	@$(DOCKER_COMPOSE) logs -f $(INFRA_SERVICES)

ps: ## Show status of all services
	@$(DOCKER_COMPOSE) ps

##@ Development Commands

shell-api: ## Open shell in API container
	@$(DOCKER_COMPOSE) exec api sh

shell-worker-core: ## Open shell in worker-core container
	@$(DOCKER_COMPOSE) exec worker-core sh

shell-worker-media: ## Open shell in worker-media container
	@$(DOCKER_COMPOSE) exec worker-media sh

shell-ethereum-event-emitter: ## Open shell in ethereum-event-emitter container
	@$(DOCKER_COMPOSE) exec ethereum-event-emitter sh

shell-tezos-event-emitter: ## Open shell in tezos-event-emitter container
	@$(DOCKER_COMPOSE) exec tezos-event-emitter sh

shell-event-bridge: ## Open shell in event-bridge container
	@$(DOCKER_COMPOSE) exec event-bridge sh

config: ## Validate and view docker-compose configuration
	@$(DOCKER_COMPOSE) config

env-api: ## Show environment variables for API service
	@$(DOCKER_COMPOSE) exec api env | grep FF_INDEXER | sort

env-worker-core: ## Show environment variables for worker-core service
	@$(DOCKER_COMPOSE) exec worker-core env | grep FF_INDEXER | sort

env-worker-media: ## Show environment variables for worker-media service
	@$(DOCKER_COMPOSE) exec worker-media env | grep FF_INDEXER | sort

env-ethereum-event-emitter: ## Show environment variables for ethereum-event-emitter service
	@$(DOCKER_COMPOSE) exec ethereum-event-emitter env | grep FF_INDEXER | sort

env-tezos-event-emitter: ## Show environment variables for tezos-event-emitter service
	@$(DOCKER_COMPOSE) exec tezos-event-emitter env | grep FF_INDEXER | sort

env-event-bridge: ## Show environment variables for event-bridge service
	@$(DOCKER_COMPOSE) exec event-bridge env | grep FF_INDEXER | sort

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
	@if [ ! -f config/.env ]; then \
		echo "$(COLOR_YELLOW)Creating config/.env from sample...$(COLOR_RESET)"; \
		cp config/.env.sample config/.env; \
	else \
		echo "$(COLOR_GREEN)config/.env already exists$(COLOR_RESET)"; \
	fi
	@if [ ! -f config/.env.local ]; then \
		echo "$(COLOR_YELLOW)Creating config/.env.local from sample...$(COLOR_RESET)"; \
		cp config/.env.sample config/.env.local; \
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

##@ Quick Start

quickstart: setup build-all up ## Complete setup, build, and start (first time setup)
	@echo ""
	@echo "$(COLOR_GREEN)╔════════════════════════════════════════════════════════╗$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  🎉  FF-Indexer v2 is running!                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  API:          http://localhost:8081                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Temporal UI:  http://localhost:8080                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  NATS Monitor: http://localhost:8222                   ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make logs' to view logs                         ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║  Run 'make ps' to view service status                 ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)║                                                        ║$(COLOR_RESET)"
	@echo "$(COLOR_GREEN)╚════════════════════════════════════════════════════════╝$(COLOR_RESET)"
	@echo ""

dev: up-infra ## Start development mode (only infrastructure, run services locally)
	@echo "$(COLOR_GREEN)Development mode: Infrastructure running$(COLOR_RESET)"
	@echo "$(COLOR_YELLOW)Run services locally with Go:$(COLOR_RESET)"
	@echo "  cd cmd/api && go run main.go"
	@echo "  cd cmd/worker-core && go run main.go"
	@echo ""
	@echo "$(COLOR_BLUE)Available services:$(COLOR_RESET)"
	@echo "  PostgreSQL: localhost:5432"
	@echo "  Temporal:   localhost:7233"
	@echo "  NATS:       localhost:4222"

