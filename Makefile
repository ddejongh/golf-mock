.DEFAULT_GOAL := help
COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

## --- Whole stack -----------------------------------------------------------

.PHONY: dev down test lint
dev: ## Start postgres, redis and the simulator (docker compose)
	$(COMPOSE) up --build

down: ## Stop the compose stack
	$(COMPOSE) down

test: go-test py-test fe-test ## Run all test suites

lint: go-lint py-lint fe-lint ## Run all linters

## --- backend-go ------------------------------------------------------------

.PHONY: go-test go-lint go-build sim sim-stdout
go-test: ## Go tests (race detector on)
	cd backend-go && go test -race ./...

go-lint: ## go vet + golangci-lint
	cd backend-go && go vet ./... && golangci-lint run

go-build: ## Build Go binaries into backend-go/bin
	cd backend-go && go build -o bin/ ./cmd/...

sim: ## Run the simulator against local Redis at 10x speed
	cd backend-go && go run ./cmd/simulator -speed 10

sim-stdout: ## Run the simulator instantly, printing JSON events
	cd backend-go && go run ./cmd/simulator -sink stdout -speed 0

## --- agent-python ----------------------------------------------------------

.PHONY: py-install py-test py-lint py-dev
py-install: ## Install Python deps with uv
	cd agent-python && uv sync

py-test: ## Python tests
	cd agent-python && uv run pytest

py-lint: ## ruff + mypy
	cd agent-python && uv run ruff check && uv run ruff format --check && uv run mypy src

py-dev: ## Run the agent with reload on :8090
	cd agent-python && uv run uvicorn venuepulse_agent.main:app --reload --port 8090

## --- frontend-vue ----------------------------------------------------------

.PHONY: fe-install fe-test fe-lint fe-dev
fe-install: ## Install frontend deps with pnpm
	cd frontend-vue && pnpm install

fe-test: ## Vitest
	cd frontend-vue && pnpm test

fe-lint: ## eslint + vue-tsc
	cd frontend-vue && pnpm lint && pnpm type-check

fe-dev: ## Vite dev server on :5173
	cd frontend-vue && pnpm dev
