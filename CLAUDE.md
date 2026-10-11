# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

A polyglot monorepo with three services, each in its own top-level directory and each with its own toolchain:

- `backend-go/`: Go gateway, ingest consumer and simulator (one module, binaries in `cmd/`)
- `agent-python/`: FastAPI agent service (uv, `src/` layout)
- `frontend-vue/`: Vue 3 + Vite dashboard (pnpm)
- `api/`: contracts (JSON Schemas, and OpenAPI once the gateway exists)
- `deploy/`: docker-compose stack and Dockerfiles

Status: milestone M1 is in progress. Only the simulator is implemented. The gateway, ingest consumer and WebSocket hub don't exist yet, and the agent and frontend are skeletons.

## Commands

The root `Makefile` wraps everything (`make help`). Per service:

| | All tests | Single test | Lint |
|---|---|---|---|
| `backend-go/` | `go test -race ./...` | `go test ./internal/simulator -run TestEngineStream` | `go vet ./... && golangci-lint run` |
| `agent-python/` | `uv run pytest` | `uv run pytest tests/test_app.py::test_health_endpoints` | `uv run ruff check && uv run ruff format --check && uv run mypy src` |
| `frontend-vue/` | `pnpm test` | `pnpm vitest run src/api/errors.spec.ts` | `pnpm lint && pnpm type-check` |

Simulator: `go run ./cmd/simulator -sink stdout -speed 0` prints a full scenario instantly as JSON lines (logs go to stderr). `-h` lists the flags, and each flag has a `VP_SIM_*` env equivalent. `make dev` runs Postgres, Redis and the simulator in docker compose.

## Architecture

The product is **VenuePulse AI**, an operational copilot for venues. Read `docs/architecture.md` before designing or implementing anything. It defines the service boundaries, contracts, domain model and planned folder layout. The invariants that must not be broken without an ADR in `docs/adr/`:

- `backend-go` is the only service that writes to Postgres.
- `agent-python` has no DB access. It reads and proposes only through Go's `/internal/v1` API.
- The AI never executes actions. It creates `ActionProposal`s, which a human operator approves.
- `api/` is the contract source of truth, and client types are generated from it.

Ingest: data sources publish `events.Envelope` (`backend-go/internal/events`, schema `api/events/envelope.schema.json`) to the Redis Stream `events:{venue_id}`, as JSON in the field `envelope`. `(source_id, event_id)` is the idempotency key. The simulator uses this same path, so a real adapter must only emit envelopes. If you change the envelope, change the Go types and the schema together.

Simulator scenarios (`backend-go/scenarios/*.yaml`) are venue-type neutral. A zone emits `occupancy` if it has `capacity` + `occupancy`, and emits queue metrics if it has `arrivals` + `service_rate_per_min`. Unknown YAML fields are rejected. `TestBundledScenariosAreValid` checks every bundled scenario.

## Conventions

- Handle errors explicitly at every boundary: wrap them with context and don't silently swallow them. In Go, wrap with `%w` and collect validation errors with `errors.Join` (see `simulator.Scenario.Validate`). In Python, raise `AgentError` subclasses (`errors.py`), which map to problem+json or SSE error codes. The frontend parses errors into `ApiError` (`src/api/errors.ts`).
- Use structured JSON logs (key/value fields, not interpolated strings): `log/slog` in Go via `internal/logging`, and `structlog` in Python via `venuepulse_agent.logging`. Every record carries `service`, and request-scoped records carry `request_id` (`X-Request-ID`).
- Config is typed and comes from `VP_*` env vars, and invalid config fails at startup. In Go, flags override env (`internal/config`). In Python, config uses `pydantic-settings` (`config.py`). Keep each service's `.env.example` in sync.
