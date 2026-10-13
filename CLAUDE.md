# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

A polyglot monorepo with three services, each in its own top-level directory and each with its own toolchain:

- `backend-go/`: one Go module with binaries in `cmd/`: `simulator`, `ingest`, `vpadmin` (migrations and seeding), and later `gateway`
- `agent-python/`: FastAPI agent service (uv, `src/` layout)
- `frontend-vue/`: Vue 3 + Vite dashboard (pnpm)
- `api/`: contracts (JSON Schemas, and OpenAPI once the gateway exists)
- `deploy/`: docker-compose stack and Dockerfiles

Status: milestone M1 is in progress. The simulator and ingest pipeline are implemented. The gateway and WebSocket hub don't exist yet, and the agent and frontend are skeletons.

## Commands

The root `Makefile` wraps everything (`make help`). Per service:

| | All tests | Single test | Lint |
|---|---|---|---|
| `backend-go/` | `go test -race ./...` | `go test ./internal/ingest -run TestSimulatorToPostgres` | `go vet ./... && golangci-lint run` |
| `agent-python/` | `uv run pytest` | `uv run pytest tests/test_app.py::test_health_endpoints` | `uv run ruff check && uv run ruff format --check && uv run mypy src` |
| `frontend-vue/` | `pnpm test` | `pnpm vitest run src/api/errors.spec.ts` | `pnpm lint && pnpm type-check` |

- Go DB tests (`internal/store`, `internal/ingest`) use `internal/testutil/pgtest`. It connects to `VP_TEST_DATABASE_URL` if set, and otherwise downloads (once) and starts an embedded Postgres 17. No docker is needed. Set `VP_TEST_SKIP_DB=1` to skip these tests. Any package that uses `pgtest.New` needs `func TestMain(m *testing.M) { pgtest.Main(m) }`.
- After editing `migrations/*.sql` or `internal/store/queries/*.sql`, run `make go-generate` (sqlc v1.31.1 via `go run`, which needs cgo). Never hand-edit `internal/store/sqlc/`.
- The simulator: `go run ./cmd/simulator -sink stdout -speed 0` prints a full scenario instantly as JSON lines (logs go to stderr). Every binary takes `-h`, and each flag has a `VP_*` env equivalent.
- Local pipeline order: `vpadmin migrate up` → `vpadmin seed -scenario …` → `ingest` + `simulator`. `make dev` does this in docker compose. `ingest` refuses to start while migrations are pending.

## Architecture

The product is **VenuePulse AI**, an operational copilot for venues. Read `docs/architecture.md` before designing or implementing anything. It defines the service boundaries, contracts, domain model and planned folder layout. The invariants that must not be broken without an ADR in `docs/adr/`:

- `backend-go` is the only service that writes to Postgres.
- `agent-python` has no DB access. It reads and proposes only through Go's `/internal/v1` API.
- The AI never executes actions. It creates `ActionProposal`s, which a human operator approves.
- `api/` is the contract source of truth, and client types are generated from it.

Ingest: data sources publish `events.Envelope` (`backend-go/internal/events`, schema `api/events/envelope.schema.json`) to the Redis Stream `events:{venue_id}`, as JSON in the field `envelope`. `(source_id, event_id)` is the idempotency key. The simulator uses this same path, so a real adapter must only emit envelopes. If you change the envelope, change the Go types and the schema together.

The ingest consumer (`internal/ingest`) reads every seeded venue's stream through consumer group `ingest`. It works at-least-once:
- Invalid entries go to `events:dlq` with a reason and are acked.
- Valid entries are acked only after the Postgres transaction commits.
- Failed entries stay pending, are reclaimed with XPENDING/XCLAIM, and are dead-lettered after `MaxDeliveries`.

Postgres dedupes on `(source_id, event_id)`. `metric_samples` is range-partitioned by UTC day, and `store.Ingest` creates partitions on demand through the SQL function `ensure_metric_samples_partition`. After commit, `internal/live` updates the latest-value hash `metrics:latest:{venue}` and publishes WS messages (`api/ws/message.schema.json`) with per-venue `seq` to pub/sub `ws:{venue}`. This step is best-effort.

Simulator scenarios (`backend-go/scenarios/*.yaml`) are venue-type neutral. A zone emits `occupancy` if it has `capacity` + `occupancy`, and emits queue metrics if it has `arrivals` + `service_rate_per_min`. Unknown YAML fields are rejected. `TestBundledScenariosAreValid` checks every bundled scenario.

## Conventions

- Handle errors explicitly at every boundary: wrap them with context and don't silently swallow them. In Go, wrap with `%w` and collect validation errors with `errors.Join` (see `simulator.Scenario.Validate`). In Python, raise `AgentError` subclasses (`errors.py`), which map to problem+json or SSE error codes. The frontend parses errors into `ApiError` (`src/api/errors.ts`).
- Use structured JSON logs (key/value fields, not interpolated strings): `log/slog` in Go via `internal/logging`, and `structlog` in Python via `venuepulse_agent.logging`. Every record carries `service`, and request-scoped records carry `request_id` (`X-Request-ID`).
- Config is typed and comes from `VP_*` env vars, and invalid config fails at startup. In Go, flags override env (`internal/config`). In Python, config uses `pydantic-settings` (`config.py`). Keep each service's `.env.example` in sync.
