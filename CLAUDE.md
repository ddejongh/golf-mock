# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

A polyglot monorepo with three services, each in its own top-level directory:

- `backend-go/`: Go backend
- `agent-python/`: Python agent service
- `frontend-vue/`: Vue 3 frontend

Each service has its own toolchain. Run build, lint, and test commands from inside that service's directory, not from the repo root.

Status: the service directories are scaffolded but empty. No build, test, or lint commands, service contracts, or inter-service wiring have been defined yet. When they are added, record them here instead of guessing.

## Architecture

The product is **VenuePulse AI**, an operational copilot for venues. Read `docs/architecture.md` before designing or implementing anything. It defines the service boundaries, contracts (`api/`), domain model and planned folder layout. The invariants that must not be broken without an ADR in `docs/adr/`:

- `backend-go` is the only service that writes to Postgres.
- `agent-python` has no DB access. It reads and proposes only through Go's `/internal/v1` API.
- The AI never executes actions. It creates `ActionProposal`s, which a human operator approves.
- `api/` (OpenAPI and WS JSON Schemas) is the contract source of truth, and client types are generated from it.

## Conventions

- Handle errors explicitly at every boundary: wrap them with context and don't silently swallow them.
- Use structured logging (key/value fields, not interpolated strings) in all services.
