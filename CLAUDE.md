# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

A polyglot monorepo with three services, each in its own top-level directory:

- `backend-go/`: Go backend
- `agent-python/`: Python agent service
- `frontend-vue/`: Vue 3 frontend

Each service has its own toolchain. Run build, lint, and test commands from inside that service's directory, not from the repo root.

Status: the service directories are scaffolded but empty. No build, test, or lint commands, service contracts, or inter-service wiring have been defined yet. When they are added, record them here instead of guessing.

## Conventions

- Handle errors explicitly at every boundary: wrap them with context and don't silently swallow them.
- Use structured logging (key/value fields, not interpolated strings) in all services.
