package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/ddejongh/venuepulse/backend-go/migrations"
)

// Migrator applies the embedded goose migrations.
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

func (s *Store) Migrator() (*Migrator, error) {
	db := stdlib.OpenDBFromPool(s.pool)
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init migrations: %w", err)
	}
	return &Migrator{db: db, provider: p}, nil
}

// Close releases the database/sql handle; the underlying pool stays open.
func (m *Migrator) Close() error {
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("close migrator: %w", err)
	}
	return nil
}

func (m *Migrator) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	res, err := m.provider.Up(ctx)
	if err != nil {
		return res, fmt.Errorf("migrate up: %w", err)
	}
	return res, nil
}

func (m *Migrator) Down(ctx context.Context) (*goose.MigrationResult, error) {
	res, err := m.provider.Down(ctx)
	if err != nil {
		return res, fmt.Errorf("migrate down: %w", err)
	}
	return res, nil
}

func (m *Migrator) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	st, err := m.provider.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("migration status: %w", err)
	}
	return st, nil
}

// HasPending reports whether any embedded migration has not been applied.
func (m *Migrator) HasPending(ctx context.Context) (bool, error) {
	pending, err := m.provider.HasPending(ctx)
	if err != nil {
		return false, fmt.Errorf("check pending migrations: %w", err)
	}
	return pending, nil
}
