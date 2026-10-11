// Package store is backend-go's Postgres access layer: a pgx pool plus the
// sqlc-generated queries in ./sqlc (regenerate with `make go-generate`).
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ddejongh/venuepulse/backend-go/internal/store/sqlc"
)

type Store struct {
	pool *pgxpool.Pool
	q    *sqlc.Queries

	// partitions caches UTC days whose metric_samples partition is known to
	// exist, so ensuring one costs a query only once per day per process.
	partitions sync.Map // time.Time (UTC midnight) -> struct{}
}

// Open connects to Postgres and verifies the connection.
func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres at %s: %w", cfg.ConnConfig.Host, err)
	}
	return &Store{pool: pool, q: sqlc.New(pool)}, nil
}

func (s *Store) Close() { s.pool.Close() }

// inTx runs fn in a transaction, committing on nil and rolling back otherwise.
func (s *Store) inTx(ctx context.Context, fn func(q *sqlc.Queries) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after commit

	if err := fn(s.q.WithTx(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// isNoRows reports whether err means a :one query matched nothing.
func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
