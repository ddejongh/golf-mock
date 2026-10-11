// Command ingest consumes venue event streams from Redis, persists them to
// Postgres and publishes live updates for the WebSocket hub.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/config"
	"github.com/ddejongh/venuepulse/backend-go/internal/ingest"
	"github.com/ddejongh/venuepulse/backend-go/internal/live"
	"github.com/ddejongh/venuepulse/backend-go/internal/logging"
	"github.com/ddejongh/venuepulse/backend-go/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host, _ := os.Hostname()
	cfg, err := config.LoadIngest(os.Args[1:], os.Getenv, os.Stderr, fmt.Sprintf("%s-%d", host, os.Getpid()))
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest: %v\n", err)
		os.Exit(2)
	}
	log, err := logging.New(os.Stderr, "ingest", cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ingest: %v\n", err)
		os.Exit(2)
	}
	redis.SetLogger(logging.RedisLogger{Log: log})

	if err := run(ctx, cfg, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("ingest failed", slog.Any("error", err))
		os.Exit(1)
	}
	log.Info("ingest stopped")
}

func run(ctx context.Context, cfg config.Ingest, log *slog.Logger) error {
	st, err := store.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := requireMigrated(ctx, st); err != nil {
		return err
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("connect to redis at %s: %w", opts.Addr, err)
	}

	icfg := ingest.DefaultConfig()
	icfg.Group, icfg.Consumer = cfg.Group, cfg.Consumer
	icfg.BatchSize, icfg.Block = cfg.BatchSize, cfg.Block
	icfg.ClaimMinIdle, icfg.ClaimInterval = cfg.ClaimMinIdle, cfg.ClaimInterval
	icfg.MaxDeliveries, icfg.RefreshInterval = cfg.MaxDeliveries, cfg.RefreshInterval
	icfg.DLQStream = cfg.DLQStream

	log.Info("ingest started", slog.String("group", cfg.Group), slog.String("consumer", cfg.Consumer))
	consumer := ingest.NewConsumer(icfg, rdb, st, &live.Publisher{Client: rdb}, log)
	return consumer.Run(ctx) //nolint:wrapcheck // Run already wraps
}

func requireMigrated(ctx context.Context, st *store.Store) error {
	m, err := st.Migrator()
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()
	pending, err := m.HasPending(ctx)
	if err != nil {
		return err
	}
	if pending {
		return errors.New("database has pending migrations; run `vpadmin migrate up`")
	}
	return nil
}
