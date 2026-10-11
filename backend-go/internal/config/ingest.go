package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

const defaultDatabaseURL = "postgres://venuepulse:venuepulse@localhost:5432/venuepulse?sslmode=disable"

type Ingest struct {
	DatabaseURL     string
	RedisURL        string
	LogLevel        string
	Group           string
	Consumer        string
	BatchSize       int64
	Block           time.Duration
	ClaimMinIdle    time.Duration
	ClaimInterval   time.Duration
	MaxDeliveries   int64
	RefreshInterval time.Duration
	DLQStream       string
}

// LoadIngest parses args over env defaults. defaultConsumer should be unique
// per process (e.g. hostname-pid) so pending entries can be attributed.
func LoadIngest(args []string, getenv func(string) string, usage io.Writer, defaultConsumer string) (Ingest, error) {
	env := envReader{getenv: getenv}
	cfg := Ingest{
		DatabaseURL:     env.str("VP_DATABASE_URL", defaultDatabaseURL),
		RedisURL:        env.str("VP_REDIS_URL", "redis://localhost:6379/0"),
		LogLevel:        env.str("VP_LOG_LEVEL", "info"),
		Group:           env.str("VP_INGEST_GROUP", "ingest"),
		Consumer:        env.str("VP_INGEST_CONSUMER", defaultConsumer),
		BatchSize:       env.int64("VP_INGEST_BATCH_SIZE", 500),
		Block:           env.duration("VP_INGEST_BLOCK", 2*time.Second),
		ClaimMinIdle:    env.duration("VP_INGEST_CLAIM_MIN_IDLE", 30*time.Second),
		ClaimInterval:   env.duration("VP_INGEST_CLAIM_INTERVAL", 15*time.Second),
		MaxDeliveries:   env.int64("VP_INGEST_MAX_DELIVERIES", 5),
		RefreshInterval: env.duration("VP_INGEST_REFRESH_INTERVAL", 10*time.Second),
		DLQStream:       env.str("VP_INGEST_DLQ_STREAM", "events:dlq"),
	}
	if err := errors.Join(env.errs...); err != nil {
		return Ingest{}, fmt.Errorf("environment: %w", err)
	}

	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(usage)
	fs.StringVar(&cfg.DatabaseURL, "database-url", cfg.DatabaseURL, "Postgres URL [VP_DATABASE_URL]")
	fs.StringVar(&cfg.RedisURL, "redis-url", cfg.RedisURL, "Redis URL [VP_REDIS_URL]")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug|info|warn|error [VP_LOG_LEVEL]")
	fs.StringVar(&cfg.Group, "group", cfg.Group, "consumer group [VP_INGEST_GROUP]")
	fs.StringVar(&cfg.Consumer, "consumer", cfg.Consumer, "consumer name, unique per process [VP_INGEST_CONSUMER]")
	fs.Int64Var(&cfg.BatchSize, "batch-size", cfg.BatchSize, "max entries per read [VP_INGEST_BATCH_SIZE]")
	fs.DurationVar(&cfg.Block, "block", cfg.Block, "XREADGROUP block timeout [VP_INGEST_BLOCK]")
	fs.DurationVar(&cfg.ClaimMinIdle, "claim-min-idle", cfg.ClaimMinIdle, "reclaim pending entries idle this long [VP_INGEST_CLAIM_MIN_IDLE]")
	fs.DurationVar(&cfg.ClaimInterval, "claim-interval", cfg.ClaimInterval, "how often to check for reclaimable entries [VP_INGEST_CLAIM_INTERVAL]")
	fs.Int64Var(&cfg.MaxDeliveries, "max-deliveries", cfg.MaxDeliveries, "dead-letter entries delivered this many times [VP_INGEST_MAX_DELIVERIES]")
	fs.DurationVar(&cfg.RefreshInterval, "refresh-interval", cfg.RefreshInterval, "how often to reload venues and zones [VP_INGEST_REFRESH_INTERVAL]")
	fs.StringVar(&cfg.DLQStream, "dlq-stream", cfg.DLQStream, "dead-letter stream [VP_INGEST_DLQ_STREAM]")
	if err := fs.Parse(args); err != nil {
		return Ingest{}, fmt.Errorf("flags: %w", err)
	}
	if fs.NArg() > 0 {
		return Ingest{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return cfg, cfg.validate()
}

func (c Ingest) validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("database-url", c.DatabaseURL)
	req("redis-url", c.RedisURL)
	req("group", c.Group)
	req("consumer", c.Consumer)
	req("dlq-stream", c.DLQStream)
	if c.BatchSize <= 0 {
		errs = append(errs, errors.New("batch-size must be > 0"))
	}
	if c.MaxDeliveries <= 0 {
		errs = append(errs, errors.New("max-deliveries must be > 0"))
	}
	for name, d := range map[string]time.Duration{
		"block": c.Block, "claim-min-idle": c.ClaimMinIdle,
		"claim-interval": c.ClaimInterval, "refresh-interval": c.RefreshInterval,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0", name))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

// Admin is the shared config for vpadmin subcommands.
type Admin struct {
	DatabaseURL string
	LogLevel    string
}

func LoadAdmin(getenv func(string) string) Admin {
	env := envReader{getenv: getenv}
	return Admin{
		DatabaseURL: env.str("VP_DATABASE_URL", defaultDatabaseURL),
		LogLevel:    env.str("VP_LOG_LEVEL", "info"),
	}
}
