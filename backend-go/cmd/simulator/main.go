// Command simulator publishes scenario-driven venue events to Redis Streams
// (or stdout), standing in for real data sources during development.
package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/config"
	"github.com/ddejongh/venuepulse/backend-go/internal/events"
	"github.com/ddejongh/venuepulse/backend-go/internal/logging"
	"github.com/ddejongh/venuepulse/backend-go/internal/simulator"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadSimulator(os.Args[1:], os.Getenv, os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulator: %v\n", err)
		os.Exit(2)
	}
	// Logs go to stderr so the stdout sink stays pure JSON lines.
	log, err := logging.New(os.Stderr, "simulator", cfg.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "simulator: %v\n", err)
		os.Exit(2)
	}

	redis.SetLogger(logging.RedisLogger{Log: log})

	if err := run(ctx, cfg, log, os.Stdout); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("simulator failed", slog.Any("error", err))
		os.Exit(1)
	}
	log.Info("simulator stopped")
}

func run(ctx context.Context, cfg config.Simulator, log *slog.Logger, stdout io.Writer) error {
	sc, err := simulator.LoadScenario(cfg.ScenarioPath)
	if err != nil {
		return err
	}

	pub, closePub, err := newPublisher(ctx, cfg, stdout)
	if err != nil {
		return err
	}
	defer func() {
		if err := closePub(); err != nil {
			log.Warn("close publisher", slog.Any("error", err))
		}
	}()

	seed := cfg.Seed
	if seed == 0 {
		if seed, err = randomSeed(); err != nil {
			return err
		}
	}

	log = log.With(slog.String("venue_id", sc.Venue.ID), slog.String("scenario", sc.Name))
	start := time.Now().UTC().Truncate(time.Second)
	for iteration := 1; ; iteration++ {
		runID, err := newRunID()
		if err != nil {
			return err
		}
		runLog := log.With(slog.String("run_id", runID), slog.Int("iteration", iteration))
		runLog.Info("simulation started",
			slog.String("sink", cfg.Sink),
			slog.Float64("speed", cfg.Speed),
			slog.Uint64("seed", seed),
			slog.Time("sim_start", start),
			slog.String("duration", sc.Duration.D().String()))

		eng := simulator.NewEngine(sc, simulator.EngineOptions{
			SourceID: cfg.SourceID,
			RunID:    runID,
			Seed:     seed,
			Start:    start,
		})
		stats, err := simulator.Run(ctx, eng, pub, simulator.RunOptions{Speed: cfg.Speed}, runLog)
		if err != nil {
			return fmt.Errorf("run %s: %w", runID, err)
		}
		runLog.Info("simulation finished",
			slog.Int("ticks", stats.Ticks),
			slog.Int("events", stats.Events),
			slog.Time("sim_end", stats.SimEnd))

		if !cfg.Loop {
			return nil
		}
		// Continue the timeline and vary the next run.
		start = stats.SimEnd
		seed++
	}
}

func newPublisher(ctx context.Context, cfg config.Simulator, stdout io.Writer) (events.Publisher, func() error, error) {
	if cfg.Sink == config.SinkStdout {
		return events.NewJSONLinesPublisher(stdout), func() error { return nil }, nil
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, nil, fmt.Errorf("parse redis url: %w", err)
	}
	client := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("connect to redis at %s: %w", opts.Addr, err)
	}
	return &events.RedisStreamPublisher{Client: client, MaxLen: cfg.StreamMaxLen}, client.Close, nil
}

func newRunID() (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}
	return "sim-" + hex.EncodeToString(b[:]), nil
}

func randomSeed() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("generate seed: %w", err)
	}
	return binary.LittleEndian.Uint64(b[:]) | 1, nil
}
