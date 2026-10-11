package simulator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
)

// RunOptions controls pacing. Speed is simulated seconds per wall-clock
// second; Speed <= 0 runs as fast as possible.
type RunOptions struct {
	Speed float64
}

// RunStats summarises a completed run.
type RunStats struct {
	Ticks  int
	Events int
	SimEnd time.Time
}

// Run steps the engine to completion, publishing each tick's batch. It stops
// early (returning ctx.Err()) if ctx is cancelled.
func Run(ctx context.Context, eng *Engine, pub events.Publisher, opts RunOptions, log *slog.Logger) (RunStats, error) {
	var stats RunStats
	var interval time.Duration
	if opts.Speed > 0 {
		interval = time.Duration(float64(eng.sc.Tick.D()) / opts.Speed)
	}
	ticker := newTicker(interval)
	defer ticker.Stop()

	// Log progress roughly every simulated 5 minutes, not every tick.
	progressEvery := max(1, int(5*time.Minute/eng.sc.Tick.D()))

	for !eng.Done() {
		if err := ticker.wait(ctx); err != nil {
			return stats, err
		}
		batch, err := eng.Step()
		if err != nil {
			return stats, fmt.Errorf("tick %d: %w", stats.Ticks+1, err)
		}
		if err := pub.Publish(ctx, batch); err != nil {
			return stats, fmt.Errorf("publish tick %d: %w", stats.Ticks+1, err)
		}
		stats.Ticks++
		stats.Events += len(batch)

		for _, env := range batch {
			if env.Type == events.TypeIncidentReported {
				log.InfoContext(ctx, "incident reported",
					slog.String("event_id", env.EventID),
					slog.Time("sim_time", env.TS),
					slog.Any("payload", env.Payload))
			}
		}
		if stats.Ticks%progressEvery == 0 {
			log.DebugContext(ctx, "simulation progress",
				slog.Int("ticks", stats.Ticks),
				slog.Int("events", stats.Events),
				slog.Time("sim_time", eng.Now()))
		}
	}
	stats.SimEnd = eng.Now()
	return stats, nil
}

// ticker paces ticks; a zero interval never blocks except to observe ctx.
type ticker struct{ t *time.Ticker }

func newTicker(d time.Duration) *ticker {
	if d <= 0 {
		return &ticker{}
	}
	return &ticker{t: time.NewTicker(d)}
}

func (t *ticker) wait(ctx context.Context) error {
	if t.t == nil {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("simulation interrupted: %w", err)
		}
		return nil
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("simulation interrupted: %w", ctx.Err())
	case <-t.t.C:
		return nil
	}
}

func (t *ticker) Stop() {
	if t.t != nil {
		t.t.Stop()
	}
}
