// Package config loads typed configuration for backend-go binaries. Flags
// take precedence over VP_* environment variables, which take precedence over
// defaults. Invalid config fails fast at startup.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
)

// Simulator sinks.
const (
	SinkRedis  = "redis"
	SinkStdout = "stdout"
)

type Simulator struct {
	ScenarioPath string
	Sink         string
	RedisURL     string
	StreamMaxLen int64
	Speed        float64
	Seed         uint64 // 0 = random
	SourceID     string
	Loop         bool
	LogLevel     string
}

// LoadSimulator parses args (without the program name) over env defaults.
// Usage and flag errors are written to usage; -h returns flag.ErrHelp.
func LoadSimulator(args []string, getenv func(string) string, usage io.Writer) (Simulator, error) {
	env := envReader{getenv: getenv}
	cfg := Simulator{
		ScenarioPath: env.str("VP_SIM_SCENARIO", "scenarios/stadium-ingress.yaml"),
		Sink:         env.str("VP_SIM_SINK", SinkRedis),
		RedisURL:     env.str("VP_REDIS_URL", "redis://localhost:6379/0"),
		StreamMaxLen: env.int64("VP_SIM_STREAM_MAXLEN", 100_000),
		Speed:        env.float("VP_SIM_SPEED", 1),
		Seed:         env.uint64("VP_SIM_SEED", 0),
		SourceID:     env.str("VP_SIM_SOURCE_ID", "simulator"),
		Loop:         env.bool("VP_SIM_LOOP", false),
		LogLevel:     env.str("VP_LOG_LEVEL", "info"),
	}
	if err := errors.Join(env.errs...); err != nil {
		return Simulator{}, fmt.Errorf("environment: %w", err)
	}

	fs := flag.NewFlagSet("simulator", flag.ContinueOnError)
	fs.SetOutput(usage)
	fs.StringVar(&cfg.ScenarioPath, "scenario", cfg.ScenarioPath, "scenario YAML file [VP_SIM_SCENARIO]")
	fs.StringVar(&cfg.Sink, "sink", cfg.Sink, "where to publish events: redis|stdout [VP_SIM_SINK]")
	fs.StringVar(&cfg.RedisURL, "redis-url", cfg.RedisURL, "Redis URL [VP_REDIS_URL]")
	fs.Int64Var(&cfg.StreamMaxLen, "stream-maxlen", cfg.StreamMaxLen, "approximate cap per stream, 0 = unbounded [VP_SIM_STREAM_MAXLEN]")
	fs.Float64Var(&cfg.Speed, "speed", cfg.Speed, "simulated seconds per real second, 0 = as fast as possible [VP_SIM_SPEED]")
	fs.Uint64Var(&cfg.Seed, "seed", cfg.Seed, "random seed, 0 = random [VP_SIM_SEED]")
	fs.StringVar(&cfg.SourceID, "source-id", cfg.SourceID, "source_id stamped on every event [VP_SIM_SOURCE_ID]")
	fs.BoolVar(&cfg.Loop, "loop", cfg.Loop, "restart the scenario when it ends [VP_SIM_LOOP]")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug|info|warn|error [VP_LOG_LEVEL]")
	if err := fs.Parse(args); err != nil {
		return Simulator{}, fmt.Errorf("flags: %w", err)
	}
	if fs.NArg() > 0 {
		return Simulator{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	return cfg, cfg.validate()
}

func (c Simulator) validate() error {
	var errs []error
	if c.ScenarioPath == "" {
		errs = append(errs, errors.New("scenario is required"))
	}
	switch c.Sink {
	case SinkRedis:
		if c.RedisURL == "" {
			errs = append(errs, errors.New("redis-url is required for the redis sink"))
		}
	case SinkStdout:
	default:
		errs = append(errs, fmt.Errorf("sink must be %s or %s, got %q", SinkRedis, SinkStdout, c.Sink))
	}
	if c.Speed < 0 {
		errs = append(errs, errors.New("speed must be >= 0"))
	}
	if c.StreamMaxLen < 0 {
		errs = append(errs, errors.New("stream-maxlen must be >= 0"))
	}
	if c.SourceID == "" {
		errs = append(errs, errors.New("source-id is required"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

// envReader reads typed env vars, collecting parse errors instead of
// stopping at the first.
type envReader struct {
	getenv func(string) string
	errs   []error
}

func (r *envReader) str(key, def string) string {
	if v := r.getenv(key); v != "" {
		return v
	}
	return def
}

func (r *envReader) parse(key string, parse func(string) error) {
	if v := r.getenv(key); v != "" {
		if err := parse(v); err != nil {
			r.errs = append(r.errs, fmt.Errorf("%s=%q: %w", key, v, err))
		}
	}
}

func (r *envReader) int64(key string, def int64) int64 {
	out := def
	r.parse(key, func(v string) (err error) { out, err = strconv.ParseInt(v, 10, 64); return })
	return out
}

func (r *envReader) uint64(key string, def uint64) uint64 {
	out := def
	r.parse(key, func(v string) (err error) { out, err = strconv.ParseUint(v, 10, 64); return })
	return out
}

func (r *envReader) float(key string, def float64) float64 {
	out := def
	r.parse(key, func(v string) (err error) { out, err = strconv.ParseFloat(v, 64); return })
	return out
}

func (r *envReader) bool(key string, def bool) bool {
	out := def
	r.parse(key, func(v string) (err error) { out, err = strconv.ParseBool(v); return })
	return out
}
