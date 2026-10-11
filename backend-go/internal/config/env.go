package config

import (
	"fmt"
	"strconv"
	"time"
)

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

func (r *envReader) duration(key string, def time.Duration) time.Duration {
	out := def
	r.parse(key, func(v string) (err error) { out, err = time.ParseDuration(v); return })
	return out
}
