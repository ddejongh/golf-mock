package simulator

import (
	"errors"
	"fmt"
	"time"
)

// Keyframe pins a curve to Value at offset At from the scenario start.
type Keyframe struct {
	At    Duration `yaml:"at"`
	Value float64  `yaml:"value"`
}

// Curve is a piecewise-linear function of elapsed time. Before the first
// keyframe it holds the first value; after the last it holds the last value.
type Curve []Keyframe

// At returns the interpolated value at elapsed time t.
func (c Curve) At(t time.Duration) float64 {
	if len(c) == 0 {
		return 0
	}
	if t <= c[0].At.D() {
		return c[0].Value
	}
	for i := 1; i < len(c); i++ {
		a, b := c[i-1], c[i]
		if t <= b.At.D() {
			span := b.At.D() - a.At.D()
			if span == 0 {
				return b.Value
			}
			frac := float64(t-a.At.D()) / float64(span)
			return a.Value + frac*(b.Value-a.Value)
		}
	}
	return c[len(c)-1].Value
}

func (c Curve) validate(duration time.Duration) error {
	if len(c) == 0 {
		return errors.New("at least one keyframe is required")
	}
	for i, k := range c {
		if k.Value < 0 {
			return fmt.Errorf("keyframe %d: value must be >= 0", i)
		}
		if k.At < 0 || k.At.D() > duration {
			return fmt.Errorf("keyframe %d: at must be within [0, duration]", i)
		}
		if i > 0 && k.At < c[i-1].At {
			return fmt.Errorf("keyframe %d: keyframes must be in ascending order of at", i)
		}
	}
	return nil
}
