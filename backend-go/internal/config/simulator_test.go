package config

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadSimulatorPrecedence(t *testing.T) {
	env := envMap(map[string]string{"VP_SIM_SPEED": "10", "VP_SIM_SINK": "stdout", "VP_SIM_LOOP": "true"})
	cfg, err := LoadSimulator([]string{"-speed", "0"}, env, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Speed != 0 {
		t.Errorf("flag should override env: speed = %v", cfg.Speed)
	}
	if cfg.Sink != SinkStdout || !cfg.Loop {
		t.Errorf("env not applied: %+v", cfg)
	}
	if cfg.SourceID != "simulator" || cfg.StreamMaxLen != 100_000 {
		t.Errorf("defaults not applied: %+v", cfg)
	}
}

func TestLoadSimulatorErrors(t *testing.T) {
	_, err := LoadSimulator(nil, envMap(map[string]string{"VP_SIM_SPEED": "fast", "VP_SIM_SEED": "-1"}), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "VP_SIM_SPEED") || !strings.Contains(err.Error(), "VP_SIM_SEED") {
		t.Errorf("want both env errors reported, got %v", err)
	}

	_, err = LoadSimulator([]string{"-sink", "kafka", "-speed", "-1"}, envMap(nil), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "sink must be") || !strings.Contains(err.Error(), "speed must be") {
		t.Errorf("want validation errors, got %v", err)
	}

	_, err = LoadSimulator([]string{"-h"}, envMap(nil), io.Discard)
	if !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: err = %v, want flag.ErrHelp", err)
	}
}
