package simulator

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
)

var testStart = time.Date(2026, 10, 11, 17, 0, 0, 0, time.UTC)

const minimalScenario = `
name: test
venue: { id: ven_t, name: T, kind: test, timezone: UTC }
duration: 10m
tick: 1m
zones:
  - id: z_gate
    service_rate_per_min: 10
    arrivals:
      peak_per_min: 20
      curve: [{ at: 0m, value: 1 }]
  - id: z_room
    capacity: 100
    occupancy: [{ at: 0m, value: 0 }, { at: 10m, value: 1 }]
incidents:
  - { at: 3m, zone: z_gate, severity: high, category: equipment_failure, service_rate_factor: 0.5, effect_duration: 2m }
`

func mustParse(t *testing.T, doc string) *Scenario {
	t.Helper()
	sc, err := ParseScenario([]byte(doc))
	if err != nil {
		t.Fatalf("ParseScenario: %v", err)
	}
	return sc
}

func newTestEngine(sc *Scenario, seed uint64) *Engine {
	return NewEngine(sc, EngineOptions{SourceID: "sim", RunID: "run", Seed: seed, Start: testStart})
}

func TestCurveAt(t *testing.T) {
	c := Curve{
		{At: Duration(10 * time.Minute), Value: 0},
		{At: Duration(20 * time.Minute), Value: 1},
		{At: Duration(20 * time.Minute), Value: 3}, // step
		{At: Duration(30 * time.Minute), Value: 3},
	}
	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, 0},
		{10 * time.Minute, 0},
		{15 * time.Minute, 0.5},
		{20 * time.Minute, 1},
		{25 * time.Minute, 3},
		{time.Hour, 3},
	}
	for _, tc := range cases {
		if got := c.At(tc.at); got != tc.want {
			t.Errorf("At(%v) = %v, want %v", tc.at, got, tc.want)
		}
	}
}

func TestBundledScenariosAreValid(t *testing.T) {
	paths, err := filepath.Glob("../../scenarios/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no bundled scenarios found")
	}
	for _, p := range paths {
		if _, err := LoadScenario(p); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}

func TestParseScenarioRejectsInvalid(t *testing.T) {
	cases := map[string]struct {
		doc  string
		want []string
	}{
		"unknown field": {
			doc:  "venue: { id: v }\nduration: 1m\ntick: 1s\nzonez: []",
			want: []string{"zonez"},
		},
		"bad duration": {
			doc:  "venue: { id: v }\nduration: soon\ntick: 1s",
			want: []string{"line 2"},
		},
		"reports all problems": {
			doc: `
venue: { timezone: Mars/Olympus }
duration: 1m
tick: 2m
zones:
  - id: a
    occupancy: [{ at: 0m, value: 1 }]
  - id: a
    arrivals: { peak_per_min: 0, curve: [{ at: 5m, value: 1 }] }
incidents:
  - { at: 0m, zone: nope, severity: urgent }
`,
			want: []string{
				"venue.id is required",
				"venue.timezone",
				"tick must not exceed duration",
				`zone "a": capacity must be > 0`,
				`zone "a": duplicate id`,
				"service_rate_per_min must be > 0",
				"peak_per_min must be > 0",
				"at must be within [0, duration]",
				`unknown zone "nope"`,
				"severity must be one of",
				"category is required",
			},
		},
		"incident effect needs both fields": {
			doc: `
venue: { id: v }
duration: 1m
tick: 1s
zones: [{ id: a, capacity: 1, occupancy: [{ at: 0m, value: 1 }] }]
incidents: [{ at: 0m, zone: a, severity: low, category: x, service_rate_factor: 0.5 }]
`,
			want: []string{"must be set together"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseScenario([]byte(tc.doc))
			if err == nil {
				t.Fatal("expected error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error missing %q:\n%v", w, err)
				}
			}
		})
	}
}

func runAll(t *testing.T, eng *Engine) []events.Envelope {
	t.Helper()
	var all []events.Envelope
	for !eng.Done() {
		batch, err := eng.Step()
		if err != nil {
			t.Fatalf("Step: %v", err)
		}
		all = append(all, batch...)
	}
	return all
}

func TestEngineIsDeterministicPerSeed(t *testing.T) {
	sc := mustParse(t, minimalScenario)
	a := runAll(t, newTestEngine(sc, 42))
	b := runAll(t, newTestEngine(sc, 42))
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different streams")
	}
	c := runAll(t, newTestEngine(sc, 43))
	if reflect.DeepEqual(a, c) {
		t.Fatal("different seeds produced identical streams")
	}
}

func TestEngineStream(t *testing.T) {
	sc := mustParse(t, minimalScenario)
	all := runAll(t, newTestEngine(sc, 1))

	// 10 ticks x (1 occupancy + 3 queue metrics) + 1 incident.
	if got, want := len(all), 10*4+1; got != want {
		t.Fatalf("got %d envelopes, want %d", got, want)
	}

	seen := map[string]bool{}
	var incidents int
	for _, env := range all {
		if err := env.Validate(); err != nil {
			t.Fatalf("invalid envelope: %v", err)
		}
		if seen[env.EventID] {
			t.Fatalf("duplicate event id %s", env.EventID)
		}
		seen[env.EventID] = true
		if env.TS.Before(testStart) || env.TS.After(testStart.Add(10*time.Minute)) {
			t.Fatalf("ts %v outside scenario window", env.TS)
		}

		switch env.Type {
		case events.TypeIncidentReported:
			incidents++
			if want := testStart.Add(3 * time.Minute); !env.TS.Equal(want) {
				t.Errorf("incident at %v, want %v", env.TS, want)
			}
		case events.TypeMetricSample:
			var s events.MetricSample
			if err := json.Unmarshal(env.Payload, &s); err != nil {
				t.Fatal(err)
			}
			if s.Value < 0 {
				t.Errorf("negative %s on %s: %v", s.Metric, s.ZoneID, s.Value)
			}
			if s.Metric == events.MetricOccupancy && s.Value > 100 {
				t.Errorf("occupancy %v exceeds capacity", s.Value)
			}
		}
	}
	if incidents != 1 {
		t.Errorf("got %d incidents, want 1", incidents)
	}
}

func TestQueueBuildsWhenArrivalsExceedServiceAndIncidentDegrades(t *testing.T) {
	// Arrivals ~20/min vs service 10/min: the queue should grow ~10/min, and
	// faster while the incident halves service from 3m to 5m.
	sc := mustParse(t, minimalScenario)
	eng := newTestEngine(sc, 7)
	var throughput []float64
	for !eng.Done() {
		batch, err := eng.Step()
		if err != nil {
			t.Fatal(err)
		}
		for _, env := range batch {
			var s events.MetricSample
			if env.Type == events.TypeMetricSample && json.Unmarshal(env.Payload, &s) == nil &&
				s.Metric == events.MetricThroughputPerMin {
				throughput = append(throughput, s.Value)
			}
		}
	}
	if got := eng.queues["z_gate"]; got < 80 || got > 140 {
		t.Errorf("final queue = %.1f, want roughly 110", got)
	}
	// Ticks 3 and 4 end inside the [3m, 5m) effect window; tick 5 is back
	// to full rate. The queue never drains, so served == capacity.
	if want := []float64{10, 10, 5, 5, 10}; !reflect.DeepEqual(throughput[:5], want) {
		t.Errorf("throughput = %v, want %v", throughput[:5], want)
	}
}

type recordingPublisher struct {
	batches [][]events.Envelope
	err     error
}

func (p *recordingPublisher) Publish(_ context.Context, b []events.Envelope) error {
	p.batches = append(p.batches, b)
	return p.err
}

var discard = slog.New(slog.DiscardHandler)

func TestRunPublishesEveryTick(t *testing.T) {
	sc := mustParse(t, minimalScenario)
	pub := &recordingPublisher{}
	stats, err := Run(context.Background(), newTestEngine(sc, 1), pub, RunOptions{}, discard)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Ticks != 10 || len(pub.batches) != 10 || stats.Events != 41 {
		t.Errorf("stats = %+v, batches = %d", stats, len(pub.batches))
	}
	if want := testStart.Add(10 * time.Minute); !stats.SimEnd.Equal(want) {
		t.Errorf("SimEnd = %v, want %v", stats.SimEnd, want)
	}
}

func TestRunStopsOnPublishErrorAndCancel(t *testing.T) {
	sc := mustParse(t, minimalScenario)

	boom := errors.New("boom")
	_, err := Run(context.Background(), newTestEngine(sc, 1), &recordingPublisher{err: boom}, RunOptions{}, discard)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want wrapping boom", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Run(ctx, newTestEngine(sc, 1), &recordingPublisher{}, RunOptions{Speed: 1}, discard)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
