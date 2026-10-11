package simulator

import (
	"cmp"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
)

// Noise applied to curve-driven values so streams don't look synthetic.
const (
	occupancyNoise = 0.02 // relative stddev
	arrivalNoise   = 0.10 // relative stddev
)

// Engine advances a scenario one tick at a time. It is deterministic for a
// given scenario, seed, start time and run ID, and is not safe for concurrent
// use.
type Engine struct {
	sc       *Scenario
	sourceID string
	runID    string
	start    time.Time
	rng      *rand.Rand

	elapsed time.Duration
	seq     uint64
	queues  map[string]float64 // zone ID -> people waiting

	incidents    []Incident // sorted by At
	nextIncident int        // index of the first not-yet-reported incident
}

// EngineOptions configures a run. RunID makes event IDs unique across runs so
// replays of the same scenario are not deduplicated by ingest.
type EngineOptions struct {
	SourceID string
	RunID    string
	Seed     uint64
	Start    time.Time
}

func NewEngine(sc *Scenario, opts EngineOptions) *Engine {
	incidents := slices.Clone(sc.Incidents)
	slices.SortStableFunc(incidents, func(a, b Incident) int { return cmp.Compare(a.At, b.At) })

	return &Engine{
		sc:        sc,
		sourceID:  opts.SourceID,
		runID:     opts.RunID,
		start:     opts.Start.UTC(),
		rng:       rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9e3779b97f4a7c15)),
		queues:    make(map[string]float64, len(sc.Zones)),
		incidents: incidents,
	}
}

// Done reports whether the scenario duration has been fully simulated.
func (e *Engine) Done() bool { return e.elapsed >= e.sc.Duration.D() }

// Now is the simulated wall-clock time of the next tick.
func (e *Engine) Now() time.Time { return e.start.Add(e.elapsed) }

// Step simulates one tick and returns the envelopes it produced, all
// timestamped at the end of the tick.
func (e *Engine) Step() ([]events.Envelope, error) {
	if e.Done() {
		return nil, nil
	}
	tick := e.sc.Tick.D()
	e.elapsed = min(e.elapsed+tick, e.sc.Duration.D())
	ts := e.Now()

	var out []events.Envelope
	for _, inc := range e.dueIncidents() {
		env, err := e.envelope(events.TypeIncidentReported, ts, events.IncidentReported{
			ZoneID:      inc.Zone,
			Severity:    inc.Severity,
			Category:    inc.Category,
			Description: inc.Description,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}

	for _, z := range e.sc.Zones {
		samples := e.sampleZone(z, tick)
		for _, s := range samples {
			env, err := e.envelope(events.TypeMetricSample, ts, s)
			if err != nil {
				return nil, err
			}
			out = append(out, env)
		}
	}
	return out, nil
}

func (e *Engine) sampleZone(z Zone, tick time.Duration) []events.MetricSample {
	var out []events.MetricSample

	if z.Occupancy != nil {
		frac := z.Occupancy.At(e.elapsed) * (1 + e.rng.NormFloat64()*occupancyNoise)
		occ := clamp(math.Round(frac*float64(z.Capacity)), 0, float64(z.Capacity))
		out = append(out, events.MetricSample{ZoneID: z.ID, Metric: events.MetricOccupancy, Value: occ})
	}

	if z.Arrivals != nil {
		minutes := tick.Minutes()
		rate := z.Arrivals.Curve.At(e.elapsed) * z.Arrivals.PeakPerMin * (1 + e.rng.NormFloat64()*arrivalNoise)
		arrivals := max(rate, 0) * minutes

		serviceRate := z.ServiceRatePerMin * e.serviceFactor(z.ID)
		queue := e.queues[z.ID] + arrivals
		served := min(queue, serviceRate*minutes)
		queue -= served
		e.queues[z.ID] = queue

		wait := 0.0
		if serviceRate > 0 {
			wait = queue / serviceRate * 60
		}
		out = append(out,
			events.MetricSample{ZoneID: z.ID, Metric: events.MetricQueueLength, Value: math.Round(queue)},
			events.MetricSample{ZoneID: z.ID, Metric: events.MetricWaitTimeS, Value: math.Round(wait)},
			events.MetricSample{ZoneID: z.ID, Metric: events.MetricThroughputPerMin, Value: round1(served / minutes)},
		)
	}
	return out
}

// serviceFactor is the product of all active incident degradations on a zone.
func (e *Engine) serviceFactor(zoneID string) float64 {
	f := 1.0
	for _, inc := range e.incidents[:e.nextIncident] {
		if inc.Zone != zoneID || inc.ServiceRateFactor == 0 {
			continue
		}
		if e.elapsed < inc.At.D()+inc.EffectDuration.D() {
			f *= inc.ServiceRateFactor
		}
	}
	return f
}

func (e *Engine) dueIncidents() []Incident {
	from := e.nextIncident
	for e.nextIncident < len(e.incidents) && e.incidents[e.nextIncident].At.D() <= e.elapsed {
		e.nextIncident++
	}
	return e.incidents[from:e.nextIncident]
}

func (e *Engine) envelope(typ events.Type, ts time.Time, payload any) (events.Envelope, error) {
	e.seq++
	id := fmt.Sprintf("%s-%08d", e.runID, e.seq)
	env, err := events.New(id, e.sourceID, e.sc.Venue.ID, typ, ts, payload)
	if err != nil {
		return events.Envelope{}, fmt.Errorf("build event %s: %w", id, err)
	}
	return env, nil
}

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

func round1(v float64) float64 { return math.Round(v*10) / 10 }
