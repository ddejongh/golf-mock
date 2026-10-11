// Package simulator generates realistic venue event streams from scenario
// files. It emits the same events.Envelope that real ingest adapters will, so
// the downstream pipeline cannot tell the difference.
package simulator

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
)

// Scenario describes a venue and how load evolves over a simulated run.
type Scenario struct {
	Name      string     `yaml:"name"`
	Venue     Venue      `yaml:"venue"`
	Duration  Duration   `yaml:"duration"`
	Tick      Duration   `yaml:"tick"`
	Zones     []Zone     `yaml:"zones"`
	Incidents []Incident `yaml:"incidents"`
}

type Venue struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Kind     string `yaml:"kind"`
	Timezone string `yaml:"timezone"`
}

// Zone emits occupancy when Occupancy is set, and queue metrics
// (queue_length, wait_time_s, throughput_per_min) when Arrivals is set.
// Behaviour is driven by which models are configured, not by Kind, so the
// simulator stays venue-type neutral.
type Zone struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Kind     string `yaml:"kind"`
	Capacity int    `yaml:"capacity"`
	// Occupancy is the fraction of Capacity occupied over time.
	Occupancy Curve `yaml:"occupancy"`
	// ServiceRatePerMin is how many people the zone can process per minute.
	ServiceRatePerMin float64   `yaml:"service_rate_per_min"`
	Arrivals          *Arrivals `yaml:"arrivals"`
}

// Arrivals models people joining a zone's queue: PeakPerMin scaled by Curve.
type Arrivals struct {
	PeakPerMin float64 `yaml:"peak_per_min"`
	Curve      Curve   `yaml:"curve"`
}

// Incident is reported at At. A ServiceRateFactor in (0,1) degrades the
// zone's service rate for EffectDuration (e.g. a scanner outage).
type Incident struct {
	At                Duration `yaml:"at"`
	Zone              string   `yaml:"zone"`
	Severity          string   `yaml:"severity"`
	Category          string   `yaml:"category"`
	Description       string   `yaml:"description"`
	ServiceRateFactor float64  `yaml:"service_rate_factor"`
	EffectDuration    Duration `yaml:"effect_duration"`
}

// Duration is a time.Duration that unmarshals from strings like "45m".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"45m\": %w", node.Line, err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) D() time.Duration { return time.Duration(d) }

// LoadScenario reads and validates a scenario file.
func LoadScenario(path string) (*Scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scenario: %w", err)
	}
	sc, err := ParseScenario(raw)
	if err != nil {
		return nil, fmt.Errorf("scenario %s: %w", path, err)
	}
	return sc, nil
}

// ParseScenario decodes and validates a scenario document. Unknown fields are
// rejected so typos surface instead of being silently ignored.
func ParseScenario(raw []byte) (*Scenario, error) {
	var sc Scenario
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	return &sc, nil
}

// Validate reports every problem found, joined, rather than only the first.
func (s *Scenario) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if s.Venue.ID == "" {
		add("venue.id is required")
	}
	if s.Venue.Timezone != "" {
		if _, err := time.LoadLocation(s.Venue.Timezone); err != nil {
			add("venue.timezone: %v", err)
		}
	}
	if s.Duration <= 0 {
		add("duration must be > 0")
	}
	if s.Tick <= 0 {
		add("tick must be > 0")
	} else if s.Tick > s.Duration {
		add("tick must not exceed duration")
	}
	if len(s.Zones) == 0 {
		add("at least one zone is required")
	}

	zoneIDs := make(map[string]bool, len(s.Zones))
	for i, z := range s.Zones {
		where := fmt.Sprintf("zones[%d]", i)
		if z.ID == "" {
			add("%s.id is required", where)
		} else {
			where = fmt.Sprintf("zone %q", z.ID)
			if zoneIDs[z.ID] {
				add("%s: duplicate id", where)
			}
			zoneIDs[z.ID] = true
		}
		if z.Occupancy == nil && z.Arrivals == nil {
			add("%s: needs occupancy and/or arrivals", where)
		}
		if z.Occupancy != nil {
			if z.Capacity <= 0 {
				add("%s: capacity must be > 0 when occupancy is set", where)
			}
			if err := z.Occupancy.validate(s.Duration.D()); err != nil {
				add("%s.occupancy: %w", where, err)
			}
		}
		if z.Arrivals != nil {
			if z.ServiceRatePerMin <= 0 {
				add("%s: service_rate_per_min must be > 0 when arrivals is set", where)
			}
			if z.Arrivals.PeakPerMin <= 0 {
				add("%s.arrivals.peak_per_min must be > 0", where)
			}
			if err := z.Arrivals.Curve.validate(s.Duration.D()); err != nil {
				add("%s.arrivals.curve: %w", where, err)
			}
		}
	}

	for i, inc := range s.Incidents {
		where := fmt.Sprintf("incidents[%d]", i)
		if !zoneIDs[inc.Zone] {
			add("%s: unknown zone %q", where, inc.Zone)
		}
		if inc.At < 0 || inc.At > s.Duration {
			add("%s: at must be within [0, duration]", where)
		}
		if !slices.Contains(events.Severities, inc.Severity) {
			add("%s: severity must be one of %v", where, events.Severities)
		}
		if inc.Category == "" {
			add("%s: category is required", where)
		}
		if inc.ServiceRateFactor != 0 && (inc.ServiceRateFactor < 0 || inc.ServiceRateFactor >= 1) {
			add("%s: service_rate_factor must be in (0, 1)", where)
		}
		if (inc.ServiceRateFactor == 0) != (inc.EffectDuration == 0) {
			add("%s: service_rate_factor and effect_duration must be set together", where)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("invalid scenario: %w", errors.Join(errs...))
	}
	return nil
}
