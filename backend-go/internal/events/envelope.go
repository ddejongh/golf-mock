// Package events defines the ingest envelope that every data source (the
// simulator today, real adapters later) publishes to Redis Streams, and the
// publishers that write it. The JSON shape is specified in
// api/events/envelope.schema.json.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Type identifies the payload carried by an Envelope.
type Type string

const (
	TypeMetricSample     Type = "metric.sample"
	TypeIncidentReported Type = "incident.reported"
)

// Metric names emitted in MetricSample payloads.
const (
	MetricOccupancy        = "occupancy"
	MetricQueueLength      = "queue_length"
	MetricWaitTimeS        = "wait_time_s"
	MetricThroughputPerMin = "throughput_per_min"
)

// Envelope is the unit of ingest. (SourceID, EventID) is unique and is the
// idempotency key used by the ingest consumer.
type Envelope struct {
	EventID  string          `json:"event_id"`
	SourceID string          `json:"source_id"`
	VenueID  string          `json:"venue_id"`
	Type     Type            `json:"type"`
	TS       time.Time       `json:"ts"`
	Payload  json.RawMessage `json:"payload"`
}

// MetricSample is the payload of a TypeMetricSample envelope.
type MetricSample struct {
	ZoneID string  `json:"zone_id"`
	Metric string  `json:"metric"`
	Value  float64 `json:"value"`
}

// IncidentReported is the payload of a TypeIncidentReported envelope.
type IncidentReported struct {
	ZoneID      string `json:"zone_id"`
	Severity    string `json:"severity"`
	Category    string `json:"category"`
	Description string `json:"description"`
}

// New builds an Envelope, marshalling payload to JSON.
func New(eventID, sourceID, venueID string, typ Type, ts time.Time, payload any) (Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", typ, err)
	}
	env := Envelope{
		EventID:  eventID,
		SourceID: sourceID,
		VenueID:  venueID,
		Type:     typ,
		TS:       ts.UTC(),
		Payload:  raw,
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// ErrInvalidEnvelope is returned (wrapped) by Validate.
var ErrInvalidEnvelope = errors.New("invalid envelope")

// Validate checks the envelope's required fields.
func (e Envelope) Validate() error {
	switch {
	case e.EventID == "":
		return fmt.Errorf("%w: event_id is required", ErrInvalidEnvelope)
	case e.SourceID == "":
		return fmt.Errorf("%w: source_id is required", ErrInvalidEnvelope)
	case e.VenueID == "":
		return fmt.Errorf("%w: venue_id is required", ErrInvalidEnvelope)
	case e.TS.IsZero():
		return fmt.Errorf("%w: ts is required", ErrInvalidEnvelope)
	case len(e.Payload) == 0:
		return fmt.Errorf("%w: payload is required", ErrInvalidEnvelope)
	}
	switch e.Type {
	case TypeMetricSample, TypeIncidentReported:
		return nil
	default:
		return fmt.Errorf("%w: unknown type %q", ErrInvalidEnvelope, e.Type)
	}
}

// StreamKey is the Redis Stream that carries a venue's ingest events.
func StreamKey(venueID string) string {
	return "events:" + venueID
}
