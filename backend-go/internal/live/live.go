// Package live publishes real-time updates for dashboards: WebSocket
// messages fanned out through Redis pub/sub (ws:{venue_id}) and a latest-value
// metric cache (metrics:latest:{venue_id}). The WS hub subscribes to the
// channels; snapshots read the cache. Message schemas live in api/ws/.
package live

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/domain"
)

// WebSocket message types published by this package.
const (
	TypeMetricUpdate    = "metric.update"
	TypeIncidentCreated = "incident.created"
)

// Message is the WebSocket envelope. Seq increases monotonically per venue;
// clients that see a gap resync from a snapshot.
type Message struct {
	Type    string          `json:"type"`
	VenueID string          `json:"venue_id"`
	Seq     int64           `json:"seq"`
	TS      time.Time       `json:"ts"`
	Payload json.RawMessage `json:"payload"`
}

// MetricUpdate is the payload of a metric.update message: the latest values
// of one zone's metrics as of TS.
type MetricUpdate struct {
	ZoneID  string             `json:"zone_id"`
	TS      time.Time          `json:"ts"`
	Metrics map[string]float64 `json:"metrics"`
}

// IncidentCreated is the payload of an incident.created message.
type IncidentCreated struct {
	Incident domain.Incident `json:"incident"`
}

func Channel(venueID string) string   { return "ws:" + venueID }
func SeqKey(venueID string) string    { return "ws:seq:" + venueID }
func LatestKey(venueID string) string { return "metrics:latest:" + venueID }

// LatestField is the metrics:latest hash field for a zone's metric.
func LatestField(zoneID, metric string) string { return zoneID + "|" + metric }

// LatestValue is stored JSON-encoded in the metrics:latest hash.
type LatestValue struct {
	Value float64   `json:"value"`
	TS    time.Time `json:"ts"`
	TSMs  int64     `json:"ts_ms"` // compared by the cache script
}

//go:embed set_if_newer.lua
var setIfNewerSrc string

var setIfNewer = redis.NewScript(setIfNewerSrc)

type Publisher struct {
	Client redis.Cmdable
}

// Draft is a message before it is assigned a sequence number.
type Draft struct {
	Type    string
	TS      time.Time
	Payload any
}

// Publish assigns consecutive sequence numbers to drafts and publishes them
// to the venue's channel in order.
func (p *Publisher) Publish(ctx context.Context, venueID string, drafts []Draft) error {
	if len(drafts) == 0 {
		return nil
	}
	payloads := make([]json.RawMessage, len(drafts))
	for i, d := range drafts {
		raw, err := json.Marshal(d.Payload)
		if err != nil {
			return fmt.Errorf("marshal %s payload: %w", d.Type, err)
		}
		payloads[i] = raw
	}

	last, err := p.Client.IncrBy(ctx, SeqKey(venueID), int64(len(drafts))).Result()
	if err != nil {
		return fmt.Errorf("allocate seq for %s: %w", venueID, err)
	}
	first := last - int64(len(drafts)) + 1

	pipe := p.Client.Pipeline()
	for i, d := range drafts {
		msg, err := json.Marshal(Message{Type: d.Type, VenueID: venueID, Seq: first + int64(i), TS: d.TS.UTC(), Payload: payloads[i]})
		if err != nil {
			return fmt.Errorf("marshal %s message: %w", d.Type, err)
		}
		pipe.Publish(ctx, Channel(venueID), msg)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("publish %d messages to %s: %w", len(drafts), Channel(venueID), err)
	}
	return nil
}

// UpdateLatest writes each metric into the latest-value cache unless the
// cache already holds a newer reading (redeliveries can arrive out of order).
func (p *Publisher) UpdateLatest(ctx context.Context, venueID string, updates []MetricUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	key := LatestKey(venueID)
	pipe := p.Client.Pipeline()
	for _, u := range updates {
		for metric, value := range u.Metrics {
			v := LatestValue{Value: value, TS: u.TS.UTC(), TSMs: u.TS.UnixMilli()}
			raw, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("marshal latest %s: %w", metric, err)
			}
			// Eval, not Run: Run's EVALSHA→EVAL fallback cannot work inside a
			// pipeline, and the script is tiny.
			setIfNewer.Eval(ctx, pipe, []string{key}, LatestField(u.ZoneID, metric), raw, strconv.FormatInt(v.TSMs, 10))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("update latest metrics for %s: %w", venueID, err)
	}
	return nil
}
