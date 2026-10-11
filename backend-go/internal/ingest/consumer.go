// Package ingest consumes events:{venue_id} Redis Streams, persists them to
// Postgres and publishes live updates (docs/architecture.md section 5.1).
//
// Delivery is at-least-once:
//   - Invalid entries go to the dead-letter stream and are acked.
//   - Valid entries are acked only after a successful Postgres commit. If the
//     commit fails they stay pending, are reclaimed after ClaimMinIdle, and
//     are dead-lettered once delivered MaxDeliveries times.
//   - Persistence is idempotent on (source_id, event_id), so redelivery is
//     safe. Live publishing is best-effort: Postgres is the source of truth
//     and dashboards resync from snapshots on sequence gaps.
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
	"github.com/ddejongh/venuepulse/backend-go/internal/live"
	"github.com/ddejongh/venuepulse/backend-go/internal/store"
)

// Store is the persistence the consumer needs.
type Store interface {
	ZoneIndex(ctx context.Context) (store.ZoneIndex, error)
	Ingest(ctx context.Context, b store.IngestBatch) (store.IngestResult, error)
}

// Live receives best-effort real-time updates after each committed batch.
type Live interface {
	Publish(ctx context.Context, venueID string, drafts []live.Draft) error
	UpdateLatest(ctx context.Context, venueID string, updates []live.MetricUpdate) error
}

type Config struct {
	Group           string
	Consumer        string
	BatchSize       int64
	Block           time.Duration
	ClaimMinIdle    time.Duration
	ClaimInterval   time.Duration
	MaxDeliveries   int64
	RefreshInterval time.Duration
	DLQStream       string
	DLQMaxLen       int64
}

// DefaultConfig returns production defaults; callers set Consumer.
func DefaultConfig() Config {
	return Config{
		Group:           "ingest",
		BatchSize:       500,
		Block:           2 * time.Second,
		ClaimMinIdle:    30 * time.Second,
		ClaimInterval:   15 * time.Second,
		MaxDeliveries:   5,
		RefreshInterval: 10 * time.Second,
		DLQStream:       "events:dlq",
		DLQMaxLen:       100_000,
	}
}

type Consumer struct {
	cfg   Config
	rdb   redis.Cmdable
	store Store
	live  Live
	log   *slog.Logger
	now   func() time.Time

	zones       store.ZoneIndex
	refreshedAt time.Time
	groups      map[string]bool // streams with a known consumer group
	claimedAt   time.Time
}

func NewConsumer(cfg Config, rdb redis.Cmdable, st Store, lv Live, log *slog.Logger) *Consumer {
	return &Consumer{
		cfg:    cfg,
		rdb:    rdb,
		store:  st,
		live:   lv,
		log:    log.With(slog.String("group", cfg.Group), slog.String("consumer", cfg.Consumer)),
		now:    time.Now,
		groups: make(map[string]bool),
	}
}

// Run consumes until ctx is cancelled. Transient Redis or Postgres failures
// are logged and retried; Run only returns on cancellation.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("ingest stopped: %w", err)
		}
		if err := c.Poll(ctx); err != nil && ctx.Err() == nil {
			c.log.ErrorContext(ctx, "ingest poll failed", slog.Any("error", err))
			sleep(ctx, time.Second)
		}
	}
}

// Poll runs one iteration: refresh venues if due, reclaim stale pending
// entries if due, then read and handle one batch of new entries.
func (c *Consumer) Poll(ctx context.Context) error {
	if c.zones == nil || c.now().Sub(c.refreshedAt) >= c.cfg.RefreshInterval {
		if err := c.refresh(ctx); err != nil {
			if c.zones == nil {
				return err
			}
			// Keep consuming with the stale index.
			c.log.WarnContext(ctx, "refresh venues failed", slog.Any("error", err))
		}
	}
	streams := c.streams()
	if len(streams) == 0 {
		sleep(ctx, c.cfg.Block)
		return nil
	}

	if c.now().Sub(c.claimedAt) >= c.cfg.ClaimInterval {
		c.claimedAt = c.now()
		for _, s := range streams {
			if err := c.reclaim(ctx, s); err != nil {
				c.log.WarnContext(ctx, "reclaim failed", slog.String("stream", s), slog.Any("error", err))
			}
		}
	}

	args := make([]string, 0, 2*len(streams))
	args = append(args, streams...)
	for range streams {
		args = append(args, ">")
	}
	res, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.cfg.Group,
		Consumer: c.cfg.Consumer,
		Streams:  args,
		Count:    c.cfg.BatchSize,
		Block:    c.cfg.Block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil // block timed out
	}
	if err != nil {
		if strings.HasPrefix(err.Error(), "NOGROUP") {
			clear(c.groups) // stream was deleted; recreate on next refresh
			c.refreshedAt = time.Time{}
		}
		return fmt.Errorf("xreadgroup: %w", err)
	}
	for _, s := range res {
		c.handle(ctx, s.Stream, s.Messages)
	}
	return nil
}

func (c *Consumer) refresh(ctx context.Context) error {
	ix, err := c.store.ZoneIndex(ctx)
	if err != nil {
		return fmt.Errorf("load zones: %w", err)
	}
	c.zones = ix
	c.refreshedAt = c.now()
	for venueID := range ix {
		stream := events.StreamKey(venueID)
		if c.groups[stream] {
			continue
		}
		// Start at 0 so events published before ingest first ran are consumed.
		err := c.rdb.XGroupCreateMkStream(ctx, stream, c.cfg.Group, "0").Err()
		if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create group on %s: %w", stream, err)
		}
		c.groups[stream] = true
		c.log.InfoContext(ctx, "consuming stream", slog.String("stream", stream))
	}
	return nil
}

func (c *Consumer) streams() []string {
	out := make([]string, 0, len(c.groups))
	for s, ok := range c.groups {
		if ok {
			out = append(out, s)
		}
	}
	slices.Sort(out)
	return out
}

// reclaim takes over entries left pending by failed batches or dead
// consumers. Entries past MaxDeliveries are dead-lettered.
func (c *Consumer) reclaim(ctx context.Context, stream string) error {
	pending, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.cfg.Group,
		Idle:   c.cfg.ClaimMinIdle,
		Start:  "-",
		End:    "+",
		Count:  c.cfg.BatchSize,
	}).Result()
	if err != nil {
		return fmt.Errorf("xpending: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	exhausted := map[string]bool{}
	ids := make([]string, len(pending))
	for i, p := range pending {
		ids[i] = p.ID
		if p.RetryCount >= c.cfg.MaxDeliveries {
			exhausted[p.ID] = true
		}
	}
	msgs, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    c.cfg.Group,
		Consumer: c.cfg.Consumer,
		MinIdle:  c.cfg.ClaimMinIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return fmt.Errorf("xclaim %d entries: %w", len(ids), err)
	}
	c.log.InfoContext(ctx, "reclaimed pending entries",
		slog.String("stream", stream), slog.Int("count", len(msgs)), slog.Int("exhausted", len(exhausted)))

	var retry []redis.XMessage
	var dead []rejected
	for _, m := range msgs {
		if exhausted[m.ID] {
			dead = append(dead, rejected{msg: m, reason: fmt.Sprintf("exceeded %d deliveries", c.cfg.MaxDeliveries)})
		} else {
			retry = append(retry, m)
		}
	}
	if err := c.deadLetter(ctx, stream, dead); err != nil {
		return err
	}
	c.handle(ctx, stream, retry)
	return nil
}

type rejected struct {
	msg    redis.XMessage
	reason string
}

// handle validates, persists and acks a batch from one stream. Errors are
// logged, not returned: un-acked entries are retried via reclaim.
func (c *Consumer) handle(ctx context.Context, stream string, msgs []redis.XMessage) {
	if len(msgs) == 0 {
		return
	}
	log := c.log.With(slog.String("stream", stream))
	venueID := strings.TrimPrefix(stream, "events:")

	batch, accepted, rejects := c.decode(ctx, venueID, msgs)
	if err := c.deadLetter(ctx, stream, rejects); err != nil {
		log.ErrorContext(ctx, "dead-letter failed", slog.Int("count", len(rejects)), slog.Any("error", err))
	}
	if len(accepted) == 0 {
		return
	}

	start := c.now()
	res, err := c.store.Ingest(ctx, batch)
	if err != nil {
		log.ErrorContext(ctx, "persist batch failed; entries stay pending for retry",
			slog.Int("entries", len(accepted)), slog.Any("error", err))
		return
	}

	c.publishLive(ctx, log, venueID, batch, res)

	if err := c.rdb.XAck(ctx, stream, c.cfg.Group, accepted...).Err(); err != nil {
		// Entries will be redelivered and deduplicated by Postgres.
		log.ErrorContext(ctx, "ack failed", slog.Int("entries", len(accepted)), slog.Any("error", err))
		return
	}
	log.DebugContext(ctx, "batch ingested",
		slog.Int("entries", len(accepted)),
		slog.Int("samples", len(batch.Samples)),
		slog.Int64("samples_inserted", res.SamplesInserted),
		slog.Int("incidents_new", len(res.NewIncidents)),
		slog.Int("rejected", len(rejects)),
		slog.Duration("duration", c.now().Sub(start)))
}

// decode turns stream entries into a store batch, returning the IDs of
// accepted entries and the rejected ones with reasons.
func (c *Consumer) decode(ctx context.Context, venueID string, msgs []redis.XMessage) (store.IngestBatch, []string, []rejected) {
	var (
		batch     store.IngestBatch
		accepted  []string
		rejects   []rejected
		refreshed bool
	)
	zoneKnown := func(zoneID string) bool {
		if c.zones.Has(venueID, zoneID) {
			return true
		}
		// The zone may have been seeded since the last refresh. Refresh at
		// most once per batch and once per second, so a misconfigured source
		// cannot turn every batch into a database round trip.
		if !refreshed && c.now().Sub(c.refreshedAt) >= time.Second {
			refreshed = true
			if err := c.refresh(ctx); err != nil {
				c.log.WarnContext(ctx, "refresh venues failed", slog.Any("error", err))
			}
		}
		return c.zones.Has(venueID, zoneID)
	}

	for _, m := range msgs {
		env, err := parseEntry(m, venueID)
		if err == nil {
			err = addToBatch(&batch, env, zoneKnown)
		}
		if err != nil {
			rejects = append(rejects, rejected{msg: m, reason: err.Error()})
			continue
		}
		accepted = append(accepted, m.ID)
	}
	return batch, accepted, rejects
}

func parseEntry(m redis.XMessage, venueID string) (events.Envelope, error) {
	raw, ok := m.Values[events.StreamField].(string)
	if !ok {
		return events.Envelope{}, fmt.Errorf("missing %q field", events.StreamField)
	}
	var env events.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return events.Envelope{}, fmt.Errorf("decode envelope: %w", err)
	}
	if err := env.Validate(); err != nil {
		return events.Envelope{}, err
	}
	if env.VenueID != venueID {
		return events.Envelope{}, fmt.Errorf("venue_id %q does not match stream venue %q", env.VenueID, venueID)
	}
	return env, nil
}

func addToBatch(b *store.IngestBatch, env events.Envelope, zoneKnown func(string) bool) error {
	switch env.Type {
	case events.TypeMetricSample:
		s, err := env.MetricSample()
		if err != nil {
			return err
		}
		if !zoneKnown(s.ZoneID) {
			return fmt.Errorf("unknown zone %q", s.ZoneID)
		}
		b.Samples = append(b.Samples, store.SampleRow{
			SourceID: env.SourceID, EventID: env.EventID, TS: env.TS,
			VenueID: env.VenueID, ZoneID: s.ZoneID, Metric: s.Metric, Value: s.Value,
		})
	case events.TypeIncidentReported:
		r, err := env.IncidentReported()
		if err != nil {
			return err
		}
		if !zoneKnown(r.ZoneID) {
			return fmt.Errorf("unknown zone %q", r.ZoneID)
		}
		b.Incidents = append(b.Incidents, store.IncidentRow{
			SourceID: env.SourceID, EventID: env.EventID, VenueID: env.VenueID, ZoneID: r.ZoneID,
			Severity: r.Severity, Category: r.Category, Description: r.Description, ReportedAt: env.TS,
		})
	default:
		return fmt.Errorf("unhandled type %q", env.Type)
	}
	return nil
}

// deadLetter copies rejected entries to the DLQ stream and acks them, in one
// MULTI so an entry is never acked without being dead-lettered.
func (c *Consumer) deadLetter(ctx context.Context, stream string, rejects []rejected) error {
	if len(rejects) == 0 {
		return nil
	}
	pipe := c.rdb.TxPipeline()
	ids := make([]string, len(rejects))
	for i, r := range rejects {
		ids[i] = r.msg.ID
		raw, _ := r.msg.Values[events.StreamField].(string)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: c.cfg.DLQStream,
			MaxLen: c.cfg.DLQMaxLen,
			Approx: c.cfg.DLQMaxLen > 0,
			Values: map[string]any{
				"stream":     stream,
				"message_id": r.msg.ID,
				"reason":     r.reason,
				"consumer":   c.cfg.Consumer,
				"envelope":   raw,
			},
		})
		c.log.WarnContext(ctx, "event dead-lettered",
			slog.String("stream", stream), slog.String("message_id", r.msg.ID), slog.String("reason", r.reason))
	}
	pipe.XAck(ctx, stream, c.cfg.Group, ids...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("dead-letter %d entries: %w", len(rejects), err)
	}
	return nil
}

// publishLive sends one metric.update per zone (latest value of each metric
// in the batch) and one incident.created per newly created incident.
func (c *Consumer) publishLive(ctx context.Context, log *slog.Logger, venueID string, b store.IngestBatch, res store.IngestResult) {
	updates := latestByZone(b.Samples)
	if err := c.live.UpdateLatest(ctx, venueID, updates); err != nil {
		log.WarnContext(ctx, "update latest metrics failed", slog.Any("error", err))
	}

	drafts := make([]live.Draft, 0, len(updates)+len(res.NewIncidents))
	for _, inc := range res.NewIncidents {
		drafts = append(drafts, live.Draft{Type: live.TypeIncidentCreated, TS: inc.ReportedAt, Payload: live.IncidentCreated{Incident: inc}})
	}
	for _, u := range updates {
		drafts = append(drafts, live.Draft{Type: live.TypeMetricUpdate, TS: u.TS, Payload: u})
	}
	slices.SortStableFunc(drafts, func(a, b live.Draft) int { return a.TS.Compare(b.TS) })
	if err := c.live.Publish(ctx, venueID, drafts); err != nil {
		log.WarnContext(ctx, "publish live updates failed", slog.Int("messages", len(drafts)), slog.Any("error", err))
	}
}

func latestByZone(samples []store.SampleRow) []live.MetricUpdate {
	type latest struct {
		value float64
		ts    time.Time
	}
	byZone := map[string]map[string]latest{}
	for _, s := range samples {
		m := byZone[s.ZoneID]
		if m == nil {
			m = map[string]latest{}
			byZone[s.ZoneID] = m
		}
		if cur, ok := m[s.Metric]; !ok || !s.TS.Before(cur.ts) {
			m[s.Metric] = latest{value: s.Value, ts: s.TS}
		}
	}

	out := make([]live.MetricUpdate, 0, len(byZone))
	for zoneID, m := range byZone {
		u := live.MetricUpdate{ZoneID: zoneID, Metrics: make(map[string]float64, len(m))}
		for metric, l := range m {
			u.Metrics[metric] = l.value
			if l.ts.After(u.TS) {
				u.TS = l.ts
			}
		}
		out = append(out, u)
	}
	slices.SortFunc(out, func(a, b live.MetricUpdate) int { return strings.Compare(a.ZoneID, b.ZoneID) })
	return out
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
