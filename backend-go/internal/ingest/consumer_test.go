package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/domain"
	"github.com/ddejongh/venuepulse/backend-go/internal/events"
	"github.com/ddejongh/venuepulse/backend-go/internal/live"
	"github.com/ddejongh/venuepulse/backend-go/internal/store"
)

var t0 = time.Date(2026, 10, 11, 17, 0, 0, 0, time.UTC)

type fakeStore struct {
	mu      sync.Mutex
	zones   store.ZoneIndex
	batches []store.IngestBatch
	err     error
}

func (f *fakeStore) ZoneIndex(context.Context) (store.ZoneIndex, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := store.ZoneIndex{}
	for v, zs := range f.zones {
		out[v] = maps.Clone(zs)
	}
	return out, nil
}

func (f *fakeStore) Ingest(_ context.Context, b store.IngestBatch) (store.IngestResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, b)
	if f.err != nil {
		return store.IngestResult{}, f.err
	}
	res := store.IngestResult{SamplesInserted: int64(len(b.Samples))}
	for i, in := range b.Incidents {
		res.NewIncidents = append(res.NewIncidents, domain.Incident{
			ID: fmt.Sprintf("inc-%d", i), VenueID: in.VenueID, ZoneID: in.ZoneID,
			Severity: in.Severity, Category: in.Category, Status: "open", ReportedAt: in.ReportedAt,
		})
	}
	return res, nil
}

func (f *fakeStore) addZone(venue, zone string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.zones[venue] == nil {
		f.zones[venue] = map[string]struct{}{}
	}
	f.zones[venue][zone] = struct{}{}
}

type harness struct {
	t     *testing.T
	mr    *miniredis.Miniredis
	rdb   *redis.Client
	store *fakeStore
	c     *Consumer
	clock time.Time
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(t0)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	h := &harness{t: t, mr: mr, rdb: rdb, store: &fakeStore{zones: store.ZoneIndex{}}, clock: t0}
	h.store.addZone("ven_a", "z1")

	cfg := DefaultConfig()
	cfg.Consumer = "c1"
	cfg.Block = 5 * time.Millisecond
	cfg.MaxDeliveries = 3
	h.c = NewConsumer(cfg, rdb, h.store, &live.Publisher{Client: rdb}, slog.New(slog.DiscardHandler))
	h.c.now = func() time.Time { return h.clock }
	return h
}

// advance moves both the consumer's clock and miniredis's clock, which
// drives stream idle times (FastForward only affects TTLs).
func (h *harness) advance(d time.Duration) {
	h.clock = h.clock.Add(d)
	h.mr.SetTime(h.clock)
}

func (h *harness) poll() {
	h.t.Helper()
	if err := h.c.Poll(context.Background()); err != nil {
		h.t.Fatalf("Poll: %v", err)
	}
}

func (h *harness) publish(envs ...events.Envelope) {
	h.t.Helper()
	pub := &events.RedisStreamPublisher{Client: h.rdb}
	if err := pub.Publish(context.Background(), envs); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) xaddRaw(venue string, values map[string]any) {
	h.t.Helper()
	if err := h.rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: events.StreamKey(venue), Values: values}).Err(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) pending(venue string) int64 {
	h.t.Helper()
	p, err := h.rdb.XPending(context.Background(), events.StreamKey(venue), "ingest").Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return p.Count
}

func (h *harness) dlq() []redis.XMessage {
	h.t.Helper()
	msgs, err := h.rdb.XRange(context.Background(), "events:dlq", "-", "+").Result()
	if err != nil {
		h.t.Fatal(err)
	}
	return msgs
}

func sample(t *testing.T, id, zone string, ts time.Time, v float64) events.Envelope {
	t.Helper()
	env, err := events.New(id, "sim", "ven_a", events.TypeMetricSample, ts, events.MetricSample{ZoneID: zone, Metric: events.MetricOccupancy, Value: v})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func incident(t *testing.T, id string, ts time.Time) events.Envelope {
	t.Helper()
	env, err := events.New(id, "sim", "ven_a", events.TypeIncidentReported, ts, events.IncidentReported{ZoneID: "z1", Severity: "high", Category: "medical"})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestPollIngestsPublishesAndAcks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	sub := h.rdb.Subscribe(ctx, live.Channel("ven_a"))
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	h.publish(
		sample(t, "e1", "z1", t0, 10),
		sample(t, "e2", "z1", t0.Add(time.Minute), 12),
		incident(t, "e3", t0.Add(30*time.Second)),
	)
	h.poll()

	if len(h.store.batches) != 1 {
		t.Fatalf("store got %d batches, want 1", len(h.store.batches))
	}
	b := h.store.batches[0]
	if len(b.Samples) != 2 || len(b.Incidents) != 1 || b.Incidents[0].Severity != "high" {
		t.Errorf("batch = %+v", b)
	}
	if n := h.pending("ven_a"); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}

	// Ordered by ts: incident (t0+30s) then the zone's metric.update (t0+1m).
	var got []live.Message
	for range 2 {
		raw, err := sub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var m live.Message
		if err := json.Unmarshal([]byte(raw.Payload), &m); err != nil {
			t.Fatal(err)
		}
		got = append(got, m)
	}
	if got[0].Type != live.TypeIncidentCreated || got[0].Seq != 1 || got[1].Type != live.TypeMetricUpdate || got[1].Seq != 2 {
		t.Fatalf("messages = %+v", got)
	}
	var mu live.MetricUpdate
	if err := json.Unmarshal(got[1].Payload, &mu); err != nil {
		t.Fatal(err)
	}
	if mu.Metrics[events.MetricOccupancy] != 12 || !mu.TS.Equal(t0.Add(time.Minute)) {
		t.Errorf("metric.update = %+v, want latest value 12", mu)
	}

	if _, err := h.rdb.HGet(ctx, live.LatestKey("ven_a"), live.LatestField("z1", events.MetricOccupancy)).Result(); err != nil {
		t.Errorf("latest cache not written: %v", err)
	}
}

func TestInvalidEntriesAreDeadLettered(t *testing.T) {
	h := newHarness(t)
	h.store.addZone("ven_b", "z1")

	badSeverity := incident(t, "e5", t0)
	badSeverity.Payload = json.RawMessage(`{"zone_id":"z1","severity":"apocalyptic","category":"x"}`)
	envJSON := func(e events.Envelope) string {
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}

	h.xaddRaw("ven_a", map[string]any{"wrong_field": "x"})
	h.xaddRaw("ven_a", map[string]any{events.StreamField: "{not json"})
	h.xaddRaw("ven_a", map[string]any{events.StreamField: envJSON(sample(t, "e3", "nope", t0, 1))})
	h.xaddRaw("ven_b", map[string]any{events.StreamField: envJSON(sample(t, "e4", "z1", t0, 1))}) // venue mismatch
	h.xaddRaw("ven_a", map[string]any{events.StreamField: envJSON(badSeverity)})
	h.publish(sample(t, "ok", "z1", t0, 1))

	h.poll()

	dlq := h.dlq()
	if len(dlq) != 5 {
		t.Fatalf("dlq has %d entries, want 5", len(dlq))
	}
	reasons := map[string]bool{}
	for _, m := range dlq {
		reasons[m.Values["reason"].(string)] = true
	}
	for _, want := range []string{
		`missing "envelope" field`,
		`unknown zone "nope"`,
		`venue_id "ven_a" does not match stream venue "ven_b"`,
	} {
		if !reasons[want] {
			t.Errorf("dlq reasons %v missing %q", reasons, want)
		}
	}
	if n := h.pending("ven_a") + h.pending("ven_b"); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
	if len(h.store.batches) != 1 || len(h.store.batches[0].Samples) != 1 {
		t.Errorf("valid entry not ingested: %+v", h.store.batches)
	}
}

func TestFailedBatchIsRetriedThenDeadLettered(t *testing.T) {
	h := newHarness(t)
	h.store.err = errors.New("db down")
	h.publish(sample(t, "e1", "z1", t0, 1))

	h.poll() // delivery 1 fails
	if n := h.pending("ven_a"); n != 1 {
		t.Fatalf("pending = %d, want 1 after failed persist", n)
	}

	// Not yet idle long enough: nothing is reclaimed.
	h.advance(h.c.cfg.ClaimInterval)
	h.poll()
	if len(h.store.batches) != 1 {
		t.Fatalf("reclaimed too early: %d batches", len(h.store.batches))
	}

	// Deliveries 2 and 3 fail via reclaim.
	for i := range 2 {
		h.advance(h.c.cfg.ClaimMinIdle)
		h.poll()
		if got := len(h.store.batches); got != i+2 {
			t.Fatalf("after reclaim %d: %d store calls, want %d", i+1, got, i+2)
		}
	}

	// Delivered MaxDeliveries times: dead-lettered, not retried.
	h.advance(h.c.cfg.ClaimMinIdle)
	h.poll()
	if len(h.store.batches) != 3 {
		t.Errorf("store called %d times, want 3", len(h.store.batches))
	}
	if dlq := h.dlq(); len(dlq) != 1 || dlq[0].Values["reason"] != "exceeded 3 deliveries" {
		t.Errorf("dlq = %+v", dlq)
	}
	if n := h.pending("ven_a"); n != 0 {
		t.Errorf("pending = %d, want 0", n)
	}
}

func TestRecoversAfterTransientFailure(t *testing.T) {
	h := newHarness(t)
	h.store.err = errors.New("db down")
	h.publish(sample(t, "e1", "z1", t0, 1))
	h.poll()

	h.store.err = nil
	h.advance(h.c.cfg.ClaimMinIdle)
	h.poll()
	if n := h.pending("ven_a"); n != 0 {
		t.Errorf("pending = %d, want 0 after recovery", n)
	}
	if len(h.dlq()) != 0 {
		t.Error("entry dead-lettered despite recovering")
	}
}

func TestStreamsAreDiscoveredAndReadFromStart(t *testing.T) {
	h := newHarness(t)
	h.poll() // ven_a known; ven_c not yet

	// Published before ven_c exists in Postgres.
	env, err := events.New("e1", "sim", "ven_c", events.TypeMetricSample, t0, events.MetricSample{ZoneID: "z9", Metric: "occupancy", Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	h.publish(env)
	h.poll()
	if len(h.store.batches) != 0 {
		t.Fatal("consumed a stream for an unknown venue")
	}

	h.store.addZone("ven_c", "z9")
	h.advance(h.c.cfg.RefreshInterval)
	h.poll()
	if len(h.store.batches) != 1 {
		t.Fatalf("event published before seeding was not consumed")
	}
}

func TestUnknownZoneTriggersRefresh(t *testing.T) {
	h := newHarness(t)
	h.poll()

	h.store.addZone("ven_a", "z2") // seeded after the last refresh
	h.advance(2 * time.Second)     // past the refresh rate limit, before RefreshInterval
	h.publish(sample(t, "e1", "z2", t0, 1))
	h.poll()

	if len(h.dlq()) != 0 || len(h.store.batches) != 1 {
		t.Errorf("dlq=%d batches=%d, want sample for newly seeded zone ingested", len(h.dlq()), len(h.store.batches))
	}
}
