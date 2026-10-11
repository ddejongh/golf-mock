package ingest

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/ddejongh/venuepulse/backend-go/internal/events"
	"github.com/ddejongh/venuepulse/backend-go/internal/live"
	"github.com/ddejongh/venuepulse/backend-go/internal/simulator"
	"github.com/ddejongh/venuepulse/backend-go/internal/store"
	"github.com/ddejongh/venuepulse/backend-go/internal/testutil/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

// Simulator → Redis Stream → Consumer → Postgres + live cache, with a replay
// of the whole stream to prove idempotency.
func TestSimulatorToPostgres(t *testing.T) {
	ctx := context.Background()
	st, dbURL := pgtest.New(t)

	sc, err := simulator.LoadScenario("../../scenarios/stadium-ingress.yaml")
	if err != nil {
		t.Fatal(err)
	}
	sc.Duration = simulator.Duration(time.Hour) // includes the 55m scanner incident
	spec := store.VenueSpec{ID: sc.Venue.ID, Name: sc.Venue.Name, Kind: sc.Venue.Kind, Timezone: sc.Venue.Timezone}
	for _, z := range sc.Zones {
		spec.Zones = append(spec.Zones, store.ZoneSpec{ID: z.ID, Name: z.Name, Kind: z.Kind, Capacity: z.Capacity})
	}
	if err := st.UpsertVenue(ctx, spec); err != nil {
		t.Fatal(err)
	}

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	eng := simulator.NewEngine(sc, simulator.EngineOptions{SourceID: "sim", RunID: "e2e", Seed: 1, Start: t0})
	stats, err := simulator.Run(ctx, eng, &events.RedisStreamPublisher{Client: rdb}, simulator.RunOptions{}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Consumer = "e2e"
	cfg.Block = 5 * time.Millisecond
	drain := func(group string) {
		t.Helper()
		cfg.Group = group
		c := NewConsumer(cfg, rdb, st, &live.Publisher{Client: rdb}, slog.New(slog.DiscardHandler))
		stream := events.StreamKey(sc.Venue.ID)
		last, err := rdb.XRevRangeN(ctx, stream, "+", "-", 1).Result()
		if err != nil || len(last) != 1 {
			t.Fatalf("read last entry: %v", err)
		}
		// miniredis does not track XINFO lag, so compare delivered IDs.
		for range 100 {
			if err := c.Poll(ctx); err != nil {
				t.Fatal(err)
			}
			info, err := rdb.XInfoGroups(ctx, stream).Result()
			if err != nil {
				t.Fatal(err)
			}
			for _, g := range info {
				if g.Name == group && g.LastDeliveredID == last[0].ID && g.Pending == 0 {
					return
				}
			}
		}
		t.Fatalf("group %s did not drain", group)
	}

	drain("ingest")
	// A second group re-reads the entire stream: every insert must dedupe.
	drain("replay")

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var samples, incidents int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM metric_samples").Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM incidents").Scan(&incidents); err != nil {
		t.Fatal(err)
	}
	if incidents != 1 {
		t.Errorf("incidents = %d, want 1", incidents)
	}
	if samples+incidents != stats.Events {
		t.Errorf("rows = %d samples + %d incidents, want %d events", samples, incidents, stats.Events)
	}
	if n, _ := rdb.XLen(ctx, "events:dlq").Result(); n != 0 {
		t.Errorf("dlq has %d entries", n)
	}
	if n, _ := rdb.HLen(ctx, live.LatestKey(sc.Venue.ID)).Result(); n == 0 {
		t.Error("latest-value cache is empty")
	}
}
