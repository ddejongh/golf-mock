package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ddejongh/venuepulse/backend-go/internal/store"
	"github.com/ddejongh/venuepulse/backend-go/internal/testutil/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

var day1 = time.Date(2026, 10, 11, 23, 59, 0, 0, time.UTC)

func seed(t *testing.T, st *store.Store) {
	t.Helper()
	err := st.UpsertVenue(context.Background(), store.VenueSpec{
		ID: "ven_a", Name: "A", Kind: "stadium",
		Zones: []store.ZoneSpec{{ID: "z1", Name: "Gate", Capacity: 100}, {ID: "z2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func count(t *testing.T, url, sql string) int {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	var n int
	if err := conn.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestMigrationsRoundTrip(t *testing.T) {
	st, _ := pgtest.New(t)
	ctx := context.Background()
	m, err := st.Migrator()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()

	if pending, err := m.HasPending(ctx); err != nil || pending {
		t.Fatalf("after up: pending=%v err=%v", pending, err)
	}
	if _, err := m.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if pending, _ := m.HasPending(ctx); !pending {
		t.Fatal("after down: want pending migrations")
	}
	if _, err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertVenueAndZoneIndex(t *testing.T) {
	st, _ := pgtest.New(t)
	seed(t, st)
	seed(t, st) // idempotent

	ix, err := st.ZoneIndex(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !ix.Has("ven_a", "z1") || !ix.Has("ven_a", "z2") || ix.Has("ven_a", "z3") || ix.Has("ven_b", "z1") {
		t.Errorf("zone index = %v", ix)
	}
}

func TestIngestIsIdempotentAndPartitionsByDay(t *testing.T) {
	st, url := pgtest.New(t)
	seed(t, st)
	ctx := context.Background()

	batch := store.IngestBatch{
		Samples: []store.SampleRow{
			{SourceID: "sim", EventID: "e1", TS: day1, VenueID: "ven_a", ZoneID: "z1", Metric: "occupancy", Value: 10},
			// Crosses midnight UTC: needs a second partition.
			{SourceID: "sim", EventID: "e2", TS: day1.Add(2 * time.Minute), VenueID: "ven_a", ZoneID: "z1", Metric: "occupancy", Value: 11},
		},
		Incidents: []store.IncidentRow{
			{SourceID: "sim", EventID: "e3", VenueID: "ven_a", ZoneID: "z2", Severity: "high", Category: "medical", Description: "d", ReportedAt: day1},
		},
	}

	res, err := st.Ingest(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res.SamplesInserted != 2 || len(res.NewIncidents) != 1 {
		t.Fatalf("first ingest: %+v", res)
	}
	inc := res.NewIncidents[0]
	if inc.ID == "" || inc.Status != "open" || inc.Severity != "high" || !inc.ReportedAt.Equal(day1) {
		t.Errorf("incident = %+v", inc)
	}

	res, err = st.Ingest(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if res.SamplesInserted != 0 || len(res.NewIncidents) != 0 {
		t.Fatalf("replay should insert nothing: %+v", res)
	}

	if n := count(t, url, "SELECT count(*) FROM metric_samples"); n != 2 {
		t.Errorf("metric_samples = %d, want 2", n)
	}
	if n := count(t, url, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'metric_samples'::regclass"); n != 2 {
		t.Errorf("partitions = %d, want 2", n)
	}
	if n := count(t, url, "SELECT count(*) FROM incidents"); n != 1 {
		t.Errorf("incidents = %d, want 1", n)
	}
}

func TestIngestRollsBackOnError(t *testing.T) {
	st, url := pgtest.New(t)
	seed(t, st)

	_, err := st.Ingest(context.Background(), store.IngestBatch{
		Samples: []store.SampleRow{
			{SourceID: "sim", EventID: "e1", TS: day1, VenueID: "ven_a", ZoneID: "z1", Metric: "occupancy", Value: 1},
		},
		Incidents: []store.IncidentRow{
			// Violates the severity CHECK constraint.
			{SourceID: "sim", EventID: "e2", VenueID: "ven_a", ZoneID: "z1", Severity: "apocalyptic", Category: "x", ReportedAt: day1},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if n := count(t, url, "SELECT count(*) FROM metric_samples"); n != 0 {
		t.Errorf("metric_samples = %d after rollback, want 0", n)
	}

	// The partition DDL rolled back too; the cache must not claim otherwise.
	res, err := st.Ingest(context.Background(), store.IngestBatch{Samples: []store.SampleRow{
		{SourceID: "sim", EventID: "e1", TS: day1, VenueID: "ven_a", ZoneID: "z1", Metric: "occupancy", Value: 1},
	}})
	if err != nil || res.SamplesInserted != 1 {
		t.Fatalf("retry after rollback: res=%+v err=%v", res, err)
	}
}
