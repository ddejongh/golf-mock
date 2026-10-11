package live

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newPublisher(t *testing.T) (*Publisher, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &Publisher{Client: rdb}, rdb
}

func TestPublishAssignsConsecutiveSeq(t *testing.T) {
	p, rdb := newPublisher(t)
	ctx := context.Background()
	sub := rdb.Subscribe(ctx, Channel("v"))
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Receive(ctx); err != nil { // subscription confirmation
		t.Fatal(err)
	}

	ts := time.Date(2026, 10, 11, 17, 0, 0, 0, time.UTC)
	for range 2 {
		err := p.Publish(ctx, "v", []Draft{
			{Type: TypeMetricUpdate, TS: ts, Payload: MetricUpdate{ZoneID: "z", TS: ts, Metrics: map[string]float64{"occupancy": 1}}},
			{Type: TypeMetricUpdate, TS: ts, Payload: MetricUpdate{ZoneID: "y", TS: ts}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for want := int64(1); want <= 4; want++ {
		raw, err := sub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var m Message
		if err := json.Unmarshal([]byte(raw.Payload), &m); err != nil {
			t.Fatal(err)
		}
		if m.Seq != want || m.VenueID != "v" || m.Type != TypeMetricUpdate {
			t.Errorf("message %d = %+v", want, m)
		}
	}
}

func TestUpdateLatestKeepsNewest(t *testing.T) {
	p, rdb := newPublisher(t)
	ctx := context.Background()
	t0 := time.Date(2026, 10, 11, 17, 0, 0, 0, time.UTC)

	update := func(ts time.Time, v float64) {
		t.Helper()
		err := p.UpdateLatest(ctx, "v", []MetricUpdate{{ZoneID: "z", TS: ts, Metrics: map[string]float64{"occupancy": v}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	read := func() LatestValue {
		t.Helper()
		raw, err := rdb.HGet(ctx, LatestKey("v"), LatestField("z", "occupancy")).Result()
		if err != nil {
			t.Fatal(err)
		}
		var lv LatestValue
		if err := json.Unmarshal([]byte(raw), &lv); err != nil {
			t.Fatal(err)
		}
		return lv
	}

	update(t0, 1)
	update(t0.Add(time.Minute), 2)
	update(t0, 99) // stale redelivery
	if got := read(); got.Value != 2 || !got.TS.Equal(t0.Add(time.Minute)) {
		t.Errorf("latest = %+v, want value 2", got)
	}
	update(t0.Add(time.Minute), 3) // same ts overwrites
	if got := read(); got.Value != 3 {
		t.Errorf("latest = %+v, want value 3", got)
	}
}
