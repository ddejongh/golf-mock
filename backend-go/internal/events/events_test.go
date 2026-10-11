package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var ts = time.Date(2026, 10, 11, 17, 0, 0, 0, time.UTC)

func sample(t *testing.T, id, venue string) Envelope {
	t.Helper()
	env, err := New(id, "sim", venue, TypeMetricSample, ts, MetricSample{ZoneID: "z", Metric: MetricOccupancy, Value: 3})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestNewValidates(t *testing.T) {
	_, err := New("", "sim", "v", TypeMetricSample, ts, struct{}{})
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Errorf("missing event_id: err = %v", err)
	}
	_, err = New("e", "sim", "v", Type("bogus"), ts, struct{}{})
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Errorf("unknown type: err = %v", err)
	}
	_, err = New("e", "sim", "v", TypeMetricSample, ts, make(chan int))
	if err == nil || !strings.Contains(err.Error(), "marshal") {
		t.Errorf("unmarshalable payload: err = %v", err)
	}
}

func TestRedisStreamPublisher(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	pub := &RedisStreamPublisher{Client: client, MaxLen: 1000}
	batch := []Envelope{sample(t, "e1", "ven_a"), sample(t, "e2", "ven_a"), sample(t, "e3", "ven_b")}
	if err := pub.Publish(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	msgs, err := client.XRange(context.Background(), StreamKey("ven_a"), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("ven_a stream has %d entries, want 2", len(msgs))
	}
	var got Envelope
	if err := json.Unmarshal([]byte(msgs[0].Values[StreamField].(string)), &got); err != nil {
		t.Fatal(err)
	}
	if got.EventID != "e1" || got.Type != TypeMetricSample || !got.TS.Equal(ts) {
		t.Errorf("round-tripped envelope = %+v", got)
	}
	if n, _ := client.XLen(context.Background(), StreamKey("ven_b")).Result(); n != 1 {
		t.Errorf("ven_b stream has %d entries, want 1", n)
	}
}

func TestRedisStreamPublisherReportsErrors(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	mr.Close()

	err := (&RedisStreamPublisher{Client: client}).Publish(context.Background(), []Envelope{sample(t, "e1", "v")})
	if err == nil || !strings.Contains(err.Error(), "xadd batch") {
		t.Errorf("err = %v, want wrapped xadd error", err)
	}
}

func TestJSONLinesPublisher(t *testing.T) {
	var buf bytes.Buffer
	pub := NewJSONLinesPublisher(&buf)
	if err := pub.Publish(context.Background(), []Envelope{sample(t, "e1", "v"), sample(t, "e2", "v")}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if !strings.Contains(lines[1], `"event_id":"e2"`) {
		t.Errorf("line 2 = %s", lines[1])
	}
}
