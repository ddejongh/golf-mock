package events

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Publisher writes a batch of envelopes to a sink.
type Publisher interface {
	Publish(ctx context.Context, batch []Envelope) error
}

// StreamField is the Redis Stream entry field holding the JSON envelope.
const StreamField = "envelope"

// RedisStreamPublisher XADDs envelopes to events:{venue_id}, one pipeline per
// batch. MaxLen > 0 caps each stream approximately (XADD MAXLEN ~).
type RedisStreamPublisher struct {
	Client redis.Cmdable
	MaxLen int64
}

func (p *RedisStreamPublisher) Publish(ctx context.Context, batch []Envelope) error {
	if len(batch) == 0 {
		return nil
	}
	pipe := p.Client.Pipeline()
	for _, env := range batch {
		raw, err := json.Marshal(env)
		if err != nil {
			return fmt.Errorf("marshal envelope %s: %w", env.EventID, err)
		}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamKey(env.VenueID),
			MaxLen: p.MaxLen,
			Approx: p.MaxLen > 0,
			Values: map[string]any{StreamField: raw},
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("xadd batch of %d: %w", len(batch), err)
	}
	return nil
}

// JSONLinesPublisher writes one JSON envelope per line. Used for running the
// simulator without Redis and for piping into other tools.
type JSONLinesPublisher struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func NewJSONLinesPublisher(w io.Writer) *JSONLinesPublisher {
	return &JSONLinesPublisher{enc: json.NewEncoder(w)}
}

func (p *JSONLinesPublisher) Publish(_ context.Context, batch []Envelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, env := range batch {
		if err := p.enc.Encode(env); err != nil {
			return fmt.Errorf("write envelope %s: %w", env.EventID, err)
		}
	}
	return nil
}
