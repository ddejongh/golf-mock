package logging

import (
	"context"
	"fmt"
	"log/slog"
)

// RedisLogger routes go-redis's internal printf logging into slog. Install
// with redis.SetLogger(logging.RedisLogger{Log: log}).
type RedisLogger struct{ Log *slog.Logger }

func (l RedisLogger) Printf(ctx context.Context, format string, v ...any) {
	l.Log.WarnContext(ctx, "redis client", slog.String("detail", fmt.Sprintf(format, v...)))
}
