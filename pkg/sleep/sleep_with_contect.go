package sleep

import (
	"context"
	"time"
)

func WithContext(ctx context.Context, duration time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			return
		}
	}
}
