package tokenbucket

import (
	"context"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

func refillWait(tokens float64, r converge.Rate) time.Duration {
	need := time.Duration((1 - tokens) / float64(r.Events) * float64(r.Per))
	if need < time.Millisecond {
		need = time.Millisecond
	}
	return need
}

type Bucket struct {
	rate  converge.Rate
	clock converge.Clock

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func New(r converge.Rate, clock converge.Clock) *Bucket {
	if r.Events <= 0 || r.Per <= 0 {
		return nil
	}
	return &Bucket{rate: r, clock: clock, tokens: float64(r.Events), last: clock.Now()}
}

func (b *Bucket) Wait(ctx context.Context) error {
	if b == nil {
		return nil
	}
	for {
		b.mu.Lock()
		now := b.clock.Now()
		b.tokens += now.Sub(b.last).Seconds() * float64(b.rate.Events) / b.rate.Per.Seconds()
		if b.tokens > float64(b.rate.Events) {
			b.tokens = float64(b.rate.Events)
		}
		b.last = now
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		need := refillWait(b.tokens, b.rate)
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.clock.After(need):
		}
	}
}
