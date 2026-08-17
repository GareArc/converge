package reconcile

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

const (
	backoffMin     = time.Second
	backoffMax     = 15 * time.Minute
	noBackoffFloor = 250 * time.Millisecond
)

func backoffAfter(consecutiveFails int) time.Duration {
	d := backoffMin
	for i := 1; i < consecutiveFails; i++ {
		d *= 2
		if d >= backoffMax {
			return jitter(backoffMax)
		}
	}
	return jitter(d)
}

func floorDelay(d time.Duration) time.Duration {
	if d < noBackoffFloor {
		return jitter(noBackoffFloor)
	}
	return d
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

type tokenBucket struct {
	rate  converge.Rate
	clock converge.Clock

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newTokenBucket(r converge.Rate, clock converge.Clock) *tokenBucket {
	if r.IsZero() {
		return nil
	}
	return &tokenBucket{rate: r, clock: clock, tokens: float64(r.Events), last: clock.Now()}
}

func (b *tokenBucket) wait(ctx context.Context) error {
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
		need := time.Duration((1 - b.tokens) / float64(b.rate.Events) * float64(b.rate.Per))
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.clock.After(need):
		}
	}
}
