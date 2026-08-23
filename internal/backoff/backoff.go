package backoff

import (
	"math/rand"
	"time"
)

const floorMin = 250 * time.Millisecond

const NoBackoffCap = 10

type Curve struct {
	Min time.Duration
	Max time.Duration
}

func (c Curve) Delay(attempt int) time.Duration {
	d := c.Min
	for i := 1; i < attempt; i++ {
		if d >= c.Max/2 {
			return jitter(c.Max)
		}
		d *= 2
	}
	if d >= c.Max {
		return jitter(c.Max)
	}
	return jitter(d)
}

func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func Floor(d time.Duration) time.Duration {
	if d < floorMin {
		return floorMin + time.Duration(rand.Int63n(int64(floorMin/2)+1))
	}
	return d
}
