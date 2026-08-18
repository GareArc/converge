package backoff

import (
	"math/rand"
	"time"
)

const floorMin = 250 * time.Millisecond

func Jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

func Floor(d time.Duration) time.Duration {
	if d < floorMin {
		return floorMin + time.Duration(rand.Int63n(int64(floorMin/2)+1))
	}
	return d
}
