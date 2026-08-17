package converge

import (
	"context"
	"time"
)

type Lease interface {
	TryAcquire(ctx context.Context, name string, ttl time.Duration) (LeaseHandle, bool, error)
}

type LeaseHandle interface {
	Extend(ctx context.Context, ttl time.Duration) error
	Release(ctx context.Context) error
	Done() <-chan struct{}
}
