package converge

import (
	"context"
	"time"
)

type Lease interface {
	// TryAcquire is non-blocking: (handle, true, nil) if the caller now
	// holds the lease, (nil, false, nil) if another holder does.
	TryAcquire(ctx context.Context, name string, ttl time.Duration) (LeaseHandle, bool, error)
}

type LeaseHandle interface {
	Extend(ctx context.Context, ttl time.Duration) error // errors once the lease is lost
	Release(ctx context.Context) error
	Done() <-chan struct{} // closed on loss or release
}
