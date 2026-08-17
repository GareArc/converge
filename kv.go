package converge

import (
	"context"
	"time"
)

type KV interface {
	// Get: ok=false when the key is absent — absence is not an error.
	Get(ctx context.Context, key string) (val []byte, ok bool, err error)
	// SetCAS writes new only if the current value equals old; old == nil
	// means "create only if absent". A successful SetCAS clears any TTL.
	SetCAS(ctx context.Context, key string, old, new []byte) (bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error // ttl 0 = no expiry
	Delete(ctx context.Context, key string) error                             // deleting an absent key is not an error
	// Scan pages keys with the prefix: pass cursor "" to start, feed next
	// back until it returns "". The cursor is keyset-stable; keys created
	// mid-scan may or may not appear.
	Scan(ctx context.Context, prefix, cursor string) (keys []string, next string, err error)
}
