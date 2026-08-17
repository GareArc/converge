package converge

import (
	"context"
	"time"
)

type KV interface {
	Get(ctx context.Context, key string) (val []byte, ok bool, err error)
	SetCAS(ctx context.Context, key string, old, new []byte) (bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, prefix, cursor string) (keys []string, next string, err error)
}
