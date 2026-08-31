package convredis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/redis/go-redis/v9"
)

var ErrLeaseLost = errors.New("convredis: lease lost")

func NewLease(rdb *redis.Client) *Lease {
	return &Lease{rdb: rdb}
}

type Lease struct {
	rdb *redis.Client
}

func (l *Lease) TryAcquire(ctx context.Context, name string, ttl time.Duration) (converge.LeaseHandle, bool, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, false, err
	}
	token := hex.EncodeToString(buf)
	ok, err := l.rdb.SetNX(ctx, name, token, ttl).Result()
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	return &leaseHandle{rdb: l.rdb, name: name, token: token, done: make(chan struct{})}, true, nil
}

type leaseHandle struct {
	rdb   *redis.Client
	name  string
	token string

	mu       sync.Mutex
	done     chan struct{}
	finished bool
}

func (h *leaseHandle) Extend(ctx context.Context, ttl time.Duration) error {
	res, err := extendScript.Run(ctx, h.rdb, []string{h.name}, h.token, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if res != 1 {
		h.finish()
		return ErrLeaseLost
	}
	return nil
}

func (h *leaseHandle) Release(ctx context.Context) error {
	defer h.finish()
	return releaseScript.Run(ctx, h.rdb, []string{h.name}, h.token).Err()
}

func (h *leaseHandle) Done() <-chan struct{} {
	return h.done
}

func (h *leaseHandle) finish() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.finished {
		h.finished = true
		close(h.done)
	}
}
