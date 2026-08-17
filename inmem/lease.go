package inmem

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

var errLeaseLost = errors.New("inmem: lease lost")

type Lease struct {
	mu    sync.Mutex
	clock converge.Clock
	held  map[string]*leaseHandle
}

func NewLease() *Lease { return NewLeaseWithClock(nil) }

func NewLeaseWithClock(c converge.Clock) *Lease {
	if c == nil {
		c = wallClock{}
	}
	return &Lease{clock: c, held: map[string]*leaseHandle{}}
}

func (l *Lease) TryAcquire(_ context.Context, name string, ttl time.Duration) (converge.LeaseHandle, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	if cur := l.held[name]; cur != nil {
		if cur.exp.After(now) {
			return nil, false, nil
		}
		cur.loseLocked()
		delete(l.held, name)
	}
	h := &leaseHandle{l: l, name: name, exp: now.Add(ttl), done: make(chan struct{})}
	l.held[name] = h
	return h, true, nil
}

func (l *Lease) Expire(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if cur := l.held[name]; cur != nil {
		cur.loseLocked()
		delete(l.held, name)
	}
}

type leaseHandle struct {
	l    *Lease
	name string
	exp  time.Time
	lost bool
	done chan struct{}
}

func (h *leaseHandle) loseLocked() {
	if !h.lost {
		h.lost = true
		close(h.done)
	}
}

func (h *leaseHandle) Extend(_ context.Context, ttl time.Duration) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	now := h.l.clock.Now()
	if h.lost || !h.exp.After(now) {
		h.loseLocked()
		if h.l.held[h.name] == h {
			delete(h.l.held, h.name)
		}
		return errLeaseLost
	}
	h.exp = now.Add(ttl)
	return nil
}

func (h *leaseHandle) Release(_ context.Context) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	h.loseLocked()
	if h.l.held[h.name] == h {
		delete(h.l.held, h.name)
	}
	return nil
}

func (h *leaseHandle) Done() <-chan struct{} { return h.done }
