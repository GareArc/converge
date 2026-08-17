package portcheck

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

type LeaseOptions struct {
	Advance func(d time.Duration)
}

func Lease(t *testing.T, open func(t *testing.T) converge.Lease, o LeaseOptions) {
	ctx := context.Background()
	const ttl = 30 * time.Second

	t.Run("exclusive while held", func(t *testing.T) {
		l := open(t)
		h, ok, err := l.TryAcquire(ctx, "job", ttl)
		if err != nil || !ok {
			t.Fatalf("first acquire = %v, %v", ok, err)
		}
		if _, ok, _ := l.TryAcquire(ctx, "job", ttl); ok {
			t.Fatal("second acquire must fail while held")
		}
		if _, ok, _ := l.TryAcquire(ctx, "other", ttl); !ok {
			t.Fatal("different name must be independent")
		}
		_ = h
	})

	t.Run("release frees and closes done", func(t *testing.T) {
		l := open(t)
		h, _, _ := l.TryAcquire(ctx, "job", ttl)
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-h.Done():
		default:
			t.Fatal("Done must be closed after Release")
		}
		if _, ok, _ := l.TryAcquire(ctx, "job", ttl); !ok {
			t.Fatal("released lease must be acquirable")
		}
	})

	t.Run("extend keeps it alive past the original ttl", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		l := open(t)
		h, _, _ := l.TryAcquire(ctx, "job", ttl)
		for range 3 {
			o.Advance(20 * time.Second)
			if err := h.Extend(ctx, ttl); err != nil {
				t.Fatal(err)
			}
		}
		if _, ok, _ := l.TryAcquire(ctx, "job", ttl); ok {
			t.Fatal("extended lease must still exclude others")
		}
	})

	t.Run("expiry hands off", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		l := open(t)
		old, _, _ := l.TryAcquire(ctx, "job", ttl)
		o.Advance(ttl + time.Second)
		h2, ok, err := l.TryAcquire(ctx, "job", ttl)
		if err != nil || !ok {
			t.Fatalf("takeover after expiry = %v, %v", ok, err)
		}
		if err := old.Extend(ctx, ttl); err == nil {
			t.Fatal("Extend on a lost lease must error")
		}
		select {
		case <-old.Done():
		default:
			t.Fatal("old handle's Done must close once the lease is lost")
		}
		_ = h2
	})

	t.Run("release after expiry must not disturb the successor", func(t *testing.T) {
		if o.Advance == nil {
			t.Skip("no clock control")
		}
		l := open(t)
		h1, ok, err := l.TryAcquire(ctx, "job", ttl)
		if err != nil || !ok {
			t.Fatalf("first acquire = %v, %v", ok, err)
		}
		o.Advance(ttl + time.Second)
		h2, ok, err := l.TryAcquire(ctx, "job", ttl)
		if err != nil || !ok {
			t.Fatalf("takeover after expiry = %v, %v", ok, err)
		}
		if err := h1.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := l.TryAcquire(ctx, "job", ttl); ok {
			t.Fatal("stale release must not free the successor's claim")
		}
		_ = h2
	})
}
