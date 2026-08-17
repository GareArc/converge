package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/convergetest/portcheck"
	"github.com/GareArc/converge/inmem"
)

var _ converge.Lease = (*inmem.Lease)(nil)

func TestLeaseContract(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	portcheck.Lease(t,
		func(t *testing.T) converge.Lease { return inmem.NewLeaseWithClock(clock) },
		portcheck.LeaseOptions{Advance: clock.Advance},
	)
}

func TestExpireForcesLoss(t *testing.T) {
	ctx := context.Background()
	l := inmem.NewLease()
	h, _, _ := l.TryAcquire(ctx, "job", time.Hour)
	l.Expire("job")
	select {
	case <-h.Done():
	default:
		t.Fatal("Expire must close Done")
	}
	if _, ok, _ := l.TryAcquire(ctx, "job", time.Hour); !ok {
		t.Fatal("expired lease must be acquirable")
	}
}
