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

func TestNamesReflectsHeldLeases(t *testing.T) {
	ctx := context.Background()
	l := inmem.NewLease()
	if names := l.Names(); len(names) != 0 {
		t.Fatalf("Names() on an empty Lease = %v, want empty", names)
	}
	h1, ok, err := l.TryAcquire(ctx, "job-a", time.Hour)
	if err != nil || !ok {
		t.Fatalf("TryAcquire(job-a) failed: ok=%v err=%v", ok, err)
	}
	if _, ok, err := l.TryAcquire(ctx, "job-b", time.Hour); err != nil || !ok {
		t.Fatalf("TryAcquire(job-b) failed: ok=%v err=%v", ok, err)
	}
	if names := l.Names(); len(names) != 2 || names[0] != "job-a" || names[1] != "job-b" {
		t.Fatalf("Names() = %v, want [job-a job-b]", names)
	}
	if err := h1.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if names := l.Names(); len(names) != 1 || names[0] != "job-b" {
		t.Fatalf("Names() after Release = %v, want [job-b]", names)
	}
	l.Expire("job-b")
	if names := l.Names(); len(names) != 0 {
		t.Fatalf("Names() after Expire = %v, want empty", names)
	}
}
