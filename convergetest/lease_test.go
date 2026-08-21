package convergetest_test

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

var _ converge.Lease = (*convergetest.Lease)(nil)

func TestLeaseTryAcquireDelegatesToBase(t *testing.T) {
	base := inmem.NewLease()
	l := convergetest.WrapLease(base, "test")
	ctx := context.Background()
	h, ok, err := l.TryAcquire(ctx, "ns/converge/worker/app-runner/lease", time.Hour)
	if err != nil || !ok || h == nil {
		t.Fatalf("TryAcquire failed: ok=%v err=%v h=%v", ok, err, h)
	}
	if names := base.Names(); len(names) != 1 || names[0] != "ns/converge/worker/app-runner/lease" {
		t.Fatalf("base.Names() = %v, want the acquired key", names)
	}
}

func TestLeaseExpireMatchesExactKey(t *testing.T) {
	base := inmem.NewLease()
	l := convergetest.WrapLease(base, "test")
	ctx := context.Background()
	h, _, _ := l.TryAcquire(ctx, "solo-name", time.Hour)
	l.Expire("solo-name")
	select {
	case <-h.Done():
	default:
		t.Fatal("Expire must close Done for an exact key match")
	}
}

func TestLeaseExpireMatchesPathSegment(t *testing.T) {
	base := inmem.NewLease()
	l := convergetest.WrapLease(base, "test")
	ctx := context.Background()
	h, _, _ := l.TryAcquire(ctx, "test/converge/worker/app-runner/lease", time.Hour)
	l.Expire("app-runner")
	select {
	case <-h.Done():
	default:
		t.Fatal("Expire must close Done when the bare job name matches a path segment")
	}
}

func TestLeaseExpireDoesNotMatchPartialSegment(t *testing.T) {
	base := inmem.NewLease()
	l := convergetest.WrapLease(base, "test")
	ctx := context.Background()
	h, _, _ := l.TryAcquire(ctx, "test/converge/worker/app-runner-2/lease", time.Hour)
	l.Expire("app-runner")
	select {
	case <-h.Done():
		t.Fatal("Expire must not match a substring that is not a full path segment")
	default:
	}
}

func TestLeaseExpireLeavesOtherLeasesHeld(t *testing.T) {
	base := inmem.NewLease()
	l := convergetest.WrapLease(base, "test")
	ctx := context.Background()
	a, _, _ := l.TryAcquire(ctx, "test/converge/worker/app-runner/lease", time.Hour)
	b, _, _ := l.TryAcquire(ctx, "test/converge/worker/other-job/lease", time.Hour)
	l.Expire("app-runner")
	select {
	case <-a.Done():
	default:
		t.Fatal("expected app-runner's lease to be expired")
	}
	select {
	case <-b.Done():
		t.Fatal("other-job's lease must not be expired")
	default:
	}
}
