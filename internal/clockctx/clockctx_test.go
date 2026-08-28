package clockctx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
)

var start = time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)

func TestWithTimeoutFiresOnClockAdvance(t *testing.T) {
	clock := convergetest.NewClock(start)
	ctx, cancel := WithTimeout(context.Background(), clock, 30*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("context done before the clock advanced")
	default:
	}

	convergetest.AdvanceUntil(t, clock, 10*time.Second, func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("Err() = %v, want context.DeadlineExceeded", ctx.Err())
	}
}

func TestWithTimeoutCancelStopsTheTimer(t *testing.T) {
	clock := convergetest.NewClock(start)
	ctx, cancel := WithTimeout(context.Background(), clock, 30*time.Second)
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err() = %v, want context.Canceled", ctx.Err())
	}
	clock.Advance(time.Minute)
	convergetest.AssertStable(t, func() bool { return errors.Is(ctx.Err(), context.Canceled) })
}

func TestWithTimeoutPropagatesParentCancellation(t *testing.T) {
	clock := convergetest.NewClock(start)
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := WithTimeout(parent, clock, 30*time.Second)
	defer cancel()

	cancelParent()
	convergetest.Await(t, func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err() = %v, want context.Canceled", ctx.Err())
	}
}

func TestWithTimeoutCancelIsIdempotent(t *testing.T) {
	clock := convergetest.NewClock(start)
	_, cancel := WithTimeout(context.Background(), clock, 30*time.Second)
	cancel()
	cancel()
}

func TestWithTimeoutReportsNoWallClockDeadline(t *testing.T) {
	clock := convergetest.NewClock(start)
	ctx, cancel := WithTimeout(context.Background(), clock, 30*time.Second)
	defer cancel()
	if d, ok := ctx.Deadline(); ok {
		t.Fatalf("Deadline() = %v, true; a semantic time limit has no wall-clock instant to report", d)
	}
	child, cancelChild := context.WithTimeout(ctx, time.Hour)
	defer cancelChild()
	d, ok := child.Deadline()
	if !ok || !d.After(time.Now().Add(59*time.Minute)) {
		t.Fatalf("child Deadline() = %v, %v; want a wall-clock deadline roughly an hour out", d, ok)
	}
}

func TestWithTimeoutCauseIsTheDeadline(t *testing.T) {
	clock := convergetest.NewClock(start)
	shutdown := errors.New("the engine shut down")
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := WithTimeout(parent, clock, 30*time.Second)
	defer cancel()
	convergetest.AdvanceUntil(t, clock, 10*time.Second, func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
	if cause := context.Cause(ctx); !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("Cause() = %v, want context.DeadlineExceeded", cause)
	}
	cancelParent(shutdown)
	if cause := context.Cause(ctx); !errors.Is(cause, context.DeadlineExceeded) {
		t.Fatalf("Cause() after a later shutdown = %v, want context.DeadlineExceeded", cause)
	}
}

func TestWithTimeoutCauseIsTheCancellation(t *testing.T) {
	clock := convergetest.NewClock(start)
	shutdown := errors.New("the engine shut down")
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := WithTimeout(parent, clock, 30*time.Second)
	cancel()
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		t.Fatalf("Cause() = %v, want context.Canceled", cause)
	}
	cancelParent(shutdown)
	if cause := context.Cause(ctx); !errors.Is(cause, context.Canceled) {
		t.Fatalf("Cause() after a later shutdown = %v, want context.Canceled", cause)
	}
}

func TestWithTimeoutCauseFollowsTheParent(t *testing.T) {
	clock := convergetest.NewClock(start)
	gone := errors.New("the parent went away")
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := WithTimeout(parent, clock, 30*time.Second)
	defer cancel()

	cancelParent(gone)
	convergetest.Await(t, func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	})
	if cause := context.Cause(ctx); !errors.Is(cause, gone) {
		t.Fatalf("Cause() = %v, want %v", cause, gone)
	}
}

func TestWithTimeoutObservesAnAlreadyCancelledParent(t *testing.T) {
	clock := convergetest.NewClock(start)
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	ctx, cancel := WithTimeout(parent, clock, 30*time.Second)
	defer cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("Done() is open on a context whose parent was already cancelled")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("Err() = %v, want context.Canceled with no wait", ctx.Err())
	}
}
