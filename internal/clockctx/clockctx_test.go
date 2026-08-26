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

func TestWithTimeoutDeadlineReflectsTheClock(t *testing.T) {
	clock := convergetest.NewClock(start)
	ctx, cancel := WithTimeout(context.Background(), clock, 30*time.Second)
	defer cancel()
	d, ok := ctx.Deadline()
	if !ok || !d.Equal(start.Add(30*time.Second)) {
		t.Fatalf("Deadline() = %v, %v, want %v, true", d, ok, start.Add(30*time.Second))
	}
}
