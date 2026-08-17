package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
)

func TestBackoffAfterGrowsExponentiallyWithinJitterBounds(t *testing.T) {
	cases := []struct {
		fails int
		base  time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{5, 16 * time.Second},
		{11, backoffMax},
		{100, backoffMax},
	}
	for _, c := range cases {
		for i := 0; i < 50; i++ {
			d := backoffAfter(c.fails)
			if d < c.base/2 || d > c.base {
				t.Fatalf("backoffAfter(%d) = %v, want within [%v, %v]", c.fails, d, c.base/2, c.base)
			}
		}
	}
}

func TestFloorDelay(t *testing.T) {
	if d := floorDelay(time.Hour); d != time.Hour {
		t.Fatalf("above-floor delay changed: %v", d)
	}
	for i := 0; i < 50; i++ {
		d := floorDelay(0)
		if d < noBackoffFloor/2 || d > noBackoffFloor {
			t.Fatalf("floorDelay(0) = %v, want within [%v, %v]", d, noBackoffFloor/2, noBackoffFloor)
		}
	}
}

func TestTokenBucketZeroRateIsUnlimited(t *testing.T) {
	if b := newTokenBucket(converge.Rate{}, convergetest.NewClock(wqStart)); b != nil {
		t.Fatal("zero rate must mean no bucket")
	}
	if b := newTokenBucket(converge.Rate{Events: 5}, convergetest.NewClock(wqStart)); b != nil {
		t.Fatal("zero per must mean no bucket")
	}
	if b := newTokenBucket(converge.Rate{Per: time.Second}, convergetest.NewClock(wqStart)); b != nil {
		t.Fatal("zero events must mean no bucket")
	}
	var b *tokenBucket
	if err := b.wait(context.Background()); err != nil {
		t.Fatal("nil bucket must never block")
	}
}

func TestTokenBucketAllowsBurstThenBlocks(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	b := newTokenBucket(converge.Rate{Events: 2, Per: time.Second}, clock)
	ctx := context.Background()
	if err := b.wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.wait(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.wait(ctx) }()
	select {
	case <-done:
		t.Fatal("third event within the window must block")
	case <-time.After(20 * time.Millisecond):
	}
	clock.Advance(time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bucket never refilled")
	}
}

func TestTokenBucketWaitHonorsContext(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	b := newTokenBucket(converge.Rate{Events: 1, Per: time.Hour}, clock)
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.wait(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.wait(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled wait must error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait ignored cancellation")
	}
}

func TestTokenBucketClampsSubMillisecondWaits(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	b := newTokenBucket(converge.Rate{Events: 1_000_000, Per: time.Second}, clock)
	ctx := context.Background()
	b.mu.Lock()
	b.tokens = 0.999999999
	b.last = clock.Now()
	b.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- b.wait(ctx) }()
	select {
	case <-done:
		t.Fatal("wait must block and not spin within 20ms")
	case <-time.After(20 * time.Millisecond):
	}
	clock.Advance(time.Millisecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait never returned after advance")
	}
}
