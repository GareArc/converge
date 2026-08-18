package tokenbucket

import (
	"context"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
)

var start = time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)

func TestNewZeroRateIsUnlimited(t *testing.T) {
	if b := New(converge.Rate{}, convergetest.NewClock(start)); b != nil {
		t.Fatal("zero rate must mean no bucket")
	}
	if b := New(converge.Rate{Events: 5}, convergetest.NewClock(start)); b != nil {
		t.Fatal("zero per must mean no bucket")
	}
	if b := New(converge.Rate{Per: time.Second}, convergetest.NewClock(start)); b != nil {
		t.Fatal("zero events must mean no bucket")
	}
	var b *Bucket
	if err := b.Wait(context.Background()); err != nil {
		t.Fatal("nil bucket must never block")
	}
}

func TestAllowsBurstThenBlocks(t *testing.T) {
	clock := convergetest.NewClock(start)
	b := New(converge.Rate{Events: 2, Per: time.Second}, clock)
	ctx := context.Background()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.Wait(ctx) }()
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

func TestWaitHonorsContext(t *testing.T) {
	clock := convergetest.NewClock(start)
	b := New(converge.Rate{Events: 1, Per: time.Hour}, clock)
	ctx, cancel := context.WithCancel(context.Background())
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- b.Wait(ctx) }()
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

func TestRefillWaitClampsToMillisecond(t *testing.T) {
	cases := []struct {
		tokens float64
		rate   converge.Rate
		want   time.Duration
	}{
		{0.999999999, converge.Rate{Events: 1_000_000, Per: time.Second}, time.Millisecond},
		{0.5, converge.Rate{Events: 1, Per: time.Hour}, 30 * time.Minute},
		{0, converge.Rate{Events: 1, Per: time.Second}, time.Second},
	}
	for _, c := range cases {
		got := refillWait(c.tokens, c.rate)
		if got != c.want {
			t.Fatalf("refillWait(%v, {Events: %d, Per: %v}) = %v, want %v", c.tokens, c.rate.Events, c.rate.Per, got, c.want)
		}
	}
}
