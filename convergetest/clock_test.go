package convergetest_test

import (
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
)

var _ converge.Clock = (*convergetest.Clock)(nil)

var start = time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)

func TestAfterFiresOnlyOnceAdvancedPast(t *testing.T) {
	c := convergetest.NewClock(start)
	ch := c.After(5 * time.Second)

	c.Advance(4 * time.Second)
	select {
	case <-ch:
		t.Fatal("fired before its time")
	default:
	}

	c.Advance(time.Second)
	select {
	case at := <-ch:
		if !at.Equal(start.Add(5 * time.Second)) {
			t.Fatalf("fired at %v", at)
		}
	default:
		t.Fatal("did not fire at its time")
	}
}

func TestAfterNonPositiveFiresImmediately(t *testing.T) {
	c := convergetest.NewClock(start)
	select {
	case <-c.After(0):
	default:
		t.Fatal("After(0) must fire immediately")
	}
}

func TestAdvanceFiresAllDueWaiters(t *testing.T) {
	c := convergetest.NewClock(start)
	a, b, far := c.After(time.Second), c.After(2*time.Second), c.After(time.Hour)
	c.Advance(5 * time.Second)
	for name, ch := range map[string]<-chan time.Time{"a": a, "b": b} {
		select {
		case <-ch:
		default:
			t.Fatalf("%s did not fire", name)
		}
	}
	select {
	case <-far:
		t.Fatal("future waiter fired early")
	default:
	}
	if got := c.Now(); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("Now() = %v", got)
	}
}

func TestAdvanceDeliversWaiterDueTime(t *testing.T) {
	c := convergetest.NewClock(start)
	a := c.After(5 * time.Second)
	b := c.After(7 * time.Second)
	c.Advance(10 * time.Second)
	if got := <-a; !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("a fired with %v, want its due time", got)
	}
	if got := <-b; !got.Equal(start.Add(7 * time.Second)) {
		t.Fatalf("b fired with %v, want its due time", got)
	}
}
