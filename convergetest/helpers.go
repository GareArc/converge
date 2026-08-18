package convergetest

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
)

const (
	awaitDeadline   = 2 * time.Second
	awaitPoll       = 2 * time.Millisecond
	stabilityWindow = 20 * time.Millisecond
)

func Await(t testing.TB, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: condition never became true")
			return
		}
		time.Sleep(awaitPoll)
	}
}

func AdvanceUntil(t testing.TB, c *Clock, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(awaitDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("convergetest: condition never became true while advancing")
			return
		}
		c.Advance(step)
		time.Sleep(awaitPoll)
	}
}

func AssertStable(t testing.TB, cond func() bool) {
	t.Helper()
	time.Sleep(stabilityWindow)
	if !cond() {
		t.Fatalf("convergetest: state changed while it must hold")
	}
}

type Recorder struct {
	mu     sync.Mutex
	events []converge.Event
}

func (r *Recorder) Observe(e converge.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *Recorder) Count(match func(converge.Event) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if match(e) {
			n++
		}
	}
	return n
}

func (r *Recorder) Events() []converge.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}
