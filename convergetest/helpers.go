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
	pollStep        = 2 * time.Millisecond
	stabilityWindow = 20 * time.Millisecond
)

type pollSpec struct {
	deadline time.Duration
	step     time.Duration
	advance  func()
	fail     func()
}

func pollUntil(t testing.TB, s pollSpec, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(s.deadline)
	for !cond() {
		if time.Now().After(deadline) {
			s.fail()
			return false
		}
		if s.advance != nil {
			s.advance()
		}
		time.Sleep(s.step)
	}
	return true
}

func Await(t testing.TB, cond func() bool) {
	t.Helper()
	pollUntil(t, pollSpec{
		deadline: awaitDeadline,
		step:     pollStep,
		fail:     func() { t.Fatalf("convergetest: condition never became true") },
	}, cond)
}

func AdvanceUntil(t testing.TB, c *Clock, step time.Duration, cond func() bool) {
	t.Helper()
	pollUntil(t, pollSpec{
		deadline: awaitDeadline,
		step:     pollStep,
		advance:  func() { c.Advance(step) },
		fail:     func() { t.Fatalf("convergetest: condition never became true while advancing") },
	}, cond)
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
