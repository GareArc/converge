package converge

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordingMQ struct {
	mu        sync.Mutex
	published []recordedPublish
	err       error
}

type recordedPublish struct {
	queue string
	msg   Message
}

func (m *recordingMQ) Publish(ctx context.Context, queue string, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.published = append(m.published, recordedPublish{queue: queue, msg: msg})
	return nil
}

func (m *recordingMQ) Consume(ctx context.Context, queue string, deliver func(Delivery)) error {
	<-ctx.Done()
	return ctx.Err()
}

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time                         { return c.t }
func (c fixedClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func TestNewAppliesDefaults(t *testing.T) {
	rt, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if rt.opts.LeaseTTL != 30*time.Second || rt.opts.DrainTimeout != 30*time.Second {
		t.Fatalf("defaults not applied: %+v", rt.opts)
	}
	if rt.opts.Observer == nil || rt.opts.Clock == nil {
		t.Fatal("nil Observer/Clock must default to no-op/wall clock")
	}
}

func TestNewRejectsNegativeDurations(t *testing.T) {
	if _, err := New(Options{LeaseTTL: -time.Second}); err == nil {
		t.Fatal("negative LeaseTTL must be rejected")
	}
	if _, err := New(Options{DrainTimeout: -time.Second}); err == nil {
		t.Fatal("negative DrainTimeout must be rejected")
	}
}

func TestNewClonesMiddleware(t *testing.T) {
	mws := []Middleware{func(next Handler) Handler { return next }}
	rt, err := New(Options{Middleware: mws})
	if err != nil {
		t.Fatal(err)
	}
	mws[0] = nil
	if rt.opts.Middleware[0] == nil {
		t.Fatal("New must clone Options.Middleware")
	}
}

func TestScopeIsTheRuntimesTriple(t *testing.T) {
	mq := &recordingMQ{}
	clock := fixedClock{t: time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)}
	rt, err := New(Options{Namespace: "svc", MQ: mq, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	s := rt.Scope()
	if s.MQ != MQ(mq) || s.Namespace != "svc" || s.Clock != Clock(clock) {
		t.Fatalf("Scope = %+v, want the runtime's MQ, namespace and clock", s)
	}
	bare, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if bare.Scope().Clock == nil {
		t.Fatal("Scope.Clock must be the defaulted clock, never nil")
	}
	if _, ok := SystemClock().(systemClock); !ok {
		t.Fatalf("SystemClock() = %T, want systemClock", SystemClock())
	}
}
