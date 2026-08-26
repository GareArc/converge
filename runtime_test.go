package converge

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeQueueJob struct {
	name  string
	queue string
	ready chan struct{}
}

func newFakeQueueJob(name, queue string) *fakeQueueJob {
	return &fakeQueueJob{name: name, queue: queue, ready: make(chan struct{})}
}

func (f *fakeQueueJob) Name() string                             { return f.name }
func (f *fakeQueueJob) Run(ctx context.Context, d JobDeps) error { <-ctx.Done(); return nil }
func (f *fakeQueueJob) Ready() <-chan struct{}                   { return f.ready }
func (f *fakeQueueJob) Stats() JobStats                          { return JobStats{Job: f.name} }
func (f *fakeQueueJob) Info() JobInfo                            { return JobInfo{Job: f.name} }
func (f *fakeQueueJob) QueueBinding() (string, MQ)               { return f.queue, nil }
func (f *fakeQueueJob) Quiet() bool                              { return true }
func (f *fakeQueueJob) Hint(id string) error                     { return nil }
func (f *fakeQueueJob) RunPassNow(ctx context.Context) error     { return nil }
func (f *fakeQueueJob) SetPaused(paused bool)                    {}

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

func TestRegisterRejectsDuplicateQueueBinding(t *testing.T) {
	rt, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.register(newFakeQueueJob("a", "q1")); err != nil {
		t.Fatal(err)
	}
	err = rt.register(newFakeQueueJob("b", "q1"))
	if err == nil {
		t.Fatal("duplicate queue binding must be rejected")
	}
	if !strings.Contains(err.Error(), "q1") || !strings.Contains(err.Error(), "a") {
		t.Fatalf("error must mention the queue and the first job's name, got %v", err)
	}
}

func TestRegisterAllowsDistinctQueueBindings(t *testing.T) {
	rt, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.register(newFakeQueueJob("a", "q1")); err != nil {
		t.Fatal(err)
	}
	if err := rt.register(newFakeQueueJob("b", "q2")); err != nil {
		t.Fatal(err)
	}
}
