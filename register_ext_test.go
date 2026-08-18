package converge_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
)

type stubJob struct {
	name  string
	ready chan struct{}
	run   func(ctx context.Context, d converge.JobDeps) error
	poked []string
	mu    sync.Mutex
}

func newStubJob(name string) *stubJob {
	return &stubJob{name: name, ready: make(chan struct{})}
}

func (s *stubJob) Name() string { return s.name }

func (s *stubJob) Run(ctx context.Context, d converge.JobDeps) error {
	close(s.ready)
	if s.run != nil {
		return s.run(ctx, d)
	}
	<-ctx.Done()
	return nil
}

func (s *stubJob) Ready() <-chan struct{} { return s.ready }

func (s *stubJob) Poke(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poked = append(s.poked, id)
	return nil
}

func (s *stubJob) Stats() converge.JobStats {
	return converge.JobStats{Job: s.name, Surface: converge.SurfaceReconcile}
}

func mustRuntime(t *testing.T) *converge.Runtime {
	t.Helper()
	rt, err := converge.New(converge.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestRegisterViaHook(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, newStubJob("a")); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterRejectsDuplicateAndEmptyNames(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, newStubJob("a")); err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newStubJob("a")); err == nil {
		t.Fatal("duplicate name must be rejected")
	}
	if err := hook.RegisterJob(rt, newStubJob("")); err == nil {
		t.Fatal("empty name must be rejected")
	}
}

func TestRegisterRejectsForeignTypes(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, "not a job"); err == nil {
		t.Fatal("non-job must be rejected")
	}
	if err := hook.RegisterJob("not a runtime", newStubJob("a")); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
}

func TestProducerDepsRoundTripsWiring(t *testing.T) {
	mq := inmem.NewMQ()
	clock := convergetest.NewClock(time.Unix(0, 0))
	rt, err := converge.New(converge.Options{MQ: mq, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	wiring, err := hook.ProducerDeps(rt)
	if err != nil {
		t.Fatal(err)
	}
	if wiring.MQ != mq {
		t.Fatalf("MQ not round-tripped: got %v", wiring.MQ)
	}
	if wiring.Clock != clock {
		t.Fatalf("Clock not round-tripped: got %v", wiring.Clock)
	}
	if got := wiring.QueueMQ("unbound"); got != nil {
		t.Fatalf("QueueMQ for unbound queue = %v, want nil", got)
	}
}

func TestProducerDepsRejectsForeignRuntime(t *testing.T) {
	if _, err := hook.ProducerDeps("not a runtime"); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
}

func TestRegisterIsGoroutineSafe(t *testing.T) {
	rt := mustRuntime(t)
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hook.RegisterJob(rt, newStubJob(fmt.Sprintf("job-%d", i)))
		}()
	}
	wg.Wait()
	if got := len(rt.Stats()); got != 50 {
		t.Fatalf("registered %d jobs, want 50", got)
	}
}
