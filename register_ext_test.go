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
	name    string
	ready   chan struct{}
	run     func(ctx context.Context, d converge.JobDeps) error
	hinted  []string
	ranPass int
	mu      sync.Mutex
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

func (s *stubJob) Quiet() bool { return true }

func (s *stubJob) Hint(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hinted = append(s.hinted, id)
	return nil
}

func (s *stubJob) RunPassNow(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ranPass++
	return nil
}

func (s *stubJob) Stats() converge.JobStats {
	return converge.JobStats{Job: s.name, Surface: converge.SurfaceReconcile}
}

func (s *stubJob) Info() converge.JobInfo {
	return converge.JobInfo{
		Job:      s.name,
		Surface:  converge.SurfaceReconcile,
		Settings: map[string]string{"stub": s.name},
	}
}

type stubQueueJob struct {
	name  string
	queue string
	mq    converge.MQ
	ready chan struct{}
}

func newStubQueueJob(name, queue string, mq converge.MQ) *stubQueueJob {
	return &stubQueueJob{name: name, queue: queue, mq: mq, ready: make(chan struct{})}
}

func (s *stubQueueJob) Name() string { return s.name }

func (s *stubQueueJob) Run(ctx context.Context, d converge.JobDeps) error {
	<-ctx.Done()
	return nil
}

func (s *stubQueueJob) Ready() <-chan struct{} { return s.ready }

func (s *stubQueueJob) Quiet() bool { return true }

func (s *stubQueueJob) Hint(id string) error { return nil }

func (s *stubQueueJob) RunPassNow(ctx context.Context) error { return nil }

func (s *stubQueueJob) Stats() converge.JobStats { return converge.JobStats{Job: s.name} }

func (s *stubQueueJob) Info() converge.JobInfo { return converge.JobInfo{Job: s.name} }

func (s *stubQueueJob) QueueBinding() (string, converge.MQ) { return s.queue, s.mq }

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

func TestInspectReturnsRegisteredJobInfo(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, newStubJob("a")); err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newStubJob("b")); err != nil {
		t.Fatal(err)
	}
	got, err := hook.Inspect(rt)
	if err != nil {
		t.Fatal(err)
	}
	infos, ok := got.([]converge.JobInfo)
	if !ok {
		t.Fatalf("Inspect returned %T, want []converge.JobInfo", got)
	}
	if len(infos) != 2 || infos[0].Job != "a" || infos[1].Job != "b" {
		t.Fatalf("infos = %+v, want [a b] in registration order", infos)
	}
	if infos[0].Settings["stub"] != "a" || infos[1].Settings["stub"] != "b" {
		t.Fatalf("infos = %+v, per-job Settings not preserved", infos)
	}
}

func TestInspectRejectsForeignRuntime(t *testing.T) {
	if _, err := hook.Inspect("not a runtime"); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
	var nilRt *converge.Runtime
	if _, err := hook.Inspect(nilRt); err == nil {
		t.Fatal("typed-nil runtime must be rejected")
	}
}

func TestOpsDepsRoundTripsWiring(t *testing.T) {
	mq := inmem.NewMQ()
	boundMQ := inmem.NewMQ()
	kv := inmem.NewKV()
	clock := convergetest.NewClock(time.Unix(0, 0))
	rt, err := converge.New(converge.Options{Namespace: "svc", MQ: mq, KV: kv, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newStubQueueJob("q-job", "bound-queue", boundMQ)); err != nil {
		t.Fatal(err)
	}
	wiring, err := hook.OpsDeps(rt)
	if err != nil {
		t.Fatal(err)
	}
	if wiring.KV != kv || wiring.MQ != mq || wiring.Clock != clock {
		t.Fatalf("ports not round-tripped: %+v", wiring)
	}
	if wiring.Namespace != "svc" {
		t.Fatalf("Namespace = %q, want svc", wiring.Namespace)
	}
	if len(wiring.Replica) != 32 {
		t.Fatalf("Replica = %q, want 32 hex chars", wiring.Replica)
	}
	if got := wiring.QueueMQ("bound-queue"); got != boundMQ {
		t.Fatalf("QueueMQ(bound-queue) = %v, want %v", got, boundMQ)
	}
	if got := wiring.QueueMQ("unbound"); got != nil {
		t.Fatalf("QueueMQ(unbound) = %v, want nil", got)
	}
}

func TestOpsDepsRejectsForeignRuntime(t *testing.T) {
	if _, err := hook.OpsDeps("nope"); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
	var nilRt *converge.Runtime
	if _, err := hook.OpsDeps(nilRt); err == nil {
		t.Fatal("typed-nil runtime must be rejected")
	}
}

func TestReplicaIDsAreDistinctPerRuntime(t *testing.T) {
	rt1 := mustRuntime(t)
	rt2 := mustRuntime(t)
	w1, err := hook.OpsDeps(rt1)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := hook.OpsDeps(rt2)
	if err != nil {
		t.Fatal(err)
	}
	if w1.Replica == w2.Replica {
		t.Fatal("replica ids must be distinct across Runtimes")
	}
	if len(w1.Replica) != 32 || len(w2.Replica) != 32 {
		t.Fatalf("replica ids must be 32 chars, got %q and %q", w1.Replica, w2.Replica)
	}
}

func TestHintReachesStubJob(t *testing.T) {
	rt := mustRuntime(t)
	s := newStubJob("a")
	if err := hook.RegisterJob(rt, s); err != nil {
		t.Fatal(err)
	}
	if err := hook.Hint(rt, "a", "x"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hinted) != 1 || s.hinted[0] != "x" {
		t.Fatalf("hinted = %v, want [x]", s.hinted)
	}
}

func TestHintUnknownJobErrors(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.Hint(rt, "no-such-job", "x"); err == nil {
		t.Fatal("hint on unknown job must error")
	}
}

func TestHintRejectsForeignRuntime(t *testing.T) {
	if err := hook.Hint("not a runtime", "a", "x"); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
	var nilRt *converge.Runtime
	if err := hook.Hint(nilRt, "a", "x"); err == nil {
		t.Fatal("typed-nil runtime must be rejected")
	}
}

func TestRunPassNowReachesStubJob(t *testing.T) {
	rt := mustRuntime(t)
	s := newStubJob("a")
	if err := hook.RegisterJob(rt, s); err != nil {
		t.Fatal(err)
	}
	if err := hook.RunPassNow(rt, context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ranPass != 1 {
		t.Fatalf("ranPass = %d, want 1", s.ranPass)
	}
}

func TestRunPassNowUnknownJobErrors(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RunPassNow(rt, context.Background(), "no-such-job"); err == nil {
		t.Fatal("run-pass-now on unknown job must error")
	}
}

func TestRunPassNowRejectsForeignRuntime(t *testing.T) {
	if err := hook.RunPassNow("not a runtime", context.Background(), "a"); err == nil {
		t.Fatal("non-runtime must be rejected")
	}
	var nilRt *converge.Runtime
	if err := hook.RunPassNow(nilRt, context.Background(), "a"); err == nil {
		t.Fatal("typed-nil runtime must be rejected")
	}
}

func TestQuietTrueWithQuietStubs(t *testing.T) {
	rt := mustRuntime(t)
	if err := hook.RegisterJob(rt, newStubJob("a")); err != nil {
		t.Fatal(err)
	}
	if err := hook.RegisterJob(rt, newStubJob("b")); err != nil {
		t.Fatal(err)
	}
	if !hook.Quiet(rt) {
		t.Fatal("quiet stubs must report quiet")
	}
}

func TestQuietRejectsForeignRuntime(t *testing.T) {
	if hook.Quiet("not a runtime") {
		t.Fatal("foreign rt must not report quiet")
	}
	var nilRt *converge.Runtime
	if hook.Quiet(nilRt) {
		t.Fatal("typed-nil runtime must not report quiet")
	}
}

func TestAttachOptionsInvokesCallbackWithBuiltRuntime(t *testing.T) {
	var got any
	wrapped := hook.AttachOptions(converge.Options{}, func(rt any) { got = rt })
	opts, ok := wrapped.(converge.Options)
	if !ok {
		t.Fatalf("AttachOptions returned %T, want converge.Options", wrapped)
	}
	rt, err := converge.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	if got != rt {
		t.Fatalf("attach callback got %v, want %v", got, rt)
	}
}

func TestAttachOptionsPassesThroughForeignValueUnchanged(t *testing.T) {
	called := false
	wrapped := hook.AttachOptions("not options", func(rt any) { called = true })
	if wrapped != "not options" {
		t.Fatalf("AttachOptions(foreign) = %v, want the input returned unchanged", wrapped)
	}
	if called {
		t.Fatal("attach must not be invoked for a foreign, non-Options value")
	}
}

func TestAttachOptionsNotInvokedOnFailure(t *testing.T) {
	called := false
	wrapped := hook.AttachOptions(converge.Options{LeaseTTL: -time.Second}, func(rt any) { called = true })
	opts, ok := wrapped.(converge.Options)
	if !ok {
		t.Fatalf("AttachOptions returned %T, want converge.Options", wrapped)
	}
	if _, err := converge.New(opts); err == nil {
		t.Fatal("negative LeaseTTL must be rejected")
	}
	if called {
		t.Fatal("attach must not be invoked when New fails")
	}
}
