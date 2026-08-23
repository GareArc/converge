package convergetest_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/internal/hook"
	"github.com/GareArc/converge/reconcile"
	"github.com/GareArc/converge/worker"
)

func TestReconcileRoundTrip(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "workspace-credentials",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) {
				return nil, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Wake("workspace-credentials", "ws_42")
	h.Drain(t)
	h.AssertReconciled(t, "workspace-credentials", "ws_42")
}

func TestParkAfterDeadLetterThreshold(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name:            "flaky",
		DeadLetterAfter: 2,
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return errors.New("downstream broken")
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) {
				return nil, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Wake("flaky", "app_13")
	h.Drain(t)
	convergetest.AdvanceUntil(t, h.Clock, 250*time.Millisecond, func() bool {
		return parked(h, "flaky", "app_13")
	})
	h.Drain(t)
	h.AssertParked(t, "flaky", "app_13")
}

func parked(h *convergetest.Harness, job, id string) bool {
	for _, e := range h.Events() {
		if p, ok := e.(converge.IDParked); ok && p.Job == job && p.ID == id {
			return true
		}
	}
	return false
}

func TestScheduleBoundaryDrivesReconcile(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	runs := 0
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "app-runner",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			mu.Lock()
			runs++
			mu.Unlock()
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"app_13"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	h.AssertReconciled(t, "app-runner", "app_13")
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 1
	})
	h.Clock.Advance(time.Hour)
	h.Drain(t)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 2
	})
}

func TestRunPassImmediateWithoutClockMovement(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	runs := 0
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "backfill",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			mu.Lock()
			runs++
			mu.Unlock()
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"app_1"}, nil
			}), reconcile.Every(24*time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 1
	})
	h.RunPass(t, "backfill")
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return runs == 2
	})
}

func TestWakeOnWorkerJobFatals(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.New(fake)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Handle(rt, worker.NewTask[string]("job", worker.TaskOpts{}), func(context.Context, string) error {
		return nil
	}, worker.HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Wake("job", "irrelevant")
	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected Wake on a worker job to Fatalf")
	}
	if !strings.Contains(msgs[0], "hint is a reconcile verb") {
		t.Fatalf("Fatalf message = %q, want mention of the worker engine's wrong-surface error", msgs[0])
	}
}

func TestWorkerRoundTripAndAssertEnqueued(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("send-invite", worker.TaskOpts{})
	var mu sync.Mutex
	var got string
	err = worker.Handle(rt, tk, func(_ context.Context, payload string) error {
		mu.Lock()
		got = payload
		mu.Unlock()
		return nil
	}, worker.HandleOpts{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "alice@example.com", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	h.AssertEnqueued(t, tk, "alice@example.com")
	h.Drain(t)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got == "alice@example.com"
	})
}

func TestFailNextPublishSurfacesOnEnqueue(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("send-invite", worker.TaskOpts{})
	if err := worker.Handle(rt, tk, func(context.Context, string) error { return nil }, worker.HandleOpts{}); err != nil {
		t.Fatal(err)
	}
	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	h.MQ.FailNextPublish(boom)
	err = tk.Enqueue(context.Background(), p, "alice@example.com", worker.EnqueueOpts{})
	if !errors.Is(err, boom) {
		t.Fatalf("Enqueue error = %v, want %v", err, boom)
	}
}

func leaseDropped(events []converge.Event, job string) bool {
	for _, e := range events {
		if lt, ok := e.(converge.LeaseTransition); ok && lt.Job == job && !lt.Acquired {
			return true
		}
	}
	return false
}

func TestLeaseExpireCancelsInFlightHandler(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("job", worker.TaskOpts{})
	started := make(chan struct{})
	var once sync.Once
	handler := func(ctx context.Context, payload string) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}
	if err := worker.Handle(rt, tk, handler, worker.HandleOpts{RunMode: converge.OnOneReplica}); err != nil {
		t.Fatal(err)
	}
	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "hello", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}

	h.Events()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	h.Clock.Advance(1000 * time.Hour)
	convergetest.AssertStable(t, func() bool { return !leaseDropped(h.Events(), "job") })

	h.Lease.Expire("job")

	convergetest.Await(t, func() bool { return leaseDropped(h.Events(), "job") })
}

func TestLargeClockAdvanceDoesNotDropLease(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "steady-runner",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"seed"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	h.AssertReconciled(t, "steady-runner", "seed")

	h.Clock.Advance(1000 * time.Hour)
	h.Drain(t)

	if leaseDropped(h.Events(), "steady-runner") {
		t.Fatal("large Clock.Advance must not drop the lease under the pinned harness LeaseTTL")
	}

	h.Wake("steady-runner", "id_1")
	h.Drain(t)
	h.AssertReconciled(t, "steady-runner", "id_1")
}

func TestDrainOnPausedJobReturns(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name:   "paused-job",
		Paused: true,
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) {
				return nil, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
}

type fakeTB struct {
	testing.TB
	mu       sync.Mutex
	msg      []string
	cleanups []func()
}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Cleanup(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, fn)
}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.msg = append(f.msg, fmt.Sprintf(format, args...))
}

func (f *fakeTB) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.msg...)
}

func (f *fakeTB) runCleanups() {
	f.mu.Lock()
	cleanups := f.cleanups
	f.cleanups = nil
	f.mu.Unlock()
	for i := len(cleanups) - 1; i >= 0; i-- {
		cleanups[i]()
	}
}

func TestVerbBeforeConvergeNewFatals(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.New(fake)
	h.Wake("some-job", "some-id")
	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected Wake before h.Build(t) to call Fatalf")
	}
	if !strings.Contains(msgs[0], "h.Build(t)") {
		t.Fatalf("Fatalf message = %q, want mention of h.Build(t)", msgs[0])
	}
}

func TestSecondAttachFatals(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.New(fake)
	if _, err := converge.New(h.Options()); err != nil {
		t.Fatal(err)
	}
	if _, err := converge.New(h.Options()); err != nil {
		t.Fatal(err)
	}
	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected a second converge.New(h.Options()) to call Fatalf")
	}
	if !strings.Contains(msgs[0], "one runtime per Harness") {
		t.Fatalf("Fatalf message = %q, want mention of one runtime per Harness", msgs[0])
	}
}

func TestNewWithCustomNamespaceReachesOptions(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{Namespace: "custom-ns"})
	if _, err := converge.New(h.Options()); err != nil {
		t.Fatal(err)
	}
	if got := h.Options().Namespace; got != "custom-ns" {
		t.Fatalf("Options().Namespace = %q, want %q", got, "custom-ns")
	}
}

func TestNewWithCustomKVReachesRuntime(t *testing.T) {
	var captured converge.KV
	h := convergetest.NewWith(t, convergetest.Options{
		KV: func(clock *convergetest.Clock) converge.KV {
			kv := inmem.NewKVWithClock(clock)
			captured = kv
			return kv
		},
	})
	if h.KV != nil {
		t.Fatalf("h.KV = %v, want nil when a custom KV constructor is supplied", h.KV)
	}
	if h.MQ == nil {
		t.Fatal("h.MQ = nil, want the wrapped default MQ (only KV was customized)")
	}

	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}

	tk := worker.NewTask[string]("dead-job", worker.TaskOpts{})
	err = worker.Handle(rt, tk, func(context.Context, string) error {
		return errors.New("boom")
	}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 1, MinBackoff: time.Second, MaxBackoff: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}

	p, err := worker.ProducerFrom(rt)
	if err != nil {
		t.Fatal(err)
	}
	if err := tk.Enqueue(context.Background(), p, "payload", worker.EnqueueOpts{}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)

	convergetest.Await(t, func() bool {
		keys, _, err := captured.Scan(context.Background(), "test/converge/worker/dead-job/dlq/", "")
		return err == nil && len(keys) == 1
	})
}

func TestOptionsClockIsSharedAcrossHarnesses(t *testing.T) {
	clock := convergetest.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	var seenByMQFactory *convergetest.Clock
	ha := convergetest.NewWith(t, convergetest.Options{
		Clock: clock,
		MQ: func(c *convergetest.Clock) converge.MQ {
			seenByMQFactory = c
			return inmem.NewMQWithClock(c)
		},
	})
	hb := convergetest.NewWith(t, convergetest.Options{Clock: clock})

	if ha.Clock != clock || hb.Clock != clock {
		t.Fatalf("ha.Clock, hb.Clock = %p, %p, want both to be the supplied %p", ha.Clock, hb.Clock, clock)
	}
	if seenByMQFactory != clock {
		t.Fatalf("MQ factory saw clock %p, want the supplied %p", seenByMQFactory, clock)
	}

	ha.Clock.Advance(time.Hour)
	if got := hb.Clock.Now(); !got.Equal(clock.Now()) {
		t.Fatalf("hb.Clock.Now() = %v after advancing via ha.Clock, want %v (a shared clock)", got, clock.Now())
	}
}

func TestNewWithCustomLeaseReachesRuntime(t *testing.T) {
	var captured *inmem.Lease
	h := convergetest.NewWith(t, convergetest.Options{
		Lease: func(clock *convergetest.Clock) converge.Lease {
			lease := inmem.NewLeaseWithClock(clock)
			captured = lease
			return lease
		},
	})
	if h.Lease != nil {
		t.Fatalf("h.Lease = %v, want nil when a custom Lease constructor is supplied", h.Lease)
	}
	if h.MQ == nil {
		t.Fatal("h.MQ = nil, want the wrapped default MQ (only Lease was customized)")
	}

	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("job", worker.TaskOpts{})
	err = worker.Handle(rt, tk, func(context.Context, string) error { return nil }, worker.HandleOpts{RunMode: converge.OnOneReplica})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)

	convergetest.Await(t, func() bool { return len(captured.Names()) == 1 })
}

func TestNewWithZeroOptionsMatchesNew(t *testing.T) {
	hDefault := convergetest.New(t)
	hZero := convergetest.NewWith(t, convergetest.Options{})

	oDefault := hDefault.Options()
	oZero := hZero.Options()

	if oZero.Namespace != oDefault.Namespace {
		t.Fatalf("NewWith(t, Options{}).Options().Namespace = %q, want %q", oZero.Namespace, oDefault.Namespace)
	}
	if oZero.LeaseTTL != oDefault.LeaseTTL {
		t.Fatalf("NewWith(t, Options{}).Options().LeaseTTL = %v, want %v", oZero.LeaseTTL, oDefault.LeaseTTL)
	}
	if oZero.DrainTimeout != oDefault.DrainTimeout {
		t.Fatalf("NewWith(t, Options{}).Options().DrainTimeout = %v, want %v", oZero.DrainTimeout, oDefault.DrainTimeout)
	}
	if hZero.MQ == nil {
		t.Fatal("NewWith(t, Options{}).MQ = nil, want the wrapped default MQ")
	}
	if hZero.KV == nil {
		t.Fatal("NewWith(t, Options{}).KV = nil, want the wrapped default KV")
	}
	if hZero.Lease == nil {
		t.Fatal("NewWith(t, Options{}).Lease = nil, want the wrapped default Lease")
	}
}

func TestStopReturnsNilOnCleanShutdownWithoutDoubleFatal(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.NewWith(fake, convergetest.Options{})
	if _, err := converge.New(h.Options()); err != nil {
		t.Fatal(err)
	}
	h.Drain(fake)

	err := h.Stop(fake)
	if err != nil {
		t.Fatalf("Stop returned %v, want nil on clean shutdown", err)
	}

	fake.runCleanups()
	if msgs := fake.messages(); len(msgs) != 0 {
		t.Fatalf("Cleanup Fatal'd after an explicit Stop: %v", msgs)
	}
}

func TestEventsReadableAfterStop(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "steady",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"id"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	h.AssertReconciled(t, "steady", "id")

	if err := h.Stop(t); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}

	events := h.Events()
	if len(events) == 0 {
		t.Fatal("Events() after Stop returned nothing, want the recorded event history")
	}
}

func TestDrivingVerbAfterStopFatalsNamingCause(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.NewWith(fake, convergetest.Options{})
	if _, err := converge.New(h.Options()); err != nil {
		t.Fatal(err)
	}
	if err := h.Stop(fake); err != nil {
		t.Fatalf("Stop returned %v, want nil", err)
	}

	h.Wake("some-job", "some-id")

	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected a driving verb called after Stop to Fatalf")
	}
	if !strings.Contains(msgs[0], "stopped via Stop(t)") {
		t.Fatalf("Fatalf message = %q, want mention that the harness was explicitly stopped", msgs[0])
	}
	if strings.Contains(msgs[0], "exited early") {
		t.Fatalf("Fatalf message = %q, must not use the crash wording for a deliberate Stop", msgs[0])
	}
}

type crashJob struct {
	ready chan struct{}
	rt    *converge.Runtime
}

func (j *crashJob) Name() string { return "crash" }

func (j *crashJob) Run(context.Context, converge.JobDeps) error {
	close(j.ready)
	<-j.rt.Ready()
	return errors.New("boom: simulated crash")
}

func (j *crashJob) Ready() <-chan struct{} { return j.ready }

func (j *crashJob) Poke(string) error { return nil }

func (j *crashJob) Stats() converge.JobStats { return converge.JobStats{Job: j.Name()} }

func (j *crashJob) Info() converge.JobInfo { return converge.JobInfo{Job: j.Name()} }

func (j *crashJob) Quiet() bool { return true }

func (j *crashJob) Hint(string) error { return nil }

func (j *crashJob) RunPassNow(context.Context) error { return nil }

func (j *crashJob) SetPaused(bool) {}

func TestRuntimeExitedEarlyFatalsWithCrashWording(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.NewWith(fake, convergetest.Options{})
	rt := h.Build(fake)
	if err := hook.RegisterJob(rt, &crashJob{ready: make(chan struct{}), rt: rt}); err != nil {
		t.Fatal(err)
	}
	h.Runtime(fake)

	convergetest.Await(t, func() bool {
		h.Wake("crash", "id")
		return len(fake.messages()) > 0
	})

	msgs := fake.messages()
	if !strings.Contains(msgs[0], "runtime exited early") {
		t.Fatalf("Fatalf message = %q, want mention that the runtime exited early", msgs[0])
	}
	if !strings.Contains(msgs[0], "boom") {
		t.Fatalf("Fatalf message = %q, want the crash error included", msgs[0])
	}
	if strings.Contains(msgs[0], "stopped via Stop(t)") {
		t.Fatalf("Fatalf message = %q, must not use the deliberate-stop wording for a genuine crash", msgs[0])
	}
}

func TestRuntimeReturnsAttachedRuntime(t *testing.T) {
	h := convergetest.New(t)
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	got := h.Runtime(t)
	if got != rt {
		t.Fatalf("Runtime(t) = %p, want %p (the attached runtime)", got, rt)
	}
}

func TestBuildReturnsUnstartedRuntime(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	if rt == nil {
		t.Fatal("Build(t) = nil, want a constructed runtime")
	}
	select {
	case <-rt.Ready():
		t.Fatal("Build(t) must not start the runtime; Ready() fired without a driving verb")
	default:
	}
	if got := h.Runtime(t); got != rt {
		t.Fatalf("Runtime(t) = %p, want %p (the same runtime Build(t) returned, now started)", got, rt)
	}
}

func TestBuildFatalsOnConstructionError(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.NewWith(fake, convergetest.Options{LeaseTTL: -time.Second})
	rt := h.Build(fake)
	if rt != nil {
		t.Fatalf("Build(t) = %v, want nil on a construction error", rt)
	}
	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected Build to Fatalf on a converge.New error")
	}
	if !strings.Contains(msgs[0], "converge.New(h.Options())") {
		t.Fatalf("Fatalf message = %q, want mention of converge.New(h.Options())", msgs[0])
	}
}

func TestDrainWithCustomMQDegradesToHookQuietWithoutPanicking(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{
		MQ: func(clock *convergetest.Clock) converge.MQ {
			return inmem.NewMQWithClock(clock)
		},
	})
	if h.MQ != nil {
		t.Fatalf("h.MQ = %v, want nil when a custom MQ constructor is supplied", h.MQ)
	}
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	err = reconcile.Register(rt, reconcile.Spec{
		Name: "steady",
		Reconciler: reconcile.Func(func(context.Context, reconcile.ID) error {
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) {
				return nil, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
}

func TestAssertEnqueuedWithCustomMQFatalsInsteadOfPanicking(t *testing.T) {
	fake := &fakeTB{}
	t.Cleanup(fake.runCleanups)
	h := convergetest.NewWith(fake, convergetest.Options{
		MQ: func(clock *convergetest.Clock) converge.MQ {
			return inmem.NewMQWithClock(clock)
		},
	})
	rt, err := converge.New(h.Options())
	if err != nil {
		t.Fatal(err)
	}
	tk := worker.NewTask[string]("send-invite", worker.TaskOpts{})
	if err := worker.Handle(rt, tk, func(context.Context, string) error { return nil }, worker.HandleOpts{}); err != nil {
		t.Fatal(err)
	}

	h.AssertEnqueued(fake, tk, "alice@example.com")

	msgs := fake.messages()
	if len(msgs) == 0 {
		t.Fatal("expected AssertEnqueued with a custom MQ to Fatalf instead of panicking")
	}
	if !strings.Contains(msgs[0], "custom MQ constructor") || !strings.Contains(msgs[0], "Await") {
		t.Fatalf("Fatalf message = %q, want mention of the custom MQ constructor and Await", msgs[0])
	}
}
