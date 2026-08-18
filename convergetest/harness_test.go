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
	convergetest.AdvanceUntil(t, h.Clock, time.Hour, func() bool {
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

	h.Lease.Expire("test/converge/worker/job/lease")

	convergetest.Await(t, func() bool {
		for _, e := range h.Events() {
			if lt, ok := e.(converge.LeaseTransition); ok && lt.Job == "job" && !lt.Acquired {
				return true
			}
		}
		return false
	})
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
		t.Fatal("expected Wake before converge.New(h.Options()) to call Fatalf")
	}
	if !strings.Contains(msgs[0], "converge.New(h.Options())") {
		t.Fatalf("Fatalf message = %q, want mention of converge.New(h.Options())", msgs[0])
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
