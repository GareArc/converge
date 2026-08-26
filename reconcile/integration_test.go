package reconcile_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

var start = time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)

func successes(events func() []converge.Event, job string) func() int {
	return func() int {
		return eventCount(events(), func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Job == job && rc.Err == nil
		})
	}
}

func eventCount(events []converge.Event, match func(converge.Event) bool) int {
	n := 0
	for _, e := range events {
		if match(e) {
			n++
		}
	}
	return n
}

func TestScenarioASafetyNetCronReconciler(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	var mu sync.Mutex
	workspaces := []string{"ws_1", "ws_2", "ws_3"}
	synced := map[reconcile.ID]int{}
	err := reconcile.Register(rt, reconcile.Spec{
		Name: "workspace-credentials",
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			synced[id]++
			return nil
		},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(func(context.Context) ([]string, error) {
					mu.Lock()
					defer mu.Unlock()
					return append([]string(nil), workspaces...), nil
				}),
				reconcile.Cron("0 3 * * *", reconcile.CronOpts{}),
			),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(synced) == 3
	})
	mu.Lock()
	workspaces = append(workspaces, "ws_4")
	mu.Unlock()
	convergetest.AdvanceUntil(t, h.Clock, time.Hour, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return synced["ws_4"] >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	for _, ws := range workspaces {
		if synced[reconcile.ID(ws)] == 0 {
			t.Fatalf("workspace %s never visited", ws)
		}
	}
}

func TestTenMinuteTourPeriodic(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	var mu sync.Mutex
	calls := 0
	err := reconcile.Periodic(rt, "license-refresh", reconcile.Every(time.Hour), func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	}, reconcile.PeriodicOpts{})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 1
	})
	convergetest.AdvanceUntil(t, h.Clock, time.Hour, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	stats := rt.Stats()
	if len(stats) != 1 || stats[0].Job != "license-refresh" || stats[0].Surface != converge.SurfaceReconcile {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestNotifyFromAnotherBinaryReconcilesTheID(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	seen := make(chan string, 4)
	if err := reconcile.Register(rt, reconcile.Spec{
		Name: "merchants",
		Reconcile: func(_ context.Context, id reconcile.ID) error {
			seen <- string(id)
			return nil
		},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) { return nil, nil }), reconcile.Every(time.Hour)),
			reconcile.Notifications(reconcile.NotificationsOpts{}),
		},
	}); err != nil {
		t.Fatal(err)
	}
	h.Drain(t)

	p, err := converge.NewProducer(h.MQ, converge.ProducerOpts{Namespace: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Notify(context.Background(), "merchants", "m-42"); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-seen:
		if id != "m-42" {
			t.Fatalf("reconciled %q, want %q", id, "m-42")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("notification never reached the reconciler")
	}
}

func TestNotificationsFromRequiresAnIDFunction(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	err := reconcile.Register(rt, reconcile.Spec{
		Name:      "workspaces",
		Reconcile: func(context.Context, reconcile.ID) error { return nil },
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
			reconcile.NotificationsFrom("legacy:queue", reconcile.NotificationsOpts{}),
		},
	})
	if err == nil {
		t.Fatal("NotificationsFrom accepted with no ID function")
	}
}

func TestMalformedNotificationsCountedAndDropped(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	var mu sync.Mutex
	var got []reconcile.ID
	err := reconcile.Register(rt, reconcile.Spec{
		Name: "member-sync",
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, id)
			return nil
		},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDs(func(context.Context) ([]reconcile.ID, error) { return nil, nil }), reconcile.Every(time.Hour)),
			reconcile.NotificationsFrom("member-events", reconcile.NotificationsOpts{ID: reconcile.IDFromJSON("workspace_id")}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	ctx := context.Background()
	if err := h.MQ.Publish(ctx, "member-events", converge.Message{Payload: []byte(`{"unexpected": true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := h.MQ.Publish(ctx, "member-events", converge.Message{Payload: []byte(`{"workspace_id": "ws_5", "type": "a-kind-invented-next-year"}`)}); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return eventCount(h.Events(), func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardUndecodable
		}) == 1
	})
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1 && got[0] == "ws_5"
	})
}

func TestScheduleRunsWithoutKV(t *testing.T) {
	clock := convergetest.NewClock(start)
	rt, err := converge.New(converge.Options{Lease: inmem.NewLeaseWithClock(clock), Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan struct{})
	var once sync.Once
	if err := reconcile.Periodic(rt, "no-kv", reconcile.Every(time.Hour), func(context.Context) error {
		once.Do(func() { close(ran) })
		return nil
	}, reconcile.PeriodicOpts{}); err != nil {
		t.Fatalf("Register with Options.KV nil: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	select {
	case <-ran:
	case err := <-done:
		t.Fatalf("Run returned %v with Options.KV nil; the last fire must fall back to in-process memory", err)
	case <-time.After(2 * time.Second):
		t.Fatal("a Schedule trigger never fired with Options.KV nil")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestMissedTickRunOnceAcrossRestart(t *testing.T) {
	clock := convergetest.NewClock(start)
	kv := inmem.NewKVWithClock(clock)
	lease := inmem.NewLeaseWithClock(clock)
	var mu sync.Mutex
	calls := 0
	boot := func() (*converge.Runtime, chan error, context.CancelFunc) {
		rt, err := converge.New(converge.Options{Lease: lease, KV: kv, Clock: clock})
		if err != nil {
			t.Fatal(err)
		}
		if err := reconcile.Periodic(rt, "nightly", reconcile.Every(time.Hour), func(ctx context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			calls++
			return nil
		}, reconcile.PeriodicOpts{}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		done := make(chan error, 1)
		go func() { done <- rt.Run(ctx) }()
		select {
		case <-rt.Ready():
		case <-time.After(2 * time.Second):
			t.Fatal("never ready")
		}
		return rt, done, cancel
	}
	_, done, cancel := boot()
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 1
	})
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		default:
			if time.Now().After(deadline) {
				t.Fatal("first runtime never stopped")
			}
			clock.Advance(10 * time.Second)
			time.Sleep(2 * time.Millisecond)
			continue
		}
		break
	}
	clock.Advance(5 * time.Hour)
	_, done2, cancel2 := boot()
	convergetest.Await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 2
	})
	convergetest.AssertStable(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 2
	})
	cancel2()
	deadline = time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done2:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("second runtime never stopped")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}
