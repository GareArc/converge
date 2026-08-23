package reconcile_test

import (
	"context"
	"errors"
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
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			synced[id]++
			return nil
		}),
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
	})
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

type foreignSignal struct{}

func (foreignSignal) Error() string                    { return "worker signal from a reconciler" }
func (foreignSignal) ControlSurface() converge.Surface { return converge.SurfaceWorker }

func TestForeignSignalParksImmediatelyEndToEnd(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	err := reconcile.Register(rt, reconcile.Spec{
		Name: "confused",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			return foreignSignal{}
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.SingleID(), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.Await(t, func() bool {
		return eventCount(h.Events(), func(e converge.Event) bool {
			ws, ok := e.(converge.WrongSurfaceSignal)
			return ok && ws.Job == "confused" && ws.Surface == converge.SurfaceWorker
		}) == 1
	})
	convergetest.Await(t, func() bool {
		return eventCount(h.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	convergetest.Await(t, func() bool {
		for _, s := range rt.Stats() {
			if s.Job == "confused" && s.Parked == 1 {
				return true
			}
		}
		return false
	})
}

func TestMalformedHintsCountedAndDropped(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	var mu sync.Mutex
	var got []reconcile.ID
	err := reconcile.Register(rt, reconcile.Spec{
		Name: "member-sync",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, id)
			return nil
		}),
		AllowUnscheduled: true,
		Triggers: []reconcile.Trigger{
			reconcile.OnMessage("member-events", reconcile.IDFromJSONField("workspace_id"), reconcile.OnMessageOpts{}),
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
		}); err != nil {
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

func TestPokeRevivesParkedID(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{Namespace: "it"})
	rt := h.Build(t)
	var mu sync.Mutex
	healed := false
	err := reconcile.Register(rt, reconcile.Spec{
		Name:            "flaky",
		DeadLetterAfter: 1,
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			if !healed {
				return errors.New("downstream broken")
			}
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.IDs(func(context.Context) ([]reconcile.ID, error) {
					return []reconcile.ID{"app_13"}, nil
				}),
				reconcile.Every(time.Hour),
			),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Runtime(t)
	convergetest.Await(t, func() bool {
		return eventCount(h.Events(), func(e converge.Event) bool {
			dl, ok := e.(converge.IDParked)
			return ok && dl.Job == "flaky" && dl.ID == "app_13"
		}) == 1
	})
	mu.Lock()
	healed = true
	mu.Unlock()
	if err := rt.Poke("flaky", "app_13"); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { return successes(h.Events, "flaky")() >= 1 })
}

func TestUnknownJobPokeFails(t *testing.T) {
	h := convergetest.NewWith(t, convergetest.Options{Namespace: "it"})
	rt := h.Build(t)
	if err := reconcile.Periodic(rt, "only-job", reconcile.Every(time.Hour), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	h.Runtime(t)
	if err := rt.Poke("no-such-job", "x"); err == nil {
		t.Fatal("poke on an unknown job must error")
	}
}

func TestScenarioBStaleMarkAppliedRefusedThenConverges(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	tr := reconcile.NewTracker(h.KV, "deploy")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "app-1"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var applied []reconcile.Version
	raceOnce := true
	err := reconcile.Register(rt, reconcile.Spec{
		Name:     "deploy",
		Versions: tr,
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			v, err := tr.Latest(ctx, id)
			if err != nil {
				return err
			}
			mu.Lock()
			race := raceOnce
			raceOnce = false
			mu.Unlock()
			if race {
				if _, err := tr.MarkChanged(ctx, id); err != nil {
					return err
				}
			}
			res := tr.MarkApplied(ctx, id, v)
			if res == nil {
				mu.Lock()
				applied = append(applied, v)
				mu.Unlock()
			}
			return res
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"app-1"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.AdvanceUntil(t, h.Clock, 100*time.Millisecond, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1
	})
	mu.Lock()
	got := applied[0]
	mu.Unlock()
	if got != 2 {
		t.Fatalf("converged at version %d; want 2", got)
	}
	if n := eventCount(h.Events(), func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 0 {
		t.Fatalf("ErrOutdated must not count as failure: %d failed RunCompleted events", n)
	}
}

func TestScenarioBParkedRevivesOnMarkChanged(t *testing.T) {
	h := convergetest.New(t)
	rt := h.Build(t)
	tr := reconcile.NewTracker(h.KV, "deploy")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "app-1"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	broken := true
	converged := false
	err := reconcile.Register(rt, reconcile.Spec{
		Name:            "deploy",
		Versions:        tr,
		DeadLetterAfter: 2,
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			mu.Lock()
			defer mu.Unlock()
			if broken {
				return errors.New("runner rejects config")
			}
			converged = true
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(context.Context) ([]string, error) {
				return []string{"app-1"}, nil
			}), reconcile.Every(time.Minute)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.Drain(t)
	convergetest.AdvanceUntil(t, h.Clock, time.Second, func() bool {
		return eventCount(h.Events(), func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	mu.Lock()
	broken = false
	mu.Unlock()
	h.Clock.Advance(time.Minute)
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return !converged })
	if _, err := tr.MarkChanged(ctx, "app-1"); err != nil {
		t.Fatal(err)
	}
	convergetest.AdvanceUntil(t, h.Clock, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return converged
	})
}
