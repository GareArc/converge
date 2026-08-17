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

type recorder struct {
	mu     sync.Mutex
	events []converge.Event
}

func (r *recorder) Observe(e converge.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) count(match func(converge.Event) bool) int {
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

type world struct {
	rt    *converge.Runtime
	clock *convergetest.Clock
	mq    *inmem.MQ
	rec   *recorder
	done  chan error
}

func newWorld(t *testing.T) *world {
	t.Helper()
	clock := convergetest.NewClock(start)
	mq := inmem.NewMQWithClock(clock)
	rec := &recorder{}
	rt, err := converge.New(converge.Options{
		Namespace: "it",
		MQ:        mq,
		Lease:     inmem.NewLeaseWithClock(clock),
		KV:        inmem.NewKVWithClock(clock),
		Observer:  rec,
		Clock:     clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &world{rt: rt, clock: clock, mq: mq, rec: rec, done: make(chan error, 1)}
}

func (w *world) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		w.stop(t)
	})
	go func() { w.done <- w.rt.Run(ctx) }()
	select {
	case <-w.rt.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("runtime never ready")
	}
	return cancel
}

func (w *world) stop(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-w.done:
			if err != nil {
				t.Fatalf("Run returned %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned")
		}
		w.clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func awaitTrue(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (w *world) advanceUntil(t *testing.T, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true while advancing")
		}
		w.clock.Advance(step)
		time.Sleep(2 * time.Millisecond)
	}
}

func assertStable(t *testing.T, cond func() bool) {
	t.Helper()
	time.Sleep(20 * time.Millisecond)
	if !cond() {
		t.Fatal("state changed while it must hold")
	}
}

func successes(rec *recorder, job string) func() int {
	return func() int {
		return rec.count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Job == job && rc.Err == nil
		})
	}
}

func TestScenarioASafetyNetCronReconciler(t *testing.T) {
	w := newWorld(t)
	var mu sync.Mutex
	workspaces := []string{"ws_1", "ws_2", "ws_3"}
	synced := map[reconcile.ID]int{}
	err := reconcile.Register(w.rt, reconcile.Spec{
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
	w.run(t)
	awaitTrue(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(synced) == 3
	})
	mu.Lock()
	workspaces = append(workspaces, "ws_4")
	mu.Unlock()
	w.advanceUntil(t, time.Hour, func() bool {
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
	w := newWorld(t)
	var mu sync.Mutex
	calls := 0
	err := reconcile.Periodic(w.rt, "license-refresh", reconcile.Every(time.Hour), func(ctx context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	w.run(t)
	awaitTrue(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 1
	})
	w.advanceUntil(t, time.Hour, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	})
	stats := w.rt.Stats()
	if len(stats) != 1 || stats[0].Job != "license-refresh" || stats[0].Surface != converge.SurfaceReconcile {
		t.Fatalf("stats = %+v", stats)
	}
}

type foreignSignal struct{}

func (foreignSignal) Error() string                    { return "worker signal from a reconciler" }
func (foreignSignal) ControlSurface() converge.Surface { return converge.SurfaceWorker }

func TestForeignSignalParksImmediatelyEndToEnd(t *testing.T) {
	w := newWorld(t)
	err := reconcile.Register(w.rt, reconcile.Spec{
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
	w.run(t)
	awaitTrue(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			ws, ok := e.(converge.WrongSurfaceSignal)
			return ok && ws.Job == "confused" && ws.Surface == converge.SurfaceWorker
		}) == 1
	})
	awaitTrue(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.IDDeadLettered)
			return ok
		}) == 1
	})
	awaitTrue(t, func() bool {
		for _, s := range w.rt.Stats() {
			if s.Job == "confused" && s.Parked == 1 {
				return true
			}
		}
		return false
	})
}

func TestMalformedHintsCountedAndDropped(t *testing.T) {
	w := newWorld(t)
	var mu sync.Mutex
	var got []reconcile.ID
	err := reconcile.Register(w.rt, reconcile.Spec{
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
	w.run(t)
	ctx := context.Background()
	if err := w.mq.Publish(ctx, "member-events", converge.Message{Payload: []byte(`{"unexpected": true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := w.mq.Publish(ctx, "member-events", converge.Message{Payload: []byte(`{"workspace_id": "ws_5", "type": "a-kind-invented-next-year"}`)}); err != nil {
		t.Fatal(err)
	}
	awaitTrue(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardUndecodable
		}) == 1
	})
	awaitTrue(t, func() bool {
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
	awaitTrue(t, func() bool {
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
	awaitTrue(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls == 2
	})
	assertStable(t, func() bool {
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
	w := newWorld(t)
	var mu sync.Mutex
	healed := false
	err := reconcile.Register(w.rt, reconcile.Spec{
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
	w.run(t)
	awaitTrue(t, func() bool {
		return w.rec.count(func(e converge.Event) bool {
			dl, ok := e.(converge.IDDeadLettered)
			return ok && dl.Job == "flaky" && dl.ID == "app_13"
		}) == 1
	})
	mu.Lock()
	healed = true
	mu.Unlock()
	if err := w.rt.Poke("flaky", "app_13"); err != nil {
		t.Fatal(err)
	}
	awaitTrue(t, func() bool { return successes(w.rec, "flaky")() >= 1 })
}

func TestUnknownJobPokeFails(t *testing.T) {
	w := newWorld(t)
	if err := reconcile.Periodic(w.rt, "only-job", reconcile.Every(time.Hour), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	w.run(t)
	if err := w.rt.Poke("no-such-job", "x"); err == nil {
		t.Fatal("poke on an unknown job must error")
	}
}
