package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

type liveEngine struct {
	e     *engine
	clock *convergetest.Clock
	rec   *eventRecorder
	lease *inmem.Lease
	kv    converge.KV
	done  chan error
}

func startRun(t *testing.T, spec Spec, shared *liveEngine) (*liveEngine, context.CancelFunc) {
	t.Helper()
	var (
		clock *convergetest.Clock
		rec   *eventRecorder
		lease *inmem.Lease
		kv    converge.KV
	)
	if shared != nil {
		clock, rec, lease = shared.clock, shared.rec, shared.lease
		kv = shared.kv
	} else {
		clock = convergetest.NewClock(wqStart)
		rec = &eventRecorder{}
		lease = inmem.NewLeaseWithClock(clock)
		kv = inmem.NewKVWithClock(clock)
	}
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	deps := converge.JobDeps{
		Lease:        lease,
		KV:           kv,
		Observer:     rec,
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	le := &liveEngine{e: e, clock: clock, rec: rec, lease: lease, kv: kv, done: make(chan error, 1)}
	go func() { le.done <- e.Run(ctx, deps) }()
	t.Cleanup(cancel)
	return le, cancel
}

func waitRun(t *testing.T, le *liveEngine) error {
	t.Helper()
	select {
	case err := <-le.done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

func acquired(rec *eventRecorder) int {
	return rec.count(func(e converge.Event) bool {
		lt, ok := e.(converge.LeaseTransition)
		return ok && lt.Acquired
	})
}

func released(rec *eventRecorder) int {
	return rec.count(func(e converge.Event) bool {
		lt, ok := e.(converge.LeaseTransition)
		return ok && !lt.Acquired
	})
}

func specWithSchedule() Spec {
	return Spec{
		Name:       "job",
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers:   []Trigger{Schedule(SingleID(), Every(time.Hour))},
	}
}

func TestRunRejectsBadDeps(t *testing.T) {
	e, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
	deps := converge.JobDeps{KV: inmem.NewKV(), Clock: convergetest.NewClock(wqStart), Observer: &eventRecorder{}}
	if err := e.Run(context.Background(), deps); err == nil {
		t.Fatal("OnOneReplica without a Lease must fail Run")
	}
	e2, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
	deps2 := converge.JobDeps{Lease: inmem.NewLease(), Clock: convergetest.NewClock(wqStart), Observer: &eventRecorder{}}
	if err := e2.Run(context.Background(), deps2); err == nil {
		t.Fatal("Schedule without KV must fail Run")
	}
}

func TestLeaderRunsPassStandbyWaits(t *testing.T) {
	leader, _ := startRun(t, specWithSchedule(), nil)
	select {
	case <-leader.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("leader never ready")
	}
	await(t, func() bool { return acquired(leader.rec) == 1 })
	await(t, func() bool { return runCount(&testEngine{rec: leader.rec}) >= 1 })

	standbySpec := specWithSchedule()
	standby, _ := startRun(t, standbySpec, leader)
	select {
	case <-standby.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("standby must be ready without the lease")
	}
	time.Sleep(20 * time.Millisecond)
	if got := acquired(leader.rec); got != 1 {
		t.Fatalf("standby must not acquire while leader holds: %d", got)
	}
}

func TestLeaseLossStepsDownAndReelects(t *testing.T) {
	leader, _ := startRun(t, specWithSchedule(), nil)
	await(t, func() bool { return acquired(leader.rec) == 1 })
	leader.lease.Expire("converge/reconcile/job/lease")
	await(t, func() bool { return released(leader.rec) >= 1 })
	await(t, func() bool { return acquired(leader.rec) >= 2 })
}

func TestLeaseLossCancelsInFlightNeutrally(t *testing.T) {
	started := make(chan struct{})
	blocked := Func(func(ctx context.Context, id ID) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	spec := specWithSchedule()
	spec.Reconciler = blocked
	le, _ := startRun(t, spec, nil)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	le.lease.Expire("converge/reconcile/job/lease")
	await(t, func() bool { return released(le.rec) >= 1 })
	if n := le.rec.count(func(e converge.Event) bool {
		rc, ok := e.(converge.RunCompleted)
		return ok && rc.Err != nil
	}); n != 0 {
		t.Fatalf("lease-loss cancellation must be neutral, got %d failed runs", n)
	}
}

func TestShutdownGivesDrainGraceThenReturnsNil(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	spec := specWithSchedule()
	spec.Reconciler = Func(func(ctx context.Context, id ID) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})
	le, cancel := startRun(t, spec, nil)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-le.done:
		t.Fatalf("Run returned before drain grace elapsed: %v", err)
	default:
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-le.done:
			if err != nil {
				t.Fatalf("clean shutdown must return nil, got %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned after drain")
		}
		le.clock.Advance(5 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestShutdownReleasesLease(t *testing.T) {
	le, cancel := startRun(t, specWithSchedule(), nil)
	await(t, func() bool { return acquired(le.rec) == 1 })
	cancel()
	if err := waitRun(t, le); err != nil {
		t.Fatal(err)
	}
	_, ok, err := le.lease.TryAcquire(context.Background(), "converge/reconcile/job/lease", time.Second)
	if err != nil || !ok {
		t.Fatalf("lease must be released on shutdown: %v %v", ok, err)
	}
}

func TestAllReplicasRunsWithoutLease(t *testing.T) {
	spec := Spec{
		Name:       "cache",
		RunMode:    converge.OnAllReplicas,
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers:   []Trigger{Schedule(SingleID(), Every(time.Hour))},
	}
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	clock := convergetest.NewClock(wqStart)
	rec := &eventRecorder{}
	deps := converge.JobDeps{
		KV:           inmem.NewKVWithClock(clock),
		Observer:     rec,
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, deps) }()
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	await(t, func() bool {
		return rec.count(func(ev converge.Event) bool {
			_, ok := ev.(converge.RunCompleted)
			return ok
		}) >= 1
	})
	if n := acquired(rec); n != 0 {
		t.Fatalf("OnAllReplicas must not touch the lease, got %d acquisitions", n)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned")
		}
		clock.Advance(time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestRegisterThroughRuntime(t *testing.T) {
	rt, err := converge.New(converge.Options{KV: inmem.NewKV(), Lease: inmem.NewLease()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(rt, okSpec()); err != nil {
		t.Fatal(err)
	}
	if err := Register(rt, okSpec()); err == nil {
		t.Fatal("duplicate name must be rejected by the runtime")
	}
	bad := okSpec()
	bad.Name = ""
	if err := Register(rt, bad); err == nil {
		t.Fatal("invalid spec must be rejected")
	}
}

func TestPeriodicSugar(t *testing.T) {
	rt, err := converge.New(converge.Options{KV: inmem.NewKV(), Lease: inmem.NewLease()})
	if err != nil {
		t.Fatal(err)
	}
	if err := Periodic(rt, "license-refresh", Every(time.Hour), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := Periodic(rt, "nil-fn", Every(time.Hour), nil); err == nil {
		t.Fatal("nil function must be rejected")
	}
	if err := Periodic(rt, "bad-cadence", Every(0), func(ctx context.Context) error { return nil }); err == nil {
		t.Fatal("bad cadence must be rejected")
	}
}

func TestStandbyPokeSurvivesIntoLeadership(t *testing.T) {
	blockName := "converge/reconcile/job/lease"
	clock := convergetest.NewClock(wqStart)
	lease := inmem.NewLeaseWithClock(clock)
	holder, ok, err := lease.TryAcquire(context.Background(), blockName, time.Hour)
	if err != nil || !ok {
		t.Fatal("test could not pre-acquire the lease")
	}
	var mu sync.Mutex
	var got []ID
	spec := Spec{
		Name: "job",
		Reconciler: Func(func(_ context.Context, id ID) error {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, id)
			return nil
		}),
		Triggers: []Trigger{Schedule(IDs(func(context.Context) ([]ID, error) { return nil, nil }), Every(time.Hour))},
	}
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	rec := &eventRecorder{}
	deps := converge.JobDeps{
		Lease:        lease,
		KV:           inmem.NewKVWithClock(clock),
		Observer:     rec,
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.Run(ctx, deps)
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("standby never ready")
	}
	if err := e.Poke("ws_7"); err != nil {
		t.Fatal(err)
	}
	holder.Release(context.Background())
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("standby poke never ran after taking leadership")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if got[0] != "ws_7" {
		t.Fatalf("first run = %q, want the standby poke", got[0])
	}
}

func TestPausedSpecThroughRegisterDropsWakes(t *testing.T) {
	spec := specWithSchedule()
	spec.Paused = true
	le, _ := startRun(t, spec, nil)
	select {
	case <-le.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	if err := le.e.Poke(""); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return le.rec.count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardPaused
		}) >= 1
	})
}
