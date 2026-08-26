package reconcile

import (
	"context"
	"errors"
	"strings"
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
	rec   *convergetest.Recorder
	lease *inmem.Lease
	kv    converge.KV
	done  chan error
}

func startRun(t *testing.T, spec Spec, shared *liveEngine) (*liveEngine, context.CancelFunc) {
	t.Helper()
	var (
		clock *convergetest.Clock
		rec   *convergetest.Recorder
		lease *inmem.Lease
		kv    converge.KV
	)
	if shared != nil {
		clock, rec, lease = shared.clock, shared.rec, shared.lease
		kv = shared.kv
	} else {
		clock = convergetest.NewClock(wqStart)
		rec = &convergetest.Recorder{}
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

func acquired(rec *convergetest.Recorder) int {
	return rec.Count(func(e converge.Event) bool {
		lt, ok := e.(converge.LeaseTransition)
		return ok && lt.Acquired
	})
}

func released(rec *convergetest.Recorder) int {
	return rec.Count(func(e converge.Event) bool {
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
	deps := converge.JobDeps{KV: inmem.NewKV(), Clock: convergetest.NewClock(wqStart), Observer: &convergetest.Recorder{}}
	if err := e.Run(context.Background(), deps); err == nil {
		t.Fatal("OnOneReplica without a Lease must fail Run")
	}
	e2, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
	deps2 := converge.JobDeps{Lease: inmem.NewLease(), Clock: convergetest.NewClock(wqStart), Observer: &convergetest.Recorder{}}
	if err := e2.Run(context.Background(), deps2); err == nil {
		t.Fatal("Schedule without KV must fail Run")
	}
}

func TestVersionsRequireKV(t *testing.T) {
	s := Spec{
		Name:             "job",
		Reconciler:       Func(func(context.Context, ID) error { return nil }),
		AllowUnscheduled: true,
		Versions:         fakeVersions{},
	}
	e, err := newEngine(s)
	if err != nil {
		t.Fatal(err)
	}
	err = e.bind(converge.JobDeps{Lease: inmem.NewLease(), Clock: convergetest.NewClock(wqStart), Observer: &convergetest.Recorder{}})
	if err == nil || !strings.Contains(err.Error(), "Versions needs Options.KV") {
		t.Fatalf("bind without KV = %v", err)
	}
}

func TestLeaderRunsPassStandbyWaits(t *testing.T) {
	leader, _ := startRun(t, specWithSchedule(), nil)
	select {
	case <-leader.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("leader never ready")
	}
	convergetest.Await(t, func() bool { return acquired(leader.rec) == 1 })
	convergetest.Await(t, func() bool { return runCount(&testEngine{rec: leader.rec}) >= 1 })

	standbySpec := specWithSchedule()
	standby, _ := startRun(t, standbySpec, leader)
	select {
	case <-standby.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("standby must be ready without the lease")
	}
	convergetest.AssertStable(t, func() bool { return acquired(leader.rec) == 1 })
}

func TestLeaseLossStepsDownAndReelects(t *testing.T) {
	leader, _ := startRun(t, specWithSchedule(), nil)
	convergetest.Await(t, func() bool { return acquired(leader.rec) == 1 })
	leader.lease.Expire("converge/reconcile/job/lease")
	convergetest.Await(t, func() bool { return released(leader.rec) >= 1 })
	convergetest.Await(t, func() bool { return acquired(leader.rec) >= 2 })
}

func TestLeaseLossCancelsInFlightNeutrally(t *testing.T) {
	started := make(chan struct{})
	var once sync.Once
	blocked := Func(func(ctx context.Context, id ID) error {
		once.Do(func() { close(started) })
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
	convergetest.Await(t, func() bool { return released(le.rec) >= 1 })
	if n := le.rec.Count(func(e converge.Event) bool {
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
	convergetest.AssertStable(t, func() bool {
		select {
		case <-le.done:
			return false
		default:
			return true
		}
	})
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
	convergetest.Await(t, func() bool { return acquired(le.rec) == 1 })
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
	rec := &convergetest.Recorder{}
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
	convergetest.Await(t, func() bool {
		return rec.Count(func(ev converge.Event) bool {
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

func TestStandbyHintSurvivesIntoLeadership(t *testing.T) {
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
	rec := &convergetest.Recorder{}
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
	if err := e.Hint("ws_7"); err != nil {
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
			t.Fatal("standby hint never ran after taking leadership")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if got[0] != "ws_7" {
		t.Fatalf("first run = %q, want the standby hint", got[0])
	}
}

type flakyLease struct {
	mu          sync.Mutex
	acquireErrs int
	held        bool
	handle      *flakyHandle
}

type flakyHandle struct {
	l           *flakyLease
	extendErrs  int
	extendCalls int
	done        chan struct{}
}

func (l *flakyLease) TryAcquire(context.Context, string, time.Duration) (converge.LeaseHandle, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acquireErrs > 0 {
		l.acquireErrs--
		return nil, false, errors.New("flaky: acquire failed")
	}
	if l.held {
		return nil, false, nil
	}
	h := &flakyHandle{l: l, done: make(chan struct{})}
	l.held = true
	l.handle = h
	return h, true, nil
}

func (l *flakyLease) armExtendErr() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handle != nil {
		l.handle.extendErrs++
	}
}

func (l *flakyLease) currentHandle() *flakyHandle {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handle
}

func (h *flakyHandle) Extend(context.Context, time.Duration) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	h.extendCalls++
	if h.extendErrs > 0 {
		h.extendErrs--
		return errors.New("flaky: extend failed")
	}
	return nil
}

func (h *flakyHandle) extendCount() int {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	return h.extendCalls
}

func (h *flakyHandle) Release(context.Context) error {
	h.l.mu.Lock()
	defer h.l.mu.Unlock()
	if h.l.handle == h {
		h.l.held = false
		h.l.handle = nil
	}
	select {
	case <-h.done:
	default:
		close(h.done)
	}
	return nil
}

func (h *flakyHandle) Done() <-chan struct{} { return h.done }

func TestLeaseAcquireRetriesTransientErrors(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	rec := &convergetest.Recorder{}
	lease := &flakyLease{acquireErrs: 2}
	e, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
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
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, deps) }()
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	deadline := time.Now().Add(2 * time.Second)
	for acquired(rec) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("lease never acquired after transient errors")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run must not return error on transient acquire failures: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestHeartbeatExtendErrorStepsDownAndReacquires(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	rec := &convergetest.Recorder{}
	lease := &flakyLease{}
	e, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatal("never ready")
	}
	convergetest.Await(t, func() bool { return acquired(rec) == 1 })
	lease.armExtendErr()
	deadline := time.Now().Add(2 * time.Second)
	for released(rec) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("heartbeat never stepped down after an Extend error")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for acquired(rec) < 2 {
		if time.Now().After(deadline) {
			t.Fatal("engine never re-acquired after step-down")
		}
		clock.Advance(10 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func startDrainingLeader(t *testing.T) (*convergetest.Clock, *flakyLease, context.CancelFunc, chan error) {
	t.Helper()
	clock := convergetest.NewClock(wqStart)
	lease := &flakyLease{}
	started := make(chan struct{})
	var once sync.Once
	spec := specWithSchedule()
	spec.Reconciler = Func(func(ctx context.Context, id ID) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	})
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	deps := converge.JobDeps{
		Lease:        lease,
		KV:           inmem.NewKVWithClock(clock),
		Observer:     &convergetest.Recorder{},
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, deps) }()
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	return clock, lease, cancel, done
}

func awaitCleanReturn(t *testing.T, clock *convergetest.Clock, done chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("clean shutdown must return nil, got %v", err)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Run never returned after drain")
		}
		clock.Advance(5 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
}

func TestHeartbeatExtendsThroughDrain(t *testing.T) {
	clock, lease, cancel, done := startDrainingLeader(t)
	h := lease.currentHandle()
	if h == nil {
		t.Fatal("lease was never acquired")
	}
	before := h.extendCount()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for h.extendCount() <= before {
		if time.Now().After(deadline) {
			t.Fatal("heartbeat never extended the lease during drain")
		}
		clock.Advance(4 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	awaitCleanReturn(t, clock, done)
}

func bootPersistent(t *testing.T, clock *convergetest.Clock, kv converge.KV, fn Func, dla int) (*testEngine, chan error, context.CancelFunc) {
	t.Helper()
	rec := &convergetest.Recorder{}
	e := &engine{cfg: config{name: "job", concurrency: 1, deadLetterAfter: dla, allowUnscheduled: true, rec: fn}, ready: make(chan struct{})}
	deps := converge.JobDeps{
		KV:           kv,
		Lease:        inmem.NewLeaseWithClock(clock),
		Observer:     rec,
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: 30 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx, deps) }()
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("engine never ready")
	}
	return &testEngine{e: e, clock: clock, rec: rec, cancel: cancel}, done, cancel
}

func TestParkedMarksSurviveRestart(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	kv := inmem.NewKVWithClock(clock)
	te1, done1, cancel1 := bootPersistent(t, clock, kv, func(context.Context, ID) error {
		return errors.New("boom")
	}, 1)
	te1.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te1.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	convergetest.Await(t, func() bool {
		_, ok, err := kv.Get(context.Background(), parkKey(te1.e, "a"))
		return err == nil && ok
	})
	cancel1()
	awaitCleanReturn(t, clock, done1)

	var mu sync.Mutex
	runs := 0
	te2, done2, cancel2 := bootPersistent(t, clock, kv, func(context.Context, ID) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return nil
	}, 1)
	convergetest.Await(t, func() bool { return te2.e.Stats().Parked == 1 })
	te2.e.hint(context.Background(), "a")
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 0 })
	if n := te2.rec.Count(func(e converge.Event) bool {
		w, ok := e.(converge.WakeDiscarded)
		return ok && w.Reason == converge.DiscardParked
	}); n != 1 {
		t.Fatalf("restored parked ID must drop hints with DiscardParked, got %d events", n)
	}
	if res := te2.e.queue.wake("a", wakePoke); res == wakeRevived {
		te2.e.parks.clear(context.Background(), "a")
	}
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })
	convergetest.Await(t, func() bool {
		_, ok, err := kv.Get(context.Background(), parkKey(te2.e, "a"))
		return err == nil && !ok
	})
	cancel2()
	awaitCleanReturn(t, clock, done2)
}

func TestHeartbeatSurvivesTransientExtendFailureDuringDrain(t *testing.T) {
	clock, lease, cancel, done := startDrainingLeader(t)
	h := lease.currentHandle()
	if h == nil {
		t.Fatal("lease was never acquired")
	}
	before := h.extendCount()
	lease.armExtendErr()
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for h.extendCount() < before+2 {
		if time.Now().After(deadline) {
			t.Fatal("heartbeat died after a transient extend failure during drain")
		}
		clock.Advance(4 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}
	awaitCleanReturn(t, clock, done)
}
