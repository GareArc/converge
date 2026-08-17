package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

type eventRecorder struct {
	mu     sync.Mutex
	events []converge.Event
}

func (r *eventRecorder) Observe(e converge.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *eventRecorder) count(match func(converge.Event) bool) int {
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

func await(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func advanceUntil(t *testing.T, te *testEngine, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never became true while advancing")
		}
		te.clock.Advance(step)
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

type testEngine struct {
	e      *engine
	clock  *convergetest.Clock
	rec    *eventRecorder
	cancel context.CancelFunc
	hctx   context.Context
	hstop  context.CancelFunc
	wg     *sync.WaitGroup
}

func startEngine(t *testing.T, cfg config, fn Func) *testEngine {
	t.Helper()
	clock := convergetest.NewClock(wqStart)
	rec := &eventRecorder{}
	if cfg.name == "" {
		cfg.name = "job"
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 1
	}
	cfg.rec = fn
	e := &engine{cfg: cfg, ready: make(chan struct{})}
	deps := converge.JobDeps{
		KV:       inmem.NewKVWithClock(clock),
		Observer: rec,
		Clock:    clock,
	}
	if err := e.bindCore(deps); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	hctx, hstop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go e.dispatch(ctx, hctx, &wg)
	t.Cleanup(func() {
		cancel()
		hstop()
	})
	return &testEngine{e: e, clock: clock, rec: rec, cancel: cancel, hctx: hctx, hstop: hstop, wg: &wg}
}

func TestSuccessRunsOnceAndStampsStats(t *testing.T) {
	var runs sync.Map
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		n, _ := runs.LoadOrStore(id, new(int))
		*(n.(*int))++
		return nil
	})
	te.e.hint("a")
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 1
	})
	s := te.e.Stats()
	if s.Job != "job" || s.Surface != converge.SurfaceReconcile {
		t.Fatalf("stats identity = %+v", s)
	}
	if s.LastSuccess.IsZero() || s.ConsecutiveFails != 0 {
		t.Fatalf("stats after success = %+v", s)
	}
}

func TestFailureBacksOffThenRecovers(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return errors.New("boom")
		}
		return nil
	})
	te.e.hint("a")
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	if s := te.e.Stats(); s.ConsecutiveFails != 1 {
		t.Fatalf("ConsecutiveFails = %d", s.ConsecutiveFails)
	}
	advanceUntil(t, te, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	await(t, func() bool { return te.e.Stats().ConsecutiveFails == 0 })
}

func TestCheckAgainSchedulesRevisitWithoutFailure(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return CheckAgain{In: 10 * time.Second}
		}
		return nil
	})
	te.e.hint("a")
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	if s := te.e.Stats(); s.ConsecutiveFails != 0 || s.LastSuccess.IsZero() {
		t.Fatalf("CheckAgain must not count as failure: %+v", s)
	}
	advanceUntil(t, te, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
}

func TestErrOutdatedRequeuesImmediately(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return fmt.Errorf("apply: %w", ErrOutdated)
		}
		return nil
	})
	te.e.hint("a")
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 100*time.Millisecond, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	if s := te.e.Stats(); s.ConsecutiveFails != 0 {
		t.Fatalf("ErrOutdated must not count as failure: %+v", s)
	}
}

func TestPanicIsAFailure(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		panic("kaboom")
	})
	te.e.hint("a")
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Err != nil
		}) == 1
	})
	if s := te.e.Stats(); s.ConsecutiveFails != 1 {
		t.Fatalf("panic must count as failure: %+v", s)
	}
}

type fakeWorkerSignal struct{}

func (fakeWorkerSignal) Error() string                    { return "worker signal" }
func (fakeWorkerSignal) ControlSurface() converge.Surface { return converge.SurfaceWorker }

func TestForeignSignalParksImmediately(t *testing.T) {
	te := startEngine(t, config{deadLetterAfter: 100}, func(ctx context.Context, id ID) error {
		return fakeWorkerSignal{}
	})
	te.e.hint("a")
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			ws, ok := e.(converge.WrongSurfaceSignal)
			return ok && ws.Surface == converge.SurfaceWorker
		}) == 1
	})
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	if s := te.e.Stats(); s.Parked != 1 {
		t.Fatalf("foreign signal must park: %+v", s)
	}
}

func TestDeadLetterAfterParksAndEvents(t *testing.T) {
	te := startEngine(t, config{deadLetterAfter: 2}, func(ctx context.Context, id ID) error {
		return errors.New("always")
	})
	te.e.hint("a")
	await(t, func() bool { return te.e.Stats().ConsecutiveFails >= 1 })
	advanceUntil(t, te, time.Second, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			dl, ok := e.(converge.IDParked)
			return ok && dl.ID == "a" && dl.Failures == 2
		}) == 1
	})
	if s := te.e.Stats(); s.Parked != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestNeutralOnCanceledContext(t *testing.T) {
	started := make(chan struct{})
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	te.e.hint("a")
	<-started
	te.hstop()
	await(t, func() bool {
		q := te.e.queue
		q.mu.Lock()
		defer q.mu.Unlock()
		st := q.ids["a"]
		return st != nil && st.phase == phaseQueued && st.fails == 0
	})
	if n := te.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("neutral run must not emit RunCompleted, got %d", n)
	}
}

func TestSingleFlightPerID(t *testing.T) {
	var mu sync.Mutex
	inflight := map[ID]int{}
	maxSeen := 0
	te := startEngine(t, config{concurrency: 8}, func(ctx context.Context, id ID) error {
		mu.Lock()
		inflight[id]++
		if inflight[id] > maxSeen {
			maxSeen = inflight[id]
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		inflight[id]--
		mu.Unlock()
		return nil
	})
	for i := 0; i < 50; i++ {
		te.e.hint("a")
		te.e.hint("b")
	}
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) >= 2
	})
	mu.Lock()
	defer mu.Unlock()
	if maxSeen > 1 {
		t.Fatalf("same ID ran %d times concurrently", maxSeen)
	}
}

func TestMiddlewareWrapsEveryRunOutermostFirst(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mkmw := func(tag string) converge.Middleware {
		return func(next converge.Handler) converge.Handler {
			return func(ctx context.Context, run converge.Run) error {
				mu.Lock()
				order = append(order, tag+":"+run.ID)
				mu.Unlock()
				return next(ctx, run)
			}
		}
	}
	clock := convergetest.NewClock(wqStart)
	rec := &eventRecorder{}
	e := &engine{cfg: config{name: "job", concurrency: 1, middleware: []converge.Middleware{mkmw("local")}, rec: Func(func(ctx context.Context, id ID) error { return nil })}, ready: make(chan struct{})}
	deps := converge.JobDeps{
		KV:         inmem.NewKVWithClock(clock),
		Observer:   rec,
		Clock:      clock,
		Middleware: []converge.Middleware{mkmw("global")},
	}
	if err := e.bindCore(deps); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	go e.dispatch(ctx, ctx, &wg)
	e.hint("x")
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 2
	})
	mu.Lock()
	defer mu.Unlock()
	if order[0] != "global:x" || order[1] != "local:x" {
		t.Fatalf("middleware order = %v", order)
	}
}

func TestRateLimitThrottlesRuns(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{concurrency: 4, rateLimit: converge.Rate{Events: 1, Per: time.Hour}}, func(ctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return nil
	})
	te.e.hint("a")
	te.e.hint("b")
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	assertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 10*time.Minute, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
}

func TestEmptyIDHintRejectedUnlessSingle(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.hint("")
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardEmptyID
		}) == 1
	})
	single := startEngine(t, config{name: "single", single: true}, func(ctx context.Context, id ID) error { return nil })
	single.e.hint("")
	await(t, func() bool {
		return single.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 1
	})
}

func TestPokeBeforeBindFails(t *testing.T) {
	e := &engine{cfg: config{name: "job"}, ready: make(chan struct{})}
	if err := e.Poke("x"); err == nil {
		t.Fatal("poke before Run must error")
	}
}

func TestPokeEmptyIDOnMultiIDJobFails(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	if err := te.e.Poke(""); err == nil {
		t.Fatal("empty poke on a multi-ID job must error")
	}
	if err := te.e.Poke("a"); err != nil {
		t.Fatal(err)
	}
}

func TestPokeOnSingleJobCoercesIDToEmpty(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	if err := te.e.Poke("whatever"); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == ""
		}) == 1
	})
	assertStable(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "whatever"
		}) == 0
	})
}

func TestKeyNamespacing(t *testing.T) {
	e := &engine{cfg: config{name: "app-runner"}}
	e.deps = converge.JobDeps{Namespace: "acme"}
	if got := e.key("lease"); got != "acme/converge/reconcile/app-runner/lease" {
		t.Fatalf("key = %q", got)
	}
	e.deps = converge.JobDeps{}
	if got := e.key("sched", "0", "last"); got != "converge/reconcile/app-runner/sched/0/last" {
		t.Fatalf("key = %q", got)
	}
}

func TestDroppedHintOnForcedParkEmitsDiscard(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	te := startEngine(t, config{deadLetterAfter: 1}, func(ctx context.Context, id ID) error {
		close(started)
		<-release
		return errors.New("boom")
	})
	te.e.hint("a")
	<-started
	te.e.hint("a")
	close(release)
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	if n := te.rec.count(func(e converge.Event) bool {
		wd, ok := e.(converge.WakeDiscarded)
		return ok && wd.Reason == converge.DiscardParked
	}); n != 1 {
		t.Fatalf("droppedHint WakeDiscarded count = %d, want 1", n)
	}
}

func TestRevivedPokeDuringParkingRunsAgain(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	calls := 0
	te := startEngine(t, config{deadLetterAfter: 1}, func(ctx context.Context, id ID) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			close(started)
			<-release
			return errors.New("boom")
		}
		return nil
	})
	te.e.hint("a")
	<-started
	if err := te.e.Poke("a"); err != nil {
		t.Fatal(err)
	}
	close(release)
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 2
	})
}

func TestSettleOnUnknownIDIsUnsettled(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	before := te.e.Stats()
	te.e.settle(context.Background(), "ghost", nil, 0)
	after := te.e.Stats()
	if before != after {
		t.Fatalf("stats changed on unsettled finish: before=%+v after=%+v", before, after)
	}
	if n := te.rec.count(func(converge.Event) bool { return true }); n != 0 {
		t.Fatalf("unsettled finish must emit no events, got %d", n)
	}
}

func TestBackoffFallbackReportsTrueTripCount(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		return CheckAgain{}
	})
	te.e.hint("a")
	advanceUntil(t, te, 300*time.Millisecond, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.BackoffFallback)
			return ok
		}) >= 1
	})
	te.rec.mu.Lock()
	defer te.rec.mu.Unlock()
	for _, e := range te.rec.events {
		if bf, ok := e.(converge.BackoffFallback); ok {
			if bf.Consecutive != noBackoffLimit+1 {
				t.Fatalf("BackoffFallback.Consecutive = %d, want %d", bf.Consecutive, noBackoffLimit+1)
			}
			return
		}
	}
	t.Fatal("no BackoffFallback event found")
}
