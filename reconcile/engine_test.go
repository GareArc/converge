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
	"github.com/GareArc/converge/internal/keys"
)

func advanceUntil(t *testing.T, te *testEngine, step time.Duration, cond func() bool) {
	t.Helper()
	convergetest.AdvanceUntil(t, te.clock, step, cond)
}

func parkKey(e *engine, id ID) string {
	return keys.ReconcileParked(e.deps.Namespace, e.cfg.name, string(id))
}

type testEngine struct {
	e      *engine
	clock  *convergetest.Clock
	rec    *convergetest.Recorder
	cancel context.CancelFunc
	hctx   context.Context
	hstop  context.CancelFunc
	wg     *sync.WaitGroup
}

func startEngineKV(t *testing.T, cfg config, kv converge.KV, fn Func) *testEngine {
	t.Helper()
	clock := convergetest.NewClock(wqStart)
	rec := &convergetest.Recorder{}
	if cfg.name == "" {
		cfg.name = "job"
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 1
	}
	cfg.rec = fn
	e := &engine{cfg: cfg, ready: make(chan struct{}), paused: cfg.paused}
	deps := converge.JobDeps{
		KV:       kv,
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

func startEngine(t *testing.T, cfg config, fn Func) *testEngine {
	t.Helper()
	return startEngineKV(t, cfg, inmem.NewKV(), fn)
}

func TestSuccessRunsOnceAndStampsStats(t *testing.T) {
	var runs sync.Map
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		n, _ := runs.LoadOrStore(id, new(int))
		*(n.(*int))++
		return nil
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	if s := te.e.Stats(); s.ConsecutiveFails != 1 {
		t.Fatalf("ConsecutiveFails = %d", s.ConsecutiveFails)
	}
	advanceUntil(t, te, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	convergetest.Await(t, func() bool { return te.e.Stats().ConsecutiveFails == 0 })
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 100*time.Millisecond, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
	if s := te.e.Stats(); s.ConsecutiveFails != 0 {
		t.Fatalf("ErrOutdated must not count as failure: %+v", s)
	}
}

func TestPanicIsAFailure(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		panic("kaboom")
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			ws, ok := e.(converge.WrongSurfaceSignal)
			return ok && ws.Surface == converge.SurfaceWorker
		}) == 1
	})
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { return te.e.Stats().ConsecutiveFails >= 1 })
	advanceUntil(t, te, time.Second, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	<-started
	te.hstop()
	convergetest.Await(t, func() bool {
		q := te.e.queue
		q.mu.Lock()
		defer q.mu.Unlock()
		st := q.ids["a"]
		return st != nil && st.phase == phaseQueued && st.fails == 0
	})
	if n := te.rec.Count(func(e converge.Event) bool {
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
		te.e.hint(context.Background(), "a")
		te.e.hint(context.Background(), "b")
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	rec := &convergetest.Recorder{}
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
	e.hint(context.Background(), "x")
	convergetest.Await(t, func() bool {
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
	te.e.hint(context.Background(), "a")
	te.e.hint(context.Background(), "b")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	advanceUntil(t, te, 10*time.Minute, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
}

func TestEmptyIDHintRejectedUnlessSingle(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.hint(context.Background(), "")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardEmptyID
		}) == 1
	})
	single := startEngine(t, config{name: "single", single: true}, func(ctx context.Context, id ID) error { return nil })
	single.e.hint(context.Background(), "")
	convergetest.Await(t, func() bool {
		return single.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 1
	})
}

func TestHintWhenNotRunningIsDropped(t *testing.T) {
	e := &engine{cfg: config{name: "job"}, ready: make(chan struct{})}
	e.hint(context.Background(), "a")
}

func TestEmptyIDHintAfterTeardownStillReportsDiscard(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	rec := &convergetest.Recorder{}
	e := &engine{cfg: config{name: "job", concurrency: 1, rec: Func(func(context.Context, ID) error { return nil })}, ready: make(chan struct{})}
	if err := e.bindCore(converge.JobDeps{KV: inmem.NewKVWithClock(clock), Observer: rec, Clock: clock}); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.queue = nil
	e.mu.Unlock()
	e.hint(context.Background(), "")
	if n := rec.Count(func(ev converge.Event) bool {
		wd, ok := ev.(converge.WakeDiscarded)
		return ok && wd.Reason == converge.DiscardEmptyID
	}); n != 1 {
		t.Fatalf("empty-id hint after teardown = %d DiscardEmptyID events, want 1", n)
	}
}

func TestPokeBeforeBindFails(t *testing.T) {
	e := &engine{cfg: config{name: "job"}, ready: make(chan struct{})}
	if err := e.Poke("x"); err == nil {
		t.Fatal("poke before Run must error")
	}
}

func TestQuietUnboundEngineIsQuiet(t *testing.T) {
	e := &engine{cfg: config{name: "job"}, ready: make(chan struct{})}
	if !e.Quiet() {
		t.Fatal("unbound engine must be quiet")
	}
}

func TestQuietFalseWhileHandlerBlocks(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		close(started)
		<-release
		return nil
	})
	if !te.e.Quiet() {
		t.Fatal("engine must start quiet")
	}
	te.e.hint(context.Background(), "a")
	<-started
	if te.e.Quiet() {
		t.Fatal("must not be quiet while a handler runs")
	}
	close(release)
	convergetest.Await(t, te.e.Quiet)
}

func TestQuietTrueWithOnlyFutureBackoffItems(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		return errors.New("boom")
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { return te.e.Stats().ConsecutiveFails == 1 })
	if !te.e.Quiet() {
		t.Fatal("a future-due backoff item must be quiet")
	}
}

func TestHintBeforeBindFails(t *testing.T) {
	e := &engine{cfg: config{name: "job"}, ready: make(chan struct{})}
	if err := e.Hint("x"); err == nil {
		t.Fatal("hint before Run must error")
	}
}

func TestHintEmptyIDOnMultiIDJobFails(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	if err := te.e.Hint(""); err == nil {
		t.Fatal("empty hint on a multi-ID job must error")
	}
}

func TestHintOnSingleJobCoercesIDToEmpty(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	if err := te.e.Hint("whatever"); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == ""
		}) == 1
	})
}

func TestHintRespectsBackoffUnlikePoke(t *testing.T) {
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
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	if err := te.e.Hint("a"); err != nil {
		t.Fatal(err)
	}
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 1 })
	if err := te.e.Poke("a"); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return calls == 2 })
}

func TestHintRevivesParkedID(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	fail := true
	runs := 0
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(context.Context, ID) error {
		mu.Lock()
		defer mu.Unlock()
		runs++
		if fail {
			return errors.New("boom")
		}
		return nil
	})
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 1 })
	mu.Lock()
	fail = false
	mu.Unlock()
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := te.e.Hint("a"); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 2 })
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 0 })
}

func TestHintDoesNotPanicRacingShutdown(t *testing.T) {
	spec := Spec{
		Name:       "job",
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers: []Trigger{Schedule(StringIDs(func(context.Context) ([]string, error) {
			return []string{"x"}, nil
		}), Every(time.Hour))},
	}
	le, cancel := startRun(t, spec, nil)
	select {
	case <-le.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			le.e.Hint("a")
		}
	}()
	cancel()
	if err := waitRun(t, le); err != nil {
		t.Fatal(err)
	}
	close(stop)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("hint loop never stopped")
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
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == ""
		}) == 1
	})
	convergetest.AssertStable(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	<-started
	te.e.hint(context.Background(), "a")
	close(release)
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
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
	te.e.hint(context.Background(), "a")
	<-started
	if err := te.e.Poke("a"); err != nil {
		t.Fatal(err)
	}
	close(release)
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 2
	})
}

func TestSettleOnUnknownIDIsUnsettled(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	before := te.e.Stats()
	te.e.settle(context.Background(), "ghost", nil, 0, versionSnapshot{})
	after := te.e.Stats()
	if before != after {
		t.Fatalf("stats changed on unsettled finish: before=%+v after=%+v", before, after)
	}
	if n := te.rec.Count(func(converge.Event) bool { return true }); n != 0 {
		t.Fatalf("unsettled finish must emit no events, got %d", n)
	}
}

func TestBackoffFallbackReportsTrueTripCount(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		return CheckAgain{}
	})
	te.e.hint(context.Background(), "a")
	advanceUntil(t, te, 300*time.Millisecond, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.BackoffFallback)
			return ok
		}) >= 1
	})
	for _, e := range te.rec.Events() {
		if bf, ok := e.(converge.BackoffFallback); ok {
			if bf.Consecutive != noBackoffLimit+1 {
				t.Fatalf("BackoffFallback.Consecutive = %d, want %d", bf.Consecutive, noBackoffLimit+1)
			}
			return
		}
	}
	t.Fatal("no BackoffFallback event found")
}

func TestVersionZeroEventOnPark(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(context.Context, ID) error {
		return errors.New("boom")
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.VersionZero)
			return ok
		}) == 1
	})
}

func TestNoVersionZeroWhenProducerMarked(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	if _, err := tr.MarkChanged(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(context.Context, ID) error {
		return errors.New("boom")
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.IDParked)
			return ok
		}) == 1
	})
	convergetest.AssertStable(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.VersionZero)
			return ok
		}) == 0
	})
	raw, ok, err := kv.Get(context.Background(), parkKey(te.e, "a"))
	if err != nil || !ok || string(raw) != "1" {
		t.Fatalf("mark = %q, %v, %v; want \"1\"", raw, ok, err)
	}
}

type erroringVersions struct{}

func (erroringVersions) Latest(context.Context, ID) (Version, error) {
	return 0, errors.New("version source down")
}

func TestVersionAdvanceRevivesParked(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	fail := true
	runs := 0
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(context.Context, ID) error {
		mu.Lock()
		defer mu.Unlock()
		runs++
		if fail {
			return errors.New("boom")
		}
		return nil
	})
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 1 })
	mu.Lock()
	fail = false
	mu.Unlock()

	te.e.hint(ctx, "a")
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })

	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 2 })
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 0 })
	convergetest.Await(t, func() bool {
		_, ok, err := kv.Get(ctx, parkKey(te.e, "a"))
		return err == nil && !ok
	})
}

func TestVersionSourceErrorKeepsParked(t *testing.T) {
	kv := inmem.NewKV()
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: erroringVersions{}}, kv, func(context.Context, ID) error {
		return errors.New("boom")
	})
	ctx := context.Background()
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 1 })
	te.e.hint(ctx, "a")
	convergetest.AssertStable(t, func() bool { return te.e.Stats().Parked == 1 })
	if n := te.rec.Count(func(e converge.Event) bool {
		w, ok := e.(converge.WakeDiscarded)
		return ok && w.Reason == converge.DiscardParked
	}); n == 0 {
		t.Fatal("undecidable revival must still event the dropped hint")
	}
}

func TestMidRunVersionBumpStillRevives(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	runs := 0
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(hctx context.Context, id ID) error {
		mu.Lock()
		defer mu.Unlock()
		runs++
		if runs == 1 {
			if _, err := tr.MarkChanged(hctx, id); err != nil {
				return err
			}
			return errors.New("boom")
		}
		return nil
	})
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 2 })
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 0 })
	convergetest.Await(t, func() bool {
		_, ok, err := kv.Get(ctx, parkKey(te.e, "a"))
		return err == nil && !ok
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.IDParked)
		return ok
	}); n != 1 {
		t.Fatalf("park-then-revive must still event the park: got %d IDParked", n)
	}
}

func TestInvalidMarkBlocksVersionRevival(t *testing.T) {
	kv := inmem.NewKV()
	tr := NewTracker(kv, "job")
	ctx := context.Background()
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	runs := 0
	te := startEngineKV(t, config{deadLetterAfter: 1, versions: tr}, kv, func(context.Context, ID) error {
		mu.Lock()
		runs++
		mu.Unlock()
		return errors.New("boom")
	})
	te.e.hint(ctx, "a")
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 1 })
	convergetest.Await(t, func() bool {
		_, ok, err := kv.Get(ctx, parkKey(te.e, "a"))
		return err == nil && ok
	})
	if _, err := tr.MarkChanged(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if err := kv.Delete(ctx, parkKey(te.e, "a")); err != nil {
		t.Fatal(err)
	}
	te.e.hint(ctx, "a")
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })
	if err := kv.Set(ctx, parkKey(te.e, "a"), []byte("junk"), 0); err != nil {
		t.Fatal(err)
	}
	te.e.hint(ctx, "a")
	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs == 1 })
	convergetest.AssertStable(t, func() bool { return te.e.Stats().Parked == 1 })
}

func TestActivationClearsMarkForActiveID(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	kv := inmem.NewKVWithClock(clock)
	e := &engine{cfg: config{name: "job", concurrency: 1, rec: Func(func(context.Context, ID) error { return nil })}, ready: make(chan struct{})}
	if err := e.bindCore(converge.JobDeps{KV: kv, Observer: &convergetest.Recorder{}, Clock: clock}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := kv.Set(ctx, parkKey(e, "a"), []byte("0"), 0); err != nil {
		t.Fatal(err)
	}
	if err := e.Poke("a"); err != nil {
		t.Fatal(err)
	}
	e.loadParked(ctx)
	if _, ok, err := kv.Get(ctx, parkKey(e, "a")); err != nil || ok {
		t.Fatalf("mark for an already-queued id must be cleared at activation: ok=%v err=%v", ok, err)
	}
	if c := e.queue.counts(); c.parked != 0 || c.depth != 1 {
		t.Fatalf("queued id must stay queued, not re-park: %+v", c)
	}
}

func TestOnAllReplicasParksInMemoryOnly(t *testing.T) {
	te := startEngine(t, config{runMode: converge.OnAllReplicas}, func(context.Context, ID) error {
		return fakeWorkerSignal{}
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool { return te.e.Stats().Parked == 1 })
	keys, _, err := te.e.deps.KV.Scan(context.Background(), parkKey(te.e, ""), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("OnAllReplicas must not write shared park marks: %v", keys)
	}
}

func TestSetPausedDropsHintWakes(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.SetPaused(true)
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.ID == "a" && wd.Reason == converge.DiscardPaused
		}) == 1
	})
	if n := te.rec.Count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	}); n != 0 {
		t.Fatalf("paused engine must not run, got %d RunCompleted", n)
	}
}

func TestSetPausedDropsPokes(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.SetPaused(true)
	if err := te.e.Poke("a"); err != nil {
		t.Fatal(err)
	}
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.ID == "a" && wd.Reason == converge.DiscardPaused
		}) == 1
	})
}

func TestSetPausedResumeAllowsHintsAgain(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.SetPaused(true)
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.Reason == converge.DiscardPaused
		}) == 1
	})
	te.e.SetPaused(false)
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 1
	})
}

func TestSetPausedSameValueIsNoOp(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	te.e.SetPaused(false)
	if te.e.Info().Paused {
		t.Fatal("same-value SetPaused(false) must not pause an unpaused engine")
	}
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			_, ok := e.(converge.RunCompleted)
			return ok
		}) == 1
	})
	te.e.SetPaused(true)
	te.e.SetPaused(true)
	if !te.e.Info().Paused {
		t.Fatal("engine must be paused")
	}
	te.e.hint(context.Background(), "b")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.ID == "b" && wd.Reason == converge.DiscardPaused
		}) == 1
	})
}

func TestInfoPausedReflectsLiveState(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	if te.e.Info().Paused {
		t.Fatal("Info().Paused must start false for an unpaused config")
	}
	te.e.SetPaused(true)
	if !te.e.Info().Paused {
		t.Fatal("Info().Paused must reflect a live SetPaused(true)")
	}
	te.e.SetPaused(false)
	if te.e.Info().Paused {
		t.Fatal("Info().Paused must reflect a live SetPaused(false)")
	}
}

func TestSetPausedStormKeepsGatesConverged(t *testing.T) {
	const iterations = 20000
	const goroutinesPerSide = 4
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })

	var wg sync.WaitGroup
	for g := 0; g < goroutinesPerSide; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				te.e.SetPaused(true)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				te.e.SetPaused(false)
			}
		}()
	}
	wg.Wait()

	te.e.mu.Lock()
	enginePaused := te.e.paused
	te.e.mu.Unlock()
	queuePaused := te.e.queue.paused()
	if enginePaused != queuePaused {
		t.Fatalf("pause gates diverged after storm: engine.paused=%v queue.paused()=%v", enginePaused, queuePaused)
	}

	te.e.SetPaused(false)
	te.e.hint(context.Background(), "storm-flow")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "storm-flow"
		}) == 1
	})

	te.e.SetPaused(true)
	te.e.hint(context.Background(), "storm-drop")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.ID == "storm-drop" && wd.Reason == converge.DiscardPaused
		}) == 1
	})
	if !te.e.Info().Paused {
		t.Fatal("Info().Paused must be true after the final SetPaused(true)")
	}
}

func TestPausedHoldsAlreadyQueuedBacklogNotDropped(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	ranB := false
	te := startEngine(t, config{concurrency: 1}, func(ctx context.Context, id ID) error {
		if id == "a" {
			once.Do(func() { close(started) })
			<-release
			return nil
		}
		mu.Lock()
		ranB = true
		mu.Unlock()
		return nil
	})
	te.e.hint(context.Background(), "a")
	<-started
	te.e.hint(context.Background(), "b")
	convergetest.Await(t, func() bool { return te.e.queue.counts().depth == 1 })

	te.e.SetPaused(true)
	close(release)
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "a"
		}) == 1
	})

	convergetest.AssertStable(t, func() bool { mu.Lock(); defer mu.Unlock(); return !ranB })
	if n := te.rec.Count(func(e converge.Event) bool {
		wd, ok := e.(converge.WakeDiscarded)
		return ok && wd.ID == "b"
	}); n != 0 {
		t.Fatalf("already-queued backlog must be held, not dropped: got %d WakeDiscarded for b", n)
	}

	te.e.SetPaused(false)
	convergetest.Await(t, func() bool { mu.Lock(); defer mu.Unlock(); return ranB })
}

func TestQuietTrueWhilePausedWithHeldDueBacklog(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	te := startEngine(t, config{concurrency: 1}, func(ctx context.Context, id ID) error {
		if id == "a" {
			once.Do(func() { close(started) })
			<-release
		}
		return nil
	})
	te.e.hint(context.Background(), "a")
	<-started
	te.e.hint(context.Background(), "b")
	convergetest.Await(t, func() bool { return te.e.queue.counts().depth == 1 })

	te.e.SetPaused(true)
	close(release)
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.ID == "a"
		}) == 1
	})

	convergetest.Await(t, te.e.Quiet)
	convergetest.AssertStable(t, te.e.Quiet)
}
