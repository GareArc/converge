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
	"github.com/GareArc/converge/internal/backoff"
)

func advanceUntil(t *testing.T, te *testEngine, step time.Duration, cond func() bool) {
	t.Helper()
	convergetest.AdvanceUntil(t, te.clock, step, cond)
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

func startEngineKV(t *testing.T, cfg config, kv converge.KV, fn func(ctx context.Context, id ID) error) *testEngine {
	t.Helper()
	clock := convergetest.NewClock(wqStart)
	rec := &convergetest.Recorder{}
	if cfg.name == "" {
		cfg.name = "job"
	}
	if cfg.concurrency == 0 {
		cfg.concurrency = 1
	}
	cfg.fn = fn
	e := &engine{cfg: cfg, ready: make(chan struct{})}
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

func startEngine(t *testing.T, cfg config, fn func(ctx context.Context, id ID) error) *testEngine {
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

func TestForeignSignalIsAPlainFailure(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error {
		return fakeWorkerSignal{}
	})
	te.e.hint(context.Background(), "a")
	convergetest.Await(t, func() bool {
		return te.rec.Count(func(e converge.Event) bool {
			rc, ok := e.(converge.RunCompleted)
			return ok && rc.Err != nil
		}) == 1
	})
	if s := te.e.Stats(); s.ConsecutiveFails != 1 {
		t.Fatalf("a foreign signal must count as an ordinary failure: %+v", s)
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
	e := &engine{cfg: config{name: "job", concurrency: 1, middleware: []converge.Middleware{mkmw("local")}, fn: func(ctx context.Context, id ID) error { return nil }}, ready: make(chan struct{})}
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
	e := &engine{cfg: config{name: "job", concurrency: 1, fn: func(context.Context, ID) error { return nil }}, ready: make(chan struct{})}
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

func TestHintRespectsBackoff(t *testing.T) {
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
}

func TestHintDoesNotPanicRacingShutdown(t *testing.T) {
	spec := Spec{
		Name:      "job",
		Reconcile: func(context.Context, ID) error { return nil },
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
			if bf.Consecutive != backoff.NoBackoffCap+1 {
				t.Fatalf("BackoffFallback.Consecutive = %d, want %d", bf.Consecutive, backoff.NoBackoffCap+1)
			}
			return
		}
	}
	t.Fatal("no BackoffFallback event found")
}
