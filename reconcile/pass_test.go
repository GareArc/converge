package reconcile

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/inmem"
)

func startSchedule(t *testing.T, te *testEngine, src IDSource, cad Cadence) {
	t.Helper()
	st := Schedule(src, cad).(*scheduleTrigger)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go te.e.runSchedule(ctx, 0, st)
}

func runCount(te *testEngine) int {
	return te.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.RunCompleted)
		return ok
	})
}

func TestFirstPassRunsImmediatelyAndPersistsLastFire(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, StringIDs(func(context.Context) ([]string, error) {
		return []string{"a", "b"}, nil
	}), Every(time.Hour))
	await(t, func() bool { return runCount(te) == 2 })
	key := te.e.key("sched", "0", "last")
	_, ok, err := te.e.deps.KV.Get(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("last-fire not persisted: %v %v", ok, err)
	}
}

func TestSteadyStateFiresOncePerPeriod(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, SingleID(), Every(time.Hour))
	await(t, func() bool { return runCount(te) == 1 })
	advanceUntil(t, te, time.Minute, func() bool { return runCount(te) == 2 })
	for i := 0; i < 10; i++ {
		te.clock.Advance(time.Minute)
	}
	assertStable(t, func() bool { return runCount(te) == 2 })
	advanceUntil(t, te, time.Minute, func() bool { return runCount(te) == 3 })
}

func seedLastFire(t *testing.T, te *testEngine, at time.Time) {
	t.Helper()
	key := te.e.key("sched", "0", "last")
	if err := te.e.deps.KV.Set(context.Background(), key, []byte(at.Format(time.RFC3339Nano)), 0); err != nil {
		t.Fatal(err)
	}
}

func TestMissedTickRunOnceRunsOneMakeupPass(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	seedLastFire(t, te, wqStart.Add(-5*time.Hour))
	startSchedule(t, te, SingleID(), Every(time.Hour))
	await(t, func() bool { return runCount(te) == 1 })
	assertStable(t, func() bool { return runCount(te) == 1 })
	advanceUntil(t, te, time.Minute, func() bool { return runCount(te) == 2 })
}

func TestMissedTickSkipSkipsAll(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	seedLastFire(t, te, wqStart.Add(-5*time.Hour))
	startSchedule(t, te, SingleID(), Cron("0 * * * *", CronOpts{MissedTick: Skip}))
	assertStable(t, func() bool { return runCount(te) == 0 })
	advanceUntil(t, te, time.Minute, func() bool { return runCount(te) == 1 })
}

func TestMissedTickCatchupReplaysEachBoundary(t *testing.T) {
	var mu sync.Mutex
	pageCalls := 0
	src := IDs(func(context.Context) ([]ID, error) {
		mu.Lock()
		pageCalls++
		mu.Unlock()
		return []ID{""}, nil
	})
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	seedLastFire(t, te, wqStart.Add(-3*time.Hour))
	startSchedule(t, te, src, Cron("0 * * * *", CronOpts{MissedTick: Catchup}))
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return pageCalls == 3
	})
	if got := runCount(te); got < 1 {
		t.Fatalf("expected at least one run from the replayed boundaries, got %d", got)
	}
}

func TestRunOnceBacklogBeyondCapIsOneMakeupPass(t *testing.T) {
	var mu sync.Mutex
	pageCalls := 0
	src := IDs(func(context.Context) ([]ID, error) {
		mu.Lock()
		pageCalls++
		mu.Unlock()
		return []ID{""}, nil
	})
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	seedLastFire(t, te, wqStart.Add(-2500*time.Second))
	startSchedule(t, te, src, Every(time.Second))
	calls := func() int { mu.Lock(); defer mu.Unlock(); return pageCalls }
	await(t, func() bool { return calls() == 1 })
	assertStable(t, func() bool { return calls() == 1 })
	if n := te.rec.count(func(e converge.Event) bool {
		_, ok := e.(converge.PassOverrun)
		return ok
	}); n != 0 {
		t.Fatalf("missed boundaries must not be reported as overruns: got %d PassOverrun events", n)
	}
	advanceUntil(t, te, time.Second, func() bool { return calls() == 2 })
}

func TestOverrunBeyondCapIsConsumedNotMadeUp(t *testing.T) {
	var mu sync.Mutex
	pageCalls := 0
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	src := IDs(func(context.Context) ([]ID, error) {
		mu.Lock()
		pageCalls++
		n := pageCalls
		mu.Unlock()
		if n == 1 {
			te.clock.Advance(2500 * time.Second)
		}
		return []ID{""}, nil
	})
	startSchedule(t, te, src, Every(time.Second))
	calls := func() int { mu.Lock(); defer mu.Unlock(); return pageCalls }
	await(t, func() bool { return calls() == 1 })
	assertStable(t, func() bool { return calls() == 1 })
	advanceUntil(t, te, time.Second, func() bool { return calls() == 2 })
}

func TestLatestBoundary(t *testing.T) {
	anchor := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	if got := latestBoundary(Every(time.Second), anchor, anchor.Add(2500*time.Second+300*time.Millisecond)); !got.Equal(anchor.Add(2500 * time.Second)) {
		t.Fatalf("Every: got %v", got)
	}
	hourly := Cron("0 * * * *", CronOpts{})
	if got := latestBoundary(hourly, anchor, anchor.Add(3*time.Hour+30*time.Minute)); !got.Equal(anchor.Add(3 * time.Hour)) {
		t.Fatalf("Cron: got %v", got)
	}
	if got := latestBoundary(hourly, anchor.Add(time.Hour), anchor); !got.Equal(anchor.Add(time.Hour)) {
		t.Fatalf("future anchor must be returned unchanged: got %v", got)
	}
}

func TestAllReplicasScheduleIsReplicaLocal(t *testing.T) {
	clock := convergetest.NewClock(wqStart)
	kv := inmem.NewKVWithClock(clock)
	var mu sync.Mutex
	calls := map[int]int{}
	boot := func(replica int) *engine {
		e := &engine{cfg: config{
			name:        "job",
			concurrency: 1,
			single:      true,
			runMode:     converge.OnAllReplicas,
			rec:         Func(func(ctx context.Context, id ID) error { return nil }),
		}, ready: make(chan struct{})}
		if err := e.bindCore(converge.JobDeps{KV: kv, Observer: &eventRecorder{}, Clock: clock}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		var wg sync.WaitGroup
		go e.dispatch(ctx, ctx, &wg)
		src := IDs(func(context.Context) ([]ID, error) {
			mu.Lock()
			calls[replica]++
			mu.Unlock()
			return []ID{""}, nil
		})
		st := Schedule(src, Every(time.Hour)).(*scheduleTrigger)
		go e.runSchedule(ctx, 0, st)
		return e
	}
	e1 := boot(1)
	boot(2)
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls[1] == 1 && calls[2] == 1
	})
	if _, ok, err := kv.Get(context.Background(), e1.key("sched", "0", "last")); err != nil || ok {
		t.Fatalf("OnAllReplicas must keep schedule state replica-local: ok=%v err=%v", ok, err)
	}
}

func TestPassResumesFromPersistedCursor(t *testing.T) {
	var mu sync.Mutex
	var pages []string
	failOnce := true
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	src := IDsByPage(func(_ context.Context, cursor string) ([]ID, string, error) {
		mu.Lock()
		defer mu.Unlock()
		pages = append(pages, cursor)
		switch cursor {
		case "":
			return []ID{"p1"}, "c1", nil
		case "c1":
			if failOnce {
				failOnce = false
				return nil, "", errors.New("db hiccup")
			}
			return []ID{"p2"}, "", nil
		}
		return nil, "", nil
	})
	startSchedule(t, te, src, Every(time.Hour))
	await(t, func() bool { return runCount(te) == 1 })
	advanceUntil(t, te, time.Second, func() bool { return runCount(te) == 2 })
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(pages); i++ {
		if pages[i] == "" {
			t.Fatalf("pass restarted from scratch after page error: %v", pages)
		}
	}
}

func TestFirstPassOverrunSkipsWithoutMakeup(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	slow := IDs(func(ctx context.Context) ([]ID, error) {
		te.clock.Advance(150 * time.Minute)
		return []ID{"a"}, nil
	})
	startSchedule(t, te, slow, Every(time.Hour))
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			_, ok := e.(converge.PassOverrun)
			return ok
		}) >= 1
	})
	assertStable(t, func() bool { return runCount(te) == 1 })
}

func TestErroredCadenceDoesNotPanic(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, SingleID(), Every(0))
	assertStable(t, func() bool { return runCount(te) == 0 })
}

func TestCursorClearedAfterPassCompletes(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	src := IDsByPage(func(_ context.Context, cursor string) ([]ID, string, error) {
		if cursor == "" {
			return []ID{"p1"}, "c1", nil
		}
		return []ID{"p2"}, "", nil
	})
	startSchedule(t, te, src, Every(time.Hour))
	await(t, func() bool { return runCount(te) == 2 })
	await(t, func() bool {
		_, ok, err := te.e.deps.KV.Get(context.Background(), te.e.key("sched", "0", "cursor"))
		return err == nil && !ok
	})
}

func TestScheduleBoundarySkippedWhilePausedLastFireUntouched(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, SingleID(), Every(time.Hour))
	await(t, func() bool { return runCount(te) == 1 })
	lastKey := te.e.key("sched", "0", "last")
	before, ok, err := te.e.deps.KV.Get(context.Background(), lastKey)
	if err != nil || !ok {
		t.Fatal("last-fire not persisted")
	}

	te.e.SetPaused(true)
	te.clock.Advance(3 * time.Hour)
	assertStable(t, func() bool { return runCount(te) == 1 })

	after, ok, err := te.e.deps.KV.Get(context.Background(), lastKey)
	if err != nil || !ok || string(after) != string(before) {
		t.Fatalf("last-fire changed while paused: before=%q after=%q", before, after)
	}
}

func TestResumeAfterMissedBoundaryRunsOneCatchupPass(t *testing.T) {
	te := startEngine(t, config{single: true}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, SingleID(), Every(time.Hour))
	await(t, func() bool { return runCount(te) == 1 })

	te.e.SetPaused(true)
	te.clock.Advance(3 * time.Hour)
	assertStable(t, func() bool { return runCount(te) == 1 })

	te.e.SetPaused(false)
	await(t, func() bool { return runCount(te) == 2 })
	assertStable(t, func() bool { return runCount(te) == 2 })
}

func TestPauseMidPassCompletesThenSkipsNextBoundary(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	src := IDs(func(context.Context) ([]ID, error) {
		once.Do(func() { close(entered) })
		<-release
		return []ID{"a"}, nil
	})
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	startSchedule(t, te, src, Every(time.Hour))
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("pass never entered the slow source")
	}

	te.e.SetPaused(true)
	close(release)

	lastKey := te.e.key("sched", "0", "last")
	await(t, func() bool {
		_, ok, err := te.e.deps.KV.Get(context.Background(), lastKey)
		return err == nil && ok
	})
	await(t, func() bool {
		return te.rec.count(func(e converge.Event) bool {
			wd, ok := e.(converge.WakeDiscarded)
			return ok && wd.ID == "a" && wd.Reason == converge.DiscardPaused
		}) == 1
	})
	assertStable(t, func() bool { return runCount(te) == 0 })

	before, _, err := te.e.deps.KV.Get(context.Background(), lastKey)
	if err != nil {
		t.Fatal(err)
	}
	te.clock.Advance(3 * time.Hour)
	assertStable(t, func() bool { return runCount(te) == 0 })

	after, ok, err := te.e.deps.KV.Get(context.Background(), lastKey)
	if err != nil || !ok || string(after) != string(before) {
		t.Fatalf("next boundary must be skipped while paused: before=%q after=%q", before, after)
	}
}

func TestQuietFalseWhilePassEnumerates(t *testing.T) {
	te := startEngine(t, config{}, func(ctx context.Context, id ID) error { return nil })
	entered := make(chan struct{})
	release := make(chan struct{})
	src := IDs(func(context.Context) ([]ID, error) {
		close(entered)
		<-release
		return []ID{"a"}, nil
	})
	startSchedule(t, te, src, Every(time.Hour))
	<-entered
	if te.e.Quiet() {
		t.Fatal("must not be quiet while a pass enumerates")
	}
	close(release)
	await(t, te.e.Quiet)
}

func TestRunPassNowInactiveErrors(t *testing.T) {
	e, err := newEngine(specWithSchedule())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RunPassNow(context.Background()); err == nil {
		t.Fatal("run-pass-now before Run must error")
	}
}

func TestRunPassNowWithoutScheduleTriggerErrors(t *testing.T) {
	spec := Spec{
		Name:             "job",
		Reconciler:       Func(func(context.Context, ID) error { return nil }),
		AllowUnscheduled: true,
		RunMode:          converge.OnAllReplicas,
	}
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	clock := convergetest.NewClock(wqStart)
	deps := converge.JobDeps{
		Observer:     &eventRecorder{},
		Clock:        clock,
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
	if err := e.RunPassNow(context.Background()); err == nil {
		t.Fatal("run-pass-now without a Schedule trigger must error")
	}
}

func TestRunPassNowEnumeratesFullIDSource(t *testing.T) {
	var mu sync.Mutex
	counts := map[ID]int{}
	spec := Spec{
		Name: "job",
		Reconciler: Func(func(_ context.Context, id ID) error {
			mu.Lock()
			counts[id]++
			mu.Unlock()
			return nil
		}),
		Triggers: []Trigger{Schedule(StringIDs(func(context.Context) ([]string, error) {
			return []string{"a", "b", "c"}, nil
		}), Every(time.Hour))},
	}
	le, _ := startRun(t, spec, nil)
	select {
	case <-le.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) == 3
	})
	if err := le.e.RunPassNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range []ID{"a", "b", "c"} {
			if counts[id] < 2 {
				return false
			}
		}
		return true
	})
}

func TestRunPassNowDoesNotDisturbScheduledLastFireOrCursor(t *testing.T) {
	var mu sync.Mutex
	scheduledRuns := 0
	spec := Spec{
		Name: "job",
		Reconciler: Func(func(context.Context, ID) error {
			mu.Lock()
			scheduledRuns++
			mu.Unlock()
			return nil
		}),
		Triggers: []Trigger{Schedule(SingleID(), Every(time.Hour))},
	}
	le, _ := startRun(t, spec, nil)
	select {
	case <-le.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return scheduledRuns == 1 })

	lastKey := le.e.key("sched", "0", "last")
	rawBefore, ok, err := le.kv.Get(context.Background(), lastKey)
	if err != nil || !ok {
		t.Fatal("scheduled last-fire not persisted")
	}

	if err := le.e.RunPassNow(context.Background()); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { mu.Lock(); defer mu.Unlock(); return scheduledRuns == 2 })

	rawAfter, ok, err := le.kv.Get(context.Background(), lastKey)
	if err != nil || !ok || string(rawAfter) != string(rawBefore) {
		t.Fatalf("ops pass must not touch scheduled last-fire: before=%q after=%q", rawBefore, rawAfter)
	}
	if _, ok, _ := le.kv.Get(context.Background(), le.e.key("sched", "0", "cursor")); ok {
		t.Fatal("scheduled cursor must not persist after completion")
	}
	if _, ok, _ := le.kv.Get(context.Background(), le.e.key("opspass", "0")); ok {
		t.Fatal("ops cursor must be cleared after completion")
	}

	advanceUntil(t, &testEngine{clock: le.clock, rec: le.rec}, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return scheduledRuns == 3
	})
}

func TestRunPassNowCtxCancellationAbortsAndReturnsCtxErr(t *testing.T) {
	first := true
	src := IDs(func(context.Context) ([]ID, error) {
		if first {
			first = false
			return []ID{""}, nil
		}
		return nil, errors.New("db hiccup")
	})
	spec := Spec{
		Name:       "job",
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers:   []Trigger{Schedule(src, Every(time.Hour))},
	}
	le, _ := startRun(t, spec, nil)
	select {
	case <-le.e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	lastKey := le.e.key("sched", "0", "last")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok, err := le.kv.Get(context.Background(), lastKey); err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduled boot pass never completed")
		}
		time.Sleep(2 * time.Millisecond)
	}
	opsCtx, opsCancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() { resultCh <- le.e.RunPassNow(opsCtx) }()
	opsCancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunPassNow error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunPassNow never returned after ctx cancellation")
	}
}

func TestRunPassNowDoesNotPanicWhenQueueNilsMidPass(t *testing.T) {
	first := true
	entered := make(chan struct{})
	release := make(chan struct{})
	src := IDs(func(context.Context) ([]ID, error) {
		if first {
			first = false
			return []ID{"a"}, nil
		}
		close(entered)
		<-release
		return []ID{"a"}, nil
	})
	spec := Spec{
		Name:       "job",
		Reconciler: Func(func(context.Context, ID) error { return nil }),
		Triggers:   []Trigger{Schedule(src, Every(time.Hour))},
	}
	e, err := newEngine(spec)
	if err != nil {
		t.Fatal(err)
	}
	clock := convergetest.NewClock(wqStart)
	deps := converge.JobDeps{
		Lease:        inmem.NewLeaseWithClock(clock),
		KV:           inmem.NewKVWithClock(clock),
		Observer:     &eventRecorder{},
		Clock:        clock,
		LeaseTTL:     30 * time.Second,
		DrainTimeout: time.Second,
	}
	if err := e.bind(deps); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runActiveDone := make(chan struct{})
	go func() {
		e.runActive(ctx, nil)
		close(runActiveDone)
	}()
	select {
	case <-e.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("never ready")
	}
	lastKey := e.key("sched", "0", "last")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok, err := deps.KV.Get(context.Background(), lastKey); err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduled boot pass never completed")
		}
		time.Sleep(2 * time.Millisecond)
	}

	runDone := make(chan error, 1)
	go func() { runDone <- e.RunPassNow(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("ops pass never entered the slow page")
	}

	cancel()
	select {
	case <-runActiveDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runActive never returned")
	}
	e.mu.Lock()
	e.queue = nil
	e.mu.Unlock()

	close(release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("RunPassNow returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunPassNow never returned")
	}
}
