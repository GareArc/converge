package reconcile

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
	"github.com/GareArc/converge/internal/backoff"
)

var wqStart = time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)

func testPolicy() wakePolicy {
	return wakePolicy{
		backoff: func(int) time.Duration { return time.Minute },
		floor: func(d time.Duration) time.Duration {
			if d < 250*time.Millisecond {
				return 250 * time.Millisecond
			}
			return d
		},
	}
}

func newTestQueue() (*wakeQueue, *convergetest.Clock) {
	clock := convergetest.NewClock(wqStart)
	return newWakeQueue(clock, testPolicy()), clock
}

func TestWakeStateMachineTable(t *testing.T) {
	type row struct {
		state string
		class wakeClass
		want  wakeResult
	}
	rows := []row{
		{"idle", wakeSweep, wakeEnqueued},
		{"idle", wakeNotify, wakeEnqueued},
		{"queued", wakeSweep, wakeCollapsed},
		{"queued", wakeNotify, wakeCollapsed},
		{"running", wakeSweep, wakeRerunArmed},
		{"running", wakeNotify, wakeRerunArmed},
		{"backoff", wakeSweep, wakeCollapsed},
		{"backoff", wakeNotify, wakeBypassed},
		{"delayed", wakeSweep, wakePulledForward},
		{"delayed", wakeNotify, wakePulledForward},
	}
	for _, r := range rows {
		t.Run(r.state+"/"+className(r.class), func(t *testing.T) {
			q, clock := newTestQueue()
			id := ID("x")
			switch r.state {
			case "idle":
			case "queued":
				q.wake(id, wakeSweep)
			case "running":
				q.wake(id, wakeSweep)
				mustPop(t, q, clock, id)
			case "backoff":
				q.wake(id, wakeSweep)
				mustPop(t, q, clock, id)
				q.finish(id, finishFailure, 0, nil)
			case "delayed":
				q.wake(id, wakeSweep)
				mustPop(t, q, clock, id)
				q.finish(id, finishDelay, time.Hour, nil)
			}
			if got := q.wake(id, r.class); got != r.want {
				t.Fatalf("%s + %s = %v, want %v", r.state, className(r.class), got, r.want)
			}
		})
	}
}

func className(c wakeClass) string {
	switch c {
	case wakeSweep:
		return "sweep"
	case wakeNotify:
		return "notify"
	default:
		return "unknown"
	}
}

func mustPop(t *testing.T, q *wakeQueue, clock *convergetest.Clock, want ID) {
	t.Helper()
	id, ok := q.next(clock.Now())
	if !ok || id != want {
		t.Fatalf("next = %q %v, want %q", id, ok, want)
	}
}

func TestBypassRunsNow(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0, nil)
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("backoff entry must not be due yet")
	}
	q.wake("x", wakeNotify)
	mustPop(t, q, clock, "x")
}

func TestBackoffEntryBecomesDueAfterDelay(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0, nil)
	due, ok := q.nextDue()
	if !ok || !due.Equal(wqStart.Add(time.Minute)) {
		t.Fatalf("nextDue = %v %v", due, ok)
	}
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "x")
}

func TestRerunAfterSuccessRequeues(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeSweep)
	res := q.finish("x", finishSuccess, 0, nil)
	if !res.rerun {
		t.Fatal("armed rerun must be reported")
	}
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishSuccess, 0, nil); res.rerun {
		t.Fatal("no rerun was armed this time")
	}
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("queue must be empty after final success")
	}
}

func TestSecondWakeWhileRerunArmedCollapses(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeNotify)
	if got := q.wake("x", wakeNotify); got != wakeCollapsed {
		t.Fatalf("second wake while rerun armed = %v, want collapsed", got)
	}
}

func TestDelayFloorsAndFallsBackAfterLimit(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	for i := 0; i < backoff.NoBackoffCap; i++ {
		mustPop(t, q, clock, "x")
		res := q.finish("x", finishDelay, 0, nil)
		if res.fallback {
			t.Fatalf("fallback fired early at requeue %d", i+1)
		}
		due, ok := q.nextDue()
		if !ok || !due.Equal(clock.Now().Add(250*time.Millisecond)) {
			t.Fatalf("requeue %d: due = %v, want floored 250ms", i+1, due)
		}
		clock.Advance(250 * time.Millisecond)
	}
	mustPop(t, q, clock, "x")
	res := q.finish("x", finishDelay, 0, nil)
	if !res.fallback {
		t.Fatal("11th consecutive no-backoff requeue must fall back to backoff")
	}
	due, ok := q.nextDue()
	if !ok || !due.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("fallback due = %v, want backoff distance", due)
	}
}

func TestSuccessResetsNoBackoffStreak(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	for i := 0; i < backoff.NoBackoffCap-1; i++ {
		mustPop(t, q, clock, "x")
		q.finish("x", finishDelay, 0, nil)
		clock.Advance(250 * time.Millisecond)
	}
	mustPop(t, q, clock, "x")
	q.wake("x", wakeSweep)
	q.finish("x", finishSuccess, 0, nil)
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishDelay, 0, nil); res.fallback {
		t.Fatal("success must reset the no-backoff streak")
	}
}

func TestNeutralRequeuesWithoutCounting(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishNeutral, 0, nil)
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishFailure, 0, nil); res.attempt != 1 {
		t.Fatalf("neutral must not consume the failure budget: %+v", res)
	}
}

func TestDelayedHonorsRequestedDelay(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishDelay, time.Hour, nil)
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("delayed entry must not be due")
	}
	clock.Advance(time.Hour)
	mustPop(t, q, clock, "x")
}

func TestOverflowDropsNewIDs(t *testing.T) {
	q, _ := newTestQueue()
	for i := 0; i < wakeQueueBound; i++ {
		q.ids[ID(strconv.Itoa(i))] = &idState{phase: phaseQueued}
	}
	if got := q.wake("one-more", wakeSweep); got != wakeDroppedOverflow {
		t.Fatalf("wake beyond bound = %v", got)
	}
	if got := q.wake(ID("0"), wakeSweep); got != wakeCollapsed {
		t.Fatal("known IDs must still be accepted at the bound")
	}
}

func TestCountsTracksInFlightAndFailing(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("a", wakeSweep)
	q.wake("b", wakeSweep)
	mustPop(t, q, clock, "a")
	c := q.counts()
	if c.inFlight != 1 {
		t.Fatalf("inFlight = %d, want 1 (a running)", c.inFlight)
	}
	if c.failing != 0 {
		t.Fatalf("failing = %d, want 0", c.failing)
	}
	q.finish("a", finishFailure, 0, errors.New("boom"))
	c = q.counts()
	if c.inFlight != 0 {
		t.Fatalf("inFlight = %d, want 0 once a is backing off", c.inFlight)
	}
	if c.failing != 1 {
		t.Fatalf("failing = %d, want 1 (a backing off)", c.failing)
	}
	q.reset()
	c = q.counts()
	if c.inFlight != 0 || c.failing != 0 {
		t.Fatalf("counts after reset = %+v", c)
	}
}

func TestFailingListsBackoffIDsSortedWithLastError(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("a", wakeSweep)
	q.wake("b", wakeSweep)
	mustPop(t, q, clock, "a")
	q.finish("a", finishFailure, 0, errors.New("a failed"))
	mustPop(t, q, clock, "b")
	q.finish("b", finishFailure, 0, errors.New("b failed"))

	got := q.failing()
	if len(got) != 2 {
		t.Fatalf("failing = %d entries, want 2", len(got))
	}
	if got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("failing order = %+v, want sorted by ID", got)
	}
	if got[0].Err == nil || got[0].Err.Error() != "a failed" {
		t.Fatalf("failing[0].Err = %v, want %q", got[0].Err, "a failed")
	}
	if got[0].Failures != 1 {
		t.Fatalf("failing[0].Failures = %d, want 1", got[0].Failures)
	}
	if want := clock.Now().Add(time.Minute); !got[0].NextTry.Equal(want) {
		t.Fatalf("failing[0].NextTry = %v, want %v", got[0].NextTry, want)
	}
}

func TestQuietReportsQueueState(t *testing.T) {
	q, clock := newTestQueue()
	if !q.quiet(clock.Now()) {
		t.Fatal("empty queue must be quiet")
	}
	q.wake("a", wakeSweep)
	if q.quiet(clock.Now()) {
		t.Fatal("a due queued id must not be quiet")
	}
	mustPop(t, q, clock, "a")
	if q.quiet(clock.Now()) {
		t.Fatal("a running id must not be quiet")
	}
	q.finish("a", finishFailure, 0, nil)
	if !q.quiet(clock.Now()) {
		t.Fatal("a future-due backoff id must be quiet")
	}
	due, ok := q.nextDue()
	if !ok {
		t.Fatal("backoff id must have a due time")
	}
	if q.quiet(due) {
		t.Fatal("an id due now must not be quiet")
	}
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "a")
}

func heapLen(q *wakeQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.heap)
}

func idsLen(q *wakeQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ids)
}

func TestUnknownFinishKindBehavesLikeFailure(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	res := q.finish("x", finishKind(99), 0, nil)
	if !res.settled || res.attempt != 1 {
		t.Fatalf("unknown finish kind = %+v, want settled failure with attempt 1", res)
	}
	due, ok := q.nextDue()
	if !ok || !due.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("unknown finish kind due = %v %v, want backoff distance", due, ok)
	}
}

func TestFinishOnUnknownIDIsUnsettled(t *testing.T) {
	q, _ := newTestQueue()
	if res := q.finish("x", finishFailure, 0, nil); res.settled {
		t.Fatalf("finish on unregistered id = %+v, want unsettled", res)
	}
}

func TestUnknownWakeClassFailsClosed(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0, nil)
	if got := q.wake("x", wakeClass(99)); got != wakeCollapsed {
		t.Fatalf("unknown class on backoff id = %v, want collapsed", got)
	}
}

func TestPendingNotifySurvivesFailure(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeNotify)
	res := q.finish("x", finishFailure, 0, nil)
	if !res.rerun || res.attempt != 1 {
		t.Fatalf("failure with pending notify = %+v, want rerun with attempt 1", res)
	}
	mustPop(t, q, clock, "x")
}

func TestPendingSweepPullsDelayForward(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeSweep)
	q.finish("x", finishDelay, time.Hour, nil)
	mustPop(t, q, clock, "x")
}

func TestRepeatedFallbacksEachTripIndependently(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	var last finishResult
	trips := 0
	for i := 0; i < 22; i++ {
		mustPop(t, q, clock, "x")
		last = q.finish("x", finishDelay, 0, nil)
		if last.fallback {
			trips++
			clock.Advance(time.Minute)
		} else {
			clock.Advance(250 * time.Millisecond)
		}
	}
	if trips != 2 {
		t.Fatalf("fallback trips over 22 requeues = %d, want 2", trips)
	}
	if !last.fallback {
		t.Fatalf("22nd requeue = %+v, want the second fallback trip", last)
	}
}

func TestHeapStaysBounded(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	for i := 0; i < 200; i++ {
		q.finish("x", finishFailure, 0, nil)
		if got := q.wake("x", wakeNotify); got != wakeBypassed {
			t.Fatalf("iteration %d: wake = %v, want bypassed", i, got)
		}
		if hl, il := heapLen(q), idsLen(q); hl > 4*il+16 {
			t.Fatalf("iteration %d: heap len %d exceeds bound for %d ids", i, hl, il)
		}
		mustPop(t, q, clock, "x")
	}
}

func TestStaleDuplicateHeapEntryDiscardedAfterDispatch(t *testing.T) {
	q, clock := newTestQueue()
	q.wake("x", wakeSweep)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0, nil)
	due, ok := q.nextDue()
	if !ok {
		t.Fatal("expected a pending backoff due time")
	}
	clock.Advance(due.Sub(clock.Now()))
	if got := q.wake("x", wakeNotify); got != wakeBypassed {
		t.Fatalf("notify at due instant = %v, want bypassed", got)
	}
	mustPop(t, q, clock, "x")
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("duplicate stale heap entry must be discarded, not redispatched")
	}
}
