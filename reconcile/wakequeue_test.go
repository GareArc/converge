package reconcile

import (
	"strconv"
	"testing"
	"time"

	"github.com/GareArc/converge/convergetest"
)

var wqStart = time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)

func testPolicy(deadLetterAfter int) wakePolicy {
	return wakePolicy{
		deadLetterAfter: deadLetterAfter,
		backoff:         func(int) time.Duration { return time.Minute },
		floor: func(d time.Duration) time.Duration {
			if d < 250*time.Millisecond {
				return 250 * time.Millisecond
			}
			return d
		},
	}
}

func newTestQueue(deadLetterAfter int, paused bool) (*wakeQueue, *convergetest.Clock) {
	clock := convergetest.NewClock(wqStart)
	return newWakeQueue(clock, testPolicy(deadLetterAfter), paused), clock
}

func TestWakeStateMachineTable(t *testing.T) {
	type row struct {
		state string
		class wakeClass
		want  wakeResult
	}
	rows := []row{
		{"idle", wakeHint, wakeEnqueued},
		{"idle", wakePoke, wakeEnqueued},
		{"idle", wakeVersion, wakeEnqueued},
		{"queued", wakeHint, wakeCollapsed},
		{"queued", wakePoke, wakeCollapsed},
		{"queued", wakeVersion, wakeCollapsed},
		{"running", wakeHint, wakeRerunArmed},
		{"running", wakePoke, wakeRerunArmed},
		{"running", wakeVersion, wakeRerunArmed},
		{"backoff", wakeHint, wakeCollapsed},
		{"backoff", wakePoke, wakeBypassed},
		{"backoff", wakeVersion, wakeBypassed},
		{"delayed", wakeHint, wakePulledForward},
		{"delayed", wakePoke, wakePulledForward},
		{"delayed", wakeVersion, wakePulledForward},
		{"parked", wakeHint, wakeDroppedParked},
		{"parked", wakePoke, wakeRevived},
		{"parked", wakeVersion, wakeRevived},
		{"paused", wakeHint, wakeDroppedPaused},
		{"paused", wakePoke, wakeDroppedPaused},
		{"paused", wakeVersion, wakeDroppedPaused},
	}
	for _, r := range rows {
		t.Run(r.state+"/"+className(r.class), func(t *testing.T) {
			q, clock := newTestQueue(1, r.state == "paused")
			id := ID("x")
			switch r.state {
			case "idle", "paused":
			case "queued":
				q.wake(id, wakeHint)
			case "running":
				q.wake(id, wakeHint)
				mustPop(t, q, clock, id)
			case "backoff":
				q2, c2 := newTestQueue(0, false)
				q, clock = q2, c2
				q.wake(id, wakeHint)
				mustPop(t, q, clock, id)
				q.finish(id, finishFailure, 0)
			case "delayed":
				q.wake(id, wakeHint)
				mustPop(t, q, clock, id)
				q.finish(id, finishDelay, time.Hour)
			case "parked":
				q.wake(id, wakeHint)
				mustPop(t, q, clock, id)
				q.finish(id, finishFailure, 0)
			}
			if got := q.wake(id, r.class); got != r.want {
				t.Fatalf("%s + %s = %v, want %v", r.state, className(r.class), got, r.want)
			}
		})
	}
}

func className(c wakeClass) string {
	switch c {
	case wakeHint:
		return "hint"
	case wakePoke:
		return "poke"
	default:
		return "version"
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
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("backoff entry must not be due yet")
	}
	q.wake("x", wakePoke)
	mustPop(t, q, clock, "x")
}

func TestBackoffEntryBecomesDueAfterDelay(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	due, ok := q.nextDue()
	if !ok || !due.Equal(wqStart.Add(time.Minute)) {
		t.Fatalf("nextDue = %v %v", due, ok)
	}
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "x")
}

func TestRerunAfterSuccessRequeues(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeHint)
	res := q.finish("x", finishSuccess, 0)
	if !res.rerun {
		t.Fatal("armed rerun must be reported")
	}
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishSuccess, 0); res.rerun {
		t.Fatal("no rerun was armed this time")
	}
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("queue must be empty after final success")
	}
}

func TestSecondWakeWhileRerunArmedCollapses(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.wake("x", wakePoke)
	if got := q.wake("x", wakeVersion); got != wakeCollapsed {
		t.Fatalf("second wake while rerun armed = %v, want collapsed", got)
	}
}

func TestDeadLetterAfterParks(t *testing.T) {
	q, clock := newTestQueue(2, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishFailure, 0); res.parked || res.attempt != 1 {
		t.Fatalf("first failure = %+v", res)
	}
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "x")
	res := q.finish("x", finishFailure, 0)
	if !res.parked || res.attempt != 2 {
		t.Fatalf("second failure = %+v, want parked", res)
	}
	c := q.counts()
	if c.parked != 1 || c.depth != 0 {
		t.Fatalf("counts = %+v", c)
	}
}

func TestReviveResetsFailureCount(t *testing.T) {
	q, clock := newTestQueue(2, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	if got := q.wake("x", wakePoke); got != wakeRevived {
		t.Fatalf("poke on parked = %v", got)
	}
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishFailure, 0); res.parked {
		t.Fatal("revived ID must get a fresh failure budget")
	}
}

func TestDelayFloorsAndFallsBackAfterLimit(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	for i := 0; i < noBackoffLimit; i++ {
		mustPop(t, q, clock, "x")
		res := q.finish("x", finishDelay, 0)
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
	res := q.finish("x", finishDelay, 0)
	if !res.fallback {
		t.Fatal("11th consecutive no-backoff requeue must fall back to backoff")
	}
	due, ok := q.nextDue()
	if !ok || !due.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("fallback due = %v, want backoff distance", due)
	}
}

func TestSuccessResetsNoBackoffStreak(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	for i := 0; i < noBackoffLimit-1; i++ {
		mustPop(t, q, clock, "x")
		q.finish("x", finishDelay, 0)
		clock.Advance(250 * time.Millisecond)
	}
	mustPop(t, q, clock, "x")
	q.wake("x", wakeHint)
	q.finish("x", finishSuccess, 0)
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishDelay, 0); res.fallback {
		t.Fatal("success must reset the no-backoff streak")
	}
}

func TestNeutralRequeuesWithoutCounting(t *testing.T) {
	q, clock := newTestQueue(1, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishNeutral, 0)
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishFailure, 0); !res.parked || res.attempt != 1 {
		t.Fatalf("neutral must not consume the failure budget: %+v", res)
	}
}

func TestForceParkParksImmediately(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	res := q.finish("x", finishForcePark, 0)
	if !res.parked {
		t.Fatal("force park must park")
	}
	if got := q.wake("x", wakeHint); got != wakeDroppedParked {
		t.Fatalf("hint on force-parked = %v", got)
	}
}

func TestDelayedHonorsRequestedDelay(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishDelay, time.Hour)
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("delayed entry must not be due")
	}
	clock.Advance(time.Hour)
	mustPop(t, q, clock, "x")
}

func TestOverflowDropsNewIDs(t *testing.T) {
	q, _ := newTestQueue(0, false)
	for i := 0; i < wakeQueueBound; i++ {
		q.ids[ID(strconv.Itoa(i))] = &idState{phase: phaseQueued}
	}
	if got := q.wake("one-more", wakeHint); got != wakeDroppedOverflow {
		t.Fatalf("wake beyond bound = %v", got)
	}
	if got := q.wake(ID("0"), wakeHint); got != wakeCollapsed {
		t.Fatal("known IDs must still be accepted at the bound")
	}
}

func TestRestoreParkReportsOverflow(t *testing.T) {
	q, _ := newTestQueue(0, false)
	for i := 0; i < wakeQueueBound; i++ {
		if !q.restorePark(ID(strconv.Itoa(i))) {
			t.Fatalf("restorePark %d: want true while under bound", i)
		}
	}
	if q.restorePark("new") {
		t.Fatal("restorePark beyond bound = true, want false")
	}
	if !q.restorePark(ID(strconv.Itoa(0))) {
		t.Fatal("restorePark of an already-tracked id at the bound = false, want true")
	}
}

func TestCountsAndReset(t *testing.T) {
	q, clock := newTestQueue(1, false)
	q.wake("a", wakeHint)
	q.wake("b", wakeHint)
	mustPop(t, q, clock, "a")
	q.wake("a", wakeHint)
	c := q.counts()
	if c.depth != 2 {
		t.Fatalf("depth = %d, want queued b + armed rerun a", c.depth)
	}
	q.finish("a", finishFailure, 0)
	if q.counts().parked != 1 {
		t.Fatal("a must be parked")
	}
	q.reset()
	c = q.counts()
	if c.depth != 0 || c.parked != 0 {
		t.Fatalf("counts after reset = %+v", c)
	}
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
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	res := q.finish("x", finishKind(99), 0)
	if !res.settled || res.attempt != 1 {
		t.Fatalf("unknown finish kind = %+v, want settled failure with attempt 1", res)
	}
	due, ok := q.nextDue()
	if !ok || !due.Equal(clock.Now().Add(time.Minute)) {
		t.Fatalf("unknown finish kind due = %v %v, want backoff distance", due, ok)
	}
}

func TestFinishOnUnknownIDIsUnsettled(t *testing.T) {
	q, _ := newTestQueue(0, false)
	if res := q.finish("x", finishFailure, 0); res.settled {
		t.Fatalf("finish on unregistered id = %+v, want unsettled", res)
	}
}

func TestUnknownWakeClassFailsClosed(t *testing.T) {
	qBackoff, clockBackoff := newTestQueue(0, false)
	qBackoff.wake("x", wakeHint)
	mustPop(t, qBackoff, clockBackoff, "x")
	qBackoff.finish("x", finishFailure, 0)
	if got := qBackoff.wake("x", wakeClass(99)); got != wakeCollapsed {
		t.Fatalf("unknown class on backoff id = %v, want collapsed", got)
	}

	qParked, clockParked := newTestQueue(1, false)
	qParked.wake("x", wakeHint)
	mustPop(t, qParked, clockParked, "x")
	qParked.finish("x", finishFailure, 0)
	if got := qParked.wake("x", wakeClass(99)); got != wakeDroppedParked {
		t.Fatalf("unknown class on parked id = %v, want dropped", got)
	}
}

func TestPendingPokeSurvivesFailure(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.wake("x", wakePoke)
	res := q.finish("x", finishFailure, 0)
	if !res.rerun || res.attempt != 1 {
		t.Fatalf("failure with pending poke = %+v, want rerun with attempt 1", res)
	}
	mustPop(t, q, clock, "x")
}

func TestPendingPokeSurvivesParking(t *testing.T) {
	q, clock := newTestQueue(2, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	clock.Advance(time.Minute)
	mustPop(t, q, clock, "x")
	q.wake("x", wakePoke)
	res := q.finish("x", finishFailure, 0)
	if !res.parked || !res.revived {
		t.Fatalf("failure with pending poke at threshold = %+v, want parked and revived", res)
	}
	mustPop(t, q, clock, "x")
	if res := q.finish("x", finishFailure, 0); res.parked {
		t.Fatal("revived-through-park id must get a fresh failure budget")
	}
}

func TestPendingHintDroppedOnParking(t *testing.T) {
	q, clock := newTestQueue(1, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeHint)
	res := q.finish("x", finishFailure, 0)
	if !res.parked || !res.droppedHint {
		t.Fatalf("failure with pending hint at threshold = %+v, want parked and droppedHint", res)
	}
	if got := q.wake("x", wakeHint); got != wakeDroppedParked {
		t.Fatalf("post-park hint = %v, want dropped", got)
	}
}

func TestPendingHintPullsDelayForward(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.wake("x", wakeHint)
	q.finish("x", finishDelay, time.Hour)
	mustPop(t, q, clock, "x")
}

func TestFallbackParksAfterRepeatedFallbacks(t *testing.T) {
	q, clock := newTestQueue(2, false)
	q.wake("x", wakeHint)
	var last finishResult
	for i := 0; i < 22; i++ {
		mustPop(t, q, clock, "x")
		last = q.finish("x", finishDelay, 0)
		if last.fallback {
			clock.Advance(time.Minute)
		} else {
			clock.Advance(250 * time.Millisecond)
		}
	}
	if !last.fallback || !last.parked {
		t.Fatalf("22nd requeue = %+v, want fallback and parked", last)
	}
	if q.counts().parked != 1 {
		t.Fatal("id must be parked after the second fallback")
	}
}

func TestHeapStaysBounded(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	for i := 0; i < 200; i++ {
		q.finish("x", finishFailure, 0)
		if got := q.wake("x", wakePoke); got != wakeBypassed {
			t.Fatalf("iteration %d: wake = %v, want bypassed", i, got)
		}
		if hl, il := heapLen(q), idsLen(q); hl > 4*il+16 {
			t.Fatalf("iteration %d: heap len %d exceeds bound for %d ids", i, hl, il)
		}
		mustPop(t, q, clock, "x")
	}
}

func TestStaleDuplicateHeapEntryDiscardedAfterDispatch(t *testing.T) {
	q, clock := newTestQueue(0, false)
	q.wake("x", wakeHint)
	mustPop(t, q, clock, "x")
	q.finish("x", finishFailure, 0)
	due, ok := q.nextDue()
	if !ok {
		t.Fatal("expected a pending backoff due time")
	}
	clock.Advance(due.Sub(clock.Now()))
	if got := q.wake("x", wakePoke); got != wakeBypassed {
		t.Fatalf("poke at due instant = %v, want bypassed", got)
	}
	mustPop(t, q, clock, "x")
	if _, ok := q.next(clock.Now()); ok {
		t.Fatal("duplicate stale heap entry must be discarded, not redispatched")
	}
}
