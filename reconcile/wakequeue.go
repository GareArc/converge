package reconcile

import (
	"container/heap"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/internal/backoff"
)

type wakeClass int

const (
	wakeHint wakeClass = iota
	wakePoke
	wakeVersion
)

func (c wakeClass) bypassesBackoff() bool {
	switch c {
	case wakePoke, wakeVersion:
		return true
	default:
		return false
	}
}

type idPhase int

const (
	phaseQueued idPhase = iota + 1
	phaseRunning
	phaseBackoff
	phaseDelayed
)

type wakeResult int

const (
	wakeEnqueued wakeResult = iota
	wakeCollapsed
	wakeRerunArmed
	wakeBypassed
	wakePulledForward
	wakeDroppedOverflow
)

type finishKind int

const (
	finishSuccess finishKind = iota
	finishFailure
	finishDelay
	finishNeutral
)

const wakeQueueBound = 65536

type wakePolicy struct {
	backoff func(consecutiveFails int) time.Duration
	floor   func(d time.Duration) time.Duration
}

type idState struct {
	phase      idPhase
	due        time.Time
	hasPending bool
	pending    wakeClass
	fails      int
	noBackoff  int
	fallbacks  int
}

type finishResult struct {
	attempt  int
	rerun    bool
	fallback bool
	settled  bool
}

type queueCounts struct {
	depth int
}

type dueItem struct {
	id  ID
	due time.Time
}

type dueHeap []dueItem

func (h dueHeap) Len() int           { return len(h) }
func (h dueHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h dueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dueHeap) Push(x any)        { *h = append(*h, x.(dueItem)) }

func (h *dueHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

type wakeQueue struct {
	mu     sync.Mutex
	clock  converge.Clock
	policy wakePolicy
	ids    map[ID]*idState
	heap   dueHeap
	notify chan struct{}
}

func newWakeQueue(clock converge.Clock, policy wakePolicy) *wakeQueue {
	return &wakeQueue{
		clock:  clock,
		policy: policy,
		ids:    map[ID]*idState{},
		notify: make(chan struct{}, 1),
	}
}

func (q *wakeQueue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *wakeQueue) push(id ID, due time.Time) {
	heap.Push(&q.heap, dueItem{id: id, due: due})
	if len(q.heap) > 4*len(q.ids)+16 {
		q.rebuildHeap()
	}
	q.signal()
}

func (q *wakeQueue) rebuildHeap() {
	fresh := make(dueHeap, 0, len(q.ids))
	for id, st := range q.ids {
		switch st.phase {
		case phaseQueued, phaseBackoff, phaseDelayed:
			fresh = append(fresh, dueItem{id: id, due: st.due})
		}
	}
	heap.Init(&fresh)
	q.heap = fresh
}

func (q *wakeQueue) wake(id ID, class wakeClass) wakeResult {
	now := q.clock.Now()
	q.mu.Lock()
	defer q.mu.Unlock()
	st := q.ids[id]
	if st == nil {
		if len(q.ids) >= wakeQueueBound {
			return wakeDroppedOverflow
		}
		q.ids[id] = &idState{phase: phaseQueued, due: now}
		q.push(id, now)
		return wakeEnqueued
	}
	switch st.phase {
	case phaseQueued:
		return wakeCollapsed
	case phaseRunning:
		if !st.hasPending {
			st.hasPending = true
			st.pending = class
			return wakeRerunArmed
		}
		if class.bypassesBackoff() && !st.pending.bypassesBackoff() {
			st.pending = class
		}
		return wakeCollapsed
	case phaseBackoff:
		if !class.bypassesBackoff() {
			return wakeCollapsed
		}
		st.phase = phaseQueued
		st.due = now
		q.push(id, now)
		return wakeBypassed
	case phaseDelayed:
		st.phase = phaseQueued
		st.due = now
		q.push(id, now)
		return wakePulledForward
	}
	return wakeCollapsed
}

func (q *wakeQueue) next(now time.Time) (ID, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.heap.Len() > 0 {
		top := q.heap[0]
		st := q.ids[top.id]
		if st == nil || st.phase == phaseRunning || !st.due.Equal(top.due) {
			heap.Pop(&q.heap)
			continue
		}
		if top.due.After(now) {
			return "", false
		}
		heap.Pop(&q.heap)
		st.phase = phaseRunning
		st.hasPending = false
		return top.id, true
	}
	return "", false
}

func (q *wakeQueue) nextDue() (time.Time, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.heap.Len() > 0 {
		top := q.heap[0]
		st := q.ids[top.id]
		if st == nil || st.phase == phaseRunning || !st.due.Equal(top.due) {
			heap.Pop(&q.heap)
			continue
		}
		return top.due, true
	}
	return time.Time{}, false
}

func (q *wakeQueue) applyBackoffOrBypass(id ID, st *idState, now time.Time, attempt int, wait time.Duration) finishResult {
	res := finishResult{attempt: attempt, settled: true}
	pendingBypass := st.hasPending && st.pending.bypassesBackoff()
	st.hasPending = false
	if pendingBypass {
		st.phase = phaseQueued
		st.due = now
		q.push(id, now)
		res.rerun = true
		return res
	}
	st.phase = phaseBackoff
	st.due = now.Add(wait)
	q.push(id, st.due)
	return res
}

func (q *wakeQueue) applyFailure(id ID, st *idState, now time.Time) finishResult {
	st.fails++
	st.noBackoff = 0
	st.fallbacks = 0
	return q.applyBackoffOrBypass(id, st, now, st.fails, q.policy.backoff(st.fails))
}

func (q *wakeQueue) applyFallback(id ID, st *idState, now time.Time) finishResult {
	return q.applyBackoffOrBypass(id, st, now, st.fallbacks, q.policy.backoff(st.fallbacks))
}

func (q *wakeQueue) finish(id ID, kind finishKind, delay time.Duration) finishResult {
	now := q.clock.Now()
	q.mu.Lock()
	defer q.mu.Unlock()
	st := q.ids[id]
	if st == nil || st.phase != phaseRunning {
		return finishResult{}
	}
	switch kind {
	case finishSuccess:
		res := finishResult{attempt: st.fails + 1, settled: true}
		if st.hasPending {
			st.hasPending = false
			st.phase = phaseQueued
			st.due = now
			st.fails = 0
			st.noBackoff = 0
			st.fallbacks = 0
			q.push(id, now)
			res.rerun = true
			return res
		}
		delete(q.ids, id)
		return res
	case finishFailure:
		return q.applyFailure(id, st, now)
	case finishDelay:
		st.noBackoff++
		if st.noBackoff > backoff.NoBackoffCap {
			st.noBackoff = 0
			st.fallbacks++
			res := q.applyFallback(id, st, now)
			res.fallback = true
			return res
		}
		res := finishResult{attempt: st.fails + 1, settled: true}
		st.fails = 0
		if st.hasPending {
			st.hasPending = false
			st.phase = phaseQueued
			st.due = now
			q.push(id, now)
			res.rerun = true
			return res
		}
		st.phase = phaseDelayed
		st.due = now.Add(q.policy.floor(delay))
		q.push(id, st.due)
		return res
	case finishNeutral:
		st.hasPending = false
		st.phase = phaseQueued
		st.due = now
		q.push(id, now)
		return finishResult{settled: true}
	default:
		return q.applyFailure(id, st, now)
	}
}

func (q *wakeQueue) quiet(now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, st := range q.ids {
		if st.phase == phaseRunning {
			return false
		}
		if !st.due.After(now) {
			return false
		}
	}
	return true
}

func (q *wakeQueue) counts() queueCounts {
	q.mu.Lock()
	defer q.mu.Unlock()
	var c queueCounts
	for _, st := range q.ids {
		switch st.phase {
		case phaseQueued, phaseBackoff, phaseDelayed:
			c.depth++
		case phaseRunning:
			if st.hasPending {
				c.depth++
			}
		}
	}
	return c
}

func (q *wakeQueue) reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ids = map[ID]*idState{}
	q.heap = nil
}
