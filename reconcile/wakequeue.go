package reconcile

import (
	"container/heap"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

type wakeClass int

const (
	wakeHint wakeClass = iota
	wakePoke
	wakeVersion
)

type idPhase int

const (
	phaseQueued idPhase = iota + 1
	phaseRunning
	phaseBackoff
	phaseDelayed
	phaseParked
)

type wakeResult int

const (
	wakeEnqueued wakeResult = iota
	wakeCollapsed
	wakeRerunArmed
	wakeBypassed
	wakePulledForward
	wakeRevived
	wakeDroppedParked
	wakeDroppedPaused
	wakeDroppedOverflow
)

type finishKind int

const (
	finishSuccess finishKind = iota
	finishFailure
	finishDelay
	finishNeutral
	finishForcePark
)

const (
	wakeQueueBound = 65536
	noBackoffLimit = 10
)

type wakePolicy struct {
	deadLetterAfter int
	backoff         func(consecutiveFails int) time.Duration
	floor           func(d time.Duration) time.Duration
}

type idState struct {
	phase     idPhase
	due       time.Time
	rerun     bool
	fails     int
	noBackoff int
}

type finishResult struct {
	attempt  int
	parked   bool
	rerun    bool
	fallback bool
}

type queueCounts struct {
	depth  int
	parked int
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
	paused bool
	ids    map[ID]*idState
	heap   dueHeap
	notify chan struct{}
}

func newWakeQueue(clock converge.Clock, policy wakePolicy, paused bool) *wakeQueue {
	return &wakeQueue{
		clock:  clock,
		policy: policy,
		paused: paused,
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
	q.signal()
}

func (q *wakeQueue) wake(id ID, class wakeClass) wakeResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.paused {
		return wakeDroppedPaused
	}
	now := q.clock.Now()
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
		if st.rerun {
			return wakeCollapsed
		}
		st.rerun = true
		return wakeRerunArmed
	case phaseBackoff:
		if class == wakeHint {
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
	case phaseParked:
		if class == wakeHint {
			return wakeDroppedParked
		}
		st.phase = phaseQueued
		st.due = now
		st.fails = 0
		st.noBackoff = 0
		q.push(id, now)
		return wakeRevived
	}
	return wakeCollapsed
}

func (q *wakeQueue) next(now time.Time) (ID, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.heap.Len() > 0 {
		top := q.heap[0]
		st := q.ids[top.id]
		if st == nil || st.phase == phaseRunning || st.phase == phaseParked || !st.due.Equal(top.due) {
			heap.Pop(&q.heap)
			continue
		}
		if top.due.After(now) {
			return "", false
		}
		heap.Pop(&q.heap)
		st.phase = phaseRunning
		st.rerun = false
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
		if st == nil || st.phase == phaseRunning || st.phase == phaseParked || !st.due.Equal(top.due) {
			heap.Pop(&q.heap)
			continue
		}
		return top.due, true
	}
	return time.Time{}, false
}

func (q *wakeQueue) finish(id ID, kind finishKind, delay time.Duration) finishResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	st := q.ids[id]
	if st == nil || st.phase != phaseRunning {
		return finishResult{}
	}
	now := q.clock.Now()
	switch kind {
	case finishSuccess:
		res := finishResult{attempt: st.fails + 1}
		if st.rerun {
			st.phase = phaseQueued
			st.due = now
			st.rerun = false
			st.fails = 0
			st.noBackoff = 0
			q.push(id, now)
			res.rerun = true
			return res
		}
		delete(q.ids, id)
		return res
	case finishFailure:
		st.fails++
		st.noBackoff = 0
		st.rerun = false
		if q.policy.deadLetterAfter > 0 && st.fails >= q.policy.deadLetterAfter {
			st.phase = phaseParked
			return finishResult{attempt: st.fails, parked: true}
		}
		st.phase = phaseBackoff
		st.due = now.Add(q.policy.backoff(st.fails))
		q.push(id, st.due)
		return finishResult{attempt: st.fails}
	case finishDelay:
		res := finishResult{attempt: st.fails + 1}
		st.rerun = false
		st.noBackoff++
		if st.noBackoff > noBackoffLimit {
			st.noBackoff = 0
			st.fails++
			res.attempt = st.fails
			res.fallback = true
			if q.policy.deadLetterAfter > 0 && st.fails >= q.policy.deadLetterAfter {
				st.phase = phaseParked
				res.parked = true
				return res
			}
			st.phase = phaseBackoff
			st.due = now.Add(q.policy.backoff(st.fails))
			q.push(id, st.due)
			return res
		}
		st.fails = 0
		st.phase = phaseDelayed
		st.due = now.Add(q.policy.floor(delay))
		q.push(id, st.due)
		return res
	case finishNeutral:
		st.phase = phaseQueued
		st.due = now
		st.rerun = false
		q.push(id, now)
		return finishResult{}
	case finishForcePark:
		st.fails++
		st.phase = phaseParked
		st.rerun = false
		return finishResult{attempt: st.fails, parked: true}
	}
	return finishResult{}
}

func (q *wakeQueue) counts() queueCounts {
	q.mu.Lock()
	defer q.mu.Unlock()
	var c queueCounts
	for _, st := range q.ids {
		switch st.phase {
		case phaseParked:
			c.parked++
		case phaseQueued, phaseBackoff, phaseDelayed:
			c.depth++
		case phaseRunning:
			if st.rerun {
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
