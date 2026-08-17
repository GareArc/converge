package inmem

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

const DefaultVisibility = 30 * time.Second

const pollInterval = 2 * time.Millisecond

type MQ struct {
	mu     sync.Mutex
	clock  converge.Clock
	queues map[string]*mqQueue
}

type mqQueue struct {
	seq     int
	backlog []storedMsg
	groups  map[string]*mqGroup
}

type storedMsg struct {
	id         int
	m          converge.Message
	enqueuedAt time.Time
	notBefore  time.Time
}

type mqGroup struct {
	pending  []*mqMsg // kept ordered by id
	inflight map[int]*mqMsg
}

type mqMsg struct {
	storedMsg
	attempt     int
	availableAt time.Time
	deadline    time.Time
}

func NewMQ() *MQ { return NewMQWithClock(nil) }

func NewMQWithClock(c converge.Clock) *MQ {
	if c == nil {
		c = wallClock{}
	}
	return &MQ{clock: c, queues: map[string]*mqQueue{}}
}

func (q *MQ) Publish(ctx context.Context, queue string, m converge.Message) error {
	return q.publish(queue, m, 0)
}

func (q *MQ) publish(queue string, m converge.Message, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now()
	qu := q.ensureQueue(queue)
	qu.seq++
	s := storedMsg{id: qu.seq, m: m, enqueuedAt: now, notBefore: now.Add(delay)}
	qu.backlog = append(qu.backlog, s)
	for _, g := range qu.groups {
		g.pending = append(g.pending, &mqMsg{storedMsg: s, availableAt: s.notBefore})
	}
	return nil
}

func (q *MQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	return q.consumeGroup(ctx, queue, "default", deliver)
}

func (q *MQ) consumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	q.mu.Lock()
	g := q.ensureGroup(queue, group)
	q.mu.Unlock()
	for {
		q.mu.Lock()
		msg := g.next(q.clock.Now())
		var d *mqDelivery
		if msg != nil {
			d = &mqDelivery{q: q, g: g, msg: msg, attempt: msg.attempt}
		}
		q.mu.Unlock()
		if d == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}
		deliver(d)
	}
}

func (q *MQ) ensureQueue(name string) *mqQueue {
	qu := q.queues[name]
	if qu == nil {
		qu = &mqQueue{groups: map[string]*mqGroup{}}
		q.queues[name] = qu
	}
	return qu
}

// New groups start from the beginning of the backlog: messages published
// before any consumer existed are kept for it.
func (q *MQ) ensureGroup(queue, group string) *mqGroup {
	qu := q.ensureQueue(queue)
	g := qu.groups[group]
	if g == nil {
		g = &mqGroup{inflight: map[int]*mqMsg{}}
		for _, s := range qu.backlog {
			g.pending = append(g.pending, &mqMsg{storedMsg: s, availableAt: s.notBefore})
		}
		qu.groups[group] = g
	}
	return g
}

func (g *mqGroup) next(now time.Time) *mqMsg {
	for id, im := range g.inflight {
		if !im.deadline.After(now) {
			delete(g.inflight, id)
			im.deadline = time.Time{}
			im.availableAt = now
			g.pending = append(g.pending, im)
		}
	}
	sort.Slice(g.pending, func(i, j int) bool { return g.pending[i].id < g.pending[j].id })
	for i, msg := range g.pending {
		if !msg.availableAt.After(now) {
			g.pending = append(g.pending[:i], g.pending[i+1:]...)
			msg.attempt++
			msg.deadline = now.Add(DefaultVisibility)
			g.inflight[msg.id] = msg
			return msg
		}
	}
	return nil
}

type mqDelivery struct {
	q       *MQ
	g       *mqGroup
	msg     *mqMsg
	attempt int // snapshot at delivery; fences stale Ack/Nack/Extend after reclaim
}

func (d *mqDelivery) Message() converge.Message { return d.msg.m }
func (d *mqDelivery) Attempt() int              { return d.attempt }
func (d *mqDelivery) EnqueuedAt() time.Time     { return d.msg.enqueuedAt }

func (d *mqDelivery) Ack(context.Context) error {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	if im, ok := d.g.inflight[d.msg.id]; !ok || im.attempt != d.attempt {
		return nil
	}
	delete(d.g.inflight, d.msg.id)
	return nil
}

func (d *mqDelivery) Nack(_ context.Context, after time.Duration) error {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	im, ok := d.g.inflight[d.msg.id]
	if !ok || im.attempt != d.attempt {
		return nil
	}
	delete(d.g.inflight, d.msg.id)
	d.msg.deadline = time.Time{}
	d.msg.availableAt = d.q.clock.Now().Add(after)
	d.g.pending = append(d.g.pending, d.msg)
	return nil
}

func (d *mqDelivery) Extend(_ context.Context, visibility time.Duration) error {
	d.q.mu.Lock()
	defer d.q.mu.Unlock()
	im, ok := d.g.inflight[d.msg.id]
	if !ok || im.attempt != d.attempt {
		return errors.New("inmem: delivery no longer in flight")
	}
	d.msg.deadline = d.q.clock.Now().Add(visibility)
	return nil
}
