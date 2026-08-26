package inmem

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/GareArc/converge"
)

func cloneMessage(m converge.Message) converge.Message {
	m.Headers = maps.Clone(m.Headers)
	m.Payload = bytes.Clone(m.Payload)
	return m
}

const DefaultVisibility = 30 * time.Second

const pollInterval = 2 * time.Millisecond

type MQ struct {
	mu        sync.Mutex
	clock     converge.Clock
	retention time.Duration
	queues    map[string]*mqQueue
}

type mqQueue struct {
	seq     int
	backlog []storedMsg
	groups  map[string]*mqGroup
	subs    []*mqSub
}

type storedMsg struct {
	id         int
	m          converge.Message
	enqueuedAt time.Time
	notBefore  time.Time
}

type mqGroup struct {
	pending  []*mqMsg
	inflight map[int]*mqMsg
}

type mqMsg struct {
	storedMsg
	attempt     int
	availableAt time.Time
	deadline    time.Time
}

type Options struct {
	Clock     converge.Clock
	Retention time.Duration
}

func NewMQ() *MQ { return NewMQWithOpts(Options{}) }

func NewMQWithClock(c converge.Clock) *MQ { return NewMQWithOpts(Options{Clock: c}) }

func NewMQWithOpts(o Options) *MQ {
	c := o.Clock
	if c == nil {
		c = wallClock{}
	}
	return &MQ{clock: c, retention: o.Retention, queues: map[string]*mqQueue{}}
}

func (q *MQ) Publish(ctx context.Context, queue string, m converge.Message) error {
	return q.publish(queue, m, 0)
}

func (q *MQ) publish(queue string, m converge.Message, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now()
	qu := q.ensureQueue(queue)
	q.pruneBacklog(qu, now)
	qu.seq++
	s := storedMsg{id: qu.seq, m: cloneMessage(m), enqueuedAt: now, notBefore: now.Add(delay)}
	qu.backlog = append(qu.backlog, s)
	for _, g := range qu.groups {
		g.pending = append(g.pending, &mqMsg{storedMsg: s, availableAt: s.notBefore})
	}
	for _, sub := range qu.subs {
		sub.pending = append(sub.pending, s)
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
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

func (q *MQ) ensureGroup(queue, group string) *mqGroup {
	qu := q.ensureQueue(queue)
	q.pruneBacklog(qu, q.clock.Now())
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

func (q *MQ) pruneBacklog(qu *mqQueue, now time.Time) {
	if q.retention <= 0 {
		return
	}
	cutoff := now.Add(-q.retention)
	i := 0
	for i < len(qu.backlog) && !qu.backlog[i].enqueuedAt.After(cutoff) {
		i++
	}
	if i == 0 {
		return
	}
	kept := make([]storedMsg, len(qu.backlog)-i)
	copy(kept, qu.backlog[i:])
	qu.backlog = kept
}

func (q *MQ) Backlog(_ context.Context, queue string) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	qu, ok := q.queues[queue]
	if !ok {
		return 0, nil
	}
	q.pruneBacklog(qu, q.clock.Now())
	return len(qu.backlog), nil
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
	attempt int
}

func (d *mqDelivery) Message() converge.Message { return cloneMessage(d.msg.m) }
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

func (q *MQ) ConsumeGroup(ctx context.Context, queue, group string, deliver func(converge.Delivery)) error {
	return q.consumeGroup(ctx, queue, group, deliver)
}

func (q *MQ) Idle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now()
	for _, qu := range q.queues {
		for _, g := range qu.groups {
			if len(g.inflight) > 0 {
				return false
			}
			for _, msg := range g.pending {
				if !msg.availableAt.After(now) {
					return false
				}
			}
		}
		for _, sub := range qu.subs {
			if len(sub.pending) > 0 {
				return false
			}
		}
	}
	return true
}

func (q *MQ) PublishDelayed(_ context.Context, queue string, m converge.Message, delay time.Duration) error {
	return q.publish(queue, m, delay)
}

func (q *MQ) ConsumeBroadcast(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	sub := &mqSub{}
	q.mu.Lock()
	qu := q.ensureQueue(queue)
	qu.subs = append(qu.subs, sub)
	q.mu.Unlock()
	defer func() {
		q.mu.Lock()
		for i, s := range qu.subs {
			if s == sub {
				qu.subs = append(qu.subs[:i], qu.subs[i+1:]...)
				break
			}
		}
		q.mu.Unlock()
	}()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		q.mu.Lock()
		var msg *storedMsg
		if len(sub.pending) > 0 {
			msg = &sub.pending[0]
			sub.pending = sub.pending[1:]
		}
		q.mu.Unlock()
		if msg == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}
		deliver(broadcastDelivery{*msg})
	}
}

type mqSub struct {
	pending []storedMsg
}

type broadcastDelivery struct{ s storedMsg }

func (d broadcastDelivery) Message() converge.Message                   { return cloneMessage(d.s.m) }
func (d broadcastDelivery) Attempt() int                                { return 1 }
func (d broadcastDelivery) EnqueuedAt() time.Time                       { return d.s.enqueuedAt }
func (d broadcastDelivery) Ack(context.Context) error                   { return nil }
func (d broadcastDelivery) Nack(context.Context, time.Duration) error   { return nil }
func (d broadcastDelivery) Extend(context.Context, time.Duration) error { return nil }
