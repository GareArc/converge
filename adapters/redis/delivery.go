package convredis

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/redis/go-redis/v9"
)

var ErrSettled = errors.New("convredis: delivery already settled")

type streamDelivery struct {
	mq      *streamsMQ
	queue   string
	group   string
	id      string
	msg     converge.Message
	attempt int
	enq     time.Time

	mu      sync.Mutex
	settled bool
}

func (d *streamDelivery) Message() converge.Message { return d.msg }
func (d *streamDelivery) Attempt() int              { return d.attempt }
func (d *streamDelivery) EnqueuedAt() time.Time     { return d.enq }

func (d *streamDelivery) Ack(ctx context.Context) error {
	if err := d.mq.rdb.XAck(ctx, streamKey(d.queue), d.group, d.id).Err(); err != nil {
		return err
	}
	d.settle()
	return d.mq.forget(ctx, d.queue, d.group, d.id)
}

func (d *streamDelivery) Nack(ctx context.Context, redeliverAfter time.Duration) error {
	return d.reschedule(ctx, redeliverAfter)
}

func (d *streamDelivery) Extend(ctx context.Context, visibility time.Duration) error {
	if d.isSettled() {
		return ErrSettled
	}
	return d.reschedule(ctx, visibility)
}

func (d *streamDelivery) reschedule(ctx context.Context, after time.Duration) error {
	due := d.mq.clock.Now().Add(after)
	return d.mq.rdb.ZAdd(ctx, pendingKey(d.queue, d.group), redis.Z{Score: dueScore(due), Member: d.id}).Err()
}

func (d *streamDelivery) settle() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.settled = true
}

func (d *streamDelivery) isSettled() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.settled
}

type broadcastDelivery struct {
	msg converge.Message
	enq time.Time
}

func newBroadcastDelivery(entry redis.XMessage) (converge.Delivery, bool) {
	msg, enq, err := decodeMessage(entry.Values)
	if err != nil {
		return nil, false
	}
	return &broadcastDelivery{msg: msg, enq: enq}, true
}

func (d *broadcastDelivery) Message() converge.Message { return d.msg }
func (d *broadcastDelivery) Attempt() int              { return 1 }
func (d *broadcastDelivery) EnqueuedAt() time.Time     { return d.enq }

func (d *broadcastDelivery) Ack(context.Context) error                   { return nil }
func (d *broadcastDelivery) Nack(context.Context, time.Duration) error   { return nil }
func (d *broadcastDelivery) Extend(context.Context, time.Duration) error { return nil }
