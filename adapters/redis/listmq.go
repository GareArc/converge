package convredis

import (
	"context"
	"errors"
	"time"

	"github.com/GareArc/converge"
	"github.com/redis/go-redis/v9"
)

const listPopBlock = time.Second

func NewListMQ(rdb *redis.Client) *ListMQ {
	return &ListMQ{rdb: rdb}
}

type ListMQ struct {
	rdb *redis.Client
}

func (m *ListMQ) Publish(ctx context.Context, queue string, msg converge.Message) error {
	return m.rdb.LPush(ctx, queue, msg.Payload).Err()
}

func (m *ListMQ) Consume(ctx context.Context, queue string, deliver func(converge.Delivery)) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := m.rdb.BRPop(ctx, listPopBlock, queue).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		deliver(&listDelivery{mq: m, queue: queue, payload: []byte(res[1]), enq: time.Now()})
	}
}

func (m *ListMQ) Backlog(ctx context.Context, queue string) (int, error) {
	n, err := m.rdb.LLen(ctx, queue).Result()
	return int(n), err
}

type listDelivery struct {
	mq      *ListMQ
	queue   string
	payload []byte
	enq     time.Time
}

func (d *listDelivery) Message() converge.Message { return converge.Message{Payload: d.payload} }

func (d *listDelivery) Attempt() int { return 1 }

func (d *listDelivery) EnqueuedAt() time.Time { return d.enq }

func (d *listDelivery) Ack(context.Context) error { return nil }

func (d *listDelivery) Nack(ctx context.Context, _ time.Duration) error {
	return d.mq.rdb.LPush(ctx, d.queue, d.payload).Err()
}

func (d *listDelivery) Extend(context.Context, time.Duration) error { return nil }
