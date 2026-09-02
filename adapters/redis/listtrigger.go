package convredis

import (
	"context"
	"errors"
	"time"

	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

const DefaultListPoll = time.Second

type ListTriggerOpts struct {
	ID   func(payload []byte) (reconcile.ID, error)
	Poll time.Duration
}

func ListTrigger(rdb *redis.Client, key string, o ListTriggerOpts) (reconcile.Trigger, error) {
	if rdb == nil {
		return nil, errors.New("convredis: ListTrigger needs a client")
	}
	if key == "" {
		return nil, errors.New("convredis: ListTrigger needs a list key")
	}
	if o.Poll < 0 {
		return nil, errors.New("convredis: ListTrigger Poll must not be negative")
	}
	id := o.ID
	if id == nil {
		id = reconcile.RawID()
	}
	poll := o.Poll
	if poll == 0 {
		poll = DefaultListPoll
	}
	return &listTrigger{rdb: rdb, key: key, id: id, poll: poll}, nil
}

type listTrigger struct {
	rdb  *redis.Client
	key  string
	id   func(payload []byte) (reconcile.ID, error)
	poll time.Duration
}

func (t *listTrigger) Run(ctx context.Context, sink reconcile.Sink) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := t.rdb.BRPop(ctx, t.poll, t.key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		id, err := t.id([]byte(res[1]))
		if err != nil {
			sink.Drop(err)
			continue
		}
		sink.Notify(id)
	}
}
