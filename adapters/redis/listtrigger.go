package convredis

import (
	"context"
	"errors"
	"time"

	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

const listPopBlock = time.Second

func ListTrigger(rdb *redis.Client, key string, id reconcile.IDFunc) reconcile.Trigger {
	return &listTrigger{rdb: rdb, key: key, idf: id}
}

type listTrigger struct {
	rdb *redis.Client
	key string
	idf reconcile.IDFunc
}

func (t *listTrigger) Run(ctx context.Context, wake func(reconcile.ID)) error {
	for {
		if ctx.Err() != nil {
			return context.Canceled
		}
		res, err := t.rdb.BRPop(ctx, listPopBlock, t.key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return context.Canceled
			}
			return err
		}
		id, err := t.idf([]byte(res[1]))
		if err != nil || id == "" {
			continue
		}
		wake(id)
	}
}
