package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

type pubsubTrigger struct {
	rdb     *redis.Client
	channel string
}

func (t *pubsubTrigger) Run(ctx context.Context, wake func(reconcile.ID)) error {
	sub := t.rdb.Subscribe(ctx, t.channel)
	defer sub.Close()
	for {
		msg, err := sub.ReceiveMessage(ctx)
		if err != nil {
			return err
		}
		wake(reconcile.ID(msg.Payload))
	}
}

func cacheIDs(ctx context.Context) ([]string, error) {
	return []string{"pricing", "catalog"}, nil
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "refresh-cache",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			fmt.Println("refreshing cache for", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			&pubsubTrigger{rdb: rdb, channel: "cache-invalidate"},
			reconcile.Schedule(reconcile.StringIDs(cacheIDs), reconcile.Every(2*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
