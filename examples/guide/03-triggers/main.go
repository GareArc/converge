package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

func inboundKey(sku reconcile.ID) string {
	return "warehouse:" + string(sku) + ":inbound"
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	mq := convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{})

	rt, err := converge.New(converge.Options{
		Namespace: "guide-03",
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	skus := func(ctx context.Context) ([]string, error) {
		return []string{"SKU-1001", "SKU-1002"}, nil
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "sync-inventory",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			inbound, err := rdb.Get(ctx, inboundKey(id)).Int()
			if err != nil && !errors.Is(err, redis.Nil) {
				return err
			}
			if inbound > 0 {
				fmt.Printf("%s: pallets still inbound: %d\n", id, inbound)
				if err := rdb.Decr(ctx, inboundKey(id)).Err(); err != nil {
					return err
				}
				return reconcile.CheckAgain{In: 500 * time.Millisecond}
			}
			fmt.Printf("%s: inventory matches the warehouse\n", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.OnMessage("stock-events", reconcile.IDFromJSONField("sku"), reconcile.OnMessageOpts{MQ: mq}),
			reconcile.Schedule(reconcile.StringIDs(skus), reconcile.Every(5*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	go func() {
		<-rt.Ready()
		select {
		case <-time.After(800 * time.Millisecond):
		case <-ctx.Done():
			return
		}
		if err := rdb.Set(ctx, inboundKey("SKU-1001"), 3, time.Minute).Err(); err != nil {
			log.Println("seed:", err)
			return
		}
		payload, err := json.Marshal(map[string]string{"sku": "SKU-1001"})
		if err != nil {
			log.Println("marshal:", err)
			return
		}
		if err := mq.Publish(ctx, "stock-events", converge.Message{Payload: payload}); err != nil {
			log.Println("publish:", err)
		}
	}()

	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
