package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	rt, err := converge.New(converge.Options{
		Namespace: "billing",
		MQ:        convredis.NewStreamsMQ(rdb, convredis.StreamsOpts{}),
		Lease:     convredis.NewLease(rdb),
		KV:        convredis.NewKV(rdb),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "refresh-licenses", reconcile.Every(10*time.Second), func(ctx context.Context) error {
		fmt.Println("refreshing licenses")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
