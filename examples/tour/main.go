package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GareArc/converge"
	convredis "github.com/GareArc/converge/adapters/redis"
	"github.com/GareArc/converge/debughttp"
	"github.com/GareArc/converge/reconcile"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	rt, err := converge.New(converge.Options{
		Lease: convredis.NewLease(rdb), // needed by the OnOneReplica run mode
		KV:    convredis.NewKV(rdb),    // engine state: last-fire times, dead-letter marks
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := reconcile.Periodic(rt, "license-refresh", reconcile.Every(time.Hour), refreshLicense); err != nil {
		log.Fatal(err)
	}

	http.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
	go http.ListenAndServe(":6060", nil)

	// Blocks until ctx cancels; then stops intake, drains in-flight work,
	// and releases leases. Returns nil on a clean shutdown.
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func refreshLicense(ctx context.Context) error {
	// re-read truth, converge, return nil on success
	return nil
}
