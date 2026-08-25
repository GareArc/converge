package main

import (
	"context"
	"log"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func main() {
	exporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Second))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())

	observer, err := convotel.NewObserver(provider.Meter("converge-docs"))
	if err != nil {
		log.Fatal(err)
	}

	rt, err := converge.New(converge.Options{
		Lease:    inmem.NewLease(),
		KV:       inmem.NewKV(),
		Observer: observer,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Periodic(rt, "sync-inventory", reconcile.Every(500*time.Millisecond), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
