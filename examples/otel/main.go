package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type flaky struct{ n int }

func (f *flaky) Reconcile(ctx context.Context, id reconcile.ID) error {
	f.n++
	if f.n%3 == 0 {
		return errors.New("synthetic failure")
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	exp, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		return err
	}
	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(5*time.Second))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		if shutdownErr := mp.Shutdown(context.Background()); err == nil {
			err = shutdownErr
		}
	}()

	obs, err := convotel.NewObserver(mp.Meter("converge"))
	if err != nil {
		return err
	}

	rt, err := converge.New(converge.Options{
		Namespace: "otel-demo",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  obs,
	})
	if err != nil {
		return err
	}

	if err := reconcile.Register(rt, reconcile.Spec{
		Name:       "demo",
		Reconciler: &flaky{},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(
				reconcile.StringIDs(func(context.Context) ([]string, error) {
					return []string{"a", "b", "c"}, nil
				}),
				reconcile.Every(2*time.Second),
			),
		},
	}); err != nil {
		return err
	}

	fmt.Println("running; metrics print every 5s, Ctrl-C to stop")
	return rt.Run(ctx)
}
