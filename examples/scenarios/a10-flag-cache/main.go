package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	convotel "github.com/GareArc/converge/adapters/otel"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

const (
	demoWindow     = 3 * time.Second
	metricInterval = 900 * time.Millisecond
	meterName      = "converge"
)

type flagCache struct {
	source map[string]bool

	mu      sync.Mutex
	cached  map[string]bool
	reloads int
}

func newFlagCache(source map[string]bool) *flagCache {
	return &flagCache{source: source}
}

func (c *flagCache) reload(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached = maps.Clone(c.source)
	c.reloads++
	return nil
}

func (c *flagCache) report() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	lines := make([]string, 0, len(c.cached)+1)
	for _, name := range slices.Sorted(maps.Keys(c.cached)) {
		lines = append(lines, fmt.Sprintf("%s=%t", name, c.cached[name]))
	}
	return append(lines, fmt.Sprintf("reloads: %d", c.reloads))
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	exporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		return err
	}
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(metricInterval))
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() {
		if shutdownErr := provider.Shutdown(context.Background()); err == nil {
			err = shutdownErr
		}
	}()

	meter := provider.Meter(meterName)
	observer, err := convotel.NewObserver(meter)
	if err != nil {
		return err
	}

	rt, err := converge.New(converge.Options{
		Namespace: "platform",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  observer,
	})
	if err != nil {
		return err
	}
	if err := convotel.RegisterGauges(meter, rt); err != nil {
		return err
	}

	flags := newFlagCache(map[string]bool{
		"checkout.express": true,
		"search.rerank":    false,
	})

	err = reconcile.Periodic(rt, "flag-cache", reconcile.Every(10*time.Second), flags.reload,
		reconcile.PeriodicOpts{RunMode: converge.OnAllReplicas})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	for _, line := range flags.report() {
		fmt.Println(line)
	}
	return nil
}
