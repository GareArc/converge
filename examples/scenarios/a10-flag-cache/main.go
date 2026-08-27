package main

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
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
	demoWindow     = 12 * time.Second
	metricInterval = 2500 * time.Millisecond
	meterName      = "converge"
	namespace      = "platform"
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

func (c *flagCache) snapshot() (string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pairs := make([]string, 0, len(c.cached))
	for _, name := range slices.Sorted(maps.Keys(c.cached)) {
		pairs = append(pairs, fmt.Sprintf("%s=%t", name, c.cached[name]))
	}
	return strings.Join(pairs, " "), c.reloads
}

type pod struct {
	name  string
	rt    *converge.Runtime
	flags *flagCache
}

func newPod(name string, opts converge.Options, provider *sdkmetric.MeterProvider, source map[string]bool) (*pod, error) {
	meter := provider.Meter(meterName + "/" + name)
	observer, err := convotel.NewObserver(meter)
	if err != nil {
		return nil, err
	}
	opts.Observer = observer

	rt, err := converge.New(opts)
	if err != nil {
		return nil, err
	}
	if err := convotel.RegisterGauges(meter, rt); err != nil {
		return nil, err
	}

	flags := newFlagCache(source)

	err = reconcile.Periodic(rt, "flag-cache", reconcile.Every(10*time.Second), flags.reload,
		reconcile.PeriodicOpts{RunMode: converge.OnAllReplicas})
	if err != nil {
		return nil, err
	}
	return &pod{name: name, rt: rt, flags: flags}, nil
}

func (p *pod) report() []string {
	cached, reloads := p.flags.snapshot()
	stats := p.rt.Stats()
	lines := make([]string, 0, len(stats))
	for _, s := range stats {
		lines = append(lines, fmt.Sprintf("%s %s run_mode=%s lease_held=%t reloads=%d cached=[%s]",
			p.name, s.Job, s.RunMode, s.LeaseHeld, reloads, cached))
	}
	return lines
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (err error) {
	exporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint(), stdoutmetric.WithWriter(os.Stderr))
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

	source := map[string]bool{
		"checkout.express": true,
		"search.rerank":    false,
	}
	shared := converge.Options{
		Namespace: namespace,
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
	}

	pods := make([]*pod, 0, 2)
	for _, name := range []string{"pod-1", "pod-2"} {
		p, err := newPod(name, shared, provider, source)
		if err != nil {
			return err
		}
		pods = append(pods, p)
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	failures := make([]error, len(pods))
	var running sync.WaitGroup
	for i, p := range pods {
		running.Add(1)
		go func() {
			defer running.Done()
			failures[i] = p.rt.Run(ctx)
		}()
	}
	running.Wait()
	if err := errors.Join(failures...); err != nil {
		return err
	}

	for _, p := range pods {
		for _, line := range p.report() {
			fmt.Println(line)
		}
	}
	return nil
}
