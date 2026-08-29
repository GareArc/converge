package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow      = 2 * time.Second
	statusPending   = "pending"
	statusCancelled = "cancelled"
)

type orderBook struct {
	now func() time.Time

	mu     sync.Mutex
	placed map[string]time.Time
	state  map[string]string
}

func newStore(now func() time.Time) *orderBook {
	return &orderBook{now: now, placed: map[string]time.Time{}, state: map[string]string{}}
}

func (b *orderBook) create(id string) { b.placeAt(id, b.now()) }

func (b *orderBook) placeAt(id string, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.placed[id] = at
	b.state[id] = statusPending
}

func (b *orderBook) status(id string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state[id]
}

func (b *orderBook) unpaidOlderThan(age time.Duration) func(ctx context.Context) ([]string, error) {
	return func(context.Context) ([]string, error) {
		cutoff := b.now().Add(-age)
		b.mu.Lock()
		defer b.mu.Unlock()
		var stale []string
		for id, at := range b.placed {
			if b.state[id] == statusPending && at.Before(cutoff) {
				stale = append(stale, id)
			}
		}
		slices.Sort(stale)
		return stale, nil
	}
}

func (b *orderBook) cancelIfUnpaid(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state[id] == statusPending {
		b.state[id] = statusCancelled
	}
	return nil
}

func expireUnpaidOrders(orders *orderBook) reconcile.Spec {
	return reconcile.Spec{
		Job:       reconcile.NewJob("expire-unpaid-orders", reconcile.JobOpts{}),
		Reconcile: func(ctx context.Context, id reconcile.ID) error { return orders.cancelIfUnpaid(ctx, string(id)) },
		Triggers: []reconcile.Trigger{reconcile.Schedule(
			reconcile.StringIDs(orders.unpaidOlderThan(30*time.Minute)), reconcile.Every(time.Minute))},
		Timeout: 10 * time.Second,
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := converge.New(converge.Options{
		Namespace: "shop",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	store := newStore(time.Now)
	store.placeAt("o-1001", time.Now().Add(-45*time.Minute))
	store.create("o-1002")

	if err := reconcile.Register(rt, expireUnpaidOrders(store)); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("o-1001 placed 45m ago: %s\n", store.status("o-1001"))
	fmt.Printf("o-1002 placed just now: %s\n", store.status("o-1002"))
	return nil
}
