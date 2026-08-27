package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

const (
	demoWindow       = 2 * time.Second
	namespace        = "payments"
	merchantPageSize = 2
)

type merchantDirectory struct{ ids []reconcile.ID }

func newMerchantDirectory(ids ...string) *merchantDirectory {
	return &merchantDirectory{ids: reconcile.ToIDs(ids...)}
}

func (d *merchantDirectory) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("merchant page cursor %q: %w", cursor, err)
		}
		start = at
	}
	end := min(start+merchantPageSize, len(d.ids))
	next := ""
	if end < len(d.ids) {
		next = strconv.Itoa(end)
	}
	return d.ids[start:end], next, nil
}

type stripeSync struct {
	mu    sync.Mutex
	syncs map[reconcile.ID]int
}

func newStripeSync() *stripeSync { return &stripeSync{syncs: map[reconcile.ID]int{}} }

func (s *stripeSync) syncMerchant(_ context.Context, id reconcile.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncs[id]++
	return nil
}

func (s *stripeSync) report() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	lines := make([]string, 0, len(s.syncs))
	for id, n := range s.syncs {
		lines = append(lines, fmt.Sprintf("%s synced %d time(s)", id, n))
	}
	slices.Sort(lines)
	return lines
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mq := inmem.NewMQ()

	rt, err := converge.New(converge.Options{
		Namespace: namespace,
		MQ:        mq,
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	stripe := newStripeSync()
	merchants := newMerchantDirectory("m-1001", "m-1002", "m-1003")

	err = reconcile.Register(rt, reconcile.Spec{
		Name:      "merchant-stripe",
		Reconcile: stripe.syncMerchant,
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDsByPage(merchants.page), reconcile.Every(15*time.Minute)),
			reconcile.Notifications(reconcile.NotificationsOpts{}),
		},
		Concurrency: 8,
		Timeout:     20 * time.Second,
	})
	if err != nil {
		return err
	}

	p, err := converge.NewProducer(mq, converge.ProducerOpts{Namespace: namespace})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()

	notified := make(chan error, 1)
	go func() {
		<-rt.Ready()
		notified <- p.Notify(ctx, "merchant-stripe", "m-1004")
	}()

	if err := rt.Run(ctx); err != nil {
		return err
	}
	if err := <-notified; err != nil {
		return err
	}

	for _, line := range stripe.report() {
		fmt.Println(line)
	}
	return nil
}
