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
	demoWindow        = 2 * time.Second
	namespace         = "infra"
	customerPageSize  = 2
	readyAfterApplies = 3
)

type namespaceSpec struct {
	Name     string
	Replicas int
}

type namespaceTemplate struct{ replicas int }

func (t *namespaceTemplate) of(id reconcile.ID) namespaceSpec {
	return namespaceSpec{Name: "cust-" + string(id), Replicas: t.replicas}
}

type customerList struct{ ids []reconcile.ID }

func (l *customerList) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("customer page cursor %q: %w", cursor, err)
		}
		start = at
	}
	end := min(start+customerPageSize, len(l.ids))
	next := ""
	if end < len(l.ids) {
		next = strconv.Itoa(end)
	}
	return l.ids[start:end], next, nil
}

type cluster struct {
	slow map[string]bool

	mu      sync.Mutex
	applies map[string]int
	ready   map[string]bool
}

func newCluster(slow map[string]bool) *cluster {
	return &cluster{slow: slow, applies: map[string]int{}, ready: map[string]bool{}}
}

func (c *cluster) apply(_ context.Context, want namespaceSpec) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applies[want.Name]++
	if c.slow[want.Name] && c.applies[want.Name] < readyAfterApplies {
		return false, nil
	}
	c.ready[want.Name] = true
	return true, nil
}

func (c *cluster) report() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	lines := make([]string, 0, len(c.applies))
	for name, n := range c.applies {
		lines = append(lines, fmt.Sprintf("%s applies=%d ready=%t", name, n, c.ready[name]))
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

	desired := &namespaceTemplate{replicas: 2}
	k8s := newCluster(map[string]bool{"cust-c-5002": true})
	customers := &customerList{ids: reconcile.ToIDs("c-5001", "c-5002", "c-5003")}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "customer-namespace",
		Reconcile: func(ctx context.Context, id reconcile.ID) error {
			ready, err := k8s.apply(ctx, desired.of(id))
			if err != nil || ready {
				return err
			}
			return reconcile.CheckAgain{In: 15 * time.Second}
		},
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.IDsByPage(customers.page), reconcile.Every(30*time.Minute)),
			reconcile.Notifications(reconcile.NotificationsOpts{}),
		},
		Concurrency: 4,
		Timeout:     2 * time.Minute,
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
		notified <- p.Notify(ctx, "customer-namespace", "c-5004")
	}()

	if err := rt.Run(ctx); err != nil {
		return err
	}
	if err := <-notified; err != nil {
		return err
	}

	for _, line := range k8s.report() {
		fmt.Println(line)
	}
	return nil
}
