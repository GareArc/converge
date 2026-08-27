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
	pollStep         = 5 * time.Millisecond
	namespace        = "infra"
	customerPageSize = 2
	newCustomer      = reconcile.ID("c-5004")
)

type namespaceSpec struct {
	Name     string
	Replicas int
}

type namespaceTemplate struct{ replicas int }

func (t *namespaceTemplate) of(id reconcile.ID) namespaceSpec {
	return namespaceSpec{Name: "cust-" + string(id), Replicas: t.replicas}
}

type customerList struct {
	mu     sync.Mutex
	ids    []reconcile.ID
	listed bool
}

func newCustomerList(ids ...string) *customerList {
	return &customerList{ids: reconcile.ToIDs(ids...)}
}

func (l *customerList) page(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("customer page cursor %q: %w", cursor, err)
		}
		start = at
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	end := min(start+customerPageSize, len(l.ids))
	next := ""
	if end < len(l.ids) {
		next = strconv.Itoa(end)
	} else {
		l.listed = true
	}
	return slices.Clone(l.ids[start:end]), next, nil
}

func (l *customerList) add(id reconcile.ID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids = append(l.ids, id)
}

func (l *customerList) fullyListed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.listed
}

type cluster struct {
	converging map[string]bool

	mu       sync.Mutex
	applies  map[string]int
	rechecks map[string]int
	ready    map[string]bool
}

func newCluster(converging map[string]bool) *cluster {
	return &cluster{
		converging: converging,
		applies:    map[string]int{},
		rechecks:   map[string]int{},
		ready:      map[string]bool{},
	}
}

func (c *cluster) apply(_ context.Context, want namespaceSpec) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.applies[want.Name]++
	if c.converging[want.Name] {
		c.rechecks[want.Name]++
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
		lines = append(lines, fmt.Sprintf("%s applies=%d ready=%t rechecks-asked=%d", name, n, c.ready[name], c.rechecks[name]))
	}
	slices.Sort(lines)
	return lines
}

func awaitFirstSweep(ctx context.Context, rt *converge.Runtime, customers *customerList) error {
	select {
	case <-rt.Ready():
	case <-ctx.Done():
		return ctx.Err()
	}
	for !customers.fullyListed() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollStep):
		}
	}
	return nil
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
	customers := newCustomerList("c-5001", "c-5002", "c-5003")

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

	onboarded := make(chan error, 1)
	go func() {
		if err := awaitFirstSweep(ctx, rt, customers); err != nil {
			onboarded <- err
			return
		}
		customers.add(newCustomer)
		onboarded <- p.Notify(ctx, "customer-namespace", string(newCustomer))
	}()

	if err := rt.Run(ctx); err != nil {
		return err
	}
	if err := <-onboarded; err != nil {
		return err
	}

	for _, line := range k8s.report() {
		fmt.Println(line)
	}
	fmt.Println("rechecks-asked counts runs that returned reconcile.CheckAgain rather than failing")
	fmt.Printf("%s was added after the sweep had listed the customers, so its run came from Notify\n", newCustomer)
	return nil
}
