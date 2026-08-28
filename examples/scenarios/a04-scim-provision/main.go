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
	demoWindow     = 2 * time.Second
	tenantPageSize = 2
)

type tenant struct {
	id   reconcile.ID
	scim bool
}

type tenantRegistry struct{ all []tenant }

func (r *tenantRegistry) withSCIM(_ context.Context, cursor string) ([]reconcile.ID, string, error) {
	start := 0
	if cursor != "" {
		at, err := strconv.Atoi(cursor)
		if err != nil {
			return nil, "", fmt.Errorf("tenant page cursor %q: %w", cursor, err)
		}
		start = at
	}
	var page []reconcile.ID
	for i := start; i < len(r.all); i++ {
		if r.all[i].scim {
			page = append(page, r.all[i].id)
		}
		if len(page) == tenantPageSize {
			return page, strconv.Itoa(i + 1), nil
		}
	}
	return page, "", nil
}

type scimClient struct {
	mu          sync.Mutex
	provisioned []reconcile.ID
}

func (c *scimClient) reconcileTenant(_ context.Context, id reconcile.ID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provisioned = append(c.provisioned, id)
	return nil
}

func (c *scimClient) settled() []reconcile.ID {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := slices.Clone(c.provisioned)
	slices.Sort(out)
	return out
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	rt, err := converge.New(converge.Options{
		Namespace: "identity",
		MQ:        inmem.NewMQ(),
		Lease:     inmem.NewLease(),
		KV:        inmem.NewKV(),
		Observer:  converge.LogObserver(slog.Default()),
	})
	if err != nil {
		return err
	}

	scim := &scimClient{}
	tenants := &tenantRegistry{all: []tenant{
		{id: "t-acme", scim: true},
		{id: "t-basic", scim: false},
		{id: "t-globex", scim: true},
		{id: "t-initech", scim: true},
		{id: "t-trial", scim: false},
	}}

	err = reconcile.Register(rt, reconcile.Spec{
		Name:        "scim-provision",
		Reconcile:   scim.reconcileTenant,
		Triggers:    []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(tenants.withSCIM), reconcile.Every(5*time.Minute))},
		Concurrency: 16,
		Timeout:     time.Minute,
	})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), demoWindow)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		return err
	}

	fmt.Printf("tenants provisioned: %v\n", scim.settled())
	return nil
}
