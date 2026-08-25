package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/inmem"
	"github.com/GareArc/converge/reconcile"
)

func tenantIDs(ctx context.Context) ([]string, error) {
	return []string{"acme", "globex"}, nil
}

func main() {
	rt, err := converge.New(converge.Options{
		Lease: inmem.NewLease(),
		KV:    inmem.NewKV(),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = reconcile.Register(rt, reconcile.Spec{
		Name: "sync-tenants",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			fmt.Println("checking tenant", id)
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(tenantIDs), reconcile.Every(2*time.Second)),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
