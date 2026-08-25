package testingguide

import (
	"context"
	"time"

	"github.com/GareArc/converge"
	"github.com/GareArc/converge/reconcile"
)

type Store struct {
	Synced []string
}

func Register(rt *converge.Runtime, store *Store) error {
	return reconcile.Register(rt, reconcile.Spec{
		Name: "sync-tenants",
		Reconciler: reconcile.Func(func(ctx context.Context, id reconcile.ID) error {
			store.Synced = append(store.Synced, string(id))
			return nil
		}),
		Triggers: []reconcile.Trigger{
			reconcile.Schedule(reconcile.StringIDs(func(ctx context.Context) ([]string, error) {
				return []string{"acme", "globex"}, nil
			}), reconcile.Every(time.Hour)),
		},
	})
}
