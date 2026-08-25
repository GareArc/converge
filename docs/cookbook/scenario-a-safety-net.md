# Scenario A: Safety-net reconciler over the whole catalog

> Assumes [chapter 02, many things to check](../guide/02-ids.md).

*"Every SKU's stock level must match the warehouse; stock events might be
missed; re-check everything nightly."*

```go
// package stockcheck
func NewReconciler(rt *converge.Runtime, repo *CatalogRepo) (*Reconciler, error) {
    r := &Reconciler{repo: repo}
    err := reconcile.Register(rt, reconcile.Spec{
        Name:       "sync-inventory",
        Reconciler: r,
        Triggers: []reconcile.Trigger{
            reconcile.Schedule(
                reconcile.StringIDs(repo.AllSKUs), // func(ctx) ([]string, error)
                reconcile.Cron("0 3 * * *", reconcile.CronOpts{}), // UTC
            ),
        },
    })
    if err != nil {
        return nil, err
    }
    return r, nil
}

func (r *Reconciler) Reconcile(ctx context.Context, id reconcile.ID) error {
    sku, err := r.repo.Get(ctx, string(id))
    if err != nil {
        return err
    }
    return r.ensureStockMatches(ctx, sku) // convergent: only writes what's wrong
}
```

Defaults do the rest: one replica runs it, serially, with persisted last-fire
(a leader crash at 02:59 doesn't skip the night's pass).

See also: [2. Many things to check](../guide/02-ids.md) for the ID
vocabulary this example relies on.
