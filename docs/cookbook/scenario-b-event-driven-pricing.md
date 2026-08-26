# Scenario B: Event-driven repricing reconciler

> Assumes [chapter 03, reacting to events](../guide/03-triggers.md) and
> [chapter 07, stale writes](../guide/07-versions.md).

Composite IDs, version tracking, self-requeue.

*"Converge each region's shelf price to the price we saved. React to events
within seconds; guarantee convergence within minutes; protect against stale
replicas."*

```go
// package pricing
func NewPriceReconciler(rt *converge.Runtime, db *DB, pricing PricingAPI, kv converge.KV) (*PriceReconciler, error) {
    r := &PriceReconciler{
        db:      db,
        pricing: pricing,
        tracker: reconcile.NewTracker(kv, "apply-price-change"), // == Spec.Name, validated
    }
    err := reconcile.Register(rt, reconcile.Spec{
        Name:            "apply-price-change",
        Reconciler:      r,
        Concurrency:     8,
        DeadLetterAfter: 10,        // park hopeless IDs...
        Versions:        r.tracker, // ...revive them when intent changes
        Triggers: []reconcile.Trigger{
            reconcile.Schedule(reconcile.IDsByPage(r.pageSKUs), reconcile.Every(5*time.Minute)),
            reconcile.OnMessage("price-events",
                reconcile.IDFromJSONFields("region", "sku"),
                reconcile.OnMessageOpts{}), // Group delivery — default for OnOneReplica
        },
    })
    if err != nil {
        return nil, err
    }
    return r, nil
}

func (r *PriceReconciler) Reconcile(ctx context.Context, id reconcile.ID) error {
    region, sku, err := reconcile.Split2(id)
    if err != nil {
        return err // malformed ID (bad poke input) — no panic
    }

    v, err := r.tracker.Latest(ctx, id) // version first, then truth (see chapter 7, stale writes)
    if err != nil {
        return err
    }
    price, err := r.db.LoadPrice(ctx, region, sku)
    if err != nil {
        return err
    }

    status, err := r.pricing.Apply(ctx, price, v) // the API checks v — prevention, not detection
    if err != nil {
        return err
    }
    if status.Propagating {
        return reconcile.CheckAgain{In: 10 * time.Second}
    }
    return r.tracker.MarkApplied(ctx, id, v)
}
```

Producer side (the API handler saving a new price):

```go
if _, err := r.tracker.MarkChanged(ctx, reconcile.JoinID(region, sku)); err != nil {
    return err // must not be dropped — see chapter 7, stale writes
}
publishPriceEvent(ctx, region, sku) // best-effort; the schedule covers message loss
```

See also: [7. Stale writes](../guide/07-versions.md) for the ordering rule
(`Latest` before truth, `v` as the write guard) this example follows.
