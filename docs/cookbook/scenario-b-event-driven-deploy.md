# Scenario B: Event-driven deploy reconciler

> Assumes [chapter 03, triggers](../guide/03-triggers.md) and
> [chapter 07, stale writes](../guide/07-versions.md).

Composite IDs, version tracking, self-requeue.

*"Converge each app's runner deployment to its saved config. React to events
within seconds; guarantee convergence within minutes; protect against stale
replicas."*

```go
// package appdeploy
func NewRunnerReconciler(rt *converge.Runtime, db *DB, runner RunnerAPI, kv converge.KV) (*RunnerReconciler, error) {
    r := &RunnerReconciler{
        db:      db,
        runner:  runner,
        tracker: reconcile.NewTracker(kv, "app-runner"), // == Spec.Name, validated
    }
    err := reconcile.Register(rt, reconcile.Spec{
        Name:            "app-runner",
        Reconciler:      r,
        Concurrency:     8,
        DeadLetterAfter: 10,        // park hopeless IDs...
        Versions:        r.tracker, // ...revive them when intent changes
        Triggers: []reconcile.Trigger{
            reconcile.Schedule(reconcile.IDsByPage(r.pageDeployIDs), reconcile.Every(5*time.Minute)),
            reconcile.OnMessage("app-events",
                reconcile.IDFromJSONFields("tenant_id", "app_id"),
                reconcile.OnMessageOpts{}), // Group delivery — default for OnOneReplica
        },
    })
    if err != nil {
        return nil, err
    }
    return r, nil
}

func (r *RunnerReconciler) Reconcile(ctx context.Context, id reconcile.ID) error {
    tenantID, appID, err := reconcile.Split2(id)
    if err != nil {
        return err // malformed ID (bad poke input) — no panic
    }

    v, err := r.tracker.Latest(ctx, id) // version first, then truth (see the versions guide)
    if err != nil {
        return err
    }
    cfg, err := r.db.LoadDeployConfig(ctx, tenantID, appID)
    if err != nil {
        return err
    }

    status, err := r.runner.Apply(ctx, cfg, v) // runner checks v — prevention, not detection
    if err != nil {
        return err
    }
    if status.Rolling {
        return reconcile.CheckAgain{In: 10 * time.Second}
    }
    return r.tracker.MarkApplied(ctx, id, v)
}
```

Producer side (the API handler saving a deploy config):

```go
if _, err := r.tracker.MarkChanged(ctx, reconcile.JoinID(tenantID, appID)); err != nil {
    return err // must not be dropped — see the versions guide
}
publishAppEvent(ctx, tenantID, appID) // best-effort; the schedule covers message loss
```

See also: [7. Stale writes](../guide/07-versions.md) for the ordering rule
(`Latest` before truth, `v` as the write guard) this example follows.
