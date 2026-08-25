# Scenario A: Safety-net reconciler over all workspaces

> Assumes [chapter 02, many things to check](../guide/02-ids.md).

*"Every workspace must have its credentials synced; events might be missed;
re-check everything nightly."*

```go
// package credcheck
func NewReconciler(rt *converge.Runtime, repo *WorkspaceRepo) (*Reconciler, error) {
    r := &Reconciler{repo: repo}
    err := reconcile.Register(rt, reconcile.Spec{
        Name:       "workspace-credentials",
        Reconciler: r,
        Triggers: []reconcile.Trigger{
            reconcile.Schedule(
                reconcile.StringIDs(repo.AllWorkspaceIDs), // func(ctx) ([]string, error)
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
    ws, err := r.repo.Get(ctx, string(id))
    if err != nil {
        return err
    }
    return r.ensureCredentials(ctx, ws) // convergent: only writes what's missing
}
```

Defaults do the rest: one replica runs it, serially, with persisted last-fire
(a leader crash at 02:59 doesn't skip the night's pass).

See also: [2. Many things to check](../guide/02-ids.md) for the ID vocabulary this example
relies on.
