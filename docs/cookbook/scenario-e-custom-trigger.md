# Scenario E: A custom trigger

Any wake source is ~20 lines. Redis pub/sub cache invalidation:

```go
type pubsubTrigger struct {
    rdb     *redis.Client
    channel string
}

func (t *pubsubTrigger) Run(ctx context.Context, wake func(reconcile.ID)) error {
    sub := t.rdb.Subscribe(ctx, t.channel)
    defer sub.Close()
    for {
        msg, err := sub.ReceiveMessage(ctx)
        if err != nil {
            return err // engine logs, backs off, restarts Run
        }
        // msg.Payload is the published ID string (a type conversion, see
        // the concepts guide's "IDs — work items" section).
        // wake = "engine, re-check this ID soon": non-blocking, cheap,
        // bounded — under overload wakes are dropped WITH a counted event
        // (safe: the schedule covers). Empty IDs are rejected + counted.
        wake(reconcile.ID(msg.Payload))
    }
}
```

Which replicas run it follows the job's run mode (see
[Reconcile → Triggers and the schedule](../guide/reconcile.md)) — the
trigger never needs to know. If the source dies, the job gets slower, never
wrong.
