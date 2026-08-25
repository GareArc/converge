# Scenario D: Consuming a queue owned by another language

> Assumes [chapter 04, the other kind of job](../guide/04-worker.md).

*"The warehouse's Python service LPUSHes `{"type": "...", "sku": "..."}` onto
a frozen Redis list. We must react."*

Apply the decision test: the message is an ID plus a kind — a
*[hint](../glossary.md#hint)*. This is a reconciler, and the foreign list is
just a trigger source:

```go
// package stocksync — the frozen Redis key name is part of the contract
const StockEventListKey = "warehouse:stock:events"

func NewStockSync(rt *converge.Runtime, rdb *redis.Client, repo *CatalogRepo) (*StockSync, error) {
    s := &StockSync{repo: repo}
    err := reconcile.Register(rt, reconcile.Spec{
        Name:       "sync-inventory",
        Reconciler: s,
        Triggers: []reconcile.Trigger{
            convredis.ListTrigger(rdb, StockEventListKey, reconcile.IDFromJSONField("sku")),
            reconcile.Schedule(reconcile.StringIDs(repo.AllSKUs), reconcile.Every(10*time.Minute)),
        },
    })
    if err != nil {
        return nil, err
    }
    return s, nil
}
```

No ack machinery, no retry counters, no
[DLQ](../glossary.md#dead-letter-dlq) lists, no kind router — any message,
including kinds Python invents next year, collapses to "re-check SKU X";
a dropped message costs at most one schedule period.

When the foreign queue carries true verbs (data you cannot re-read) and you
*can't* change the producer, see the
[inbox pattern](outbox-inbox.md#inbox) — the outbox's mirror image.

See also: [`ListTrigger` reference](../reference/adapters.md#listtrigger).
