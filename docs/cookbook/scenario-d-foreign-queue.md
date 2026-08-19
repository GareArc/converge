# Scenario D: Consuming a queue owned by another language

*"The Python side LPUSHes `{"type": "...", "workspace_id": "..."}` onto a
frozen Redis list. We must react."*

Apply the decision test: the message is an ID plus a kind — a *hint*. This is
a reconciler, and the foreign list is just a trigger source:

```go
// package tasksync — the frozen Redis key name is part of the contract
const MemberSyncListKey = "enterprise:member:sync"

func NewMemberSync(rt *converge.Runtime, rdb *redis.Client, repo *MemberRepo) (*MemberSync, error) {
    s := &MemberSync{repo: repo}
    err := reconcile.Register(rt, reconcile.Spec{
        Name:       "member-sync",
        Reconciler: s,
        Triggers: []reconcile.Trigger{
            convredis.ListTrigger(rdb, MemberSyncListKey, reconcile.IDFromJSONField("workspace_id")),
            reconcile.Schedule(reconcile.StringIDs(repo.AllWorkspaceIDs), reconcile.Every(10*time.Minute)),
        },
    })
    if err != nil {
        return nil, err
    }
    return s, nil
}
```

No ack machinery, no retry counters, no DLQ lists, no kind router — any
message, including kinds Python invents next year, collapses to "re-check
workspace X"; a dropped message costs at most one schedule period.

When the foreign queue carries true verbs (data you cannot re-read) and you
*can't* change the producer, see the
[inbox pattern](outbox-inbox.md#inbox) — the outbox's mirror image.

See also: [`ListTrigger` reference](../reference/adapters.md#listtrigger).
