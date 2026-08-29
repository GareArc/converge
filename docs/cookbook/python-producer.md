# Credential sync from a Python service

> Assumes [chapter 3, telling a job to look sooner](../guide/03-notifications.md)
> and [chapter 4, when the message is the work](../guide/04-worker.md).
> There is no scenario program for this page: the Go side is two
> declarations, and the interesting half is the `XADD` a service in another
> language writes.

A Python service owns workspaces. When one is created, deleted, or has its
credentials rotated, Go has to make provider credentials match. Today that
is one Redis list, a `WorkspaceSyncTask{Type, WorkspaceID}` struct, a cron
consumer with a lock, a processing list nothing recovers, three retries, and
a dead-letter queue nobody reads.

Here are the three shapes it becomes — two of them
[reconcile](../glossary.md#reconcile) jobs, one a
[worker](../glossary.md#worker) task — and how to pick. The exact bytes each
one puts on the wire are in the [wire reference](../reference/wire.md); this
page is about which of them you want.

## Shape A: reconcile, no verb

Ask the question: can you list the workspaces whose credentials still need
attention without reading the list? Yes — every verb Python sends is
derivable from the store. The workspace exists or it does not; the
credential is current or it is not. So this is a reconcile job, and the
message is a [notification](../glossary.md#notification):

```go
var credentials = reconcile.NewJob("workspace-credentials", reconcile.JobOpts{
    Notifications: "dify:workspace-credentials",
})

reconcile.Register(rt, reconcile.Spec{
    Job: credentials,
    Triggers: []reconcile.Trigger{
        reconcile.Schedule(reconcile.IDsByPage(workspaces.Page), reconcile.Every(10*time.Minute)),
        reconcile.Notifications(),
    },
    Reconcile: func(ctx context.Context, id reconcile.ID) error {
        ws, found, err := workspaces.Get(ctx, id)
        if err != nil {
            return err
        }
        if !found {
            return joins.RemoveAll(ctx, id)
        }
        return joins.EnsureAllocated(ctx, ws)
    },
})
```

The job declares its [notifications](../glossary.md#notifications) channel by
a name Python can read. Go builds a notifier once with
`credentials.NewProducer(rt.Scope())` and calls `Notify(ctx, workspaceID)`;
Python writes

```text
XADD dify:workspace-credentials * payload '{"id":"ws-1"}' enq '2026-08-28T12:00:00Z'
```

which in `redis-py` is the whole producer:

```python
import json
from datetime import datetime, timezone

import redis

r = redis.Redis(host="redis", port=6379)


def notify(workspace_id: str) -> None:
    r.xadd("dify:workspace-credentials", {
        "payload": json.dumps({"id": workspace_id}),
        "enq": datetime.now(timezone.utc).isoformat(),
    })
```

After a bulk import, send one `{"all":true}` payload to the same key instead
of one notification per workspace.

`enq` is not optional and there is no default: an entry the Streams adapter
cannot read a timestamp from is acknowledged and discarded without an event.
`datetime.now(timezone.utc).isoformat()` is the right shape; a naive
`datetime.now()` is not, because it has no offset.

Count what this deletes. The processing list and the dead-letter queue go,
because nothing on this channel is authoritative: a lost notification costs
ten minutes. The watermark goes, because the
[schedule](../glossary.md#schedule) lists from the store. The `default:`
branch on `Type` goes, because there is no verb. The missing existence check
goes, because the function looks first. The abandoned dual write goes,
because both writers now write the same thing to the same place.

## Shape B: worker, when the payload is the event

If Python emits something Go cannot re-derive — a one-time rotation token
that exists only in that message — the message is the work, and this is a
worker task with a declared [queue](../glossary.md#queue):

```go
var rotate = worker.NewTask[RotateRequest]("credential-rotate", worker.TaskOpts{
    Queue: "dify:credential:rotate",
})

worker.Handle(rt, rotate, func(ctx context.Context, r RotateRequest) error {
    return providers.Rotate(ctx, r.WorkspaceID, r.Token)
}, worker.HandleOpts{Retry: worker.RetryPolicy{MaxAttempts: 5}})
```

Python:

```python
r.xadd("dify:credential:rotate", {
    "payload": json.dumps({"workspace_id": workspace_id, "token": token}),
    "enq": datetime.now(timezone.utc).isoformat(),
})
```

Go builds a producer once with `rotate.NewProducer(rt.Scope())` and calls
`Enqueue(ctx, RotateRequest{...}, worker.EnqueueOpts{})`.

No `converge.*` header is needed, and Python is better off sending none. A
message with none is attempt one, gets a synthetic message ID derived from
its own bytes, and is timed from `enq`. The one header worth knowing about
is the one you should not guess at: `converge.schema-version` is compared
byte for byte against the Go side's decimal version. If you send it at all it
is the string `"1"` — `f"{version:02d}"` produces `01`, which is not a match,
and the message is shelved unread. Send nothing and you have made no claim.
What Python gains for free is everything the hand-built version never
finished: an entry stays pending until the handler acknowledges it, comes
back if the process dies, and lands on the [shelf](../glossary.md#shelf) —
readable, requeueable — after five attempts.

Two verbs means two tasks and two queues, one per verb. The producer chooses
the action by choosing the key; an unknown verb is impossible because there
is no key for it. Do not put a `type` field back and switch on it — if
Python genuinely cannot be changed to write two keys, one task whose payload
carries the verb and whose handler shelves unknown values is the fallback,
and it is the same code you have today plus a budget and a shelf.

## Shape C: two keys, when the payload is state but Redis is the store

Sometimes the payload really is state, and Redis is where Python keeps it.
Then Python does `SET dify:credential:<id> <json>` and notifies; the
reconcile function `GET`s by ID, applies it, and `DEL`s; the schedule `SCAN`s
`dify:credential:*` to list what is still pending. That is Shape A with a
second key, and it needs nothing from this page that Shape A does not.

## Picking

| the message is | shape |
| --- | --- |
| derivable from a store you can query | A: reconcile, declared notifications |
| the only copy of the work | B: worker, declared queue, one task per verb |
| state, but Redis is the store | C: reconcile, two keys |

If you find yourself wanting a `type` field on a Shape A notification, read
[the verb rule](../guide/03-notifications.md#a-notification-has-no-verb)
first.
