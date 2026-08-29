# A queue somebody else owns

> Assumes [chapter 3, telling a job to look sooner](../guide/03-notifications.md).
> The program is
> [`a14-foreign-queue`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a14-foreign-queue/main.go),
> and it is the one scenario that needs a real Redis.

Another team's service already pushes JSON onto a Redis list when a
workspace's credentials are rotated. You cannot change what it pushes, you
cannot ask it to add headers, and within a year it will be pushing message
types nobody has told you about. You still have to react.

Ask the question about *their* message: can you list the workspaces whose
credentials still need syncing without reading their list? Yes — the
workspaces are in your database, and you can re-read any of them whenever
you like. Their message is a
[notification](../glossary.md#notification) about which workspace to look at,
so this is a [reconcile](../glossary.md#reconcile) job and their list is one
of its triggers:

```go
Triggers: []reconcile.Trigger{
    reconcile.Schedule(reconcile.IDsByPage(workspaces.page), reconcile.Every(5*time.Minute)),
    reconcile.NotificationsFrom(foreignQueue, reconcile.NotificationsOpts{
        ID: reconcile.IDFromJSON("workspace_id"),
        MQ: convredis.NewListMQ(rdb),
    }),
},
```

That is the only place in the whole surface where you name a queue, and the
string is used exactly as you wrote it — `"enterprise:workspace:sync:queue"`,
not namespaced and not prefixed, because it is not converge's to name.

## The ID function is a firewall, not a parser

`reconcile.IDFromJSON("workspace_id")` is doing less than it looks like it is
doing, and that is the whole design. It is not decoding their message. It
takes the one field you are prepared to depend on and throws the rest away —
the type, the reason, the timestamps, the fields they add next quarter.

This is why the coupling survives their releases. There is no router keyed on
their `"type"` field to fall out of date, no payload struct of theirs living
in your repository, no version to negotiate. Every message they can ever
publish, including kinds that do not exist yet, means the same thing to you:
*look at this workspace*. Your function then reads your own store and
converges, exactly as it does after a [sweep](../glossary.md#sweep).

Two functions are supplied for this, and they are the only two:
`reconcile.IDFromJSON(field)` for a JSON object with a string field, and
`reconcile.RawID()` for a queue whose payload is just the identifier. If your
producer's shape needs more than that, write the function yourself — it is
`func(payload []byte) (reconcile.ID, error)` and nothing else.

## What happens to a message it rejects

This is the part worth being precise about, because it is not recoverable.

Every delivery on a notifications trigger is **acknowledged, decodable or
not**. On a Redis list the message is already gone before your ID function
even runs: the adapter pops it to receive it, and the pop is destructive.
There is no retry, no requeue, and nothing to look at afterwards.

`IDFromJSON` rejects a payload four ways: the bytes are not a JSON object
(which covers not being JSON at all), the field is missing, the field is not
a string, or the field is the empty string. `RawID` rejects one thing, an
empty payload. Both are in the
[reconcile reference](../reference/reconcile.md#id-functions-for-foreign-queues).
In every case converge reports a `NotificationDropped` event and moves on,
which `converge.LogObserver` renders as one warn-level line:

```text
WARN converge: notification dropped job=workspace-credentials id="" err="converge: notification: undecodable"
```

That line, and the `converge.notifications.dropped` counter, are the entire
record. **The payload is not kept anywhere** — the event carries the job, an
ID it does not have, and the error; the reconcile surface has no
[shelf](../glossary.md#shelf) to put it on. If you need to see what they
actually sent, converge is not going to keep it for you, and
[the outbox pattern's mirror image](outbox-inbox.md#the-inbox-table) is the
shape that will.

Two more drops get the same treatment, with a different error: an ID that
decodes to the empty string on a job whose source is not `SingleID`
(`converge: notification: empty id`), and a notification naming an ID
converge has never seen when the job is already tracking 65536 of them
(`converge: notification: inbox overflow`). The second of those is the only
drop that can tell you *which* ID it lost — the overflow line carries it,
where the other two report `id=""` because there is no decoded ID to
report.

None of the three is an incident, and that is the point of having chosen this
surface. The cost of any dropped message is bounded by the
[cadence](../glossary.md#cadence): five minutes, in the program above. If
that number is not an acceptable worst case, shorten it — do not go looking
for a way to make their queue reliable.

## What you can see

`convredis.NewListMQ` reports a [backlog](../glossary.md#backlog) through
`LLEN`, so `/debug/jobs` shows the depth of a queue you do not own. That is a
genuinely useful number: it is your lag behind their producer, and it is the
one thing on that page that is about *them*.

It is all or nothing across triggers. If a job has two notifications triggers
and either one cannot report a depth, the job's backlog is unknown rather
than a partial sum, because a partial sum would be a lie.

## The limits of a list

`NewListMQ` exists for this one job and is honest about the rest — the full
list is in the [adapters reference](../reference/adapters.md#list-mq):

- It has no consumer groups and no broadcast, so a job reading a foreign list
  runs under the default [run mode](../glossary.md#run-mode), `OnOneReplica`.
  Setting `OnAllReplicas` fails at `Run` with the missing capability named.
- `Publish` writes the payload and drops everything else, so a list cannot
  carry a worker message's headers.
- `EnqueuedAt` is the moment the payload was popped, not the moment it was
  written. Do not measure their latency with it.

## What you cannot do

**You cannot point a worker job at a foreign queue.** `worker.Handle` reads
the [inbox](../glossary.md#inbox) converge names after the job, and there is
no equivalent of `NotificationsFrom` on that surface. This is not an
oversight: a worker message has to arrive carrying an
[envelope](../glossary.md#envelope), and a queue somebody else writes does
not have one.

If their message really is a verb you cannot re-read — "charge this card",
not "this workspace changed" — you have two options and neither of them is a
setting:

- **The [inbox table pattern](outbox-inbox.md#the-inbox-table)**, which makes
  their message durable in a table of yours first, and is almost always the
  right answer.
- **Ask them to publish onto converge's inbox directly.** The queue name is
  visible in `/debug/jobs`, and the one header they must set is
  `converge.schema-version`; without it the message is set aside with reason
  `schema version` before your handler is entered, and without
  `converge.message-id` converge derives a synthetic `anon-` identity from
  the message's kind and payload. It works, and it makes another team's
  release schedule part of your wire format.

## When the source is not a queue at all

A trigger does not have to be a queue. `reconcile.Trigger` is one method —
`Run(ctx context.Context, notify func(ID)) error` — and anything that can
learn an identifier can implement it: a pub/sub subscription, a filesystem
watch, a long-poll against somebody's API. Call `notify(id)` for each one you
learn about, and return when `ctx` is done.

Three things about that seam are worth knowing before you rely on it, and
none of them is obvious from the signature:

- **Your `Run` is restarted for you**, under a bounded backoff from one
  second to one minute, for as long as the job is active. A transient
  disconnect does not need handling inside your loop.
- **The error you return is discarded.** Nothing logs it, no event carries
  it, and no metric counts the restart. A trigger that is failing every
  second looks exactly like one that is idle, so if you want to know, you
  have to say so yourself before you return.
- **An ID from a custom trigger is treated as a sweep, not as a
  notification.** It will queue an ID and it will pull an ID forward out of a
  `CheckAgain` delay, but it will *not* pull one out of failure backoff —
  that bypass belongs to `Notifications` and `NotificationsFrom` only. If you
  are writing a trigger specifically so an operator can retry a failing
  thing, send a notification instead.

The schedule is what makes the job correct while any of this is down, and
with a custom trigger that is not a figure of speech: a trigger whose backend
has been unreachable for an hour is invisible, and the job is merely slower.

## Running it

`a14-foreign-queue` talks to a real Redis at `127.0.0.1:6379`, or wherever
`REDIS_ADDR` points, and exits cleanly with a message if there is not one.
Point it at something disposable: on startup it deletes every key under its
own namespace and the demo list itself, so that each run starts from a known
state.
