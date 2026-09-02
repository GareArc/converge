# Outbox and inbox

> Assumes [chapter 3](../guide/03-notifications.md) and
> [chapter 4](../guide/04-worker.md). Nothing here can be shown end to end,
> because the half that matters is a database transaction converge does not
> own.

One sentence on vocabulary first. converge has no "inbox" of its own: a
[reconcile](../glossary.md#reconcile) job has
[notifications](../glossary.md#notifications) and a worker
task has a [queue](../glossary.md#queue), and those are the only two channels
it names. So *inbox table* on this page is unambiguous — it is the second
half of the outbox pattern, a table in your database, and it keeps the name
the literature gives it.

## The problem both patterns solve

converge's queue boundary does not span your database transaction, and it
cannot: `Enqueue` is a call to your message queue, `COMMIT` is a call to your
database, and nothing makes two remote calls atomic.

So there are two failures, and they are opposites:

- The enqueue succeeds and the transaction rolls back. A receipt is sent for
  an order that does not exist.
- The transaction commits and the enqueue fails, or the process dies between
  them. The order exists and nobody is ever told.

Moving less data does not help. Enqueueing a row ID instead of the payload
leaves the enqueue exactly where it was — outside the transaction — and the
same two failures remain. The fix is to make the *record of the intent* part
of the transaction, and to move it out afterwards. Both patterns below are
that idea, once on each side of the wire, and both are the two surfaces
composed: a [worker](../guide/04-worker.md) job for the unit of work, a
[reconcile](../glossary.md#reconcile) job for the drain.

Neither makes anything exactly-once. Both turn "might be lost" into "might be
delivered twice", which is the trade converge already asks you to be ready
for.

## The outbox

**In the same transaction as the domain write, insert the payload into an
outbox table.** Commit now means the work durably exists, and the transaction
is back to being one call to one system.

Then a small reconcile job drains it. Its IDs are the unsent rows:

```go
var outboxDrain = reconcile.NewJob("outbox-drain", reconcile.JobOpts{})

err = reconcile.Register(rt, reconcile.Spec{
    Job:       outboxDrain,
    Reconcile: outbox.sendOne,
    Triggers: []reconcile.Trigger{
        reconcile.Schedule(reconcile.IDsByPage(outbox.unsent), reconcile.Every(2*time.Second)),
        reconcile.Notifications(),
    },
    Concurrency: 8,
    Timeout:     30 * time.Second,
})
```

`sendOne` re-reads the row, enqueues it, and marks it sent — **in that
order**:

```go
func (o *outboxTable) sendOne(ctx context.Context, id reconcile.ID) error {
    row, ok, err := o.load(ctx, string(id))
    if err != nil || !ok || row.Sent {
        return err
    }
    if err := o.receipts.Enqueue(ctx, row.Receipt, worker.EnqueueOpts{}); err != nil {
        return err
    }
    return o.markSent(ctx, string(id))
}
```

`o.receipts` is `jobs.SendReceipt.NewProducer(rt.Scope())`, built once when
the drain is constructed. `Enqueue` is typed on the task's payload, so
whatever form the table holds a row in, the drain is where it becomes a
`ReceiptPayload` again.

Marking sent before enqueueing would turn a crash between the two into a lost
message. Enqueueing first turns the same crash into a duplicate, which the
handler's idempotency absorbs — and being safe to run twice is the one thing
converge asks of a handler anyway.

The committing side then says *look at this row sooner*, after the commit and
never before:

```go
if err := tx.Commit(); err != nil {
    return err
}
p.Notify(ctx, reconcile.ID(row.ID))
```

`p` is `outboxDrain.NewProducer(rt.Scope())` — the same job value the drain
registered, so the row is announced on the channel that job is reading and
nowhere else.

The [notification](../glossary.md#notification) is the right tool here for
exactly the reason it is a cheap one: it is allowed to be lost. Nothing
downstream depends on it arriving, so its error does not have to be handled,
retried, or logged as a failure. If it vanishes, the next
[sweep](../glossary.md#sweep) picks the row up.

Which makes the [cadence](../glossary.md#cadence) the number that matters.
Two seconds, above, is the outbox's real latency guarantee — the worst case
when a notification is lost — and it is not a number you can raise casually,
because you are choosing how late a receipt may be. Two consequences follow:
index the "unsent" predicate, since this query runs every two seconds
forever, and use `IDsByPage` rather than a list, since a backed-up outbox is
exactly when the table is largest.

Two more details worth settling once:

- **The drain runs on one replica.** That is the default
  ([`OnOneReplica`](../guide/05-run-modes.md)) and you want it: the table is
  the shared state, and eight IDs at a time in one process is plenty for a
  drain.
- **Rows are never deleted by converge.** Whether "sent" is a column you set
  or a row you delete is yours; either way, the ID stops being listed and the
  job forgets it existed.

## The inbox table

The mirror image, on the consuming side. Use it when a queue you do not own
carries a true verb — data you cannot re-read — and you cannot change the
producer. If their message merely names something you can look up, you do not
want this at all; you want
[a queue somebody else owns](foreign-queue.md), which is a much smaller
amount of machinery.

The pattern is two steps:

1. **A minimal consumer moves each foreign message into a durable inbox
   table.** That move is the only lossy step in the whole design, which is
   why it is kept as small as possible: read, insert, acknowledge, nothing
   else.
2. **A reconcile job converges the table**, exactly like the outbox drain
   above — one ID per unprocessed row, a schedule, and your own idempotent
   handler.

The order inside step 1 is the entire pattern: **commit the inbox row first,
acknowledge the foreign message second.** Acknowledging first turns a crash
between the two into a lost message. Committing first turns the same crash
into a duplicate row — so key the table on their message ID, or on whatever
idempotency key they give you, and make the insert conflict-safe
(`INSERT ... ON CONFLICT DO NOTHING`, or a duplicate-key error treated as
success). The duplicate collapses into a no-op and the retry still
acknowledges.

You write that consumer. `convredis.ListTrigger` is *not* it, and reaching
for it here is the mistake this section exists to prevent: that trigger
pops every element whether it decoded or not, and keeps nothing anywhere.
It is the right answer when losing a message costs latency and the wrong
one when losing a message costs the message.
