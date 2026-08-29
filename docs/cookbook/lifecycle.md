# Jobs that end

> Assumes [chapter 6, taking it to production](../guide/06-production.md),
> which shows the whole program and watches it stop. That program is
> [`a12-legacy-migration`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a12-legacy-migration/main.go).

A password-hash migration is a job with an end in it. Every credential still
on `sha1` has to be rehashed, and one day there will be none left, and after
that the job is a query returning nothing forever.

converge gives you one way to say so — a
[stop condition](../glossary.md#stop-condition) on the registration — and it
is worth being deliberate about, because destruction is terminal and there is
no route, verb or handler that undoes it.

## First: does it need to end?

Often, no. A migration whose ID source goes empty is already finished in
every way that matters: it does no work, it sweeps once a period, and it
costs one indexed query. Leaving it running has a real advantage — the job
*is* the proof that nothing is left, and if a row somehow comes back it is
handled rather than missed.

Declare a stop condition when one of these is true instead:

- The query is expensive enough that running it forever is not free.
- A cutover is what actually ends the work — after that moment, migrating a
  row would be wrong rather than merely pointless.
- The job must stop before the code is removed, and those two are separate
  deployments.

## Two forms, and how to choose

```go
Until: converge.Deadline(cutover)
Until: converge.StopKey("migration/credentials-finished")
```

**`Deadline(t)`** is for an end you know in advance. It needs no cooperation
from anybody: the first time an engine checks at or after `t`, the job stops
and converge writes its own [tombstone](../glossary.md#tombstone) so the
decision survives restarts. A cutover date is the case it was built for.

**`StopKey(key)`** is for an end somebody has to *decide*. The job stops once
that key exists in `KV`: a raw key used exactly as you wrote it, whose
presence is the whole signal, and which *is* the tombstone. The
[operations reference](../reference/operations.md#destroying-a-job) has the
rules; two of their consequences are worth planning around.

This is the only lever an operator has over a job's life, and it is
deliberately not an HTTP route. Setting the key means having access to the
`KV`, and that is the whole of the ops surface for ending a job.

## One-way, and slow to be noticed

It is also a lever that does not come back. Destruction latches in
memory the moment a replica sees the tombstone, so deleting the key
afterwards does not restore the job on any process that already stopped it —
it only lets a *fresh* process start the job again, which is the worst of
both. Retiring a job for real is a code change, and a separate deployment
from the one that set the key. Which is why the question at the top of this
page is worth answering honestly: a stop condition you did not need is a
decision you cannot take back.

Nothing pushes the decision, either. Every engine polls for the tombstone, so
there is a gap between writing the key and the job stopping, and how long
depends on the surface and the [run mode](../glossary.md#run-mode). Three of
the four combinations check on a clock tick — the
[lease](../glossary.md#lease) heartbeat at `LeaseTTL/3`, ten seconds at the
default, or a worker consumer's own thirty seconds. The fourth is the one to
plan around: a [reconcile](../glossary.md#reconcile) job on `OnAllReplicas`
has no lease heartbeat to piggyback on and checks only at the start of each
[sweep](../glossary.md#sweep), so its [cadence](../glossary.md#cadence) *is*
its detection latency. Set a stop key on a broadcast job that sweeps hourly
and you may wait an hour.

## What happens to work that keeps arriving

This is the part that bites, because nothing about it is loud.

**The ID source is never walked again.** A row that becomes eligible after
the job is destroyed is simply never reconciled. There is no error, no
event, and no number that moves.

**Producers keep succeeding.** `Notify` and `Enqueue` publish to a job's
channel — its [notifications](../glossary.md#notifications) or its
[queue](../glossary.md#queue) — without asking whether anything is reading
it, so a code path that still sends to a destroyed job goes on working,
silently, forever. The messages accumulate.

**And converge stops reporting the pile.** A destroyed job is no longer
active on any replica, so it stops polling: `BacklogKnown` and
`ShelvedKnown` — the depth of the queue, and the depth of the job's
[shelf](../glossary.md#shelf) — go false and stay false. The
[backlog](../glossary.md#backlog) growing behind a destroyed job is exactly
the number converge will not show you. The gap is honest, but it means the
queue itself, not `/debug/jobs`, is where you find out. On Redis Streams it
also means `StreamsOpts.Retention` decides whether that pile is trimmed by age
or grows until somebody notices.

The conclusion is a deployment rule rather than a setting: **retire the
senders in the same release as the job, or before it.** A job's stop
condition ends the consumer; it does not close the door.

## Destruction does not drain

Ending a job is not a shutdown, and it does not behave like one.
`Options.DrainTimeout` is the grace period for a cancelled *runtime*. When a
stop condition fires, the job stops taking new work and cancels what is in
flight **immediately**.

What that costs depends on the surface:

- A reconcile run that is cancelled this way is settled neutrally: the ID
  goes back in line for a job that will never run again, and no
  `RunCompleted` is reported at all. The work simply did not happen.
- A worker delivery in flight is also settled neutrally: the message is
  republished with its
  [logical attempt](../glossary.md#logical-attempt) folded back, so it has
  not spent an attempt — into a queue nothing will read again.

So a job whose runs are long, or whose queue is not empty, should be
destroyed at a moment you chose. For a worker job that means draining it
first: watch the backlog reach zero, *then* write the stop key.

## The shelf outlives the job

A destroyed worker job stops reporting its shelf depth, but the records are
still in `KV`. They do not expire and nothing collects them.

That is recoverable rather than lost, because `worker.ShelfFrom` does not
need a running runtime: a small binary that calls `converge.New` with the
same `Namespace`, `KV` and `MQ`, registers nothing and never calls `Run` can
still list, read and purge them.

```go
shelf, err := worker.ShelfFrom(rt, "legacy-import")
if err != nil {
    return err
}
if err := shelf.PurgeAll(ctx); err != nil {
    return err
}
```

Decide which you want before you destroy the job. Purging is throwing away
evidence; keeping is leaving records in `KV` that nothing is watching. Both
are defensible; drifting into the second one by accident is not.

## Run returns

`Runtime.Run` returns once every job has ended, and a destroyed job counts as
ended. If the migration is the only job in its binary, `rt.Run(ctx)` comes
back with `nil` the moment the cutover passes — before anybody cancelled the
context.

That is a clean end and not a failure, and it is exactly what you want from a
one-purpose migration binary. In a service that is also serving traffic, it
means whatever your `main` does after `Run` returns now happens on a Tuesday
afternoon. Look at that line before you add `Until` to a job in a
long-running process.
