# Where a job runs

Your service runs as four replicas. You register one job. How many times
does the work happen?

That is the one declaration both kinds of job share, and it has three
possible answers. Before the three, though, one thing converge deliberately
does not do.

## The thing converge will not do

**converge does not split one [reconcile](../glossary.md#reconcile) job's
[IDs](../glossary.md#id) across your replicas.** Ten thousand orders on four
replicas does not mean two and a half thousand each. One replica does all
ten thousand, and `Spec.Concurrency` is how many of them it does at once.

This is a decision, not a gap. Sharding IDs across replicas means agreeing
who owns which shard, rebalancing when a replica dies, and a whole class of
split-brain question that converge would then have to answer honestly on
every backend it supports. The answer it gives instead is: reconcile work is
usually waiting on something, so run more of it inside one process. If a
single replica genuinely cannot keep up with your ID source, converge is not
the tool for that job, and it would rather tell you than pretend.

Worker jobs *are* spread across replicas — that is what the third value
below is for. The non-goal is specific to splitting one reconcile job's IDs.

## Three values

A **run mode** is who runs a job across your replicas. It is
`Spec.RunMode` on a [reconcile](../glossary.md#reconcile) job and
`HandleOpts.RunMode` on a [worker](../glossary.md#worker) job, and there are
three of them.

| Value | Who runs it | Takes a [lease](../glossary.md#lease) |
| --- | --- | --- |
| `converge.OnOneReplica` | exactly one replica at a time | yes |
| `converge.OnAllReplicas` | every replica, independently | no |
| `converge.Competing` | every replica, sharing out the messages | no |

**One rule governs all three: `Competing` is a worker mode.** Setting it on
a reconcile job is refused when you register it, with the reason in the
error. It is illegal for a reason that is not arbitrary: `Competing` means
the replicas split a stream of messages between them, and on the reconcile
surface a message is only a [notification](../glossary.md#notification).
Splitting notifications is meaningless — the schedule already covers every
ID on whichever replica holds the job.

That is the only rule about which value goes with which kind of job. It is
not converge's only registration-time refusal, though: there is a second one
and it belongs to `OnAllReplicas`, below.

The defaults follow from the same idea. Reconcile jobs default to
`OnOneReplica`, because a reconcile job's work is defined by your store and
running it four times over just does the same work four times. Worker jobs
default to `Competing`, because messages are the work and splitting them is
how you go faster.

## OnOneReplica, and what a lease is for

Under `OnOneReplica` the replicas race for a lease. Whichever one takes
it runs the job; the others sit and wait. If the holder dies, the lease
expires — after `Options.LeaseTTL`, 30 seconds by default — and another
replica picks the job up.

A lease is an efficiency device and never a correctness device. There is a
window during which a replica believes it holds a lease that has in fact
expired elsewhere, and converge does not pretend otherwise. Duplicate work
is rare; it is not impossible. Correctness comes from your function being
safe to run twice, which is exactly what a reconcile function already is.

## OnAllReplicas, for things that live in the process

Some work is per-process by nature. A feature-flag cache lives in one
replica's memory, so every replica has to refresh its own:

```go
err = reconcile.Periodic(rt, "flag-cache", reconcile.Every(10*time.Second), flags.reload,
    reconcile.PeriodicOpts{RunMode: converge.OnAllReplicas})
```

That is
[`examples/scenarios/a10-flag-cache/main.go`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a10-flag-cache/main.go),
which runs two replicas inside one process so you can watch both of them
reload.

`OnAllReplicas` takes no lease and keeps no shared record of when it last
ran. On the worker surface it is deliberately narrow, and here is the second
refusal: **a broadcast worker job cannot set a `Retry` policy**, and
converge rejects that registration rather than accepting a promise it could
not keep. A failed run on a broadcast job is discarded — every replica had
its own copy and acknowledged it for itself, so there is no redelivery to
wait for and nothing durable to set aside. A retry budget would have nowhere
to land. Use `OnAllReplicas` for cache warming and local state, not for work
that must not be lost.

## Competing, the worker default

```go
var trackingEvent = worker.NewTask[TrackingEvent]("tracking-event", worker.TaskOpts{Version: 2})
err = worker.Handle(rt, trackingEvent, shipments.applyEvent, worker.HandleOpts{Concurrency: 32})
```

No `RunMode` here, so this is `Competing`: every replica reads the same
[inbox](../glossary.md#inbox), each message goes to exactly one of them, and
adding replicas adds throughput. `Concurrency` is per replica on top of that
— 32 in flight each, times however many replicas you run. That is
[`examples/scenarios/a09-tracking-events/main.go`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a09-tracking-events/main.go).

Nothing about ordering is promised, by converge or by this run mode. Two
messages for the same shipment can be handled at the same time, on different
replicas, in either order — which is why `applyEvent` in that program compares
timestamps before it writes. If your handler needs "the latest one wins",
write that yourself.

## What each run mode asks of your backend

A run mode is a demand on the transport, and converge checks it when the
job starts rather than when the first message arrives:

- `Competing` needs the backend to support consumer groups.
- `OnAllReplicas` needs the backend to support broadcast.
- `OnOneReplica` needs `Options.Lease`.

A worker job that is *not* broadcast asks for two more, whichever value it
uses: `Options.KV`, because that is where its [shelf](../glossary.md#shelf)
lives, and an MQ that can publish a message with a delay, because that is
how `Snooze` republishes one. (Ordinary retries do not need it — those go
back through the transport as a negative acknowledgement.)

Redis Streams and the `inmem` backend satisfy all of it. If a backend does
not, `rt.Run` fails at startup with a message naming the job and the missing
capability — not quietly, and not at 3am when the first message lands.

## Next

[Chapter 6](06-production.md) takes everything so far off `inmem` and onto
Redis, and covers what you can see once it is running.
