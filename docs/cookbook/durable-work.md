# Work that takes a while

> Assumes [chapter 4, when the message is the work](../guide/04-worker.md).
> The programs are
> [`a08-transcode`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a08-transcode/main.go) and
> [`a09-tracking-events`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a09-tracking-events/main.go).

Transcoding a video is the awkward case for a queue. The work is
irreplaceable, so it belongs on the [worker](../glossary.md#worker) surface.
But one unit of it takes half an hour rather than half a second, it cannot
start until an upload that some other process is still writing has finished,
and the payload describing it will grow a field within the year. Each of
those three pulls on a different part of the surface.

## Not ready yet is not a failure

`a08-transcode` is handed an upload that may still be uploading:

```go
err = worker.Handle(rt, transcode, func(ctx context.Context, j TranscodeJob) error {
    if !uploads.complete(ctx, j.UploadID) {
        return worker.Snooze{In: 30 * time.Second}
    }
    return ffmpeg.run(ctx, j)
}, worker.HandleOpts{Concurrency: 1, Timeout: 30 * time.Minute})
```

Returning an error there would work, and it would be wrong. An error spends
one of the message's retries and puts it into failure backoff, so a
dependency that takes twenty minutes eats twenty minutes' worth of a budget
meant for things that are actually broken.

A [snooze](../glossary.md#snooze) costs nothing instead. The delivery is
acknowledged, the message is republished after your delay, and the
[logical attempt](../glossary.md#logical-attempt) is folded back into the
message's [envelope](../glossary.md#envelope) so it does not move. That is
also why the bound on a snooze is `Retry.MaxAge` and never `MaxAttempts`: on
a job of this shape, `MaxAge` is the setting that means something. Ask how
long you are willing to wait for the upload, and set that; the rest of
`RetryPolicy` is in the
[worker reference](../reference/worker.md#retrypolicy).

The exact bounds — how `MaxAge` clips the delay you asked for, and what
converge substitutes once a message has snoozed ten times — are in
[that page's outcomes section](../reference/worker.md#outcomes-snooze-discard-shelve).
Two things about them shape a job like this one.

**The snooze count belongs to the message, not to the replica.** It rides in
the envelope, so it survives redelivery, a restart and a move to another
replica: a message that has snoozed two hundred times is still at the far end
of the curve wherever it lands next. Exactly one thing clears it, and that is
a requeue off the [shelf](../glossary.md#shelf) — which starts the message
over in every other respect too.

**The [reconcile](../glossary.md#reconcile) surface's equivalent bound is a
different shape.** Its counter resets — ten deferrals at your delay out of
every eleven, indefinitely — where this one only ever climbs. Do not carry
one over.

Two more, about wiring rather than tuning:

- **It needs a transport that can publish with a delay.** Redis Streams and
  `inmem` both can. Every durable worker job is checked for that capability
  at `Run` whether or not it ever snoozes, and refused by name if the
  transport lacks it — which is better than finding out on the first slow
  upload.
- **A broadcast job cannot snooze.** Under `OnAllReplicas` there is nothing
  durable to republish to, so a `Snooze` is `Discarded` — and on Redis a
  delayed message is only released into the stream by a group consumer's
  poll, so it would never arrive anyway. Long dependency waits and
  `OnAllReplicas` do not go together.

## A thirty-minute run

Two kinds of [time limit](../glossary.md#time-limit) apply to a long run, and
only one of them is the job's.

`HandleOpts.Timeout` is the job's: thirty minutes here, after which the
handler's context is cancelled. converge also derives the transport's
redelivery window from it — `Timeout` plus a minute, or five minutes flat if
`Timeout` is unset — and then extends that window every third of it for as
long as your handler is still running. So a live run holds its message for
however long it runs. The window is what happens when the replica *dies*, not
a race your handler has to win.

`Options.DrainTimeout` is the runtime's, and it defaults to thirty seconds.
When the process is shutting down, in-flight runs get that long and are then
cancelled. A thirty-minute transcode gets thirty seconds. You can raise
`DrainTimeout` past your longest `Timeout`, but read what you are asking for:
every deploy now takes up to half an hour.

The better answer is to make the work survivable instead. A run the engine
took the context away from — a shutdown, a lost [lease](../glossary.md#lease),
a [stop condition](../glossary.md#stop-condition) firing — and which then
returns an error is settled **neutrally**: the
message is republished with its logical attempt folded back, so it has not
spent an attempt, and the next replica picks it up as if nothing had
happened. Nothing is reported, because nothing went wrong. Note the
boundary: your own `Timeout` is not one of those three. It cancels a context
derived from the run's, not the run's, so blowing the thirty minutes is an
ordinary failure that spends an attempt and backs off like any other. Write
the handler so that starting again is cheap — checkpoint progress in your own
store, skip what is already done — and a deploy in the middle of a transcode
costs you the unfinished part and nothing else.

The other two fields on that call are about the shape of the resource, not
the shape of the message. `Concurrency: 1` is right when the work saturates a
CPU; `RateLimit` is right when many messages share one downstream, and it is
a ceiling on the whole job **on this replica**, not per payload and not
cluster-wide.

## The two surfaces do not share outcomes

The reconcile surface has a value that looks exactly like `Snooze`, and
returning it here is the most expensive mistake on this page:

| Returned from | Value | What happens |
| --- | --- | --- |
| a worker handler | `reconcile.CheckAgain` | shelved immediately, reason `wrong surface` |
| a reconcile function | `worker.Snooze` | ordinary failure; the ID enters failure backoff with your value as the error |

Neither is quietly reinterpreted as the other surface's equivalent, and the
worker direction does not even spend a retry first — the message stops on the
first delivery. If a handler of yours is shelving everything with
`wrong surface`, that is what happened.

## Changing a payload that is already in flight

`a09-tracking-events` declares a schema version:

```go
var trackingEvent = worker.NewTask[TrackingEvent]("tracking-event", worker.TaskOpts{Version: 2})
```

The version rides in the message's envelope, and before a delivery is decoded
converge compares it to the handler's. **The comparison is exact, and it
fails in both directions.** A version 1 message reaching a version 2 handler
is shelved with reason `schema version`; so is a version 2 message reaching a
version 1 handler. Nothing is decoded, so nothing is guessed at.

Read that twice before planning a rolling deploy around it. During the roll,
both kinds of skew exist at once: replicas still on the old build shelve the
new messages, and replicas on the new build shelve whatever is left of the
old ones. And a requeue does not undo it — the version header survives a
requeue, so a message shelved for `schema version` goes straight back to the
[shelf](../glossary.md#shelf) unless the handler it lands on has caught up.

So use the version for what it is good at, and not as a migration tool:

- **Do not bump it for an additive change.** JSON ignores fields it does not
  know and leaves missing ones at their zero value, so adding an optional
  field needs no version at all. Bump when an existing field changes meaning
  or type — when decoding an old message into the new struct would produce a
  plausible, wrong answer.
- **When you do bump, prefer a new task name.** A name is a
  [queue](../glossary.md#queue): `tracking-event-v2` gets its own queue and
  its own handler, both run side by side, the old queue drains to empty on
  its own, and then you delete the old handler. Nothing is ever shelved for
  skew, because no message ever meets the wrong handler.
- **Treat a `schema version` shelf as a report, not an outage.** Each record
  keeps the payload and the headers, so you can see exactly which producer is
  still on the old shape. That is the whole value of the field: it turns a
  silent mis-decode into a
  [shelved message](../glossary.md#shelved-message) you can read.

One last thing that is not about versions but is on this page for the same
reason. `Competing` — the default here, with `Concurrency: 32` per replica —
promises nothing about order. Two events for the same shipment can be handled
at the same time on different replicas, which is why the program compares
timestamps before it writes. If your payload has a version *of the entity*
rather than of the schema, that comparison is yours to write; converge has no
per-key ordering to offer, on any backend.
