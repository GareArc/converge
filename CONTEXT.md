# Converge

One model for all background work: a level-triggered reconcile surface and
an edge-triggered worker surface on one hexagonal kernel. This file is the
project's canonical language. Every exported name, error string, event
field, and page of documentation is written in these words and no others,
and the _Avoid_ lines are binding — a word listed there is wrong in code, in
prose, and in messages, except in a sentence whose job is to say that the
concept is gone. One name on this page is not converge's to change: the
*inbox table* in the outbox pattern is another system's table, and it keeps
the name the literature gives it.

## Language

### The two surfaces

**Job**:
A registered unit of background work on either surface, identified by the
unique name it is registered under. Jobs are static: they are declared in
code before `Run` and cannot be added or removed while the process is
running, because a job that appeared on one replica would be invisible to
the others.
_Avoid_: cron entry, background task (as the umbrella term)

**Surface**:
Which of the two models a job runs on — reconcile or worker. One question
decides it: can you write a query that lists everything still to be done,
without reading the queue? Yes means reconcile, no means worker. (Its
mirror — if this message were lost, would anything be wrong? — is the
explanation, not the test.) The choice settles everything else about the
job: what starts a run, what the function is handed, and what a failure
costs.
_Avoid_: mode, engine (as a user-facing term)

**Reconcile**:
The level-triggered surface: the truth lives in the caller's store, and the
function re-reads that store and makes the world match it. A message on
this surface is only a notification about which ID to look at, never the
work itself, so losing one costs latency and never correctness.
_Avoid_: controller, sync loop, resync

**Worker**:
The edge-triggered surface: the message is the work. It is delivered at
least once, retried under the job's retry policy, and shelved when that
budget is spent.
_Avoid_: consumer, job queue

**Task**:
A typed worker contract — name, payload type, schema version, and the queue
its messages travel on — shared between the producer and the consumer, so
the two cannot drift without the compiler noticing. The queue is derived
from the name unless the task declares one; either way it is a property of
the task, printable for a producer that cannot import it.
_Avoid_: job (that is the umbrella term)

### Reconcile concepts

**ID**:
The name of one unit of reconcile work — one SKU, one customer, one region.
An ID is the only thing that starts and stops dynamically: it enters the
caller's source of truth, is reconciled for as long as it is there, and
leaves. Work that runs "until X is done" is the ID `X` in a static job, not
a job of its own.
_Avoid_: key, workqueue key, Redis key

**Notification**:
A message that says "look at this ID sooner". Lossy by contract,
deduplicated, unordered, and it resets that ID's backoff — a notification is
new information, so the ID runs at the next opportunity instead of serving
out a penalty computed before the notification arrived. That reset is the
only bypass in the library; there is no separate operator verb, and the
schedule is what makes losing a notification harmless.
_Avoid_: wake, hint, poke, trigger (as a verb), signal

**Sweep**:
One walk of a job's ID source, queueing every ID it yields. A sweep queues
work; it does not run it, and a sweep still in progress when the next one
falls due is reported as an overrun rather than stacked on top.
_Avoid_: pass, tick, cycle

**Trigger**:
An independent source of pending IDs for a reconcile job, all of them
feeding one deduplicated queue. A trigger only ever names an ID; it never
runs the function itself and never carries instructions.
_Avoid_: subscription, listener

**Schedule**:
The trigger that sweeps the whole ID source once per cadence period — the
correctness guarantee, and the one trigger every reconcile job must have.
Every other trigger is a latency accelerator: switch them all off and the
job is slower to notice a change, but it still converges.
_Avoid_: resync, cron (for the concept), sweep (that is the action, not the
trigger)

**Cadence**:
When the schedule fires: `Every` or `Cron`. A missed firing has one
behavior and no option — if one or more scheduled times were missed while
the job was not running, the job runs once on return and then resumes the
cadence.
_Avoid_: interval, frequency

**Version**:
A counter on intent, read before the function reads anything else and
carried with whatever the function writes, so a write based on state that
has since changed can be refused instead of applied. Converge stores no
versions of its own: a `VersionSource` is the caller's, typically a column
that already moves when the intent for an ID changes.
_Avoid_: fence, fencing token, generation, tracker

**Failing ID**:
An ID whose last run failed and which is serving out backoff. Backoff is
bounded, so a failing ID keeps retrying at a floor rate rather than being
benched; a notification resets it to run at once. The count is reported as
`JobStats.Failing`, and the individual IDs with their last error are
readable from the debug handler.
_Avoid_: parked, shelved (that word is the worker's)

### Worker concepts

**Message ID**:
The identity header a producer mints once, at enqueue
(`converge.message-id`). It is carried unchanged through every retry, every
snooze republish, and a requeue off the shelf, so it is the single value
that follows one piece of work from end to end in logs.
_Avoid_: correlation ID, trace ID

**Logical attempt**:
How many times converge has genuinely tried a message's work: the
`converge.attempt` header's base plus the current transport delivery count.
It is the number the handler is shown and the number `Retry.MaxAttempts` is
measured against. A snooze folds the delivery back into the header, so it
never advances the logical attempt.
_Avoid_: retry count

**Transport delivery**:
`Delivery.Attempt()` — how many times the MQ port has redelivered this
in-flight message; it climbs with every Nack. Republishing a message as a
fresh one resets its transport delivery to one, and the logical attempt is
what survives that reset.
_Avoid_: attempt (ambiguous alone — say logical attempt or transport
delivery)

**Snooze**:
A worker outcome: acknowledge the current delivery, republish it after a
delay, and leave the logical attempt untouched. A snooze costs no retries,
so it is bounded by `Retry.MaxAge` and never by `Retry.MaxAttempts`.
_Avoid_: delay, reschedule

**Envelope**:
The `converge.*` header protocol on a worker message — seeded once at
enqueue, folded on snooze and neutral republish so the logical attempt
survives, and reset to fresh (attempt zero, no snoozes) by a requeue off
the shelf. One module owns every read and write of these headers, so the
kernel and the surfaces cannot drift.
_Avoid_: wire format, raw headers

**Shelf**:
Worker-only: the durable store a message is set aside in when it will not
be tried again — its attempts or its age ran out, its schema version did
not match the handler's, its payload would not decode, the handler returned
the wrong surface's `CheckAgain`, or the handler returned `Shelve`. Nothing
leaves the shelf on its own; a requeue is a deliberate act by a person.
_Avoid_: parked, quarantine, poison queue

**Shelved message**:
What a message on the shelf is kept as: its payload, its headers, the
reason it stopped, and when that happened. The reason is a plain string:
one of `max attempts`, `max age`, `schema version`, `undecodable`,
`wrong surface`, or the handler's own `Shelve.Reason`.
_Avoid_: shelf entry, failed message

### Kernel concepts

**Notifications**:
A reconcile job's channel: where its notifications arrive. It carries
pointers — an ID, or "all" — and never the work, so it may be lost, flushed,
or trimmed and the sweep covers it. Declared on the job as
`JobOpts.Notifications`, otherwise derived as
`<namespace>/converge/notifications/<job>`; the verb is `Notify`, the
trigger is `Notifications()`, and a source some other system writes is read
with `convredis.ListTrigger`. *Channel* is the umbrella word for this and for a
queue; it names no third thing.
_Avoid_: inbox, topic, stream, queue (that word is the worker's)

**Queue**:
A worker task's channel: where its messages wait. It carries the only copy
of the work, so it may not be lost — pending until acknowledged, retried,
shelved when the retries run out. Declared on the task as `TaskOpts.Queue`,
otherwise derived as `<namespace>/converge/queue/<task>`; the verb is
`Enqueue`. The `MQ` port's `queue` parameter is the transport underneath
both channels and is not this word.
_Avoid_: inbox, topic, list, stream (transport-specific words),
notifications (that word is reconcile's)

**Run mode**:
Who runs a job across replicas: `OnOneReplica` (one replica holds a lease
and runs it), `OnAllReplicas` (every replica runs it, no lease), or
`Competing` (replicas share the queue and split its messages). There is one
rule: `Competing` is a worker mode.
_Avoid_: posture, delivery mode, leader election (that names one
implementation)

**Outcome**:
How one run ended: succeeded, retrying, deferred, discarded, or shelved. It
is what an observer is told on `RunCompleted`. Retrying is what a returned
error means; the last three are reached by returning a control-flow value
instead, and those are reports to the engine, not failures.
_Avoid_: sentinel error, status code

**Lease**:
The device that keeps duplicate work rare under `OnOneReplica`: one replica
holds it and runs the job, the others wait, and it expires if the holder
dies. An efficiency device, never a correctness device — correctness comes
from idempotency and versions.
_Avoid_: lock, distributed lock

**Time limit**:
How long one run may take, after which its context is cancelled. It is
spelled `Timeout` on every surface, zero means no limit, and on the worker
surface it is also what the engine derives redelivery from.
_Avoid_: visibility, deadline (that word belongs to `Deadline(t)`), lease
TTL

**Stop condition**:
The declared reason a job is destroyed: a deadline or a stop key. A job has
three cluster-wide states — not started, active, destroyed — and
destruction is terminal, declared where the job is registered, and needs no
cooperation from the function.
_Avoid_: pause, disable, detach

**Tombstone**:
The durable KV marker that a job is destroyed. The engine writes it when a
deadline passes; on a stop key the operator's key is the tombstone. Engines
read it at lease acquire, at each lease renewal, and at each sweep start,
so a destroyed job stays destroyed across restarts until its code is
removed.
_Avoid_: kill switch, disable flag, soft delete

**Backlog**:
The real depth of a job's channel — its notifications or its queue — read
from the MQ rather than counted in process. Only some adapters can report
it, so it always travels with a known flag: not known is not the same as
zero, and neither the stats nor the metrics invent a number for it.
_Avoid_: queue depth, lag

**Replica ID**:
The random ID a `Runtime` mints for itself at construction, so one running
copy of a service can be told apart from the others wherever converge has
to name a process. The library invents it rather than borrowing a name the
deployment supplies, so nothing depends on how instances happen to be
named.
_Avoid_: instance ID, pod name (deployment-specific words)
