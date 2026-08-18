# Converge

One model for all background work: a level-triggered reconcile surface and an
edge-triggered worker surface on one kernel. This glossary is the project's
canonical language; the terminology map in the user guide links these terms
to their names in the literature.

## Language

### The two surfaces

**Job**:
A registered unit of background work on either surface, identified by its
unique name (`Spec.Name` or task name).
_Avoid_: cron entry, background task (as the umbrella term)

**Surface**:
Which of the two models a job runs on — reconcile or worker.
_Avoid_: mode, engine (as a user-facing term)

**Reconcile**:
The level-triggered surface: given an ID, the handler re-reads truth and
converges state; messages are only hints.
_Avoid_: controller, sync loop, resync

**Worker**:
The edge-triggered surface: the message is the work itself, delivered
at-least-once with retry and dead-lettering.
_Avoid_: consumer, job queue

**Task**:
A typed worker contract — name, payload type, queue — shared between
producer and consumer.
_Avoid_: job (that is the umbrella term)

### Reconcile concepts

**ID**:
The name of one unit of reconcile work (one workspace, one app, one deploy).
_Avoid_: key, workqueue key, Redis key

**Wake**:
Anything that makes an ID due to run: a hint, a poke, a schedule visit, or a
version change.

**Hint**:
A trigger-sourced wake — "re-check this ID". Lossy by design; never bypasses
backoff.
_Avoid_: event, notification

**Poke**:
A wake carrying human or producer intent. Bypasses backoff and revives
parked IDs.
_Avoid_: force-sync, kick

**Trigger**:
An independent source of wakes feeding a job's dedup queue.
_Avoid_: subscription, listener

**Schedule**:
The periodic trigger that visits every ID each period — the correctness
guarantee; every other trigger is a latency accelerator.
_Avoid_: resync, sweep, cron (for the concept)

**Cadence**:
When the schedule fires: `Every` or `Cron`.
_Avoid_: interval, frequency

**Pass**:
One full scheduled visit of a job's ID space.
_Avoid_: sweep, scan

**Parked**:
Reconcile-only: an ID benched after `DeadLetterAfter` consecutive failures.
Revived by a poke or a version change.
_Avoid_: dead-lettered (that is the worker term)

**Version**:
A counter on intent, used to refuse stale writes and revive parked IDs.
_Avoid_: fence, fencing token, generation

**Tracker**:
The KV-backed version source converge provides; jobs with their own version
column implement `VersionSource` instead.

### Worker concepts

**Queue**:
A named transport channel on the MQ port.
_Avoid_: topic, list, stream (transport-specific words)

**Message ID**:
The identity header a producer mints once, at enqueue
(`converge.message-id`). Carried unchanged through every retry and snooze
republish; surfaces to the handler as the worker `Run.ID`.
_Avoid_: correlation ID, trace ID

**Logical attempt**:
What `Meta.Attempt` reports: the `converge.attempt` header's base plus the
current transport delivery count. A snooze folds the delivery back into the
header, so it never advances the logical attempt.
_Avoid_: retry count

**Transport delivery**:
`Delivery.Attempt()` — how many times the MQ port has redelivered this
in-flight message; climbs with every Nack. A snooze republishes as a fresh
message, so its transport delivery resets to one — the logical attempt is
what survives that reset.
_Avoid_: attempt (ambiguous alone — say logical attempt or transport
delivery)

**Snooze**:
A worker outcome: Ack the current delivery, republish it after a delay,
leave the logical attempt untouched. Capped by `Retry.MaxAge`, never by
`Retry.MaxAttempts`.
_Avoid_: delay, reschedule

**Dead-letter (DLQ)**:
Worker-only: a message moved aside after its retry budget or age is
exhausted, kept with its final error. Revived only by an ops requeue.
_Avoid_: parked (that is the reconcile term)

**Dead-letter record**:
The JSON document a dead-lettered message becomes, stored at KV key
`{ns}/converge/worker/{job}/dlq/{message-id}`. Requeuing it is an ops verb
the engine does not perform itself (plan 05).
_Avoid_: DLQ entry (say dead-letter record)

### Kernel concepts

**Run mode**:
Who runs a job across replicas: one replica, split across replicas, or all
replicas.
_Avoid_: posture, leader election (that names one implementation)

**Outcome**:
A control-flow value returned from a handler (`CheckAgain`, `Snooze`,
`Discard`) — a report to the engine, not a failure.
_Avoid_: sentinel error, status code

**Lease**:
The device that keeps duplicate work rare under `OnOneReplica`. An
efficiency device, never a correctness device — correctness comes from
idempotency and versions.
_Avoid_: lock, distributed lock
