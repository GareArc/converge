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

**Envelope**:
The `converge.*` header protocol on a worker message — seeded once at
enqueue, folded on snooze and neutral republish so the logical attempt
survives, reset to fresh (attempt zero, no snoozes) by a requeue. One
module owns every read and write of these headers.
_Avoid_: wire format, raw headers

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

### Control plane concepts

**Control queue**:
The broadcast MQ queue every replica's control listener consumes
(`internal/ctl.Queue`), carrying ops commands cluster-wide.
_Avoid_: command bus, admin topic

**Replica ID**:
The random ID a `Runtime` mints for itself at construction. Ops-verb
responses carry it so a caller sees which replica actually acted, not just
that "a" replica did.
_Avoid_: instance ID, pod name (deployment-specific words)

**Ops verb**:
A control-plane operation dispatched cluster-wide: poke, hint, run-pass,
pause, or resume. Poke and hint carry the reconcile meaning defined above;
pause and resume additionally persist a durable flag.
_Avoid_: admin command, RPC

**Durable pause flag**:
The KV record (`internal/ctl.PausedKey`) a pause/resume verb writes and
deletes. Every replica re-reads it at startup, so a pause survives a
restart instead of living only in one process's memory. Dispatch writes
the flag before publishing the broadcast: the broadcast is lossy, the
flag is what a restarted replica obeys, so pause fails safe toward
paused. A publish failure after the flag write surfaces as the dispatch
error, and re-dispatching the same verb is idempotent.
_Avoid_: soft pause, in-memory flag

**Which-replica-acted response**:
What dispatching an ops verb returns: one response per replica that
received the command (`Replica`, `Acted`, `Err`, `At`), so the caller sees
exactly who did the work instead of a single opaque success/failure.
_Avoid_: broadcast ack, fire-and-forget

**DLQ op**:
A dead-letter list, requeue, or purge call. Unlike ops verbs, DLQ ops are
direct KV/MQ data operations — never routed through the control plane —
because any replica can read and write the same durable dead-letter
records.
_Avoid_: control op (that is the routed, per-replica kind)

**Payload display opt-in**:
`debughttp.OpsOpts.ShowPayloads` — dead-letter payloads may hold user data,
so the JSON `payload` key is present only when a caller explicitly enables
it; headers are shown either way.
_Avoid_: redaction (nothing is masked; the field is simply absent)

**Ops exposure**:
`debughttp.OpsHandler` mutates runtime state and carries no authentication
of its own — the host mounts it only behind its own authentication and
authorization. Where mutation is not needed, mount `ReadOnlyHandler`
instead.
_Avoid_: admin API (converge ships handlers, never an exposed endpoint)

**Response expiry**:
Ops-verb responses are written to KV with a short TTL, so a dispatcher
polling for replies sees a clean, bounded set instead of an ever-growing
history of past commands.
_Avoid_: response log, audit trail
