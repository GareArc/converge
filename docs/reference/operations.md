# Operations reference

What you can see from outside a running converge process, and the two things
a person can do to a job from outside it: put a shelved message back, and end
a job for good.

[Chapter 6 of the guide](../guide/06-production.md) walks this end to end.
This page is the routes, the JSON, the exact staleness of every number, and
the failure modes.

- [debughttp.ReadOnlyHandler](#debughttp-readonlyhandler)
- [Readiness](#readiness)
- [Reading JobStats](#reading-jobstats)
- [How stale a number can be](#how-stale-a-number-can-be)
- [Requeueing a shelved message](#requeueing-a-shelved-message)
- [Destroying a job](#destroying-a-job)

## debughttp.ReadOnlyHandler

```go
func ReadOnlyHandler(rt *converge.Runtime) http.Handler
```

Package `github.com/GareArc/converge/debughttp`. Read-only introspection over
a `Runtime`. It panics if `rt` is nil — that is a wiring mistake, not a
runtime condition.

**The routes are absolute and cannot be moved.** The handler is its own
`*http.ServeMux` and matches on the full request path:

| Method and pattern | What it answers |
| --- | --- |
| `GET /debug/jobs` | every job |
| `GET /debug/jobs/{$}` | the same, for the trailing-slash form |
| `GET /debug/jobs/{job}` | one job, with the surface-specific detail |

Mounting it under a prefix of your own does not work: the pattern it matches
is `/debug/jobs`, whatever path you registered it at. Register it at exactly
that path, and register the trailing-slash form too so the single-job route is
reachable:

```go
jobs := debughttp.ReadOnlyHandler(rt)
mux.Handle("/debug/jobs", jobs)
mux.Handle("/debug/jobs/", jobs)
```

It serves `application/json` on every response, success or error. Nothing in
it mutates anything: there is no route that requeues, destroys, or notifies.

### All jobs

`GET /debug/jobs` answers with `{"jobs": [ ... ]}`, one object per registered
job in registration order. Each object is
[`JobStats`](kernel.md#stats-types) and [`JobInfo`](kernel.md#stats-types)
merged:

| Field | From |
| --- | --- |
| `job`, `surface`, `run_mode`, `queue`, `settings` | `JobInfo`. `queue` is the inbox on a worker job and empty on a reconcile one. |
| `state` | `JobStats.State`, rendered — `not started`, `active`, `destroyed` |
| `lease_held`, `in_flight`, `failing`, `consecutive_fails` | `JobStats`, per replica |
| `backlog`, `backlog_known`, `backlog_at` | `JobStats`. Read `backlog` only when `backlog_known` is true. |
| `shelved`, `shelved_known`, `shelved_at` | the same rule |
| `last_success`, `last_error`, `last_error_at` | `JobStats`; times are RFC3339 with nanoseconds, or `""` when unset, and `last_error` is `""` when there is none |

Every timestamp is rendered in UTC. A zero time renders as the empty string
rather than `0001-01-01T00:00:00Z`.

### One job

`GET /debug/jobs/{job}` answers with that same object plus **one** extra
field, chosen by surface. Never both — neither kind of job has both.

| Surface | Extra field | Contents |
| --- | --- | --- |
| reconcile | `failing_ids` | the IDs currently serving out failure backoff, sorted by ID, each with `id`, `failures`, and — when it has one — `error` and `next_try` |
| worker | `shelved_messages` | the job's shelf, as [`worker.ShelvedMessage`](worker.md#the-shelf) records |

Both are capped at 100 entries. When the list was longer, a sibling boolean
says so — `failing_truncated` or `shelved_truncated`. When the list is empty,
both the list and its truncation flag are omitted from the JSON entirely.

A reconcile job has no shelf, and the worker engine keeps a *count* of the
messages it is retrying but not a list of them. So a worker job's failing
messages are not enumerable here: what you have instead is `last_error`,
`last_error_at`, `consecutive_fails`, and the warn-level `RunCompleted` log
lines — each of which carries the message ID, which is the thread to pull.

**`shelved_messages` is read from `KV` on every request**, by scanning the
job's shelf prefix. That is a real cost on a deep shelf, and it is why the
list route does not include it. It also means the per-job route on a worker
job needs `Options.KV`: without one it answers `500` with
`{"error": "worker: job \"...\": Shelf needs Options.KV"}`.

Errors are `{"error": "..."}` with the status: `404` for an unknown job name,
`500` for anything that failed while gathering the answer.

## Readiness

```go
func (rt *Runtime) Ready() <-chan struct{}
```

Closed once every registered job has started on this replica. It stays open
until `Run` is called, so a probe that reads it before startup correctly
reports not-ready. A probe is a `select` with a `default`.

Readiness means *this replica has started the job*, not *this replica is
running the work*. An `OnOneReplica` job signals it before the lease race
resolves, so the replicas that are only standing by report ready too — which
is what you want: they are live, and one of them takes over if the holder
dies.

`Run` probes `KV` before it starts anything, so a runtime that cannot reach
its `KV` fails at `Run` rather than coming up and reporting ready.

## Reading JobStats

`rt.Stats()` returns one [`JobStats`](kernel.md#stats-types) per registered
job, in registration order. It takes no context and does no I/O.

**Per-replica, computed on read:**

| Field | Meaning |
| --- | --- |
| `InFlight` | IDs or deliveries mid-run on this replica right now |
| `LeaseHeld` | whether this replica holds the job's lease. Always false for a run mode that takes none |
| `Failing` | reconcile: IDs serving out failure backoff. Worker: messages this replica nacked for retry whose delay has not yet elapsed — entries expire and are pruned as the field is read, so it falls on its own |
| `ConsecutiveFails`, `LastError`, `LastErrorAt`, `LastSuccess` | the last outcomes this replica saw |

Both surfaces bound what feeds `Failing` at 65536 entries: the worker's
retrying set, and the reconcile queue of pending IDs — beyond which a
notification for an ID converge has never seen is dropped with
`converge.ErrInboxOverflow`. A failing ID is not a lost one; it keeps being
retried at the ceiling rate. But a `Failing` count that grows and never falls
is worth a look.

Three things about the last-outcome fields will otherwise surprise you:

- A worker `Discard` counts as a **success**: it stamps `LastSuccess` and
  clears `ConsecutiveFails`. A message that never needed doing is not a fault.
- A handler-requested `Shelve` counts as a **failure** for
  `ConsecutiveFails` only. It carries no error, so `LastError` and
  `LastErrorAt` are left holding the last real one. A `ConsecutiveFails` that
  climbs while `LastErrorAt` stays put is deliberate shelving, not a stalled
  clock.
- A reconcile `CheckAgain` or `ErrOutdated` counts as neither: it stamps
  `LastSuccess` and clears `ConsecutiveFails`, because the run did not fail.

**Cluster-wide, polled:**

| Field | Meaning |
| --- | --- |
| `Backlog` / `BacklogKnown` / `BacklogAt` | the real depth of the job's inbox, read from the MQ |
| `Shelved` / `ShelvedKnown` / `ShelvedAt` | how many messages are on the job's shelf **right now** — a depth, not a count of how many runs were ever shelved. It falls when somebody requeues or purges, which is what you want to alert on |

**`*Known` false means unknown, not zero.** converge does not invent the
number, `convotel` does not publish it, and a gap on a dashboard is the
honest answer. The cases where it is unknown:

| Situation | Which |
| --- | --- |
| the job is not active on this replica (standing by for a lease, stopped, or never started) | both |
| an `OnAllReplicas` job, on every backend | `Backlog` — every replica reads every message, so there is no shared depth that would mean anything |
| a reconcile job with no notifications trigger | `Backlog` — a schedule-only job has no inbox to be behind on |
| a reconcile job with several notifications triggers, one of which cannot report | `Backlog` — it is all or nothing; a partial sum would be a lie |
| the MQ has no `BacklogReporter` / `GroupBacklogReporter` capability | `Backlog` |
| Redis Streams on **Redis 6.2 or older** | `Backlog` — the adapter reads `XINFO GROUPS`'s `lag`, added in 7.0, and reports `converge.ErrBacklogUnknown` rather than guessing |
| a reconcile job, always | `Shelved` — there is no shelf on that surface |
| an `OnAllReplicas` worker job | `Shelved` — a broadcast job has no shelf either |

If you expected a backlog number and got a blank on Redis, check the server
version before you check anything else.

`State` is the one field that is not per-replica in the same way:
`NotStarted` and `Active` are what this replica says about itself, while
`Destroyed` is cluster-wide and terminal.

## How stale a number can be

`rt.Stats()` takes no context, which fixes the design: nothing in it may block
on a network call. So the two cluster-wide numbers are **polled in the
background and cached**, and what `Stats` returns is the last reading.

- The poll happens once when the job becomes active on this replica, and then
  every **`Options.LeaseTTL / 3`** — 10 seconds at the default 30-second TTL,
  the same cadence as the lease heartbeat.
- Each poll gets `LeaseTTL / 3` to complete. A poll that fails or times out
  changes nothing: the previous reading and its timestamp stand, and
  `BacklogAt` is what tells you it is going stale.
- A backlog reading is therefore **up to `LeaseTTL/3` old**, plus however long
  the backend took to answer. On a worker job the shelf depth is polled on the
  same cadence, with the same guarantee.
- When the job stops being active — the lease moves, the runtime shuts down,
  the job is destroyed — both `*Known` flags go back to false. The number is
  not held over as a stale truth.

`BacklogAt` and `ShelvedAt` exist so you can tell a fresh zero from an old
one. Use them; a dashboard that plots `Backlog` without checking `BacklogAt`
will draw a flat line through an outage.

Everything else in `JobStats` is computed at the moment you call it.

## Requeueing a shelved message

The one mutation an operator performs, and it is deliberately not an HTTP
route. It goes through [`worker.ShelfFrom`](worker.md#the-shelf):

```go
shelf, err := worker.ShelfFrom(rt, "deliver-webhook")
if err != nil {
    return err
}
if err := shelf.Requeue(ctx, messageID); err != nil {
    return err
}
```

**This does not need a running runtime.** `ShelfFrom` reads the `KV`, `MQ`,
namespace and clock the `Runtime` was constructed with, so a small operator
binary can call `converge.New` with the same `Namespace`, `KV` and `MQ`,
register nothing, never call `Run`, and still list, inspect, requeue and
purge.

The order matters: **fix the cause, then requeue.** A requeue resets the
retry budget and the age clock, so requeueing into a still-broken dependency
just spends the budget again and puts the message back where it was.

Two failures to plan for:

- `worker.ErrNotShelved` — no record under that message ID. Either somebody
  else already requeued it, or the ID is wrong.
- `worker: job %q: requeue %q: republished but record not purged` — the
  republish succeeded and the delete did not. The message is **live and still
  on the shelf**. Purge the record; do not requeue again, or you will have two
  copies in flight.

`Purge` and `PurgeAll` throw the evidence away without republishing. Purging
a record that is not there succeeds — absence is not an error.

## Destroying a job

A job ends because it was declared to end. There is no API that stops one, and
no pause, disable or resume: everything about a job's life is declared where
it is registered, with
[`converge.StopCondition`](kernel.md#stop-conditions-and-state) as
`Spec.Until` or `HandleOpts.Until`. Either form needs `Options.KV`, and
without one the job fails at `Run`.

**`converge.Deadline(t)`** ends the job at a moment you already know. The
engine writes converge's own tombstone key when the deadline passes, so the
decision survives restarts.

**`converge.StopKey(key)`** ends the job when `key` exists in `KV`. That key
*is* the tombstone, and this is the operator's lever:

- The string is used **exactly as given** — not namespaced, not prefixed. It
  is a raw `KV` key, so pick one your `Namespace` would not collide with.
- **Presence is the signal**; the value is never read. Write anything.
- Setting it is a deliberate act by a person with access to the `KV`. converge
  exposes no route, verb or handler that does it for you.
- Destruction is terminal. Deleting the key afterwards does not bring the job
  back on a replica that has already seen it, because destruction latches in
  memory once observed. A fresh process would not see the tombstone and would
  start the job again — which is why "delete the key" is not an undo, and the
  real way to retire a job is to delete its code.

**When each engine notices**, which is how long "up to" is:

| Surface and run mode | Checks at |
| --- | --- |
| reconcile, `OnOneReplica` | before each attempt to take the lease, at each lease heartbeat (`LeaseTTL/3`), and at the start of each sweep |
| reconcile, `OnAllReplicas` | at the start of each sweep only — there is no lease heartbeat to piggyback on, so a broadcast reconcile job with a long cadence can be slow to notice |
| worker, `OnOneReplica` | before each attempt to take the lease, at each lease heartbeat, at intake start, and every 30 seconds while consuming |
| worker, `Competing` / `OnAllReplicas` | at intake start, before each consumer restart, and every 30 seconds while consuming |

When it fires, the job stops taking new work, cancels the work in flight,
gives up its lease, emits `converge.JobDestroyed` once on that replica, and
reports `State` `Destroyed` in `Stats()` from then on. `Runtime.Run` still
returns nil: a destroyed job is a job that finished, not a job that failed.
`Run` returns once every job has ended, so a runtime whose only job was
destroyed comes back before your context is cancelled.
