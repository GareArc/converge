# Adapters, bridges, and stdlib-only core packages

Core module (stdlib + cron parser only, CI-enforced): `converge`,
`converge/reconcile`, `converge/worker`, `converge/inmem`,
`converge/convergetest`, `converge/debughttp` (`ReadOnlyHandler`,
`OpsHandler`).

Separate Go modules (each declares its supported core version range;
core ports are frozen at v1 — additions arrive as new capability
interfaces, never signature changes):

| Module | Provides |
|---|---|
| `adapters/redis` (`convredis`) | `NewStreamsMQ` (GroupConsumer, BroadcastConsumer, DelayedPublisher), `NewLease`, `NewKV`, `ListTrigger` |
| `adapters/otel` (`convotel`) | `NewObserver` |
| `bridges/kratos` (`convkratos`) | `Server(rt)` — a `transport.Server` |

Durability note: at-least-once holds **to the durability of your MQ**. Redis
Streams with default persistence can lose acknowledged writes on failover —
for hard durability run AOF `everysec` or better, or choose a broker with
synchronous replication when it matters.

## `adapters/redis` (`convredis`)

### Version floors

Two independent floors, enforced two different ways:

- **`github.com/redis/go-redis/v9` v9.7.0+**, enforced by Go's own minimum
  version selection from the `go.mod` `require` line — it is a floor, not a
  snapshot; do not lower it below a version whose API the adapter uses.
- **Redis server 6.2+**, which cannot live in `go.mod`. It is enforced by
  CI: the integration test tier runs against both `redis:6.2-alpine` (the
  declared floor) and `redis:7-alpine` (current) in the CI matrix. The
  command set the adapter actually issues (Streams consumer-group commands,
  `XCLAIM`, Lua scripting, `SET ... NX PX`, `SCAN`) dates back to Redis 5.0,
  but only the tested floor — 6.2 — is the supported floor.

### MQ — `NewStreamsMQ`

One stream per queue. `Consume` is `ConsumeGroup` against the reserved group
`converge`; `ConsumeGroup` is a real consumer group (`XGROUP CREATE ... 0
MKSTREAM`); `ConsumeBroadcast` is a per-subscriber `XREAD` from `$`.

**Visibility is adapter-managed, not idle-time-based.**
`StreamsOpts.Visibility` defaults to `DefaultVisibility` whenever it is
zero or negative — only a strictly positive duration overrides the
default. On delivery, the adapter scores the entry into a pending ZSET at
`Clock.Now() + visibility`; `Extend` re-scores; `Ack` removes it.
Handle-driven settlement is attempt-guarded: a handle whose entry was
reclaimed or acked cannot disturb the live attempt — its `Extend` reports
the loss, its `Nack` and `Ack` are no-ops. Redelivery claims due members
from that ZSET (via a guarded Lua script so concurrent consumers never
double-claim) and hands them to the stream with `XCLAIM` — never
`XAUTOCLAIM` and never Redis's own idle-time bookkeeping. The same
`visibility` window doubles as the retry grace for delayed-message
release (below).

**A PEL-reconciliation pass walks the entire PEL every consume iteration,
paginated.** The adapter lists the group's pending entries list (`XPENDING`)
a page at a time, advancing the cursor past each page's last entry ID until
the PEL is exhausted, and for any entry ID that is *not* already a member of
the pending ZSET, adds it (`ZADD NX`) at `Clock.Now() + visibility`. This
closes the window where a hard crash could otherwise strand a
delivered-but-untracked entry in Redis's PEL forever, invisible to the
ZSET-driven reclaim loop. A side effect of running this every iteration is
that the rare **Ack race is self-healing**, not merely "impossible": if
reconciliation resurrects an entry that was actually already acked, the
next reclaim attempt tries to `XCLAIM` it, `XCLAIM` returns empty (Redis has
already removed it from the PEL), and the adapter's cleanup step removes the
resurrected ZSET/attempts bookkeeping. No message is redelivered from this
path; the entry is inert from the moment it's resurrected.

**Delayed publish is claim-then-act, not pop-then-publish.** `PublishDelayed`
adds a JSON record — carrying a random per-call nonce, so two calls with
byte-identical payloads never collide as ZSET members — to a delayed ZSET
scored at the due time. Release, on each consume iteration, atomically
re-scores due members forward by one `visibility` window (the "claim"),
`XADD`s each into the stream, then `ZREM`s it on success. A crash or
transient publish error between the `XADD` and the `ZREM` leaves the record
in the ZSET, still re-scored into the future — it will be claimed and
re-published again after the grace window elapses. The contract this gives
you: **duplicates are possible, loss is not** — within Redis durability, per
the note at the top of this page. Each cycle publishes the
claimed record at most once, but while the `ZREM` keeps failing, every
grace window can add another copy — the duplicate count is not globally
bounded. Consumers must be idempotent, as everywhere under at-least-once
delivery.

**NOGROUP recovery replays the backlog, bluntly.** If the stream or the
consumer group vanishes out from under the adapter — `DEL`, `FLUSHDB`,
key eviction, or an operator's `XGROUP DESTROY` — the next read fails with
`NOGROUP`, and the adapter recreates the group at ID `0` (`MKSTREAM`) and
keeps going. If the stream itself still exists (only the group was
destroyed), the freshly created group sees the stream's **entire surviving
backlog** as unread and replays all of it. This is not a bug to route
around: it is the same "a newly created group receives the queue's full
backlog" contract the port guarantees for any late-created group, applied to
the recovery case. An operator who destroys a group on a live queue should
expect a full replay.

**Streams are unbounded in v1.** `Ack` is an attempt-guarded `XACK` plus
bookkeeping cleanup (`ZREM`/`HDEL` on the adapter's own pending/attempts
keys) — it never issues `XDEL`. Backlog retention is the contract a
late-created group depends on, so the stream itself only grows; there is no
trim option yet.
Operators should plan for unbounded Redis Streams growth per queue until a
real consumer motivates a trim/retention knob.

**Undecodable entries are settled, not retried.** An entry a converge
publisher did not write — a foreign producer using the same stream, or a
corrupted record — cannot be decoded into a `converge.Message`, so it can
never reach a handler or the worker surface's dead-letter path. Retrying it
forever would only burn a reclaim per visibility window, so the adapter
acks it and drops its bookkeeping on first contact. Converge streams are
not a place for foreign writers; anything they add is discarded.

#### EnqueuedAt divergence

The Redis adapter stamps a delayed message's `converge.enqueued-at` header
at **release time** — when it is moved from the delayed ZSET into the
stream — not at the original `PublishDelayed` call. `converge/inmem` stamps
it at **call time** instead.
The port does not pin which is correct; the worker surface is unaffected by
the difference, since `Meta.EnqueuedAt` and `Retry.MaxAge` accounting both
read whichever value the adapter in use actually wrote to the header.

### Auxiliary key prefixes

All adapter-owned bookkeeping keys use a fixed `convredis:` prefix — not
configurable in v1, since no consumer has needed it — while user-facing
KV/lease keys pass through verbatim (the kernel already namespaces those via
`Options.Namespace`):

| Key | Purpose |
|---|---|
| `convredis:s:{queue}` | the queue's stream |
| `convredis:p:{queue}:{group}` | pending ZSET (visibility, scored by `Clock`) |
| `convredis:a:{queue}:{group}` | attempts hash, keyed by stream entry ID |
| `convredis:d:{queue}` | delayed-publish ZSET |

### Lease — `NewLease`

`TryAcquire` is `SET name token NX PX ttl`; `Extend` and `Release` are Lua
scripts that compare the stored token before acting. One asymmetry worth
knowing: **`Release` closes `Done` even when the release call itself fails
transiently** (a network error still marks the handle finished), whereas
`Extend` keeps `Done` open on a transient error and closes it only on
confirmed loss (the script ran and the token no longer matched) — a
transient `Extend` failure is retried by the caller, not treated as loss.

### KV — `NewKV`

`Get`/`Set`/`Delete` map directly to `GET`/`SET ... PX`/`DEL`; `SetCAS` is a
single Lua script (a nil `old` means create-only-if-absent; CAS clears any
TTL on success, per the port contract). `Scan` wraps native Redis `SCAN`,
translating the adapter's `""` cursor to Redis's `0`. **`Scan` is
at-least-once under concurrent mutation**: Redis's own `SCAN` can return a
key more than once while the keyspace is rehashing concurrently with the
scan. The portcheck scan contract test is quiescent (no concurrent writer)
and stays exactly-once, so this shows up only under real production
concurrency — which is why the worker package's dead-letter listing
(`worker.DLQFrom(rt, job).List`, surfaced by `debughttp.OpsHandler`) dedups
by KV key: a duplicate-returning `Scan` can never surface a duplicate
dead-letter record to a caller.

### `ListTrigger`

`ListTrigger(rdb, key, id)` is a `BRPOP` loop (1s block) over a single Redis
list, turning each popped element into a wake via the supplied `IDFunc`.
Extraction failures are skipped, not fatal — lossy by design, since the
schedule trigger covers whatever a dropped list entry misses. `Run` returns
`context.Canceled` on context cancellation. See
[Scenario D](../cookbook/scenario-d-foreign-queue.md) for a worked example.

## Operational notes

Backend health, dead-backend behavior, and steady-state Redis round-trip
cost for this adapter are covered on the
[Operations reference](operations.md#operational-visibility) page, not here —
they're operator-facing concerns, not API surface.

## `adapters/otel` (`convotel`)

`NewObserver` wraps an OpenTelemetry `metric.Meter` as a
`converge.Observer`. It registers one histogram and five counters at
construction time; after that, `Observe` only records — there is no
async instrument, no `*converge.Runtime` argument, and no leadership
tracking anywhere in the adapter.

### Version floor

**`go.opentelemetry.io/otel/metric` v1.44.0+**, enforced by Go's own
minimum version selection from the `go.mod` `require` line — it is a
floor, not a snapshot.

### Instruments

| Instrument | Type | Unit | Attributes | Fed by |
|---|---|---|---|---|
| `converge.run.duration` | histogram (float64) | `s` | `converge.job`, `converge.surface`, `converge.status` | `RunCompleted` |
| `converge.parked` | counter (int64) | — | `converge.job` | `IDParked` |
| `converge.dead_letters` | counter (int64) | — | `converge.job`, `converge.queue`, `converge.reason` | `MessageDeadLettered` |
| `converge.discarded` | counter (int64) | — | `converge.job`, `converge.surface`, and `converge.reason` (reconcile only) / `converge.queue` (worker only) | `WakeDiscarded`, `MessageDiscarded` |
| `converge.lease.transitions` | counter (int64) | — | `converge.job`, `converge.acquired` | `LeaseTransition` |
| `converge.anomalies` | counter (int64) | — | `converge.job`, `converge.kind` | `VersionZero`, `WrongSurfaceSignal`, `BackoffFallback`, `PassOverrun` |

Attribute values: `converge.surface` is `Surface.String()`
(`"reconcile"` / `"worker"`); `converge.status` is `"ok"` /
`"error"`; `converge.reason` is the sealed reason type's `String()`;
`converge.kind` is one of `"version-zero"`, `"wrong-surface"`,
`"backoff-fallback"`, `"pass-overrun"`; `converge.acquired` is a
bool.

**Runs and durations are one instrument, not two.** The histogram's
own `_count` series already is the run counter — every
`RunCompleted` records exactly one histogram observation — so there
is no separate counter to keep in sync with it.

### Why there are no gauges

OpenTelemetry has no cross-process aggregation: each replica exports
its own series, and the backend sums them at query time. That is
exactly right for counters and histograms — `sum` across replicas is
the correct total — and wrong for every gauge converge could offer.
Under `converge.OnOneReplica`, only the lease holder does any work,
so a staleness gauge would read "never converged" from every
non-leader replica, and neither `max` nor `avg` recovers the truth
out of that mix. A queue-depth gauge over a shared Redis stream is
observed identically by every replica, so `sum` multiplies it by the
replica count — while under `converge.SplitAcrossReplicas`, where
each replica owns a disjoint slice, `sum` is exactly the right
aggregation for that same metric name. One instrument would need
three different query-side rules depending on run mode and replica
role. So `convotel` exports counters and histograms only, driven
purely by `Observer` events: `NewObserver` takes no
`*converge.Runtime`, registers no asynchronous instruments, and
tracks no leadership.

The replacement for a staleness gauge is a query over the run
counter:

```promql
sum by (converge_job) (increase(converge_run_duration_seconds_count{converge_status="ok"}[10m])) == 0
```

Summing before comparing makes the query leadership-agnostic — it
does not care which replica did the work, or that the leader changed
mid-window — which is strictly more correct than any gauge could be.
One caveat: `increase(...) == 0` only matches while the series
still exists, so if every replica exporting a job disappears, the
vector goes empty and the alert silently stops firing — pair it
with `absent_over_time` if that matters to you.

### Cardinality

No ID and no message ID is ever an attribute, on any instrument.
Worker discard reasons specifically are excluded too:
`converge.MessageDiscarded.Reason` is a free-form `string`, copied
straight from the value a handler returns in `worker.Discard{Reason:
…}`. A handler that returns `Discard{Reason: fmt.Sprintf("no such SKU
%s", id)}` would create one time series per SKU if that string
became a label. Reconcile discard reasons are attributes, by
contrast, because `WakeDiscardReason` is a sealed type with five
fixed values — its cardinality is bounded by the type, not by
caller input. The two surfaces share one `converge.discarded`
instrument, distinguished by `converge.surface`. Per-ID detail
belongs to `debughttp`, which can show the IDs themselves.

### `QueueDepth` is deliberately unmapped

`converge.QueueDepth` events reach `Observe` but produce no metric.
Backlog depth belongs to the broker's own exporter — `XLEN` via a
Redis exporter, for the Streams adapter — not to converge, for the
same reason a queue-depth gauge is excluded above: converge cannot
know, from inside the kernel, which aggregation the deployment's run
mode makes correct.

## `bridges/kratos` (`convkratos`)

`Server(rt)` wraps a `*converge.Runtime` as a kratos
`transport.Server`. `Start` runs the runtime; `Stop` cancels it and
waits for the drain to finish before returning.

### Version floor

**`github.com/go-kratos/kratos/v2` v2.9.2+**, enforced by Go's own
minimum version selection from the `go.mod` `require` line.

### The drain-deadline invariant

Kratos's `StopTimeout` defaults to zero, which means `Stop` receives
a context with no deadline. That default is correct here: converge's
own `DrainTimeout` (30s by default, `converge.Options.DrainTimeout`)
already governs how long the drain is allowed to take, and an
unbounded `Stop` context simply lets it run out that clock.

The hazard is setting `kratos.StopTimeout(t)` with a `t` shorter than
`DrainTimeout` — that truncates the drain by cutting `Stop` off
before `rt.Run` has actually returned. The bridge cannot detect this
itself: `converge.Runtime` exposes no `DrainTimeout` accessor, and
`stopTimeout` is an app-level kratos option, invisible to a
`transport.Server`. So instead of failing silently, `Stop` makes the
symptom explicit — it returns `ErrDrainIncomplete` when its context
expires before the runtime finishes.

If you set `kratos.StopTimeout(t)`, set `t` greater than
`converge.Options.DrainTimeout`.

### `transport.Endpointer` is deliberately not implemented

converge has no network endpoint to advertise; registering one would
put a meaningless entry into service discovery. `Server(rt)` is a
`transport.Server` only.

### Context ownership

Kratos never cancels the context it passes to `Start` — it shuts
servers down through a separate `Stop` context instead. `convkratos`
derives and owns its own cancellable context from the one it is
given, so `Stop` has something to cancel regardless of what the
caller's `Start` context does.

### `Stop` before `Start`

Kratos can genuinely call `Stop` before `Start` runs — its start
goroutine signals its waitgroup *before* entering `Start`, so an app
whose context is already cancelled at boot can shut a server down
that never ran. The bridge treats that as success, not error: a
`Stop` that arrives first returns `nil` immediately, and the `Start`
that follows also returns `nil` without ever calling `rt.Run`. This
is deliberate and correct, but it means an operator whose app
context is already cancelled at boot sees a "healthy" server that
never ran a single job.

### One `Start` per server

A second `Start` on a server that is already running returns
`ErrAlreadyStarted` and does nothing else. This is a real error
rather than a tolerated no-op, because the alternative is worse than
a loud failure: without the guard the second call would overwrite
the cancel belonging to the first runtime, so a later `Stop` would
cancel a context nobody is running under, report a clean drain that
never happened, and leave the first runtime live. Kratos itself
starts each server once — this guards against embedders that do not.

Note the asymmetry with the previous section: a `Stop` arriving
before `Start` is a race kratos can legitimately produce, so it
succeeds; a second `Start` is a caller mistake, so it is reported.
