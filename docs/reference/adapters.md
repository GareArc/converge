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
| `bridges/k8s` | informer → `reconcile.Trigger` |

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
Handle-driven re-scores are attempt-guarded: a handle whose entry was
reclaimed or acked cannot postpone the live attempt — its `Extend` reports
the loss, its `Nack` is a no-op. Redelivery claims due members from that
ZSET (via a guarded Lua script so concurrent consumers never double-claim)
and hands them to the stream with `XCLAIM` — never `XAUTOCLAIM` and never
Redis's own idle-time bookkeeping. The same `visibility` window doubles as
the retry grace for delayed-message release (below).

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

**Streams are unbounded in v1.** `Ack` is `XACK` plus bookkeeping cleanup
(`ZREM`/`HDEL` on the adapter's own pending/attempts keys) — it never
issues `XDEL`. Backlog retention is the contract a late-created group
depends on, so the stream itself only grows; there is no trim option yet.
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
[Operations](../guide/operations.md#operational-visibility) page, not here —
they're operator-facing concerns, not API surface.
