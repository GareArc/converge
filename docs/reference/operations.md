# Operations reference

The condensed reference for running converge in production — see
[9. Running it in production](../guide/09-operations.md) for a worked
walkthrough.

## Runtime lifecycle

```text
converge.New(Options)              declare the world: ports, default MQ. Immutable after.
reconcile.Register / worker.Handle declare the jobs — from anywhere, until Run
rt.Run(ctx)                        go. Blocks. Cancel → stop intake → drain → release leases
```

- Registration is **distributed**: each module registers its own jobs in its
  own constructor, against the shared injected `*converge.Runtime`.
  Registration is goroutine-safe, validates immediately, and returns an
  error: a bad spec fails *that constructor*, which fails boot, loudly.
- `Run` freezes the registry; registering afterward is an error. `rt.Ready()`
  closes when all consumers and triggers are live (health checks, kratos
  readiness).
- Shutdown: intake stops, in-flight runs get `Options.DrainTimeout` (default
  30s — raise it if your handlers legitimately run longer), leases release
  under `context.WithoutCancel`. **`Run` returns `nil` on clean shutdown**;
  a non-nil return is always a real failure.
- `Options.Namespace` prefixes every lease name, KV key, and engine-owned
  queue — two services sharing one Redis never collide, even with same-named
  jobs.

## Keyspace

For operators inspecting a backend directly, every key converge writes is
built by `internal/keys`, namespaced by `Options.Namespace` (`{ns}`, cleanly
omitted when empty for every key below except `Tracker` — see its note):

| Key | Purpose |
|---|---|
| `{ns}/converge/ctl` | ops-verb request queue |
| `{ns}/converge/ctl/res/{opID}/{replica}` | one replica's response to ops verb `{opID}` |
| `{ns}/converge/ctl/paused/{job}` | durable pause flag for `{job}` |
| `{ns}/converge/worker/{job}/lease` | worker job's held lease |
| `{ns}/converge/worker/{job}/dlq/{messageID}` | dead-lettered message record |
| `{ns}/converge/reconcile/{job}/lease` | reconcile job's held lease |
| `{ns}/converge/reconcile/{job}/parked/{id}` | parked ID's record |
| `converge/tracker/{ns}/{id}` | last-seen version for parked-ID revival — `{ns}` sits after the fixed `tracker` segment, not as a leading prefix like the keys above, and is not cleanly omitted when empty: `Tracker` concatenates the namespace raw, so an empty namespace yields a literal double slash, `converge/tracker//{id}` |

`adapters/redis` additionally namespaces its own bookkeeping (streams,
pending ZSETs, attempts) under a fixed `convredis:` prefix — see
[Adapters → Auxiliary key prefixes](adapters.md#auxiliary-key-prefixes).

## Introspection and ops handlers

`debughttp` splits introspection from mutation into two separate,
separately-mountable handlers:

- `debughttp.ReadOnlyHandler(rt)` — jobs, schedules, last runs, effective
  settings. Safe to expose without authentication of its own.
- `debughttp.OpsHandler(rt, debughttp.OpsOpts{})` — everything
  `ReadOnlyHandler` shows, plus mutating verbs. It carries **no
  authentication of its own** — mount it only behind your own admin auth.
  DLQ payload display is opt-in (`OpsOpts.ShowPayloads`): payloads may
  contain user data, so the JSON `payload` key is present only when a caller
  explicitly enables it.

```go
adminMux.Handle("/debug/jobs/", debughttp.OpsHandler(rt, debughttp.OpsOpts{}))
publicMux.Handle("/debug/jobs/", debughttp.ReadOnlyHandler(rt))
```

Both handlers register their listing route at `/debug/jobs` **and**
`/debug/jobs/{$}` — the trailing-slash exact-match alias — so a
`http.Handle("/debug/jobs/", ...)` mount serves the job list correctly
instead of 404ing on the bare trailing slash.

## Ops verbs

`OpsHandler` exposes: poke an ID, run a full pass now (backfills, incident
recovery), pause/resume, and DLQ list / requeue / purge. Control operations
are **routed through KV/MQ so they act cluster-wide** — pausing a job pauses
it everywhere, and the response reports which replica(s) acted. They are not
per-replica no-ops behind a load balancer.

**Poke and hint don't need the leaseholder; run-pass does.** `Poke` and
`Hint` only require the job's wake queue to be bound, which is true for
every running replica — leaseholder or standby. Under `OnOneReplica`, that
means **every** replica reports `Acted: true` for a poke or hint: the wake
is enqueued locally on whichever replica received the command, but it's
still only the leaseholder's dispatch loop that actually runs the work.
Run-pass is different — `RunPassNow` is gated on the replica currently
holding the lease, so only the leaseholder reports `Acted: true`; standbys
report an error. All three (poke, hint, run-pass) are *early-return*
dispatches: rather than waiting out the full response-collection timeout,
dispatch returns as soon as any acting response has been observed, along
with whatever other responses were already collected in that same polling
round — not necessarily one from every replica. Pause and resume are not
early-return: because a durable pause must be authoritative cluster-wide,
their dispatch waits (up to the timeout) and returns one response per
replica that answered.

Every mutating verb is one route on `OpsHandler`:

| Verb | Route | ID required | Dispatch |
|---|---|---|---|
| Poke | `POST /debug/jobs/{job}/poke` | yes (`id` form value) | early-return |
| Run a full pass now | `POST /debug/jobs/{job}/run-pass` | no | early-return, leaseholder only |
| Pause | `POST /debug/jobs/{job}/pause` | no | waits out the full timeout |
| Resume | `POST /debug/jobs/{job}/resume` | no | waits out the full timeout |
| List dead letters | `GET /debug/jobs/{job}/dlq` | no | direct KV read, not cluster-dispatched |
| Requeue a dead letter | `POST /debug/jobs/{job}/dlq/{id}/requeue` | yes (path segment) | direct KV/MQ write, not cluster-dispatched |
| Purge one dead letter | `DELETE /debug/jobs/{job}/dlq/{id}` | yes (path segment) | direct KV write, not cluster-dispatched |
| Purge all dead letters for a job | `DELETE /debug/jobs/{job}/dlq` | no | direct KV write, not cluster-dispatched |

The four DLQ ops act on whichever replica received the request — a
dead-letter record lives in KV, which every replica can read and write
directly, so there's nothing to broadcast.

### Pause and resume

Pausing a job writes a durable flag to KV before broadcasting, so a pause
survives a restart even if the broadcast itself is lost — every replica
re-reads the flag at startup. Pausing suppresses **wake application**, not
the schedule itself: a paused reconciler's trigger still runs its passes and
still enumerates the full ID space on schedule, at the same cost as when
unpaused (DB queries, cursor pages) — only the resulting wakes are dropped.
Budget for that cost when a job you plan to pause has an expensive ID
source.

## Operational visibility

**converge does not watch its own backend.** The `MQ`/`Lease`/`KV` ports have
no health channel: if Redis (or whatever backend you've wired) becomes
permanently unreachable, `Consume` and the trigger loops retry silently and
indefinitely — nothing in converge surfaces "the backend is down" as an
event or a metric. Pair converge with your own backend monitoring
(connection health, replication lag, disk); converge will not tell you
Redis is down.

**Steady-state cost of an idle Redis-backed consumer.** With the shipped
[Redis adapter](adapters.md), every consume iteration issues
**four** Redis round trips, whether or not there's any work: the
delayed-release claim script (against the delayed ZSET), the `XPENDING`
call that drives PEL reconciliation, the redelivery claim script (against
the pending ZSET), and the `XREADGROUP` call that reads new entries — all
four run every pass through the loop, not just when there's a due delayed
message or a due redelivery. The reconciliation pass is paginated at
`pendingPageCount` entries per page: the "one `XPENDING` call" above is the
idle-PEL case, and a deeper PEL costs one further `XPENDING`/`ZADD NX`
round-trip pair per additional page of PEL depth, every iteration, until
the whole PEL has been walked. At the 100ms poll cadence (roughly 10
iterations/second), that's **~40 Redis round trips per second per idle
consumer**. Under load, each batch of up to 16 delivered messages adds one
further `XPENDING`-driven reconciliation cost and one further redelivery
claim-script cost per batch; delayed (snoozed/scheduled-retry) messages
drain at up to 16 per poll iteration, roughly 160 messages/second per
consumer. Size Redis connection and CPU budgets accordingly for
consumer-heavy deployments.
