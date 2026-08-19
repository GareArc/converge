# Operations

## Runtime lifecycle

```
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
`http.Handle("/debug/jobs/", ...)` mount (as used in the
[ten-minute tour](tour.md)) serves the job list correctly instead of 404ing
on the bare trailing slash.

## Ops verbs

`OpsHandler` exposes: poke an ID, run a full pass now (backfills, incident
recovery), pause/resume, and DLQ list / requeue / purge. Control operations
are **routed through KV/MQ so they act cluster-wide** — pausing a job pauses
it everywhere, poking reaches the leaseholder, and the response reports
which replica acted. They are not per-replica no-ops behind a load balancer.

**Not every ops verb waits for every replica.** Poke, hint, and run-pass are
*early-return* ops: because only one replica can meaningfully act (the
leaseholder), dispatch returns as soon as **any** replica reports it acted,
rather than waiting out the full response-collection timeout — the response
you get back is the first acting replica's, not a roster of every replica.
Pause and resume are not early-return: because a durable pause must be
authoritative cluster-wide, their dispatch waits (up to the timeout) and
returns one response per replica that answered.

### Pause and resume

Pausing a job writes a durable flag to KV before broadcasting, so a pause
survives a restart even if the broadcast itself is lost — every replica
re-reads the flag at startup. Pausing suppresses **wake application**, not
the schedule itself: a paused reconciler's trigger still runs its passes and
still enumerates the full ID space on schedule, at the same cost as when
unpaused (DB queries, cursor pages) — only the resulting wakes are dropped.
Budget for that cost when a job you plan to pause has an expensive ID
source. See [Reconcile → Triggers and the schedule](reconcile.md) for the
schedule-side detail.

## Operational visibility

**converge does not watch its own backend.** The `MQ`/`Lease`/`KV` ports have
no health channel: if Redis (or whatever backend you've wired) becomes
permanently unreachable, `Consume` and the trigger loops retry silently and
indefinitely — nothing in converge surfaces "the backend is down" as an
event or a metric. Pair converge with your own backend monitoring
(connection health, replication lag, disk); converge will not tell you
Redis is down.

**Steady-state cost of an idle Redis-backed consumer.** With the shipped
[Redis adapter](../reference/adapters.md), an idle consumer performs roughly
20 Redis round trips per second at its 100ms poll cadence (an `XPENDING`
call, a claim script, and an `XREADGROUP` call, every 100ms). Under load,
each batch of up to 16 delivered messages adds one further `XPENDING` call
and one further claim-script call; delayed (snoozed/scheduled-retry)
messages drain at up to 16 per poll iteration, roughly 160 messages/second
per consumer. Size Redis connection and CPU budgets accordingly for
consumer-heavy deployments.
