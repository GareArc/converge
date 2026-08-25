# Converge guide

> Module: `github.com/GareArc/converge`.

Converge gives your services **one model for all background work**: periodic
loops, event-driven sync, queue consumers, cache invalidation, k8s-style
control loops. It replaces the cron+lock, list-queue, and hand-rolled
reconciler patterns with two models on one kernel — and the package layout
*is* the model split:

```go
import (
    "github.com/GareArc/converge"           // the kernel: runtime, ports, run modes
    "github.com/GareArc/converge/reconcile" // the level-triggered model
    "github.com/GareArc/converge/worker"    // the edge-triggered model
)
```

A file's imports declare its model: code importing `worker` but not
`reconcile` is provably pure queue-processing, and vice versa.

## Where to go next

- [A first job](01-first-job.md) — one function, on a schedule, with nothing
  installed.
- [2. IDs](02-ids.md) — IDs, and where the list of things to check comes
  from.
- [3. Triggers](03-triggers.md) — asking converge to check something now, or
  asking to be checked again later.
- [4. The other kind of job](04-worker.md) — sending one message for one
  thing that happened, and what converge does when your handler fails to
  handle it.
- [5. Run modes](05-run-modes.md) — running three copies of your service,
  and which one does the work.
- [6. Going to production](06-production.md) — the four things you swap to
  move off in-memory storage, and what changes (and what doesn't) when you
  do.
- [7. Stale writes](07-versions.md) — protecting a write when "safe to run
  twice" isn't enough on its own.
- [8. Testing your jobs](08-testing.md) — running the real engine in a Go
  test, against in-memory storage and a clock you control.
- [9. Running it in production](09-operations.md) — looking at a running
  job from outside the process, pausing it, and working the
  [dead-letter](../glossary.md#dead-letter-dlq) queue.
- [10. Seeing what it is doing](10-observability.md) — turning what
  converge reports into metrics, and setting the one alert that catches a
  job that quietly stopped running.
- [Cookbook](../cookbook/scenario-a-safety-net.md) — worked scenarios A–F.
- [Reference](../reference/kernel.md) — the condensed API.

Terminology throughout these docs follows the project's canonical glossary:
[`CONTEXT.md`](../../CONTEXT.md).

## The two models

Every background job is one of exactly two things:

| | **`reconcile`** | **`worker`** |
|---|---|---|
| Style | level-triggered | edge-triggered |
| You write | `Reconcile(ctx, id) error` | `Handle(ctx, payload) error` |
| A message is | a *hint* — "re-check this ID" | the *work itself* |
| Lost message means | slightly later convergence (the scheduled pass catches it) | lost work — so delivery is at-least-once (ack, retry, DLQ), to the durability of your MQ |
| Must be | idempotent, re-reads truth | idempotent (at-least-once delivery) |
| Examples | credential sync, deploy convergence, cache warmers | send an email, run an export, call a webhook once |

**The decision test:** *given only an ID, can the handler recompute everything
by re-reading storage?* Yes → `reconcile`. No (the message is the only copy of
the data) → `worker`.

**The rule that prevents the classic reconcile incident:** your reconcile
handler runs **every scheduled pass, even when nothing has changed**. Every
side effect must therefore be conditioned on observed state. "Notify the
workspace when it exceeds quota" must not send a notification per pass — it
converges toward "a notification exists": check the stored `notified_at`
fact, send only if absent, record that it was sent. If a side effect can't be
made convergent like this, it's a verb — use `worker`.

When in doubt, it's a reconciler. Most "queues" that carry `{"workspace_id": X,
"type": "changed"}` are reconcilers wearing worker costumes.

## What converge deliberately does not do

- **No exactly-once.** At-least-once + idempotent handlers.
- **No correctness-by-lock.** Leases reduce duplicate work; version tracking
  provides correctness.
- **No workflow orchestration** — multi-step sagas are Temporal's territory.
- **No CRD controllers** — keep controller-runtime.
- **No batching** (v1): `Reconcile` is per-ID. If your downstream demands
  bulk calls, aggregate inside the handler's own storage layer, or ask for
  `ReconcileBatch` when a real job needs it.
- **No per-key ordering on the worker surface** (v1): ordered verbs need
  `OnOneReplica` + `Concurrency: 1`, or a future partition-key capability.
- **No absolute-time or cancellable delayed jobs** (v1): `Delay` is relative;
  "cancel the reminder" is a reconciler over your own table.
- **No per-tenant keyed rate limits** (v1): `RateLimit` is per-job and
  process-local; keyed fairness is planned against a real consumer.
- **No priorities, no unique-job dedup on the worker surface** (dedup is what
  the reconcile surface *is*), **no hot config reload**.
- **No sharded reconcilers** (v1): `SplitAcrossReplicas` on a reconcile spec
  is a clear registration error; the shard-lease design exists in the design
  doc and ships when the first job needs it.
