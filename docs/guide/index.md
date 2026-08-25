# Converge guide

> Module: `github.com/GareArc/converge`.

Converge gives your services **one model for all background work**: periodic
loops, event-driven sync, queue consumers, cache invalidation, k8s-style
control loops. It replaces the cron+lock, list-queue, and hand-rolled
reconciler patterns with two models on one kernel — and the package layout
*is* the model split:

```go
import (
    "github.com/GareArc/converge"           // runtime, Options, run modes
    "github.com/GareArc/converge/reconcile" // "is everything as it should be?"
    "github.com/GareArc/converge/worker"    // "do this one specific thing that just happened?"
)
```

A file's imports declare its model: code importing `worker` but not
`reconcile` is provably pure queue-processing, and vice versa.

## The core path

Chapters 1–6 are the core path: a job on a schedule, a list of IDs it looks
after, ways to check something sooner than the schedule would, the other
kind of job for one-time work, how many of your copies actually run any of
it, and somewhere durable to keep all of that bookkeeping. Finish them and
you can ship.

- [1. A first job](01-first-job.md) — one function, on a schedule, with
  nothing installed.
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

## Chapters you reach for

Chapters 7–10 are opt-in: reach for one when your job's situation calls for
it, not because chapter 6 left it unfinished.

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

Outside the numbered path: the
[cookbook](../cookbook/scenario-a-safety-net.md) has worked scenarios A–F,
and the [reference](../reference/kernel.md) is the condensed API.

Every term this guide uses has a plain-language definition in
[the glossary](../glossary.md). For what converge deliberately does not do,
see the [README](https://github.com/GareArc/converge/blob/main/README.md#what-it-deliberately-does-not-do) for the
short version, or [the reference](../reference/kernel.md#v1-limits) for the
precise one.

## The two models

Every background job is one of exactly two things:

| | **`reconcile`** | **`worker`** |
|---|---|---|
| Style | level-triggered | edge-triggered |
| You write | `Reconcile(ctx, id) error` | `Handle(ctx, payload) error` |
| A message is | a *[hint](../glossary.md#hint)* — "re-check this ID" | the *work itself* |
| Lost message means | slightly later convergence (the scheduled pass catches it) | lost work — so delivery is at-least-once (ack, retry, [DLQ](../glossary.md#dead-letter-dlq)), to the durability of your MQ |
| Must be | idempotent, re-reads truth | idempotent (at-least-once delivery) |
| Examples | credential sync, deploy convergence, cache warmers | send an email, run an export, call a webhook once |

**The decision test:** *given only an ID, can the handler recompute everything
by re-reading storage?* Yes → `reconcile`. No (the message is the only copy of
the data) → `worker`.

**The rule that prevents the classic [reconcile](../glossary.md#reconcile)
incident:** your reconcile handler runs **every scheduled pass, even when
nothing has changed**. Every side effect must therefore be conditioned on
observed state. "Notify the workspace when it exceeds quota" must not send a
notification per pass — it converges toward "a notification exists": check
the stored `notified_at` fact, send only if absent, record that it was sent.
If a side effect can't be made convergent like this, it's a verb — use
`worker`.

When in doubt, it's a reconciler. Most "queues" that carry `{"workspace_id": X,
"type": "changed"}` are reconcilers wearing worker costumes.
