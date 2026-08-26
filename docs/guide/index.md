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

Chapters 1, 2, 4 and 5 run with nothing installed. Chapters 3 and 6 need a
Redis, because a message has to come from somewhere real and bookkeeping
has to outlive a process; both show the one `docker run` line that gets you
one.

- [1. A first job](01-first-job.md) — one function, on a schedule, with
  nothing installed.
- [2. Many things to check](02-ids.md) — the IDs one job looks after, and
  where that list comes from.
- [3. Reacting to events](03-triggers.md) — the `Triggers` list: turning a
  queue into work alongside the schedule. Needs a Redis.
- [4. The other kind of job](04-worker.md) — sending one message for one
  thing that happened, and what converge does when your handler fails to
  handle it.
- [5. More than one copy](05-run-modes.md) — running three copies of your
  service, and which one does the work.
- [6. Going to production](06-production.md) — the whole composition root
  of a deployable service: config, all three ports on Redis, metrics,
  middleware, a debug endpoint, and a clean shutdown. Needs a Redis.

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
see the [README](https://github.com/GareArc/converge/blob/main/README.md#non-goals) for the
short version, or [the reference](../reference/kernel.md#v1-limits) for the
precise one.

## The two models

Every background job is one of exactly two things:

| | **`reconcile`** | **`worker`** |
|---|---|---|
| Style | level-triggered — you are given *what to look at* | edge-triggered — you are given *what to do* |
| You write | `Reconcile(ctx, id) error` | `Handle(ctx, payload) error` |
| A message is | a *[hint](../glossary.md#hint)* — "re-check this ID" | the *work itself* |
| Lost message means | the state is put right a little later: the next scheduled round looks at that ID anyway | the work is gone, and no later round notices — so converge keeps hold of it. It records a message as handled only once your handler has dealt with it, hands it back again if that fails, and sets it aside as a [dead-letter](../glossary.md#dead-letter-dlq) rather than dropping it. That reaches only as far as the queue underneath stores messages durably |
| Must be | safe to run twice, and written to look at how things actually are rather than trust what it was told | safe to run twice: your handler can be called more than once for the same message |
| Examples | keeping credentials in sync, keeping a deployment in step with what it should be, warming a cache | send an email, run an export, call a webhook once |

**The decision test:** *given only an ID, can the handler recompute everything
by re-reading storage?* Yes → `reconcile`. No (the message is the only copy of
the data) → `worker`.

**The rule that prevents the classic [reconcile](../glossary.md#reconcile)
incident:** your reconcile handler runs **every time the schedule comes
around, whether or not anything has changed**. So every side effect it
performs has to depend on what it finds, not on the fact that it ran. "Email
the buyer when a SKU is back in stock" must not send an email every time —
write it to work toward "a notification exists": read the stored
`notified_at` fact, send only if it is absent, and record that you sent it.
That turns "every pass" into "once, plus a rare repeat if the process dies
between sending and recording" — conditional, not exactly-once, which is the
most any handler on either surface gets. A side effect you cannot make
conditional like that is a one-time action; send it through `worker`
instead.

When in doubt, it is a reconcile job. A queue whose messages carry
`{"sku": X, "type": "changed"}` is not carrying work — it is
carrying a name to go and look at, which makes it a reconcile job dressed as
a worker.
