# The safety net

> Assumes [chapter 2, one job, many things](../guide/02-ids.md). The programs
> are [`a11-purge-pii`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a11-purge-pii/main.go) and
> [`a05-plan-tier-backfill`](https://github.com/GareArc/converge/blob/main/examples/scenarios/a05-plan-tier-backfill/main.go).

Most jobs exist to make something happen. This page is about the other kind:
the job that exists so that nothing is ever *permanently* wrong. Deleted
accounts whose personal data has to actually be gone thirty days later. A
plan tier that should have been recomputed when somebody bought seats and
was not. Nobody sends these jobs anything, nothing is waiting on them, and
on a good day they do nothing at all.

A [reconcile](../glossary.md#reconcile) job is already this shape. What is
worth deciding deliberately is which of two forms it takes, because the two
differ in exactly the place you will care about later: what a failure costs,
and what you can see when the net catches something.

## Two forms

**One run over everything.** `purge-deleted-accounts` is a single query with
a `WHERE` clause, run once a night:

```go
err = reconcile.Periodic(rt, "purge-deleted-accounts",
    reconcile.Cron("0 3 * * *", reconcile.CronOpts{}),
    privacy.purgeExpired,
    reconcile.PeriodicOpts{Timeout: 2 * time.Hour})
```

There is nothing to enumerate, so there is nothing to name: the job runs
under the empty ID, which is what the `id=""` in its log line is.

**One run per record.** `account-plan-tier` walks accounts a page at a time
and is called once for each:

```go
err = reconcile.Register(rt, reconcile.Spec{
    Name:      "account-plan-tier",
    Reconcile: accounts.computePlanTier,
    Triggers:  []reconcile.Trigger{reconcile.Schedule(reconcile.IDsByPage(accounts.page), reconcile.Every(time.Hour))},
    Versions:  accounts,
})
```

Both are safety nets. They differ in what a failure costs and in what you can
see afterwards:

| | One run over everything | One run per record |
| --- | --- | --- |
| Calls per period | one | one per record |
| A failure | the whole pass failed | that record failed |
| Backoff | one curve for the job | one curve per record |
| `/debug/jobs/{job}` shows | one [failing ID](../glossary.md#failing-id), named `""` | every failing ID, each with its last error |
| Running several at once | nothing to parallelise | `Spec.Concurrency` |
| Reach for it when | the database can do the whole thing in one statement | one record can be wrong on its own |

The second row is the one that decides it. A bulk purge that dies on one bad
row tells you only that the purge failed; converge has no way to say which
row, because as far as it is concerned there was one unit of work and it
returned an error. If you want the name of the thing that is stuck, the thing
has to be an [ID](../glossary.md#id).

## A failed pass does not wait for tomorrow

This surprises people about the nightly shape, so it is worth being explicit.
The [cadence](../glossary.md#cadence) is when the job goes looking for work.
It is not when a failure is retried.

Return an error at 03:00 and the empty ID becomes a failing ID: it is tried
again about a second later, then two, then four, doubling to a ceiling of
fifteen minutes, and it goes on being tried at that floor rate for as long as
it keeps failing. A nightly job whose database is down at 03:00 does not skip
the night. It converges within fifteen minutes of the database coming back,
and the [sweep](../glossary.md#sweep) at 03:00 tomorrow is not what saved
it.

The same thing read from the other end: a safety net that is failing costs
you between four and eight calls an hour, forever, per failing ID — every
delay is jittered into the half-interval below the ceiling, so the fifteen
minutes is a maximum and not a period. That is the price of never benching
anything, and it is why the ceiling is fifteen minutes rather than an hour.

## Picking the period

One question sets the [cadence](../glossary.md#cadence): **how long may the
world stay wrong?** Not how often the data changes — the net is not how you
find out about changes, and if you are shortening the period because
[notifications](../glossary.md#notification) sometimes go missing, you have
it backwards. Notifications are allowed to go missing precisely because this
job exists.

converge will tell you when the period is too short for the work. Sweeps
never overlap: a scheduled time that falls due while a sweep is still running
is not queued behind it. When the sweep finishes, converge reports one
`ScheduleOverrun` per firing that was missed — a warn-level line with the
time it was due and how late that is — moves its last-swept mark past them,
and waits for the next one. One overrun after a slow night is noise. A steady
stream of them means the period is shorter than a pass takes, and the fix is a
longer period or a faster query, not more concurrency.

## The backfill that never has to be run once

`a05-plan-tier-backfill` is the case that makes the whole idea worth it. It
looks like a migration — recompute every account's tier — but it is written
as a permanent job with a `Versions` source, and that one field is the
difference between "we ran the backfill" and "the tier is never wrong".

converge reads your counter before the run and again after it. If it moved,
the ID is queued again, because whatever the function decided was decided
from state that has since changed. In the program, buying seats bumps the
account's generation mid-run:

```text
run completed job=account-plan-tier id=a-1001 attempt=1 outcome=succeeded
run completed job=account-plan-tier id=a-1002 attempt=1 outcome=succeeded
run completed job=account-plan-tier id=a-1003 attempt=1 outcome=succeeded
run completed job=account-plan-tier id=a-1001 attempt=1 outcome=succeeded
a-1001 seats=12 tier=pro runs=2
a-1002 seats=20 tier=pro runs=1
a-1003 seats=2 tier=starter runs=1
```

Two things in that output are worth reading twice. `a-1001` ran twice, and
**both runs report `succeeded`** — the function did succeed; the re-run is
not a retry and not a failure, and nothing in the log distinguishes it from
an ordinary second visit. The second is that `attempt` stayed at 1, because
this ID had never failed — and a version re-run *clears* the failure count
rather than adding to it, so one that follows three failures reports
`attempt=4` and leaves the ID back at zero.

The re-run is throttled the same way a `CheckAgain` is: ten in a row at the
250ms floor, then a delay from converge's own 1s-to-15m curve, stepping one
place further along it each time it substitutes. An ID whose version moves on
every single run slows down instead of spinning. The exact rule is in the
[reconcile reference](../reference/reconcile.md#versions).

One edge to know before you rely on this: **an error from your
`VersionSource` disables the check for that run, silently.** It is treated as
"no version known", the run settles normally, and nothing is reported. A
version source that is down does not fail your job — it stops protecting it,
which is a different and quieter thing.

## What a safety net does not need

It does not need a notifications trigger. Adding one is cheap and often
right — it is how the job reacts in seconds instead of in a period — but it
must never be load-bearing. Delete every trigger except the schedule and a
safety net is slower and still correct. If deleting them would make it
*wrong*, what you have is not a safety net, and [the worker
surface](../guide/04-worker.md) is the honest place for it.

It also does not need a [run mode](../glossary.md#run-mode). The default,
`OnOneReplica`, is what you want: one replica walks the whole set, and
walking it four times in parallel would only do the same work four times.
